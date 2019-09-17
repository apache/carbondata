/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.CarbonExpressions.{CarbonScalaUDF, CarbonSubqueryAlias, MatchCastExpression}
import org.apache.spark.sql.catalyst.analysis.{UnresolvedAlias, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Cast, Divide, Expression, Literal, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Count, _}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil
import org.apache.spark.sql.execution.datasources.{FindDataSourceTable, LogicalRelation}
import org.apache.spark.sql.parser.CarbonSpark2SqlParser
import org.apache.spark.sql.types._
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonCommonConstantsInternal}
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.{AggregationDataMapSchema, CarbonTable, DataMapSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.preagg.{AggregateQueryPlan, AggregateTableSelector, QueryColumn}
import org.apache.carbondata.core.profiler.ExplainCollector
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonUtil, ThreadLocalSessionInfo}

/**
 * model class to store aggregate expression logical plan
 * and its column schema mapping
 * @param expression aggregate expression
 * @param columnSchema list of column schema from table
 */
case class AggExpToColumnMappingModel(
    expression: Expression,
    var columnSchema: Option[Object] = None) {
  override def equals(o: Any) : Boolean = o match {
    case that: AggExpToColumnMappingModel =>
      that.expression==this.expression
    case _ => false
  }
  // TODO need to update the hash code generation code
  override def hashCode : Int = 1
}
/**
 * Class for applying Pre Aggregate rules
 * Responsibility.
 * 1. Check plan is valid plan for updating the parent table plan with child table
 * 2. Updated the plan based on child schema
 *
 * Rules for Updating the plan
 * 1. Grouping expression rules
 *    1.1 Change the parent attribute reference for of group expression
 * to child attribute reference
 *
 * 2. Aggregate expression rules
 *    2.1 Change the parent attribute reference for of group expression to
 * child attribute reference
 *    2.2 Change the count AggregateExpression to Sum as count
 * is already calculated so in case of aggregate table
 * we need to apply sum to get the count
 *    2.2 In case of average aggregate function select 2 columns from aggregate table with
 * aggregation
 * sum and count. Then add divide(sum(column with sum), sum(column with count)).
 * Note: During aggregate table creation for average table will be created with two columns
 * one for sum(column) and count(column) to support rollup
 *
 * 3. Filter Expression rules.
 *    3.1 Updated filter expression attributes with child table attributes
 * 4. Update the Parent Logical relation with child Logical relation
 * 5. Order By Query rules.
 *    5.1 Update project list based on updated aggregate expression
 *    5.2 Update sort order attributes based on pre aggregate table
 * 6. timeseries function
 *    6.1 validate maintable has timeseries datamap
 *    6.2 timeseries function is valid function or not
 * 7. Streaming
 * Examples1:
 * Query:
 *   SELECT name, sum(Salary) as totalSalary
 *   FROM maintable.
 * UpdatedQuery:
 *   SELECT name, sum(totalSalary) FROM(
 *          SELECT name, sum(Salary) as totalSalary
 *          FROM maintable
 *          GROUP BY name
 *          UNION ALL
 *          SELECT maintable_name,sum(maintable_salary) as totalSalary
 *          FROM maintable_agg
 *          GROUP BY maintable_name)
 *   GROUP BY name)
 * Example2:
 * Query:
 *   SELECT name, AVG(Salary) as avgSalary
 *        FROM maintable.
 * UpdatedQuery:
 *   SELECT name, Divide(sum(sumSalary)/sum(countsalary))
 *   FROM(
 *    SELECT name, sum(Salary) as sumSalary,count(salary) countsalary
 *      FROM maintable
 *      GROUP BY name
 *    UNION ALL
 *    SELECT maintable_name,sum(maintable_salary) as sumSalary, count(maintable_salary) countsalary
 *      FROM maintable_agg
 *      GROUP BY maintable_name)
 *   GROUP BY name)
 *
 * Rules for updating plan in case of streaming table:
 * In case of streaming data will be fetched from both fact and aggregate as aggregate table
 * will be updated only after each hand-off, so current streamed data won't be available on
 * aggregate table.
 * 7.1 Add one union node to add both fact and aggregate table plan to get the data from both table
 * 7.2 On top of Union Node add one Aggregate node to aggregate both table results
 * 7.3 In case of average(avg(column)) special handling is required for streaming
 *     7.3.1 Fact Plan will updated to return sum(column) and count(column) to do rollup
 *     7.3.2 Aggregate Plan will updated to return sum(column) and count(column) to do rollup
 * 7.4 In newly added Aggregate node all the aggregate expression must have same expression id as
 *     fact and fact plan will updated with new expression id. As query like order by this can be
 *     referred. In example1 sum(totalSalary) as totalSalary will have same expression id
 *     as in fact and fact plan sum(salary) will be updated with new expression id
 *
 * @param sparkSession spark session
 */
case class CarbonPreAggregateQueryRules(sparkSession: SparkSession) extends Rule[LogicalPlan] {

  /**
   * map for keeping parent attribute reference to child attribute reference
   * this will be used to updated the plan in case of join or order by
   */
  val updatedExpression = mutable.HashMap[AttributeReference, AttributeReference]()

  /**
   * parser
   */
  lazy val parser = new CarbonSpark2SqlParser

  /**
   * Below method will be used to validate the logical plan
   * @param logicalPlan query logical plan
   * @return isvalid or not
   */
  private def isValidPlan(logicalPlan: LogicalPlan) : Boolean = {
    var isValidPlan = true
    logicalPlan.transform {
      case aggregate@Aggregate(grp, aExp, child) =>
        isValidPlan = !aExp.exists { p =>
          if (p.isInstanceOf[UnresolvedAlias]) return false
          p.name.equals("preAggLoad") || p.name.equals("preAgg")
        }
        val updatedAggExp = aExp.filterNot(_.name.equalsIgnoreCase("preAggLoad"))
        Aggregate(grp, updatedAggExp, child)
    }
    isValidPlan
  }
  override def apply(plan: LogicalPlan): LogicalPlan = {
    var needAnalysis = true
    plan.transformExpressions {
      // first check if any preAgg scala function is applied it is present is in plan
      // then call is from create preaggregate table class so no need to transform the query plan
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAgg") =>
        needAnalysis = false
        al
      case al@Alias(udf: ScalaUDF, name) if name.equalsIgnoreCase("preAggLoad") =>
        needAnalysis = false
        al
      // in case of query if any unresolve alias is present then wait for plan to be resolved
      // return the same plan as we can tranform the plan only when everything is resolved
      case unresolveAlias@UnresolvedAlias(_, _) =>
        needAnalysis = false
        unresolveAlias
      case attr@UnresolvedAttribute(_) =>
        needAnalysis = false
        attr
    }
    if(needAnalysis) {
      needAnalysis = isValidPlan(plan)
      if(needAnalysis) {
        needAnalysis = validateStreamingTablePlan(plan)
      }
    }
    // if plan is not valid for transformation then return same plan
    if (!needAnalysis) {
      plan
    } else {
      val updatedPlan = transformPreAggQueryPlan(plan)
      val newPlan = updatePlan(updatedPlan)
      newPlan
    }

  }

  /**
   * Below method will be used validate whether plan is already updated in case of streaming table
   * In case of streaming table it will add UnionNode to get the data from fact and aggregate both
   * as aggregate table will be updated after each handoff.
   * So if plan is already updated no need to transform the plan again
   * @param logicalPlan
   * query plan
   * @return whether need to update the query plan or not
   */
  def validateStreamingTablePlan(logicalPlan: LogicalPlan) : Boolean = {
    var needTransformation: Boolean = true
    logicalPlan.transform {
      case union @ Union(Seq(plan1, plan2)) =>
        plan2.collect{
          case logicalRelation: LogicalRelation if
          logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
          logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable
            .isChildDataMap =>
            needTransformation = false
        }
        union
    }
    needTransformation
  }

  /**
   * Below method will be used to update the child plan
   * This will be used for updating expression like join condition,
   * order by, project list etc
   * @param plan child plan
   * @return updated plan
   */
  def updatePlan(plan: LogicalPlan) : LogicalPlan = {
    val updatedPlan = plan transform {
      case Aggregate(grp, aggExp, child) =>
        Aggregate(
          updateExpression(grp),
          updateExpression(aggExp.asInstanceOf[Seq[Expression]]).asInstanceOf[Seq[NamedExpression]],
          child)
      case Filter(filterExp, child) =>
        Filter(updateExpression(Seq(filterExp)).head, child)
      case Project(pList, child) =>
        Project(
          updateExpression(pList.asInstanceOf[Seq[Expression]]).asInstanceOf[Seq[NamedExpression]],
          child)
      case Sort(sortOrders, global, child) =>
        Sort(updateSortExpression(sortOrders), global, child)
      case Join(left, right, joinType, condition) =>
        val updatedCondition = condition match {
          case Some(expr) => Some(updateExpression(Seq(expr)).head)
          case _ => condition
        }
        Join(left, right, joinType, updatedCondition)
    }
    updatedPlan
  }

  /**
   * Below method will be used to update the sort expression
   * @param sortExp sort order expression in query
   * @return updated sort expression
   */
  def updateSortExpression(sortExp : Seq[SortOrder]) : Seq[SortOrder] = {
    sortExp map { order =>
      SortOrder(order.child transform  {
        case attr: AttributeReference =>
          updatedExpression.find { p => p._1.sameRef(attr) } match {
            case Some((_, childAttr)) =>
              CarbonToSparkAdapter.createAttributeReference(
                childAttr.name,
                childAttr.dataType,
                childAttr.nullable,
                childAttr.metadata,
                childAttr.exprId,
                attr.qualifier,
                attr)
            case None =>
              attr
          }
      }, order.direction )
    }
  }

  /**
   * Below method will be used to update the expression like group by expression
   * @param expressions sequence of expression like group by
   * @return updated expressions
   */
  def updateExpression(expressions : Seq[Expression]) : Seq[Expression] = {
    expressions map { expression =>
      expression transform {
        case attr: AttributeReference =>
          updatedExpression.find { p => p._1.sameRef(attr) } match {
            case Some((_, childAttr)) =>
              CarbonToSparkAdapter.createAttributeReference(
                childAttr.name,
                childAttr.dataType,
                childAttr.nullable,
                childAttr.metadata,
                childAttr.exprId,
                attr.qualifier,
                attr)
            case None =>
              attr
          }
      }
    }
  }

  /**
   * Below method will be used to validate and transform the main table plan to child table plan
   * rules for transforming is as below.
   * 1. Grouping expression rules
   *    1.1 Change the parent attribute reference for of group expression
   * to child attribute reference
   *
   * 2. Aggregate expression rules
   *    2.1 Change the parent attribute reference for of group expression to
   * child attribute reference
   *    2.2 Change the count AggregateExpression to Sum as count
   * is already calculated so in case of aggregate table
   * we need to apply sum to get the count
   *    2.2 In case of average aggregate function select 2 columns from aggregate table with
   * aggregation sum and count. Then add divide(sum(column with sum), sum(column with count)).
   * Note: During aggregate table creation for average table will be created with two columns
   * one for sum(column) and count(column) to support rollup
   * 3. Filter Expression rules.
   *    3.1 Updated filter expression attributes with child table attributes
   * 4. Update the Parent Logical relation with child Logical relation
   * 5. timeseries function
   *    5.1 validate parent table has timeseries datamap
   *    5.2 timeseries function is valid function or not
   * 6. Streaming
   * Rules for updating plan in case of streaming table:
   * In case of streaming data will be fetched from both fact and aggregate as aggregate table
   * will be updated only after each hand-off, so current streamed data won't be available on
   * aggregate table.
   * 6.1 Add one union node to add both fact and aggregate table plan to
   *     get the data from both table
   * 6.2 On top of Union Node add one Aggregate node to aggregate both table results
   * 6.3 In case of average(avg(column)) special handling is required for streaming
   *     7.3.1 Fact Plan will updated to return sum(column) and count(column) to do rollup
   *     7.3.2 Aggregate Plan will updated to return sum(column) and count(column) to do rollup
   * 6.4 In newly added Aggregate node all the aggregate expression must have same expression id as
   *     fact and fact plan will updated with new expression id. As query like order by this can be
   *     referred. In example1 sum(totalSalary) as totalSalary will have same expression id
   *     as in fact and fact plan sum(salary) will be updated with new expression id
   *
   * @param logicalPlan parent logical plan
   * @return transformed plan
   */
  def transformPreAggQueryPlan(logicalPlan: LogicalPlan): LogicalPlan = {
    var isPlanUpdated = false
    val updatedPlan = logicalPlan.transform {
      case agg@Aggregate(
        grExp,
        aggExp,
        CarbonSubqueryAlias(alias1, child@CarbonSubqueryAlias(alias2, l: LogicalRelation)))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.
             metaData.hasAggregateDataMapSchema && !isPlanUpdated =>
        val carbonTable = getCarbonTable(l)
        if (isSpecificSegmentNotPresent(carbonTable)) {
          val list = scala.collection.mutable.HashSet.empty[QueryColumn]
          val aggregateExpressions = scala.collection.mutable.HashSet.empty[AggregateExpression]
          val isValidPlan = extractQueryColumnsFromAggExpression(
            grExp,
            aggExp,
            carbonTable,
            list,
            aggregateExpressions)
          if (isValidPlan) {
            val (aggDataMapSchema, childPlan) = getChildDataMapForTransformation(list,
              aggregateExpressions,
              carbonTable,
              agg)
            if (null != aggDataMapSchema && null != childPlan) {
              val attributes = childPlan.output.asInstanceOf[Seq[AttributeReference]]
              val (updatedGroupExp, updatedAggExp, newChild, None) =
                getUpdatedExpressions(grExp,
                  aggExp,
                  child,
                  None,
                  aggDataMapSchema,
                  attributes,
                  childPlan,
                  carbonTable,
                  agg)
              isPlanUpdated = true
              setExplain(aggDataMapSchema)
              val updateAggPlan =
                Aggregate(
                updatedGroupExp,
                updatedAggExp,
                CarbonReflectionUtils.getSubqueryAlias(
                  sparkSession,
                  Some(alias1),
                  CarbonReflectionUtils.getSubqueryAlias(
                    sparkSession,
                    Some(alias2),
                    newChild,
                    None),
                  None))
              getAggregateQueryPlan(
                updateAggPlan,
                grExp,
                aggExp,
                carbonTable,
                aggDataMapSchema,
                agg)
            } else {
              agg
            }
          } else {
            agg
          }
        } else {
          agg
        }
      // case for aggregation query
      case agg@Aggregate(
      grExp,
      aggExp,
      child@CarbonSubqueryAlias(alias, l: LogicalRelation))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.
             metaData.hasAggregateDataMapSchema && !isPlanUpdated =>
        val carbonTable = getCarbonTable(l)
        if(isSpecificSegmentNotPresent(carbonTable)) {
          val list = scala.collection.mutable.HashSet.empty[QueryColumn]
          val aggregateExpressions = scala.collection.mutable.HashSet.empty[AggregateExpression]
          val isValidPlan = extractQueryColumnsFromAggExpression(
            grExp,
            aggExp,
            carbonTable,
            list,
            aggregateExpressions)
          if (isValidPlan) {
            val (aggDataMapSchema, childPlan) = getChildDataMapForTransformation(list,
              aggregateExpressions,
              carbonTable,
              agg)
            if (null != aggDataMapSchema && null != childPlan) {
              val attributes = childPlan.output.asInstanceOf[Seq[AttributeReference]]
              val (updatedGroupExp, updatedAggExp, newChild, None) =
                getUpdatedExpressions(grExp,
                  aggExp,
                  child,
                  None,
                  aggDataMapSchema,
                  attributes,
                  childPlan,
                  carbonTable,
                  agg)
              isPlanUpdated = true
              setExplain(aggDataMapSchema)
              val updateAggPlan =
                Aggregate(
                updatedGroupExp,
                updatedAggExp,
                CarbonReflectionUtils.getSubqueryAlias(
                  sparkSession,
                  Some(alias),
                  newChild,
                  None))
              getAggregateQueryPlan(
                updateAggPlan,
                grExp,
                aggExp,
                carbonTable,
                aggDataMapSchema,
                agg)
            } else {
              agg
            }
          } else {
            agg
          }
        } else {
          agg
        }
      // case of handling aggregation query with filter
      case agg@Aggregate(
      grExp,
      aggExp,
      Filter(expression, child@CarbonSubqueryAlias(alias, l: LogicalRelation)))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.
             metaData.hasAggregateDataMapSchema && !isPlanUpdated =>
        val carbonTable = getCarbonTable(l)
        if(isSpecificSegmentNotPresent(carbonTable)) {
          val list = scala.collection.mutable.HashSet.empty[QueryColumn]
          val aggregateExpressions = scala.collection.mutable.HashSet.empty[AggregateExpression]
          var isValidPlan = extractQueryColumnsFromAggExpression(
            grExp,
            aggExp,
            carbonTable,
            list,
            aggregateExpressions)
          if (isValidPlan) {
            isValidPlan = !CarbonReflectionUtils.hasPredicateSubquery(expression)
          }
          // getting the columns from filter expression
          if (isValidPlan) {
            extractColumnFromExpression(expression, list, carbonTable, true)
          }
          if (isValidPlan) {
            val (aggDataMapSchema, childPlan) = getChildDataMapForTransformation(list,
              aggregateExpressions,
              carbonTable,
              agg)
            if (null != aggDataMapSchema && null != childPlan) {
              val attributes = childPlan.output.asInstanceOf[Seq[AttributeReference]]
              val (updatedGroupExp, updatedAggExp, newChild, updatedFilterExpression) =
                getUpdatedExpressions(grExp,
                  aggExp,
                  child,
                  Some(expression),
                  aggDataMapSchema,
                  attributes,
                  childPlan,
                  carbonTable,
                  agg)
              isPlanUpdated = true
              setExplain(aggDataMapSchema)
              val updateAggPlan =
                Aggregate(
                updatedGroupExp,
                updatedAggExp,
                Filter(
                  updatedFilterExpression.get,
                  CarbonReflectionUtils.getSubqueryAlias(
                    sparkSession,
                    Some(alias),
                    newChild,
                    None)))
              getAggregateQueryPlan(
                updateAggPlan,
                grExp,
                aggExp,
                carbonTable,
                aggDataMapSchema,
                agg)
            } else {
              agg
            }
          } else {
            agg
          }
        } else {
          agg
        }
      case agg@Aggregate(
      grExp,
      aggExp,
      Filter(
      expression,
      CarbonSubqueryAlias(alias1, child@CarbonSubqueryAlias(alias2, l: LogicalRelation))))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.
             metaData.hasAggregateDataMapSchema && !isPlanUpdated =>
        val carbonTable = getCarbonTable(l)
        if(isSpecificSegmentNotPresent(carbonTable)) {
          val list = scala.collection.mutable.HashSet.empty[QueryColumn]
          val aggregateExpressions = scala.collection.mutable.HashSet.empty[AggregateExpression]
          var isValidPlan = extractQueryColumnsFromAggExpression(
            grExp,
            aggExp,
            carbonTable,
            list,
            aggregateExpressions)
          if (isValidPlan) {
            isValidPlan = !CarbonReflectionUtils.hasPredicateSubquery(expression)
          }
          // getting the columns from filter expression
          if (isValidPlan) {
            extractColumnFromExpression(expression, list, carbonTable, true)
          }
          if (isValidPlan) {
            val (aggDataMapSchema, childPlan) = getChildDataMapForTransformation(list,
              aggregateExpressions,
              carbonTable,
              agg)
            if (null != aggDataMapSchema && null != childPlan) {
              val attributes = childPlan.output.asInstanceOf[Seq[AttributeReference]]
              val (updatedGroupExp, updatedAggExp, newChild, updatedFilterExpression) =
                getUpdatedExpressions(grExp,
                  aggExp,
                  child,
                  Some(expression),
                  aggDataMapSchema,
                  attributes,
                  childPlan,
                  carbonTable,
                  agg)
              isPlanUpdated = true
              setExplain(aggDataMapSchema)
              val updateAggPlan =
                Aggregate(
                  updatedGroupExp,
                  updatedAggExp,
                  Filter(
                    updatedFilterExpression.get,
                    CarbonReflectionUtils.getSubqueryAlias(
                      sparkSession,
                      Some(alias1),
                      CarbonReflectionUtils.getSubqueryAlias(
                        sparkSession,
                        Some(alias2),
                        newChild,
                        None),
                      None)))
              getAggregateQueryPlan(
                updateAggPlan,
                grExp,
                aggExp,
                carbonTable,
                aggDataMapSchema,
                agg)
            } else {
              agg
            }
          } else {
            agg
          }
        } else {
          agg
        }

    }
    if(isPlanUpdated) {
      CarbonUtils.threadSet(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP, "true")
    }
    updatedPlan
  }

  // set datamap match information for EXPLAIN command
  private def setExplain(dataMapSchema: AggregationDataMapSchema): Unit = {
    ExplainCollector.recordMatchedOlapDataMap(
      dataMapSchema.getProvider.getShortName, dataMapSchema.getDataMapName)
  }

  /**
   * Method to get the aggregate query plan
   * @param aggPlan
   * aggregate table query plan
   * @param grExp
   * fact group by expression
   * @param aggExp
   * fact aggregate expression
   * @param carbonTable
   * fact table
   * @param aggregationDataMapSchema
   * selected aggregation data map
   * @param factAggPlan
   * fact aggregate query plan
   * @return updated plan
   */
  def getAggregateQueryPlan(aggPlan: LogicalPlan,
      grExp: Seq[Expression],
      aggExp: Seq[NamedExpression],
      carbonTable: CarbonTable,
      aggregationDataMapSchema: DataMapSchema,
      factAggPlan: LogicalPlan): LogicalPlan = {
    // to handle streaming table with pre aggregate
    if (carbonTable.isStreamingSink) {
      setSegmentsForStreaming(carbonTable, aggregationDataMapSchema)
      // get new fact expression
      val factExp = updateFactTablePlanForStreaming(factAggPlan)
      // get new Aggregate node expression
      val aggPlanNew = updateAggTablePlanForStreaming(aggPlan)
      val streamingNodeExp = getExpressionsForStreaming(aggExp)
      // clear the expression as in case of streaming it is not required
      updatedExpression.clear
      // Add Aggregate node to aggregate data from fact and aggregate
      Aggregate(
        createNewAggGroupBy(grExp, factAggPlan),
        streamingNodeExp.asInstanceOf[Seq[NamedExpression]],
        // add union node to get the result from both
        Union(
          factExp,
      aggPlanNew))
    } else {
      aggPlan
    }
  }

  /**
   * create group by expression for newly Added Aggregate node
   * @param grExp fact group by expression
   * @param plan fact query plan
   * @return group by expression
   */
  private def createNewAggGroupBy(grExp: Seq[Expression], plan: LogicalPlan): Seq[Expression] = {
    grExp.map {
      case attr: AttributeReference =>
        val aggModel = AggExpToColumnMappingModel(
          removeQualifiers(PreAggregateUtil.normalizeExprId(attr, plan.allAttributes)))
        if (factPlanGrpExpForStreaming.get(aggModel).isDefined) {
          factPlanGrpExpForStreaming.get(aggModel).get
        } else {
          attr
        }
      case exp: Expression =>
        val aggModel = AggExpToColumnMappingModel(
          removeQualifiers(PreAggregateUtil.normalizeExprId(exp, plan.allAttributes)))
        factPlanGrpExpForStreaming.get(aggModel).get
    }
  }
  /**
   * Method to set the segments when query is fired on streaming table with pre aggregate
   * Adding a property streaming_seg so while removing from session params we can differentiate
   * it was set from CarbonPreAggregateRules
   * @param parentTable
   * parent arbon table
   * @param dataMapSchema
   * child datamap schema
   */
  def setSegmentsForStreaming(parentTable: CarbonTable, dataMapSchema: DataMapSchema): Unit = {
    val mainTableKey = parentTable.getDatabaseName + '.' + parentTable.getTableName
    val factManager = new SegmentStatusManager(parentTable.getAbsoluteTableIdentifier)
    CarbonUtils
      .threadSet(CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING + mainTableKey, "true")
    CarbonUtils
      .threadSet(
        CarbonCommonConstants.CARBON_INPUT_SEGMENTS + mainTableKey,
        factManager.getValidAndInvalidSegments.getValidSegments.asScala.mkString(","))
    CarbonUtils
      .threadSet(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS + mainTableKey, "true")
    // below code is for aggregate table
    val identifier = TableIdentifier(
      dataMapSchema.getChildSchema.getTableName,
      Some(parentTable.getDatabaseName))
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val carbonRelation =
      catalog.lookupRelation(identifier)(sparkSession).asInstanceOf[CarbonRelation]
    val segmentStatusManager = new SegmentStatusManager(carbonRelation.carbonTable
      .getAbsoluteTableIdentifier)
    val validSegments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala
      .mkString(",")
    val childTableKey = carbonRelation.carbonTable.getDatabaseName + '.' +
                   carbonRelation.carbonTable.getTableName
    CarbonUtils
      .threadSet(CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING + childTableKey, "true")
    CarbonUtils
      .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + childTableKey, validSegments)
    CarbonUtils
      .threadSet(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS + childTableKey, "false")
  }

  /**
   * Map to keep expression name to its alias mapping. This will be used while adding a node when
   * plan for streaming table is updated.
   * Note: In case of average fact table plan will have two alias as sum(column) and count(column)
   * to support rollup
   */
  private val factPlanExpForStreaming = mutable.HashMap[String, Seq[NamedExpression]]()

  private val factPlanGrpExpForStreaming = mutable
    .HashMap[AggExpToColumnMappingModel, AttributeReference]()

  /**
   * Below method will be used to get the expression for Aggregate node added for streaming
   * Expression id will be same as fact plan as it can be referred in query
   *
   * @param aggExp
   * main table aggregate expression
   * @return updated aggregate expression
   */
  private def getExpressionsForStreaming(aggExp: Seq[Expression]): Seq[Expression] = {
    val updatedExp = aggExp map {
      case attr: AttributeReference =>
        attr
      case alias@Alias(aggExp: AggregateExpression, name) =>
        // in case of aggregate expression get the fact alias based on expression name
        val factAlias = factPlanExpForStreaming(name)
        // create attribute reference object for each expression
        val attrs = factAlias.map { factAlias =>
          CarbonToSparkAdapter.createAttributeReference(
            name,
            alias.dataType,
            alias.nullable,
            Metadata.empty,
            factAlias.exprId,
            alias.qualifier,
            alias)
        }
        // add aggregate function in Aggregate node added for handling streaming
        // to aggregate results from fact and aggregate table
        val updatedAggExp = getAggregateExpressionForAggregation(aggExp, attrs)
        // same reference id will be used as it can be used by above nodes in the plan like
        // sort, project, join
        CarbonToSparkAdapter.createAliasRef(
          updatedAggExp.head,
          name,
          alias.exprId,
          alias.qualifier,
          Option(alias.metadata),
          Some(alias))
      case alias@Alias(expression, name) =>
        CarbonToSparkAdapter.createAttributeReference(
          name,
          alias.dataType,
          alias.nullable,
          Metadata.empty,
          alias.exprId,
          alias.qualifier,
          alias)
    }
    updatedExp
  }

  /**
   * Below method will be used to update the fact plan in case of streaming table
   * This is required to handle average aggregte function as in case of average we need to return
   * two columns data sum(column) and count(column) to get the correct result
   *
   * @param logicalPlan
   * fact table Aggregate plan
   * @return updated aggregate plan for fact
   */
  private def updateFactTablePlanForStreaming(logicalPlan: LogicalPlan) : LogicalPlan = {
    // only aggregate expression needs to be updated
    logicalPlan.transform{
      case agg@Aggregate(grpExp, aggExp, _) =>
        agg
          .copy(aggregateExpressions = updateAggExpInFactForStreaming(aggExp, grpExp, agg)
            .asInstanceOf[Seq[NamedExpression]])
    }
  }

  /**
   * Below method will be used to update the aggregate table plan for streaming
   * @param logicalPlan
   * aggergate table logical plan
   * @return updated logical plan
   */
  private def updateAggTablePlanForStreaming(logicalPlan: LogicalPlan) : LogicalPlan = {
    // only aggregate expression needs to be updated
    logicalPlan.transform{
      case agg@Aggregate(grpExp, aggExp, _) =>
        agg
          .copy(aggregateExpressions = updateAggExpInAggForStreaming(aggExp, grpExp, agg)
            .asInstanceOf[Seq[NamedExpression]])
    }
  }

  /**
   * Below method will be used to update the aggregate plan for streaming
   * @param namedExp
   * aggregate expression
   * @param grpExp
   * group by expression
   * @param plan
   * aggregate query plan
   * @return updated aggregate expression
   */
  private def updateAggExpInAggForStreaming(namedExp : Seq[NamedExpression],
      grpExp: Seq[Expression], plan: LogicalPlan) : Seq[Expression] = {
    // removing alias from expression to compare with grouping expression
    // as in case of alias all the projection column will be updated with alias
    val updatedExp = namedExp.map {
      case Alias(attr: AttributeReference, name) =>
        attr
      case exp: Expression =>
        exp
    }
    addGrpExpToAggExp(grpExp, updatedExp, plan)
  }

  /**
   * below method will be used to updated the aggregate expression with missing
   * group by expression, when only aggregate expression is selected in query
   *
   * @param grpExp
   * group by expressions
   * @param aggExp
   * aggregate expressions
   * @param plan
   * logical plan
   * @return updated aggregate expression
   */
  private def addGrpExpToAggExp(grpExp: Seq[Expression],
      aggExp: Seq[Expression],
      plan: LogicalPlan): Seq[Expression] = {
    // set to add all the current aggregate expression
    val expressions = mutable.LinkedHashSet.empty[AggExpToColumnMappingModel]
    aggExp.foreach {
      case Alias(exp, _) =>
      expressions +=
      AggExpToColumnMappingModel(
        PreAggregateUtil.normalizeExprId(exp, plan.allAttributes), None)
      case attr: AttributeReference =>
        expressions +=
        AggExpToColumnMappingModel(
          PreAggregateUtil.normalizeExprId(attr, plan.allAttributes), None)
    }
    val newAggExp = new ArrayBuffer[Expression]
    newAggExp ++= aggExp
    // for each group by expression check if already present in set if it is present
    // then no need to add otherwise add
    var counter = 0
    grpExp.foreach{gExp =>
      val normalizedExp = AggExpToColumnMappingModel(
        PreAggregateUtil.normalizeExprId(gExp, plan.allAttributes), None)
      if(!expressions.contains(normalizedExp)) {
        gExp match {
          case attr: AttributeReference =>
            newAggExp += attr
          case exp: Expression =>
            newAggExp += CarbonToSparkAdapter.createAliasRef(
              exp,
              "dummy_" + counter,
              NamedExpression.newExprId)
            counter = counter + 1
        }
      }
    }
    newAggExp
  }
  /**
   * Below method will be used to update the aggregate expression for streaming fact table plan
   * @param namedExp
   * streaming Fact plan aggregate expression
   * @return
   * Updated streaming fact plan aggregate expression
   */
  private def updateAggExpInFactForStreaming(namedExp : Seq[NamedExpression],
  grpExp: Seq[Expression], plan: LogicalPlan) : Seq[Expression] = {
    val addedExp = addGrpExpToAggExp(grpExp, namedExp, plan)
    val updatedExp = addedExp.flatMap {
      case attr: AttributeReference =>
        Seq(attr)
      case alias@Alias(aggExp: AggregateExpression, name) =>
        // get the new aggregate expression
        val newAggExp = getAggFunctionForFactStreaming(aggExp)
        val updatedExp = newAggExp.map { exp =>
          CarbonToSparkAdapter.createAliasRef(exp,
            name,
            NamedExpression.newExprId,
            alias.qualifier,
            Some(alias.metadata),
            Some(alias))
        }
        // adding to map which will be used while Adding an Aggregate node for handling streaming
        // table plan change
        factPlanExpForStreaming.put(name, updatedExp)
        updatedExp
      case alias@Alias(exp: Expression, name) =>
        val newAlias = Seq(alias)
        val attr = CarbonToSparkAdapter.createAttributeReference(name,
          alias.dataType,
          alias.nullable,
          alias.metadata,
          alias.exprId,
          alias.qualifier,
          alias)
        factPlanGrpExpForStreaming.put(
          AggExpToColumnMappingModel(
            removeQualifiers(PreAggregateUtil.normalizeExprId(exp, plan.allAttributes))),
            attr)
        factPlanExpForStreaming.put(name, newAlias)
        newAlias
    }
    updatedExp
  }
  /**
   * Below method will be used to update the fact table query aggregate function expression
   * Rules for updating the expression.
   * In case of average return sum(expression), count(expression) to get the correct result
   * @param aggExp
   * actual query aggregate expression
   * @return seq of expression as in case of average we need to return two sum and count
   *
   */
  def getAggFunctionForFactStreaming(aggExp: AggregateExpression): Seq[Expression] = {
    aggExp.aggregateFunction match {
      case Average(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        val newExp = Seq(AggregateExpression(Sum(Cast(exp, DoubleType)),
          aggExp.mode,
          isDistinct = false),
          Cast(AggregateExpression(Count(exp), aggExp.mode, false), DoubleType))
        newExp
      case Average(exp: Expression) =>
        val dataType =
          if (exp.dataType.isInstanceOf[DecimalType]) {
            // decimal must not go as double precision.
            exp.dataType.asInstanceOf[DecimalType]
          } else {
            DoubleType
          }
        val newExp = Seq(AggregateExpression(Sum(Cast(exp, dataType)), aggExp.mode, false),
          Cast(AggregateExpression(Count(exp), aggExp.mode, false), dataType))
        newExp
      case _ =>
        val newExp = Seq(aggExp)
        newExp
    }
  }

  /**
   * Below method will be used to validate query plan and get the proper aggregation data map schema
   * and child relation plan object if plan is valid for transformation
   * @param queryColumns list of query columns from projection and filter
   * @param aggregateExpressions list of aggregate expression (aggregate function)
   * @param carbonTable parent carbon table
   * @param parentLogicalPlan parent logical relation
   * @return if plan is valid then aggregation data map schema and its relation plan
   */
  def getChildDataMapForTransformation(queryColumns: scala.collection.mutable.HashSet[QueryColumn],
      aggregateExpressions: scala.collection.mutable.HashSet[AggregateExpression],
      carbonTable: CarbonTable,
      parentLogicalPlan: LogicalPlan): (AggregationDataMapSchema, LogicalPlan) = {
    // getting all the projection columns
    val listProjectionColumn = queryColumns
      .filter(queryColumn => !queryColumn.isFilterColumn)
      .toList
    // getting all the filter columns
    val listFilterColumn = queryColumns
      .filter(queryColumn => queryColumn.isFilterColumn)
      .toList
    val isProjectionColumnPresent = (listProjectionColumn.size + listFilterColumn.size) > 0
    // create a query plan object which will be used to select the list of pre aggregate tables
    // matches with this plan
    val queryPlan = new AggregateQueryPlan(listProjectionColumn.asJava, listFilterColumn.asJava)
    // create aggregate table selector object
    val aggregateTableSelector = new AggregateTableSelector(queryPlan, carbonTable)
    // select the list of valid child tables
    val selectedDataMapSchemas = aggregateTableSelector.selectPreAggDataMapSchema()
    // query has only aggregate expression then selected data map will be empty
    // the validate all the child data map otherwise validate selected data map
    var selectedAggMaps = if (isProjectionColumnPresent) {
      selectedDataMapSchemas
    } else {
      carbonTable.getTableInfo.getDataMapSchemaList
    }
    // if it does not match with any pre aggregate table return the same plan
    if (!selectedAggMaps.isEmpty) {
      // filter the selected child schema based on size to select the pre-aggregate tables
      // that are enabled
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      val relationBuffer = selectedAggMaps.asScala.map { selectedDataMapSchema =>
        val identifier = TableIdentifier(
          selectedDataMapSchema.getRelationIdentifier.getTableName,
          Some(selectedDataMapSchema.getRelationIdentifier.getDatabaseName))
        val carbonRelation =
          catalog.lookupRelation(identifier)(sparkSession).asInstanceOf[CarbonRelation]
        val relation = sparkSession.sessionState.catalog.lookupRelation(identifier)
        (selectedDataMapSchema, carbonRelation, relation)
      }.filter(_._2.sizeInBytes != 0L).sortBy(_._2.sizeInBytes)
      if (relationBuffer.isEmpty) {
        // If the size of relation Buffer is 0 then it means that none of the pre-aggregate
        // tables have data yet.
        // In this case we would return the original plan so that the query hits the parent
        // table.
        (null, null)
      } else {
        // if query does not have any aggregate function no need to validate the same
        val tuple = if (aggregateExpressions.nonEmpty && !selectedAggMaps.isEmpty) {
          relationBuffer.collectFirst {
            case a@(datamapSchema, _, _)
              if validateAggregateExpression(datamapSchema,
                carbonTable,
                parentLogicalPlan,
                aggregateExpressions.toSeq) =>
              a
          }
        } else {
          Some(relationBuffer.head)
        }
        tuple match {
          case Some((dataMapSchema, _, logicalPlan)) => (dataMapSchema
            .asInstanceOf[AggregationDataMapSchema], new FindDataSourceTable(sparkSession)
            .apply(logicalPlan))
          case None => (null, null)
        }
        // If the relationBuffer is enabled then find the table with the minimum size.
      }
    } else {
      (null, null)
    }
  }

  /**
   * Below method will be used to validate aggregate expression with the data map
   * and will return the selected valid data maps
   * @param selectedDataMap list of data maps
   * @param carbonTable parent carbon table
   * @param parentLogicalPlan parent logical plan
   * @param queryAggExpLogicalPlans query agg expression logical plan
   * @return valid data map
   */
  def validateAggregateExpression(selectedDataMap: DataMapSchema,
      carbonTable: CarbonTable,
      parentLogicalPlan: LogicalPlan,
      queryAggExpLogicalPlans: Seq[AggregateExpression]): Boolean = {
    val mappingModel = getExpressionToColumnMapping(selectedDataMap,
      carbonTable,
      parentLogicalPlan)
    queryAggExpLogicalPlans.forall{p =>
      mappingModel.exists{m =>
        matchExpression(
          PreAggregateUtil.normalizeExprId(p, parentLogicalPlan.allAttributes),
          m.expression)}
    }
  }

  /**
   * Below method will be used to update the expression
   * It will remove the qualifiers
   * @param expression
   * expression
   * @return updated expressions
   */
  private def removeQualifiers(expression: Expression) : Expression = {
    expression.transform {
      case attr: AttributeReference =>
        CarbonToSparkAdapter.createAttributeReference(
          attr.name,
          attr.dataType,
          attr.nullable,
          attr.metadata,
          attr.exprId,
          None,
          attr)
    }
  }

  /**
   * Below method will be used to match two expressions
   * @param firstExp
   * first expression
   * @param secondExp
   * second expressios
   * @return is similare
   */
  private def matchExpression(firstExp: Expression, secondExp: Expression) : Boolean = {
    val first = removeQualifiers(firstExp)
    val second = removeQualifiers(secondExp)
    first == second
  }

  /**
   * Below method will be used to to get the logical plan for each aggregate expression in
   * child data map and its column schema mapping if mapping is already present
   * then it will use the same otherwise it will generate and stored in aggregation data map
   * @param selectedDataMap child data map
   * @param carbonTable parent table
   * @param parentLogicalPlan logical relation of actual plan
   * @return map of logical plan for each aggregate expression in child query and its column mapping
   */
  def getExpressionToColumnMapping(selectedDataMap: DataMapSchema,
      carbonTable: CarbonTable,
      parentLogicalPlan: LogicalPlan): mutable.Set[AggExpToColumnMappingModel] = {
    val aggDataMapSchema = selectedDataMap.asInstanceOf[AggregationDataMapSchema]
    if(null == aggDataMapSchema.getAggExpToColumnMapping) {
      // add preAGG UDF to avoid all the PreAggregate rule
      val childDataMapQueryString = parser.addPreAggFunction(
        PreAggregateUtil.getChildQuery(aggDataMapSchema))
      // get the logical plan
      val aggPlan = sparkSession.sql(childDataMapQueryString).logicalPlan
      // getting all aggregate expression from query
      val dataMapAggExp = getAggregateExpFromChildDataMap(aggPlan)
      // in case of average child table will have two columns which will be stored in sequence
      // so for average expression we need to get two columns for mapping
      var counter = 0
      // sorting the columns based on schema ordinal so search will give proper result
      val sortedColumnList = aggDataMapSchema.getChildSchema.getListOfColumns.asScala
        .sortBy(_.getSchemaOrdinal)
      val expressionToColumnMapping = mutable.LinkedHashSet.empty[AggExpToColumnMappingModel]
      dataMapAggExp.foreach { aggExp =>
        val updatedExp = PreAggregateUtil.normalizeExprId(aggExp, aggPlan.allAttributes)
        val model = AggExpToColumnMappingModel(updatedExp, None)
        if (!expressionToColumnMapping.contains(model)) {
          // check if aggregate expression is of type avg
          // get the columns
          val columnSchema = aggDataMapSchema
            .getAggColumnBasedOnIndex(counter, sortedColumnList.asJava)
          // increment the counter so when for next expression above code will be
          // executed it will search from that schema ordinal
          counter = columnSchema.getSchemaOrdinal + 1
          model.columnSchema = Some(columnSchema)
          expressionToColumnMapping += model
        }
      }
      aggDataMapSchema.setAggExpToColumnMapping(expressionToColumnMapping.asJava)
      // return the mapping
      expressionToColumnMapping
    } else {
      aggDataMapSchema.getAggExpToColumnMapping
        .asInstanceOf[java.util.Set[AggExpToColumnMappingModel]].asScala
        .asInstanceOf[mutable.LinkedHashSet[AggExpToColumnMappingModel]]
    }
  }

  /**
   * Below method will be used to get aggregate expression
   * @param logicalPlan logical plan
   * @return list of aggregate expression
   */
  def getAggregateExpFromChildDataMap(logicalPlan: LogicalPlan): Seq[AggregateExpression] = {
    val list = scala.collection.mutable.ListBuffer.empty[AggregateExpression]
    logicalPlan match {
      case _@Aggregate(_, aggExp, _) =>
        aggExp map {
          case Alias(attr: AggregateExpression, _) =>
            list ++= PreAggregateUtil.validateAggregateFunctionAndGetFields(attr)
          case _ =>
        }
    }
    list
  }

  /**
   * Below method will be used to check whether specific segment is set for maintable
   * if it is present then no need to transform the plan and query will be executed on
   * maintable
   * @param carbonTable parent table
   * @return is specific segment is present in session params
   */
  def isSpecificSegmentNotPresent(carbonTable: CarbonTable) : Boolean = {
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (carbonSessionInfo != null) {
      carbonSessionInfo.getSessionParams
        .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier
                       .getDatabaseName + "." + carbonTable.getTableName, "").isEmpty
    } else {
      true
    }
  }

  /**
   * Below method will be used to extract the query columns from
   * filter expression
   * @param expression filter expression
   * @param queryColumns query column set
   * @param carbonTable parent table
   * @return isvalid filter expression for aggregate
   */
  def extractColumnFromExpression(expression: Expression,
      queryColumns: scala.collection.mutable.HashSet[QueryColumn],
      carbonTable: CarbonTable,
      isFilterColumn: Boolean = false) {
    // map to maintain attribute reference present in the filter to timeseries function
    // if applied this is added to avoid duplicate column
    val mapOfColumnSeriesFun = scala.collection.mutable.HashMap.empty[AttributeReference, String]
    expression.transform {
      case attr: AttributeReference =>
        if (mapOfColumnSeriesFun.get(attr).isEmpty) {
          mapOfColumnSeriesFun.put(attr, null)
        }
        attr
      case udf@CarbonScalaUDF(_) =>
        // for handling timeseries function
        if (udf.asInstanceOf[ScalaUDF].function.getClass.getName.equalsIgnoreCase(
          "org.apache.spark.sql.execution.command.timeseries.TimeseriesFunction") &&
            CarbonUtil.hasTimeSeriesDataMap(carbonTable)) {
          mapOfColumnSeriesFun.put(udf.children.head.asInstanceOf[AttributeReference],
            udf.children.last.asInstanceOf[Literal].value.toString)
        } else {
          // for any other scala udf
          udf.transform {
            case attr: AttributeReference =>
              if (mapOfColumnSeriesFun.get(attr).isEmpty) {
                mapOfColumnSeriesFun.put(attr, null)
              }
              attr
          }
        }
        udf
    }
    mapOfColumnSeriesFun.foreach { f =>
      if (f._2 == null) {
        queryColumns +=
        getQueryColumn(f._1.name, carbonTable, isFilterColumn)
      } else {
        queryColumns += getQueryColumn(f._1.name,
          carbonTable,
          isFilterColumn,
          timeseriesFunction = f._2)
      }
    }
  }

  /**
   * Below method will be used to get the child attribute reference
   * based on parent name
   *
   * @param dataMapSchema child schema
   * @param attributeReference parent attribute reference
   * @param attributes child logical relation
   * @param canBeNull this is added for strict validation in which case child attribute can be
   * null and when it cannot be null
   * @return child attribute reference
   */
  def getChildAttributeReference(dataMapSchema: DataMapSchema,
      attributeReference: AttributeReference,
      attributes: Seq[AttributeReference],
      canBeNull: Boolean = false,
      timeseriesFunction: String = ""): AttributeReference = {
    val aggregationDataMapSchema = dataMapSchema.asInstanceOf[AggregationDataMapSchema]
    val columnSchema = if (timeseriesFunction.isEmpty) {
      aggregationDataMapSchema.getChildColByParentColName(attributeReference.name.toLowerCase)
    } else {
      aggregationDataMapSchema
        .getTimeseriesChildColByParent(attributeReference.name.toLowerCase,
          timeseriesFunction)
    }
    // here column schema cannot be null, if it is null then aggregate table selection
    // logic has some problem
    if (!canBeNull && null == columnSchema) {
      throw new AnalysisException("Column does not exists in Pre Aggregate table")
    }
    if(null == columnSchema && canBeNull) {
      null
    } else {
      // finding the child attribute from child logical relation
      attributes.find(p => p.name.equals(columnSchema.getColumnName)).get
    }
  }

  /**
   * Below method will be used to get the updated expression for pre aggregated table.
   * It will replace the attribute of actual plan with child table attributes.
   * Updation will be done for below expression.
   * 1. Grouping expression
   * 2. aggregate expression
   * 3. child logical plan
   * 4. filter expression if present
   *
   * @param groupingExpressions actual plan grouping expression
   * @param aggregateExpressions actual plan aggregate expression
   * @param child child logical plan
   * @param filterExpression filter expression
   * @param aggDataMapSchema pre aggregate table schema
   * @param attributes pre aggregate table logical relation
   * @param aggPlan aggregate logical plan
   * @return tuple of(updated grouping expression,
   * updated aggregate expression,
   * updated child logical plan,
   * updated filter expression if present in actual plan)
   */
  def getUpdatedExpressions(groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      child: LogicalPlan, filterExpression: Option[Expression] = None,
      aggDataMapSchema: AggregationDataMapSchema,
      attributes: Seq[AttributeReference],
      aggPlan: LogicalPlan,
      parentTable: CarbonTable,
      parentLogicalPlan: LogicalPlan): (Seq[Expression], Seq[NamedExpression], LogicalPlan,
    Option[Expression]) = {
    val aggExpColumnMapping = if (null != aggDataMapSchema.getAggExpToColumnMapping) {
      Some(aggDataMapSchema.getAggExpToColumnMapping
        .asInstanceOf[java.util.Set[AggExpToColumnMappingModel]].asScala
        .asInstanceOf[mutable.LinkedHashSet[AggExpToColumnMappingModel]])
    } else {
      None
    }

    // transforming the group by expression attributes with child attributes
    val updatedGroupExp = groupingExpressions.map { exp =>
      exp.transform {
        case attr: AttributeReference =>
          val childAttr = getChildAttributeReference(aggDataMapSchema, attr, attributes)
          childAttr
      }
    }
    // below code is for updating the aggregate expression.
    // Note: In case of aggregate expression updation we need to return alias as
    //       while showing the final result we need to show based on actual query
    //       for example: If query is "select name from table group by name"
    //       if we only update the attributes it will show child table column name in final output
    //       so for handling this if attributes does not have alias we need to return alias of
    // parent
    //       table column name
    // Rules for updating aggregate expression.
    // 1. If it matches with attribute reference return alias of child attribute reference
    // 2. If it matches with alias return same alias with child attribute reference
    // 3. If it matches with alias of any supported aggregate function return aggregate function
    // with child attribute reference. Please check class level documentation how when aggregate
    // function will be updated

    val updatedAggExp = aggregateExpressions.flatMap {
      // case for attribute reference
      case attr: AttributeReference =>
        val childAttr = getChildAttributeReference(aggDataMapSchema,
          attr,
          attributes)
        val newExpressionId = NamedExpression.newExprId
        val childTableAttr = CarbonToSparkAdapter.createAttributeReference(attr.name,
          childAttr.dataType,
          childAttr.nullable,
          childAttr.metadata,
          newExpressionId,
          childAttr.qualifier,
          attr)
        updatedExpression.put(attr, childTableAttr)
        // returning the alias to show proper column name in output
        Seq(Alias(childAttr,
          attr.name)(newExpressionId,
          childAttr.qualifier).asInstanceOf[NamedExpression])
      // case for alias
      case alias@Alias(attr: AttributeReference, name) =>
        val childAttr = getChildAttributeReference(aggDataMapSchema,
          attr,
          attributes)
        val newExpressionId = NamedExpression.newExprId
        val parentTableAttr = CarbonToSparkAdapter.createAttributeReference(name,
          alias.dataType,
          alias.nullable,
          Metadata.empty,
          alias.exprId,
          alias.qualifier,
          alias)
        val childTableAttr = CarbonToSparkAdapter.createAttributeReference(name,
          alias.dataType,
          alias.nullable,
          Metadata.empty,
          newExpressionId,
          alias.qualifier,
          alias)
        updatedExpression.put(parentTableAttr, childTableAttr)
        // returning alias with child attribute reference
        Seq(Alias(childAttr,
          name)(newExpressionId,
          childAttr.qualifier).asInstanceOf[NamedExpression])
      // for aggregate function case
      case alias@Alias(attr: AggregateExpression, name) =>
        // get the updated aggregate aggregate function
        val aggExp = if (aggExpColumnMapping.isDefined) {
          getUpdatedAggregateExpressionForChild(attr,
            aggDataMapSchema,
            attributes,
            parentTable,
            parentLogicalPlan,
            aggExpColumnMapping.get,
            parentTable.isStreamingSink)
        } else {
          Seq(attr)
        }
        if(!parentTable.isStreamingSink) {
          // for normal table
          // generate new expression id for child
          val newExpressionId = NamedExpression.newExprId
          // create a parent attribute reference which will be replced on node which may be referred
          // by node like sort join
          val parentTableAttr = CarbonToSparkAdapter.createAttributeReference(name,
            alias.dataType,
            alias.nullable,
            Metadata.empty,
            alias.exprId,
            alias.qualifier,
            alias)
          // creating a child attribute reference which will be replced
          val childTableAttr = CarbonToSparkAdapter.createAttributeReference(name,
            alias.dataType,
            alias.nullable,
            Metadata.empty,
            newExpressionId,
            alias.qualifier,
            alias)
          // adding to map, will be used during other node updation like sort, join, project
          updatedExpression.put(parentTableAttr, childTableAttr)
          // returning alias with child attribute reference
          Seq(Alias(aggExp.head,
            name)(newExpressionId,
            alias.qualifier).asInstanceOf[NamedExpression])
        } else {
          // for streaming table
          // create alias for aggregate table
          val aggExpForStreaming = aggExp.map{ exp =>
            CarbonToSparkAdapter.createAliasRef(exp,
              name,
              NamedExpression.newExprId,
              alias.qualifier,
              Some(alias.metadata),
              Some(alias)).asInstanceOf[NamedExpression]
          }
          aggExpForStreaming
        }
      case alias@Alias(expression: Expression, name) =>
        val updatedExp =
          if (expression.isInstanceOf[ScalaUDF] &&
              expression.asInstanceOf[ScalaUDF].function.getClass.getName.equalsIgnoreCase(
                "org.apache.spark.sql.execution.command.timeseries.TimeseriesFunction")) {
            expression.asInstanceOf[ScalaUDF].transform {
              case attr: AttributeReference =>
                val childAttributeReference = getChildAttributeReference(aggDataMapSchema,
                  attr,
                  attributes,
                  timeseriesFunction =
                    expression.asInstanceOf[ScalaUDF].children(1).asInstanceOf[Literal].value
                      .toString)
                childAttributeReference
            }
          } else {
            expression.transform{
              case attr: AttributeReference =>
                val childAttributeReference = getChildAttributeReference(aggDataMapSchema,
                  attr,
                  attributes)
                childAttributeReference
            }
          }
        val newExpressionId = NamedExpression.newExprId
        val parentTableAttr = CarbonToSparkAdapter.createAttributeReference(name,
          alias.dataType,
          alias.nullable,
          Metadata.empty,
          alias.exprId,
          alias.qualifier,
          alias)
        val childTableAttr = CarbonToSparkAdapter.createAttributeReference(name,
          alias.dataType,
          alias.nullable,
          Metadata.empty,
          newExpressionId,
          alias.qualifier,
          alias)
        updatedExpression.put(parentTableAttr, childTableAttr)
        Seq(Alias(updatedExp, name)(newExpressionId,
          alias.qualifier).asInstanceOf[NamedExpression])
    }
    // transforming the logical relation
    val newChild = child.transform {
      case _: LogicalRelation =>
        aggPlan
      case _: SubqueryAlias =>
        aggPlan match {
          case s: SubqueryAlias => s.child
          case others => others
        }
    }
    // updating the filter expression if present
    val updatedFilterExpression = if (filterExpression.isDefined) {
      val filterExp = filterExpression.get
      Some(filterExp.transform {
        case attr: AttributeReference =>
          getChildAttributeReference(aggDataMapSchema, attr, attributes)
      })
    } else {
      None
    }
    (updatedGroupExp, updatedAggExp, newChild, updatedFilterExpression)
  }

  /**
   * Below method will be used to get the aggregate expression based on match
   * Aggregate expression updation rules
   * 1 Change the count AggregateExpression to Sum as count
   * is already calculated so in case of aggregate table
   * we need to apply sum to get the count
   * 2 In case of average aggregate function select 2 columns from aggregate table
   * with aggregation sum and count.
   * Then add divide(sum(column with sum), sum(column with count)).
   * Note: During aggregate table creation for average aggregation function
   * table will be created with two columns one for sum(column) and count(column)
   * to support rollup
   *
   * @param aggExp aggregate expression
   * @param dataMapSchema child data map schema
   * @param attributes child logical relation
   * @param parentTable parent carbon table
   * @param parentLogicalPlan logical relation
   * @return updated expression
   */
  def getUpdatedAggregateExpressionForChild(aggExp: AggregateExpression,
      dataMapSchema: AggregationDataMapSchema,
      attributes: Seq[AttributeReference],
      parentTable: CarbonTable,
      parentLogicalPlan: LogicalPlan,
      aggExpColumnMapping: mutable.LinkedHashSet[AggExpToColumnMappingModel],
      isStreamingTable: Boolean):
  Seq[Expression] = {
    // get the updated aggregate expression, in case of average column
    // it will be divided in two aggergate expression
    val updatedAggExp = PreAggregateUtil.validateAggregateFunctionAndGetFields(aggExp)
    // get the attributes to be updated for child table
    val attrs = aggExpColumnMapping.collect {
      case (schemaAggExpModel)
        if updatedAggExp
          .exists(p =>
            matchExpression(schemaAggExpModel.expression,
            PreAggregateUtil.normalizeExprId(p, parentLogicalPlan.allAttributes))) =>
        attributes filter (_.name.equalsIgnoreCase(
          schemaAggExpModel.columnSchema.get.asInstanceOf[ColumnSchema].getColumnName))
    }.flatten
    // getting aggregate table aggregate expressions
    getAggregateExpressionForAggregation(aggExp, attrs.toSeq, isStreamingTable)
  }

  /**
   * Below method will be used to update the aggregate expression.
   * 1.In case of average below expression will be returned.
   * 1.1 Streaming table
   *    1.1.1 Aggregate table
   *        It will return sum(expression) and count(expression)
   *    1.2.1 Aggregate node added for streaming
   *        It will return Divide(sum(expression)/count(expression))
   * 2.1 Normal table
   *    2.1.1 Aggregate table
   *      It will return Divide(sum(expression)/count(expression))
   * 2. In case of count for aggregate table and aggregate node added for streaming
   *    table count will be aggregated to sum
   *
   * @param aggExp
   * aggregate expression
   * @param attrs
   * aggregate function Attribute, in case of average it will be two to support rollup
   * @return
   * aggregate expression
   */
  def getAggregateExpressionForAggregation(aggExp: AggregateExpression,
      attrs: Seq[AttributeReference],
      isStreamingTable: Boolean = false): Seq[Expression] = {
    aggExp.aggregateFunction match {
      case Sum(MatchCastExpression(_, changeDataType: DataType)) =>
        Seq(AggregateExpression(Sum(Cast(attrs.head, changeDataType)), aggExp.mode, false))
      case Sum(_) =>
        Seq(AggregateExpression(Sum(attrs.head), aggExp.mode, false))
      case Max(MatchCastExpression(_, changeDataType: DataType)) =>
        Seq(AggregateExpression(Max(Cast(attrs.head, changeDataType)), aggExp.mode, false))
      case Max(_) =>
        Seq(AggregateExpression(Max(attrs.head), aggExp.mode, false))
      case Min(MatchCastExpression(_, changeDataType: DataType)) =>
        Seq(AggregateExpression(Min(Cast(attrs.head, changeDataType)), aggExp.mode, false))
      case Min(_) =>
        Seq(AggregateExpression(Min(attrs.head), aggExp.mode, false))
      // Change the count AggregateExpression to Sum as count
      // is already calculated so in case of aggregate table
      // we need to apply sum to get the count
      case Count(Seq(expression: Expression)) =>
        Seq(AggregateExpression(Sum(Cast(attrs.head, LongType)), aggExp.mode, false))

      case Average(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        // for handling Normal table case/Aggregate node added in case of streaming table
        if (!isStreamingTable) {
          // In case of average aggregate function select 2 columns from aggregate table
          // with aggregation sum and count.
          // Then add divide(sum(column with sum), sum(column with count)).
          Seq(Divide(AggregateExpression(Sum(Cast(
            attrs.head,
            DoubleType)),
            aggExp.mode,
            false),
            AggregateExpression(Sum(Cast(
              attrs.last,
              DoubleType)),
              aggExp.mode,
              false)))
        } else {
          // in case of streaming aggregate table return two aggregate function sum and count
          Seq(AggregateExpression(Sum(Cast(
            attrs.head,
            DoubleType)),
            aggExp.mode,
            false),
            AggregateExpression(Sum(Cast(
              attrs.last,
              DoubleType)),
              aggExp.mode,
              false))
        }
      case Average(exp: Expression) =>
        val dataType =
          if (exp.dataType.isInstanceOf[DecimalType]) {
            // decimal must not go as double precision.
            exp.dataType.asInstanceOf[DecimalType]
          } else {
            DoubleType
          }
        // for handling Normal table case/Aggregate node added in case of streaming table
        if (!isStreamingTable) {
          // In case of average aggregate function select 2 columns from aggregate table
          // with aggregation sum and count.
          // Then add divide(sum(column with sum), sum(column with count)).
          Seq(Divide(AggregateExpression(Sum(Cast(
            attrs.head,
            dataType)),
            aggExp.mode,
            false),
            AggregateExpression(Sum(Cast(
              attrs.last,
              dataType)),
              aggExp.mode,
              false)))
        } else {
          // in case of streaming aggregate table return two aggregate function sum and count
          Seq(AggregateExpression(Sum(Cast(
            attrs.head,
            dataType)),
            aggExp.mode,
            false),
            AggregateExpression(Sum(Cast(
              attrs.last,
              dataType)),
              aggExp.mode,
              false))
        }
    }
  }
  /**
   * Method to get the carbon table and table name
   * @param parentLogicalRelation parent table relation
   * @return tuple of carbon table
   */
  def getCarbonTable(parentLogicalRelation: LogicalRelation): CarbonTable = {
    val carbonTable = parentLogicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
      .carbonRelation
      .metaData.carbonTable
    carbonTable
  }

  /**
   * Below method will be used to get the query columns from plan
   * @param groupByExpression group by expression
   * @param aggregateExpressions aggregate expression
   * @param carbonTable parent carbon table
   * @param queryColumns list of attributes
   * @return plan is valid
   */
  def extractQueryColumnsFromAggExpression(groupByExpression: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      carbonTable: CarbonTable,
      queryColumns: scala.collection.mutable.HashSet[QueryColumn],
      aggreagteExps: scala.collection.mutable.HashSet[AggregateExpression]): Boolean = {
    var isValid = true
    groupByExpression foreach  { expression =>
      extractColumnFromExpression(expression, queryColumns, carbonTable)
    }
    aggregateExpressions.map {
      case attr: AttributeReference =>
        queryColumns += getQueryColumn(attr.name,
          carbonTable)
      case Alias(attr: AttributeReference, _) =>
        queryColumns += getQueryColumn(attr.name,
          carbonTable);
      case Alias(attr: AggregateExpression, _) =>
        if (attr.isDistinct) {
          isValid = false
        }
        val aggExp = PreAggregateUtil.validateAggregateFunctionAndGetFields(attr)
        if (aggExp.nonEmpty) {
          aggreagteExps ++= aggExp
        } else {
          isValid = false
        }
      case Alias(expression: Expression, _) =>
        if (expression.isInstanceOf[ScalaUDF] &&
            expression.asInstanceOf[ScalaUDF].function.getClass.getName.equalsIgnoreCase(
              "org.apache.spark.sql.execution.command.timeseries.TimeseriesFunction") &&
            CarbonUtil.hasTimeSeriesDataMap(carbonTable)) {
          queryColumns += getQueryColumn(expression.asInstanceOf[ScalaUDF].children(0)
            .asInstanceOf[AttributeReference].name,
            carbonTable,
            timeseriesFunction = expression.asInstanceOf[ScalaUDF].children(1).asInstanceOf[Literal]
              .value.toString)
        } else {
          expression.transform {
            case attr: AttributeReference =>
              queryColumns += getQueryColumn(attr.name,
                carbonTable)
              attr
            case attr: AggregateExpression =>
              if (attr.isDistinct) {
                isValid = false
              }
              val aggExp = PreAggregateUtil.validateAggregateFunctionAndGetFields(attr)
              if (aggExp.nonEmpty) {
                aggreagteExps ++= aggExp
              } else {
                isValid = false
              }
              attr

          }
        }
    }
    isValid
  }

  /**
   * Below method will be used to get the query column object which
   * will have details of the column and its property
   *
   * @param columnName parent column name
   * @param carbonTable parent carbon table
   * @param isFilterColumn is filter is applied on column
   * @return query column
   */
  def getQueryColumn(columnName: String,
      carbonTable: CarbonTable,
      isFilterColumn: Boolean = false,
      timeseriesFunction: String = ""): QueryColumn = {
    val columnSchema = carbonTable.getColumnByName(carbonTable.getTableName, columnName.toLowerCase)
    if(null == columnSchema) {
      null
    } else {
        new QueryColumn(
          columnSchema.getColumnSchema,
        isFilterColumn,
        timeseriesFunction.toLowerCase)
    }
  }
}

/**
 * Data loading rule class to validate and update the data loading query plan
 * Validation rule:
 * 1. update the avg aggregate expression with two columns sum and count
 * 2. Remove duplicate sum and count expression if already there in plan
 * @param sparkSession spark session
 */
case class CarbonPreAggregateDataLoadingRules(sparkSession: SparkSession)
  extends Rule[LogicalPlan] {
  lazy val parser = new CarbonSpark2SqlParser
  override def apply(plan: LogicalPlan): LogicalPlan = {
    val validExpressionsMap = scala.collection.mutable.HashSet.empty[AggExpToColumnMappingModel]
    val namedExpressionList = scala.collection.mutable.LinkedHashSet.empty[NamedExpression]
    plan transform {
      case aggregate@Aggregate(groupingExpressions,
      aExp,
      CarbonSubqueryAlias(_, logicalRelation: LogicalRelation))
        if validateAggregateExpressions(aExp) &&
           logicalRelation.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
        aExp.foreach {
          case attr: AttributeReference =>
            namedExpressionList += attr
          case alias@Alias(_: AttributeReference, _) =>
            namedExpressionList += alias
          case alias@Alias(aggExp: AggregateExpression, name) =>
            // get the updated expression for avg convert it to two expression
            // sum and count
            val expressions = PreAggregateUtil.validateAggregateFunctionAndGetFields(aggExp)
            // if size is more than one then it was for average
            if(expressions.size > 1) {
              val sumExp = PreAggregateUtil.normalizeExprId(
                expressions.head,
                aggregate.allAttributes)
              // get the logical plan fro count expression
              val countExp = PreAggregateUtil.normalizeExprId(
                expressions.last,
                aggregate.allAttributes)
              // check with same expression already sum is present then do not add to
              // named expression list otherwise update the list and add it to set
              if (!validExpressionsMap.contains(AggExpToColumnMappingModel(sumExp))) {
                namedExpressionList +=
                CarbonToSparkAdapter.createAliasRef(expressions.head,
                  name + "_ sum",
                  NamedExpression.newExprId,
                  alias.qualifier,
                  Some(alias.metadata),
                  Some(alias))
                validExpressionsMap += AggExpToColumnMappingModel(sumExp)
              }
              // check with same expression already count is present then do not add to
              // named expression list otherwise update the list and add it to set
              if (!validExpressionsMap.contains(AggExpToColumnMappingModel(countExp))) {
                namedExpressionList +=
                CarbonToSparkAdapter.createAliasRef(expressions.last, name + "_ count",
                    NamedExpression.newExprId,
                    alias.qualifier,
                    Some(alias.metadata),
                    Some(alias))
                validExpressionsMap += AggExpToColumnMappingModel(countExp)
              }
            } else {
              // get the logical plan for expression
              val exp = PreAggregateUtil.normalizeExprId(
                expressions.head,
                aggregate.allAttributes)
              // check with same expression already  present then do not add to
              // named expression list otherwise update the list and add it to set
              if (!validExpressionsMap.contains(AggExpToColumnMappingModel(exp))) {
                namedExpressionList+=alias
                validExpressionsMap += AggExpToColumnMappingModel(exp)
              }
            }
          case alias@Alias(_: Expression, _) =>
            namedExpressionList += alias
        }
        groupingExpressions foreach {
          case namedExpr: NamedExpression => namedExpressionList += namedExpr
          case _ => namedExpressionList
        }
        aggregate.copy(aggregateExpressions = namedExpressionList.toSeq)
      case plan: LogicalPlan => plan
    }
  }

  /**
   * Called by PreAggregateLoadingRules to validate if plan is valid for applying rules or not.
   * If the plan has PreAggLoad i.e Loading UDF and does not have PreAgg i.e Query UDF then it is
   * valid.
   * @param namedExpression named expressions
   * @return valid or not
   */
  private def validateAggregateExpressions(namedExpression: Seq[NamedExpression]): Boolean = {
    val filteredExpressions = namedExpression.filterNot(_.isInstanceOf[UnresolvedAlias])
    filteredExpressions.exists { expr =>
      !expr.name.equalsIgnoreCase("PreAgg") && expr.name.equalsIgnoreCase("preAggLoad")
    }
  }
}
