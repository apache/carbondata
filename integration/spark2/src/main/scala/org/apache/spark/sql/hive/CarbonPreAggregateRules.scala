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

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.{AggregationDataMapSchema, CarbonTable, DataMapSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.preagg.{AggregateQueryPlan, AggregateTableSelector, QueryColumn}
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
 * Rules for Upadating the plan
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
        isValidPlan = !aExp.exists(p => p.name.equals("preAggLoad") || p.name.equals("preAgg"))
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
   * Below method will be used to update the child plan
   * This will be used for updating expression like join condition,
   * order by, project list etc
   * @param plan child plan
   * @return updated plan
   */
  private def updatePlan(plan: LogicalPlan) : LogicalPlan = {
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
  private def updateSortExpression(sortExp : Seq[SortOrder]) : Seq[SortOrder] = {
     sortExp map { order =>
      SortOrder(order.child transform  {
        case attr: AttributeReference =>
          updatedExpression.find { p => p._1.sameRef(attr) } match {
            case Some((_, childAttr)) =>
              AttributeReference(
                childAttr.name,
                childAttr.dataType,
                childAttr.nullable,
                childAttr.metadata)(childAttr.exprId, attr.qualifier, attr.isGenerated)
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
  private def updateExpression(expressions : Seq[Expression]) : Seq[Expression] = {
    expressions map { expression =>
      expression transform {
        case attr: AttributeReference =>
          updatedExpression.find { p => p._1.sameRef(attr) } match {
            case Some((_, childAttr)) =>
              AttributeReference(
                childAttr.name,
                childAttr.dataType,
                childAttr.nullable,
                childAttr.metadata)(childAttr.exprId, attr.qualifier, attr.isGenerated)
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
   *
   * @param logicalPlan parent logical plan
   * @return transformed plan
   */
  private def transformPreAggQueryPlan(logicalPlan: LogicalPlan): LogicalPlan = {
    val updatedPlan = logicalPlan.transform {
      case agg@Aggregate(
        grExp,
        aggExp,
        CarbonSubqueryAlias(alias1, child@CarbonSubqueryAlias(alias2, l: LogicalRelation)))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] &&
           l.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonRelation.
             metaData.hasAggregateDataMapSchema =>
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
              Aggregate(updatedGroupExp,
                updatedAggExp,
                CarbonReflectionUtils
                  .getSubqueryAlias(sparkSession,
                    Some(alias1),
                    CarbonReflectionUtils
                      .getSubqueryAlias(sparkSession, Some(alias2), newChild, None),
                    None))
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
             metaData.hasAggregateDataMapSchema =>
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
              Aggregate(updatedGroupExp,
                updatedAggExp,
                CarbonReflectionUtils
                  .getSubqueryAlias(sparkSession, Some(alias), newChild, None))
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
             metaData.hasAggregateDataMapSchema =>
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
              Aggregate(updatedGroupExp,
                updatedAggExp,
                Filter(updatedFilterExpression.get,
                  CarbonReflectionUtils
                    .getSubqueryAlias(sparkSession, Some(alias), newChild, None)))
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
             metaData.hasAggregateDataMapSchema =>
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
              Aggregate(updatedGroupExp,
                updatedAggExp,
                Filter(updatedFilterExpression.get,
                  CarbonReflectionUtils
                    .getSubqueryAlias(sparkSession,
                      Some(alias1),
                      CarbonReflectionUtils
                        .getSubqueryAlias(sparkSession, Some(alias2), newChild, None),
                      None)))
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
    updatedPlan
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
  private def getChildDataMapForTransformation(
      queryColumns: scala.collection.mutable.HashSet[QueryColumn],
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
      // that are nonEmpty
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
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
        // If the relationBuffer is nonEmpty then find the table with the minimum size.
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
  private def validateAggregateExpression(selectedDataMap: DataMapSchema,
      carbonTable: CarbonTable,
      parentLogicalPlan: LogicalPlan,
      queryAggExpLogicalPlans: Seq[AggregateExpression]): Boolean = {
    val mappingModel = getExpressionToColumnMapping(selectedDataMap,
      carbonTable,
      parentLogicalPlan)
    queryAggExpLogicalPlans.forall{p =>
      mappingModel.exists{m =>
        PreAggregateUtil.normalizeExprId(p, parentLogicalPlan.allAttributes) == m.expression}
    }
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
  private def getExpressionToColumnMapping(selectedDataMap: DataMapSchema,
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
  private def getAggregateExpFromChildDataMap(
      logicalPlan: LogicalPlan): Seq[AggregateExpression] = {
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
  private def isSpecificSegmentNotPresent(carbonTable: CarbonTable) : Boolean = {
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
  private def extractColumnFromExpression(expression: Expression,
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
  private def getChildAttributeReference(dataMapSchema: DataMapSchema,
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
  private def getUpdatedExpressions(groupingExpressions: Seq[Expression],
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

    val updatedAggExp = aggregateExpressions.map {
      // case for attribute reference
      case attr: AttributeReference =>
        val childAttr = getChildAttributeReference(aggDataMapSchema,
          attr,
          attributes)
        val newExpressionId = NamedExpression.newExprId
        val childTableAttr = AttributeReference(attr.name,
          childAttr.dataType,
          childAttr.nullable,
          childAttr.metadata)(newExpressionId, childAttr.qualifier, attr.isGenerated)
        updatedExpression.put(attr, childTableAttr)
        // returning the alias to show proper column name in output
        Alias(childAttr,
          attr.name)(newExpressionId,
          childAttr.qualifier).asInstanceOf[NamedExpression]
      // case for alias
      case alias@Alias(attr: AttributeReference, name) =>
        val childAttr = getChildAttributeReference(aggDataMapSchema,
          attr,
          attributes)
        val newExpressionId = NamedExpression.newExprId
        val parentTableAttr = AttributeReference(name,
          alias.dataType,
          alias.nullable) (alias.exprId, alias.qualifier, alias.isGenerated)
        val childTableAttr = AttributeReference(name,
          alias.dataType,
          alias.nullable) (newExpressionId, alias.qualifier, alias.isGenerated)
        updatedExpression.put(parentTableAttr, childTableAttr)
        // returning alias with child attribute reference
        Alias(childAttr,
          name)(newExpressionId,
          childAttr.qualifier).asInstanceOf[NamedExpression]
      // for aggregate function case
      case alias@Alias(attr: AggregateExpression, name) =>
        // get the updated aggregate aggregate function
        val aggExp = if (aggExpColumnMapping.isDefined) {
          getUpdatedAggregateExpressionForChild(attr,
            aggDataMapSchema,
            attributes,
            parentTable,
            parentLogicalPlan,
            aggExpColumnMapping.get)
        } else {
          attr
        }
        val newExpressionId = NamedExpression.newExprId
        val parentTableAttr = AttributeReference(name,
          alias.dataType,
          alias.nullable) (alias.exprId, alias.qualifier, alias.isGenerated)
        val childTableAttr = AttributeReference(name,
          alias.dataType,
          alias.nullable) (newExpressionId, alias.qualifier, alias.isGenerated)
        updatedExpression.put(parentTableAttr, childTableAttr)
        // returning alias with child attribute reference
        Alias(aggExp,
          name)(newExpressionId,
          alias.qualifier).asInstanceOf[NamedExpression]
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
        val parentTableAttr = AttributeReference(name,
          alias.dataType,
          alias.nullable) (alias.exprId, alias.qualifier, alias.isGenerated)
        val childTableAttr = AttributeReference(name,
          alias.dataType,
          alias.nullable) (newExpressionId, alias.qualifier, alias.isGenerated)
        updatedExpression.put(parentTableAttr, childTableAttr)
        Alias(updatedExp, name)(newExpressionId,
          alias.qualifier).asInstanceOf[NamedExpression]
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
  private def getUpdatedAggregateExpressionForChild(aggExp: AggregateExpression,
      dataMapSchema: AggregationDataMapSchema,
      attributes: Seq[AttributeReference],
      parentTable: CarbonTable,
      parentLogicalPlan: LogicalPlan,
      aggExpColumnMapping: mutable.LinkedHashSet[AggExpToColumnMappingModel]):
  Expression = {
    // get the updated aggregate expression, in case of average column
    // it will be divided in two aggergate expression
    val updatedAggExp = PreAggregateUtil.validateAggregateFunctionAndGetFields(aggExp)
    // get the attributes to be updated for child table
    val attrs = aggExpColumnMapping.collect {
      case (schemaAggExpModel)
        if updatedAggExp
          .exists(p =>
            schemaAggExpModel.expression ==
            PreAggregateUtil.normalizeExprId(p, parentLogicalPlan.allAttributes)) =>
        attributes filter (_.name.equalsIgnoreCase(
          schemaAggExpModel.columnSchema.get.asInstanceOf[ColumnSchema].getColumnName))
    }.flatten

    aggExp.aggregateFunction match {
      case Sum(MatchCastExpression(_, changeDataType: DataType)) =>
        AggregateExpression(Sum(Cast(attrs.head, changeDataType)), aggExp.mode, isDistinct = false)
      case Sum(_) =>
        AggregateExpression(Sum(attrs.head), aggExp.mode, isDistinct = false)
      case Max(MatchCastExpression(_, changeDataType: DataType)) =>
        AggregateExpression(Max(Cast(attrs.head, changeDataType)), aggExp.mode, isDistinct = false)
      case Max(_) =>
        AggregateExpression(Max(attrs.head), aggExp.mode, isDistinct = false)
      case Min(MatchCastExpression(_, changeDataType: DataType)) =>
        AggregateExpression(Min(Cast(attrs.head, changeDataType)), aggExp.mode, isDistinct = false)
      case Min(_) =>
        AggregateExpression(Min(attrs.head), aggExp.mode, isDistinct = false)
      // Change the count AggregateExpression to Sum as count
      // is already calculated so in case of aggregate table
      // we need to apply sum to get the count
      case Count(Seq(expression: Expression)) =>
        AggregateExpression(Sum(Cast(attrs.head, LongType)), aggExp.mode, isDistinct = false)
      // In case of average aggregate function select 2 columns from aggregate table
      // with aggregation sum and count.
      // Then add divide(sum(column with sum), sum(column with count)).
      case Average(MatchCastExpression(exp: Expression, changeDataType: DataType)) =>
        Divide(AggregateExpression(Sum(Cast(
          attrs.head,
          DoubleType)),
          aggExp.mode,
          isDistinct = false),
          AggregateExpression(Sum(Cast(
            attrs.last,
            DoubleType)),
            aggExp.mode,
            isDistinct = false))
      // In case of average aggregate function select 2 columns from aggregate table
      // with aggregation sum and count.
      // Then add divide(sum(column with sum), sum(column with count)).
      case Average(exp: Expression) =>
        Divide(AggregateExpression(Sum(attrs.head),
          aggExp.mode,
          isDistinct = false),
          AggregateExpression(Sum(attrs.last),
            aggExp.mode,
            isDistinct = false))
    }
  }
  /**
   * Method to get the carbon table and table name
   * @param parentLogicalRelation parent table relation
   * @return tuple of carbon table
   */
  private def getCarbonTable(parentLogicalRelation: LogicalRelation): CarbonTable = {
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
  private def extractQueryColumnsFromAggExpression(groupByExpression: Seq[Expression],
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
  private def getQueryColumn(columnName: String,
      carbonTable: CarbonTable,
      isFilterColumn: Boolean = false,
      timeseriesFunction: String = ""): QueryColumn = {
    val columnSchema = carbonTable.getColumnByName(carbonTable.getTableName, columnName.toLowerCase)
    if(null == columnSchema) {
      null
    } else {
        new QueryColumn(columnSchema.getColumnSchema,
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
                  Alias(expressions.head, name + "_ sum")(NamedExpression.newExprId,
                    alias.qualifier,
                    Some(alias.metadata),
                    alias.isGenerated)
                  validExpressionsMap += AggExpToColumnMappingModel(sumExp)
                }
                // check with same expression already count is present then do not add to
                // named expression list otherwise update the list and add it to set
                if (!validExpressionsMap.contains(AggExpToColumnMappingModel(countExp))) {
                  namedExpressionList +=
                  Alias(expressions.last, name + "_ count")(NamedExpression.newExprId,
                    alias.qualifier,
                    Some(alias.metadata),
                    alias.isGenerated)
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
