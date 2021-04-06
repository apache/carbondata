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

package org.apache.spark.sql.optimizer

import java.util
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.{break, breakable}

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonToSparkAdapter, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Expression, Literal, NamedExpression, ScalaUDF, SortOrder}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, TreeNode}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{DataType, DataTypes}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.preagg.TimeSeriesFunctionEnum
import org.apache.carbondata.mv.expressions.modular.{ModularSubquery, ScalarModularSubquery}
import org.apache.carbondata.mv.plans.modular.{ExpressionHelper, GroupBy, HarmonizedRelation, LeafNode, Matchable, ModularPlan, ModularRelation, Select, SimpleModularizer}
import org.apache.carbondata.mv.plans.util.BirdcageOptimizer
import org.apache.carbondata.view.{MVCatalogInSpark, MVPlanWrapper, MVTimeGranularity, TimeSeriesFunction}

/**
 * The primary workflow for rewriting relational queries using Spark libraries.
 * Designed to allow easy access to the intermediate phases of query rewrite for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class MVRewrite(catalog: MVCatalogInSpark, logicalPlan: LogicalPlan,
    session: SparkSession) {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  private def getAliasName(expression: NamedExpression): String = {
    expression match {
      case Alias(_, name) => name
      case reference: AttributeReference => reference.name
    }
  }

  private def getAliasMap(
      outputList1: Seq[NamedExpression],
      outputList2: Seq[NamedExpression]): Map[AttributeKey, NamedExpression] = {
    // when mv is created with duplicate columns like select sum(age),sum(age) from table,
    // the outputList2 will have duplicate, so handle that case here
    if (outputList1.length == outputList2.groupBy(_.name).size) {
      outputList2.zip(outputList1).flatMap {
        case (output1, output2) =>
          var aliasMapping = output1 collect {
            case reference: AttributeReference =>
              (AttributeKey(reference), Alias(output2, reference.name)(exprId = output2.exprId))
          }
          output1 match {
            case alias: Alias =>
              aliasMapping = Seq((AttributeKey(alias.child),
                Alias(output2, alias.name)(exprId = output2.exprId))) ++ aliasMapping
            case _ =>
          }
          Seq((AttributeKey(output1),
            Alias(output2, output1.name)(exprId = output2.exprId))) ++ aliasMapping
      }.toMap
    } else {
      throw new UnsupportedOperationException("Cannot create mapping with unequal sizes")
    }
  }

  private def getUpdatedOutputList(outputList: Seq[NamedExpression],
      modularPlan: Option[ModularPlan]): Seq[NamedExpression] = {
    modularPlan.collect {
      case planWrapper: MVPlanWrapper =>
        val viewSchema = planWrapper.viewSchema
        val viewColumnsOrderMap = viewSchema.getColumnsOrderMap
        if (null != viewColumnsOrderMap && !viewColumnsOrderMap.isEmpty) {
          val updatedOutputList = new util.ArrayList[NamedExpression]()
          var i = 0
          while (i < viewColumnsOrderMap.size()) {
            val updatedOutput = outputList.filter(
              output => output.name.equalsIgnoreCase(viewColumnsOrderMap.get(i))
            ).head
            updatedOutputList.add(updatedOutput)
            i = i + 1
          }
          updatedOutputList.asScala
        } else {
          outputList
        }
      case _ => outputList
    }.get
  }

  /**
   * Check if modular plan can be rolled up by rewriting and matching the modular plan
   * with existing mv datasets.
   * @param modularPlan to check if can be rolled up
   * @return new modular plan
   */
  private def getRolledUpModularPlan(modularPlan: ModularPlan): Option[ModularPlan] = {
    // check if modular plan contains timeseries udf
    val timeSeriesFunction = modularPlan match {
      case select: Select =>
        getTimeSeriesFunction(select.outputList)
      case groupBy: GroupBy =>
        getTimeSeriesFunction(groupBy.outputList)
      case _ => (null, null)
    }
    // set canDoRollUp to false, in case of Join queries and if filter has timeseries udf function
    // TODO: support rollUp for join queries
    var canRollUp = true
    optimizedPlan.transformDown {
      case join: Join =>
        canRollUp = false
        join
      case filter@Filter(condition: Expression, _) =>
        condition.collect {
          case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
            canRollUp = false
        }
        filter
    }
    if (null != timeSeriesFunction._2 && canRollUp) {
      // check for rollup and rewrite the plan
      // collect all the datasets which contains timeseries MV's
      val viewSchemaWrappers = catalog.lookupFeasibleSchemas(modularPlan)
      var granularityList: java.util.List[TimeSeriesFunctionEnum] =
        new java.util.ArrayList[TimeSeriesFunctionEnum]()
      // Get all the lower granularities for the query from datasets
      viewSchemaWrappers.foreach { viewSchemaWrapper =>
        if (viewSchemaWrapper.viewSchema.isTimeSeries) {
          viewSchemaWrapper.logicalPlan.transformExpressions {
            case alias@Alias(function: ScalaUDF, _) =>
              if (function.function.isInstanceOf[TimeSeriesFunction]) {
                val granularity =
                  function.children.last.asInstanceOf[Literal].toString().toUpperCase()
                if (MVTimeGranularity.get(timeSeriesFunction._2.toUpperCase).seconds >
                  MVTimeGranularity.get(granularity).seconds) {
                  granularityList.add(TimeSeriesFunctionEnum.valueOf(granularity))
                }
              }
              alias
          }
        }
      }
      if (!granularityList.isEmpty) {
        granularityList = granularityList.asScala.sortBy(_.getOrdinal)(Ordering[Int].reverse).asJava
        var originalTable: String = null
        // get query Table
        modularPlan.collect {
          case relation: ModularRelation =>
            originalTable = relation.tableName
        }
        var queryGranularity: String = null
        var rewrittenPlan = modularPlan
        // replace granularities in the plan and check if plan can be changed
        breakable {
          granularityList.asScala.foreach { func =>
            rewrittenPlan = rewriteGranularityInPlan(modularPlan, func.getName)
            rewrittenPlans.add(rewrittenPlan)
            val logicalPlan = session.sql(rewrittenPlan.asCompactSQL).queryExecution.optimizedPlan
            rewrittenPlans.clear()
            var rolledUpTable: String = null
            logicalPlan.collect {
              case relation: LogicalRelation =>
                rolledUpTable = relation.catalogTable.get.identifier.table
              case relation: HiveTableRelation =>
                rolledUpTable = relation.tableMeta.identifier.table
            }
            if (!rolledUpTable.equalsIgnoreCase(originalTable)) {
              queryGranularity = func.getName
              break()
            }
          }
        }
        if (null != queryGranularity) {
          // rewrite the plan and set it as rolled up plan
          val rewrittenPlans = rewriteWithSchemaWrapper0(rewrittenPlan)
          if (rewrittenPlans.isDefined) {
            rewrittenPlans.get.map(_.setRolledUp())
            rewrittenPlans
          } else {
            None
          }
        } else {
          None
        }
      } else {
        None
      }
    } else {
      None
    }
  }

  private def getTimeSeriesFunction(outputList: scala.Seq[NamedExpression]): (String, String) = {
    outputList.collect {
      case Alias(function: ScalaUDF, name) =>
        if (function.function.isInstanceOf[TimeSeriesFunction]) {
          function.children.collect {
            case literal: Literal =>
              return (name, literal.value.toString)
          }
        }
    }
    (null, null)
  }

  /**
   * Rewrite the updated mv query with corresponding MV table.
   */
  private def rewrite(modularPlan: ModularPlan): ModularPlan = {
    if (modularPlan.find(_.rewritten).isDefined) {
      LOGGER.debug(s"Getting updated plan for the rewritten modular plan: " +
                   s"{ ${ modularPlan.toString().trim } }")
      var updatedPlan = modularPlan transform {
        case select: Select =>
          updatePlan(select)
        case groupBy: GroupBy =>
          updatePlan(groupBy)
      }
      if (modularPlan.isRolledUp) {
        // If the rewritten query is rolled up, then rewrite the query based on the original modular
        // plan. Make a new outputList based on original modular plan and wrap rewritten plan with
        // select & group-by nodes with new outputList.

        // For example:
        // Given User query:
        // SELECT timeseries(col,'day') from maintable group by timeseries(col,'day')
        // If plan is rewritten as per 'hour' granularity of mv1,
        // then rewritten query will be like,
        // SELECT mv1.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries
        // (projectjoindate, hour)`
        // FROM
        // default.mv1
        // GROUP BY mv1.`UDF:timeseries_projectjoindate_hour`
        //
        // Now, rewrite the rewritten plan as per the 'day' granularity
        // SELECT timeseries(gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)`,'day' ) AS
        // `UDF:timeseries(projectjoindate, day)`
        //  FROM
        //  (SELECT mv2.`UDF:timeseries_projectjoindate_hour` AS `UDF:timeseries
        //  (projectjoindate, hour)`
        //  FROM
        //    default.mv2
        //  GROUP BY mv2.`UDF:timeseries_projectjoindate_hour`) gen_subsumer_0
        // GROUP BY timeseries(gen_subsumer_0.`UDF:timeseries(projectjoindate, hour)`,'day' )
        harmonizedPlan match {
          case select: Select =>
            val outputList = select.outputList
            val rolledUpOutputList = updatedPlan.asInstanceOf[Select].outputList
            var finalOutputList: Seq[NamedExpression] = Seq.empty
            val mapping = outputList zip rolledUpOutputList
            val newSubsume = subqueryNameGenerator.newSubsumerName()

            mapping.foreach { outputLists =>
              val name: String = getAliasName(outputLists._2)
              outputLists._1 match {
                case a@Alias(scalaUdf: ScalaUDF, aliasName) =>
                  if (scalaUdf.function.isInstanceOf[TimeSeriesFunction]) {
                    val newName = newSubsume + ".`" + name + "`"
                    val transformedUdf = updateTimeSeriesFunction(scalaUdf, newName)
                    finalOutputList = finalOutputList.:+(Alias(transformedUdf, aliasName)(a.exprId,
                      a.qualifier).asInstanceOf[NamedExpression])
                  }
                case Alias(attr: AttributeReference, _) =>
                  finalOutputList = finalOutputList.:+(
                    CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume))
                case attr: AttributeReference =>
                  finalOutputList = finalOutputList.:+(
                    CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume))
              }
            }
            val newChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
            val newAliasMap = new collection.mutable.HashMap[Int, String]()

            val sel_plan = select.copy(outputList = finalOutputList,
              inputList = finalOutputList,
              predicateList = Seq.empty)
            newChildren += sel_plan
            newAliasMap += (newChildren.indexOf(sel_plan) -> newSubsume)
            updatedPlan = select.copy(outputList = finalOutputList,
              inputList = finalOutputList,
              aliasMap = newAliasMap.toMap,
              predicateList = Seq.empty,
              children = Seq(updatedPlan)).setRewritten()

          case groupBy: GroupBy =>
            updatedPlan match {
              case select: Select =>
                val selectOutputList = groupBy.outputList
                val rolledUpOutputList = updatedPlan.asInstanceOf[Select].outputList
                var finalOutputList: Seq[NamedExpression] = Seq.empty
                var predicateList: Seq[Expression] = Seq.empty
                val mapping = selectOutputList zip rolledUpOutputList
                val newSubsume = subqueryNameGenerator.newSubsumerName()

                mapping.foreach { outputLists =>
                  val aliasName: String = getAliasName(outputLists._2)
                  outputLists._1 match {
                    case a@Alias(scalaUdf: ScalaUDF, _) =>
                      if (scalaUdf.function.isInstanceOf[TimeSeriesFunction]) {
                        val newName = newSubsume + ".`" + aliasName + "`"
                        val transformedUdf = updateTimeSeriesFunction(scalaUdf, newName)
                        groupBy.predicateList.foreach {
                          case udf: ScalaUDF if udf.isInstanceOf[ScalaUDF] =>
                            predicateList = predicateList.:+(transformedUdf)
                          case attr: AttributeReference =>
                            predicateList = predicateList.:+(
                              CarbonToSparkAdapter.createAttributeReference(attr,
                                attr.name,
                                newSubsume))
                        }
                        finalOutputList = finalOutputList.:+(Alias(transformedUdf, a.name)(a
                          .exprId, a.qualifier).asInstanceOf[NamedExpression])
                      }
                    case attr: AttributeReference =>
                      finalOutputList = finalOutputList.:+(
                        CarbonToSparkAdapter.createAttributeReference(attr, aliasName, newSubsume))
                    case Alias(attr: AttributeReference, _) =>
                      finalOutputList = finalOutputList.:+(
                        CarbonToSparkAdapter.createAttributeReference(attr, aliasName, newSubsume))
                    case a@Alias(agg: AggregateExpression, name) =>
                      val newAgg = agg.transform {
                        case attr: AttributeReference =>
                          CarbonToSparkAdapter.createAttributeReference(attr, name, newSubsume)
                      }
                      finalOutputList = finalOutputList.:+(Alias(newAgg, name)(a.exprId,
                        a.qualifier).asInstanceOf[NamedExpression])
                    case other => other
                  }
                }
                val newChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
                val newAliasMap = new collection.mutable.HashMap[Int, String]()

                val sel_plan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  predicateList = Seq.empty)
                newChildren += sel_plan
                newAliasMap += (newChildren.indexOf(sel_plan) -> newSubsume)
                updatedPlan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  aliasMap = newAliasMap.toMap,
                  children = Seq(updatedPlan)).setRewritten()
                updatedPlan = groupBy.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  predicateList = predicateList,
                  alias = Some(newAliasMap.mkString),
                  child = updatedPlan).setRewritten()
                updatedPlan = select.copy(outputList = finalOutputList,
                  inputList = finalOutputList,
                  children = Seq(updatedPlan)).setRewritten()
            }
        }
      }
      updatedPlan
    } else {
      modularPlan
    }
  }

  private def rewriteWithSchemaWrapper(modularPlan: ModularPlan): ModularPlan = {
    val rewrittenPlan = modularPlan.transformAllExpressions {
      case subquery: ModularSubquery =>
        if (subquery.children.isEmpty) {
          rewriteWithSchemaWrapper0(subquery.plan) match {
            case Some(rewrittenPlan) =>
              ScalarModularSubquery(rewrittenPlan, subquery.children, subquery.exprId)
            case None =>
              subquery
          }
        }
        else {
          throw new UnsupportedOperationException(s"Rewrite expression $subquery isn't supported")
        }
      case other => other
    }
    rewriteWithSchemaWrapper0(rewrittenPlan).getOrElse(rewrittenPlan)
  }

  private def rewriteWithSchemaWrapper0(modularPlan: ModularPlan): Option[ModularPlan] = {
    val rewrittenPlan = modularPlan transformDown {
      case plan =>
        if (plan.rewritten || !plan.isSPJGH) {
          plan
        } else {
          LOGGER.info("Query matching has been initiated with available mv schema's")
          val rewrittenPlans =
            for {schemaWrapper <- catalog.lookupFeasibleSchemas(plan).toStream
                 subsumer <- SimpleModularizer.modularize(
                   BirdcageOptimizer.execute(schemaWrapper.logicalPlan)).map(_.semiHarmonized)
                 subsumee <- unifySubsumee(plan)
                 rewrittenPlan <- rewriteWithSchemaWrapper0(
                   unifySubsumer2(
                     unifySubsumer1(
                       subsumer,
                       subsumee,
                       schemaWrapper.modularPlan),
                     subsumee),
                   subsumee)} yield {
              rewrittenPlan
            }
          rewrittenPlans.headOption.map(_.setRewritten()).getOrElse(plan)
        }
    }
    if (rewrittenPlan.fastEquals(modularPlan)) {
      if (rewrittenPlans.asScala.exists(plan => plan.sameResult(rewrittenPlan))) {
        return None
      }
      getRolledUpModularPlan(rewrittenPlan)
    } else {
      Some(rewrittenPlan)
    }
  }

  private def rewriteWithSchemaWrapper0(
      subsumer: ModularPlan,
      subsumee: ModularPlan): Option[ModularPlan] = {
    if (subsumer.getClass == subsumee.getClass) {
      (subsumer.children, subsumee.children) match {
        case (Nil, Nil) => None
        case (r, e) if r.forall(_.isInstanceOf[LeafNode]) && e.forall(_.isInstanceOf[LeafNode]) =>
          val matchesPlans =
            MVMatchMaker.execute(subsumer, subsumee, None, subqueryNameGenerator)
          if (matchesPlans.hasNext) {
            Some(matchesPlans.next)
          } else {
            None
          }
        case (subsumerChild :: Nil, subsumeeChild :: Nil) =>
          val compensation = rewriteWithSchemaWrapper0(subsumerChild, subsumeeChild)
          val matchesPlans = compensation.map {
            case comp if comp.eq(subsumerChild) =>
              MVMatchMaker.execute(subsumer, subsumee, None, subqueryNameGenerator)
            case _ =>
              MVMatchMaker.execute(subsumer, subsumee, compensation,
                subqueryNameGenerator)
          }
          if (matchesPlans.isDefined && matchesPlans.get.hasNext) {
            Some(matchesPlans.get.next)
          } else {
            None
          }
        case _ => None
      }
    } else {
      None
    }
  }

  /**
   * Replace the identified immediate lower level granularity in the modular plan
   * to perform rollup
   *
   * @param plan             to be re-written
   * @param queryGranularity to be replaced
   * @return plan with granularity changed
   */
  private def rewriteGranularityInPlan(plan: ModularPlan, queryGranularity: String) = {
    val newPlan = plan.transformDown {
      case p => p.transformAllExpressions {
        case udf: ScalaUDF =>
          if (udf.function.isInstanceOf[TimeSeriesFunction]) {
            val transformedUdf = udf.transformDown {
              case _: Literal =>
                new Literal(UTF8String.fromString(queryGranularity.toLowerCase),
                  DataTypes.StringType)
            }
            transformedUdf
          } else {
            udf
          }
        case alias@Alias(udf: ScalaUDF, name) =>
          if (udf.function.isInstanceOf[TimeSeriesFunction]) {
            var literal: String = null
            val transformedUdf = udf.transformDown {
              case l: Literal =>
                literal = l.value.toString
                new Literal(UTF8String.fromString(queryGranularity.toLowerCase),
                  DataTypes.StringType)
            }
            Alias(transformedUdf,
              name.replace(", " + literal, ", " + queryGranularity))(alias.exprId,
              alias.qualifier).asInstanceOf[NamedExpression]
          } else {
            alias
          }
      }
    }
    newPlan
  }

  // match the join table of subsumer and subsumee
  // when the table names are the same
  private def isJoinMatched(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      subsumerTable: LeafNode,
      subsumeeTable: LeafNode,
      subsumerIndex: Int,
      subsumeeIndex: Int): Boolean = {
    (subsumerTable, subsumeeTable) match {
      case _: (ModularRelation, ModularRelation) =>
        val subsumerTableParent = subsumer.find(plan => plan.children.contains(subsumerTable)).get
        val subsumeeTableParent = subsumee.find(plan => plan.children.contains(subsumeeTable)).get
        (subsumerTableParent, subsumeeTableParent) match {
          case  (subsumerSelect: Select, subsumeeSelect: Select) =>
            val intersectJoinEdges = subsumeeSelect.joinEdges intersect subsumerSelect.joinEdges
            if (intersectJoinEdges.nonEmpty) {
              return intersectJoinEdges.exists(
                join =>
                  join.left == subsumerIndex &&
                  join.left == subsumeeIndex || join.right == subsumerIndex &&
                                                join.right == subsumeeIndex
              )
            }
          case _ => return false
        }
    }
    true
  }

  // add Select operator as placeholder on top of subsumee to facilitate matching
  private def unifySubsumee(subsumee: ModularPlan): Option[ModularPlan] = {
    subsumee match {
      case groupBy@GroupBy(_, _, _, _, Select(_, _, _, _, _, _, _, _, _, _), _, _, _) =>
        Some(
          Select(
            groupBy.outputList,
            groupBy.outputList,
            Nil,
            Map.empty,
            Nil,
            groupBy :: Nil,
            groupBy.flags,
            groupBy.flagSpec,
            Seq.empty)
        )
      case other => Some(other)
    }
  }

  // add Select operator as placeholder on top of subsumer to facilitate matching
  private def unifySubsumer1(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      modularPlan: ModularPlan): ModularPlan = {
    // Update MV table relation to the subsumer modular plan
    val updatedSubsumer = subsumer match {
      // In case of order by it adds extra select but that can be ignored while doing selection.
      case select@Select(_, _, _, _, _, Seq(g: GroupBy), _, _, _, _) =>
        select.copy(
          children = Seq(g.copy(modularPlan = Some(modularPlan))),
          outputList = updateDuplicateColumns(select.outputList))
      case select: Select =>
        select.copy(
          modularPlan = Some(modularPlan),
          outputList = updateDuplicateColumns(select.outputList))
      case groupBy: GroupBy =>
        groupBy.copy(
          modularPlan = Some(modularPlan),
          outputList = updateDuplicateColumns(groupBy.outputList))
      case other => other
    }
    (updatedSubsumer, subsumee) match {
      case (
        groupBy@GroupBy(_, _, _, _, Select(_, _, _, _, _, _, _, _, _, _), _, _, _),
        Select(_, _, _, _, _,
          Seq(GroupBy(_, _, _, _, Select(_, _, _, _, _, _, _, _, _, _), _, _, _)), _, _, _, _)
        ) =>
        Select(
          groupBy.outputList,
          groupBy.outputList,
          Nil,
          Map.empty,
          Nil,
          groupBy :: Nil,
          groupBy.flags,
          groupBy.flagSpec,
          Seq.empty).setSkip()
      case _ => updatedSubsumer.setSkip()
    }
  }

  private def unifySubsumer2(subsumer: ModularPlan, subsumee: ModularPlan): ModularPlan = {
    val subsumerTables = subsumer.collect { case node: LeafNode => node }
    val subsumeeTables = subsumee.collect { case node: LeafNode => node }
    val tableMappings = for {
      subsumerTableIndex <- subsumerTables.indices
      subsumeeTableIndex <- subsumeeTables.indices
      if subsumerTables(subsumerTableIndex) == subsumeeTables(subsumeeTableIndex) &&
         isJoinMatched(
           subsumer,
           subsumee,
           subsumerTables(subsumerTableIndex),
           subsumeeTables(subsumeeTableIndex),
           subsumerTableIndex,
           subsumeeTableIndex
      )
    } yield {
      (subsumerTables(subsumerTableIndex), subsumeeTables(subsumeeTableIndex))
    }
    tableMappings.foldLeft(subsumer) {
      case (currentSubsumer, tableMapping) =>
        val mappedOperator =
          tableMapping._1 match {
            case relation: HarmonizedRelation if relation.hasTag =>
              tableMapping._2.asInstanceOf[HarmonizedRelation].addTag
            case _ =>
              tableMapping._2
          }
        val nextSubsumer = currentSubsumer.transform {
          case node: ModularRelation if node.fineEquals(tableMapping._1) => mappedOperator
          case tableMapping._1 if !tableMapping._1.isInstanceOf[ModularRelation] => mappedOperator
        }
        // reverse first due to possible tag for left join
        val rewrites = AttributeMap(tableMapping._1.output.zip(mappedOperator.output))
        nextSubsumer.transformUp {
          case plan => plan.transformExpressions {
            case attribute: Attribute if rewrites contains attribute =>
              rewrites(attribute).withQualifier(attribute.qualifier)
          }
        }
    }
  }

  private def updateTimeSeriesFunction(function: ScalaUDF, newName: String): Expression = {
    function.transformDown {
      case reference: AttributeReference =>
        AttributeReference(newName, reference.dataType)(
          exprId = reference.exprId,
          qualifier = reference.qualifier)
      case literal: Literal =>
        Literal(UTF8String.fromString("'" + literal.toString() + "'"),
          org.apache.spark.sql.types.DataTypes.StringType)
    }
  }

  private def updateDuplicateColumns(outputList: Seq[NamedExpression]): Seq[NamedExpression] = {
    val duplicateColNames = outputList.groupBy(_.name).filter(_._2.length > 1).flatMap(_._2).toList
    val updatedOutputList = outputList.map {
      output =>
        val duplicateColumn = duplicateColNames.find(name => name.semanticEquals(output))
        if (duplicateColumn.isDefined) {
          val attributesOfDuplicateCol = duplicateColumn.get.collect {
            case reference: AttributeReference => reference
          }
          val attributesOfCol = output.collect {
            case reference: AttributeReference => reference
          }
          // here need to check the whether the duplicate columns is of same tables,
          // since query with duplicate columns is valid, we need to make sure, not to change their
          // names with above defined qualifier name, for example in case of some expression like
          // cast((FLOOR((cast(col_name) as double))).., upper layer even exprid will be same,
          // we need to find the attribute ref(col_name) at lower level and check where expid is
          // same or of same tables, so doin the semantic equals
          // from spark 3.1, qualifier will have 3 values, last will be table name
          val qualifiedName = if (output.qualifier.nonEmpty) {
            s"${ output.exprId.id }_${output.qualifier.lastOption }"
          } else {
            s"${ output.exprId.id }_${ output.name }"
          }
          if (!attributesOfDuplicateCol.forall(attribute =>
            attributesOfCol.exists(a => a.semanticEquals(attribute)))) {
            Alias(output, qualifiedName)(exprId = output.exprId)
          } else if (output.qualifier.nonEmpty) {
            Alias(output, qualifiedName)(exprId = output.exprId)
            // this check is added in scenario where the column is direct Attribute reference and
            // since duplicate columns select is allowed, we should just put alias for those columns
            // and update, for this also above isStrictDuplicate will be true so, it will not be
            // updated above
          } else if (duplicateColumn.get.isInstanceOf[AttributeReference] &&
                     output.isInstanceOf[AttributeReference]) {
            Alias(output, qualifiedName)(exprId = output.exprId)
          } else {
            output
          }
        } else {
          output
        }
    }
    updatedOutputList
  }

  /**
   * Update the modular plan as per the mv table relation inside it.
   *
   * @param modularPlan plan to be updated
   * @return Updated modular plan.
   */
  private def updatePlan(modularPlan: ModularPlan): ModularPlan = {
    modularPlan match {
      case select: Select if select.modularPlan.isDefined =>
        val planWrapper = select.modularPlan.get.asInstanceOf[MVPlanWrapper]
        val plan = planWrapper.modularPlan.asInstanceOf[Select]
        val aliasMap = getAliasMap(plan.outputList, select.outputList)
        // Update the flagSpec as per the mv table attributes.
        val updatedFlagSpec = updateFlagSpec(select, plan, aliasMap, keepAlias = false)
        // when the output list contains multiple projection of same column, but relation
        // contains distinct columns, mapping may go wrong with columns, so select distinct
        val updatedPlanOutputList = getUpdatedOutputList(plan.outputList, select.modularPlan)
        val outputList =
        for ((output1, output2) <- select.outputList.distinct zip updatedPlanOutputList) yield {
          if (output1.name != output2.name) {
            Alias(output2, output1.name)(exprId = output1.exprId)
          } else {
            output2
          }
        }
        plan.copy(outputList = outputList, flags = select.flags, flagSpec = updatedFlagSpec)
          .setRewritten()
      case select: Select => select.children match {
        case Seq(groupBy: GroupBy) if groupBy.modularPlan.isDefined =>
          val planWrapper = groupBy.modularPlan.get.asInstanceOf[MVPlanWrapper]
          val plan = planWrapper.modularPlan.asInstanceOf[Select]
          val aliasMap = getAliasMap(plan.outputList, groupBy.outputList)
          // Update the flagSpec as per the mv table attributes.
          val updatedFlagSpec = updateFlagSpec(select, plan, aliasMap, keepAlias = false)
          if (!planWrapper.viewSchema.isRefreshIncremental) {
            val updatedPlanOutputList = getUpdatedOutputList(plan.outputList, groupBy.modularPlan)
            val outputList =
              for ((output1, output2) <- groupBy.outputList zip updatedPlanOutputList) yield {
                if (output1.name != output2.name) {
                  Alias(output2, output1.name)(exprId = output1.exprId)
                } else {
                  output2
                }
              }
            // Directly keep the relation as child.
            select.copy(
              outputList = select.outputList.map {
                output => outputList.find(_.name.equals(output.name)).get
              },
              children = Seq(plan),
              aliasMap = plan.aliasMap,
              flagSpec = updatedFlagSpec).setRewritten()
          } else {
            val child = updatePlan(groupBy).asInstanceOf[Matchable]
            // First find the indices from the child output list.
            val outputIndices = select.outputList.map {
              output =>
                groupBy.outputList.indexWhere {
                  case alias: Alias if output.isInstanceOf[Alias] =>
                    alias.child.semanticEquals(output.asInstanceOf[Alias].child)
                  case alias: Alias if alias.child.semanticEquals(output) =>
                    true
                  case other if output.isInstanceOf[Alias] =>
                    other.semanticEquals(output.asInstanceOf[Alias].child)
                  case other =>
                    other.semanticEquals(output) || other.toAttribute.semanticEquals(output)
                }
            }
            // Get the outList from converted child output list using already selected indices
            val outputList =
              outputIndices.map(child.outputList(_)).zip(select.outputList).map {
                case (output1, output2) =>
                  output1 match {
                    case alias: Alias if output2.isInstanceOf[Alias] =>
                      Alias(alias.child, output2.name)(exprId = output2.exprId)
                    case alias: Alias =>
                      alias
                    case other if output2.isInstanceOf[Alias] =>
                      Alias(other, output2.name)(exprId = output2.exprId)
                    case other =>
                      other
                  }
              }
            // TODO Remove the unnecessary columns from selection.
            // Only keep columns which are required by parent.
            select.copy(
              outputList = outputList,
              inputList = child.outputList,
              flagSpec = updatedFlagSpec,
              children = Seq(child)).setRewritten()
          }
        case _ => select
      }
      case groupBy: GroupBy if groupBy.modularPlan.isDefined =>
        val planWrapper = groupBy.modularPlan.get.asInstanceOf[MVPlanWrapper]
        val plan = planWrapper.modularPlan.asInstanceOf[Select]
        val updatedPlanOutputList = getUpdatedOutputList(plan.outputList, groupBy.modularPlan)
        val outputListMapping = groupBy.outputList zip updatedPlanOutputList
        val (outputList: Seq[NamedExpression], updatedPredicates: Seq[Expression]) =
          getUpdatedOutputAndPredicateList(
          groupBy,
          outputListMapping)
        groupBy.copy(
          outputList = outputList,
          inputList = plan.outputList,
          predicateList = updatedPredicates,
          child = plan,
          modularPlan = None).setRewritten()
      case groupBy: GroupBy if groupBy.predicateList.nonEmpty => groupBy.child match {
        case select: Select if select.modularPlan.isDefined =>
          val planWrapper = select.modularPlan.get.asInstanceOf[MVPlanWrapper]
          val plan = planWrapper.modularPlan.asInstanceOf[Select]
          val updatedPlanOutputList = getUpdatedOutputList(plan.outputList, select.modularPlan)
          val outputListMapping = groupBy.outputList zip updatedPlanOutputList
          val (outputList: Seq[NamedExpression], updatedPredicates: Seq[Expression]) =
            getUpdatedOutputAndPredicateList(
              groupBy,
              outputListMapping)
          groupBy.copy(
            outputList = outputList,
            inputList = plan.outputList,
            predicateList = updatedPredicates,
            child = select,
            modularPlan = None)
        case _ => groupBy
      }
      case other => other
    }
  }

  private def getUpdatedOutputAndPredicateList(groupBy: GroupBy,
      outputListMapping: Seq[(NamedExpression, NamedExpression)]):
  (Seq[NamedExpression], Seq[Expression]) = {
    val outputList = for ((output1, output2) <- outputListMapping) yield {
      output1 match {
        case Alias(aggregateExpression: AggregateExpression, _)
          if aggregateExpression.aggregateFunction.isInstanceOf[Sum] =>
          val aggregate = aggregateExpression.aggregateFunction.asInstanceOf[Sum]
          val uFun = aggregate.copy(child = output2)
          Alias(aggregateExpression.copy(aggregateFunction = uFun),
            output1.name)(exprId = output1.exprId)
        case Alias(aggregateExpression: AggregateExpression, _)
          if aggregateExpression.aggregateFunction.isInstanceOf[Max] =>
          val max = aggregateExpression.aggregateFunction.asInstanceOf[Max]
          val uFun = max.copy(child = output2)
          Alias(aggregateExpression.copy(aggregateFunction = uFun),
            output1.name)(exprId = output1.exprId)
        case Alias(aggregateExpression: AggregateExpression, _)
          if aggregateExpression.aggregateFunction.isInstanceOf[Min] =>
          val min = aggregateExpression.aggregateFunction.asInstanceOf[Min]
          val uFun = min.copy(child = output2)
          Alias(aggregateExpression.copy(aggregateFunction = uFun),
            output1.name)(exprId = output1.exprId)
        case Alias(aggregateExpression: AggregateExpression, _)
          if aggregateExpression.aggregateFunction.isInstanceOf[Count] ||
             aggregateExpression.aggregateFunction.isInstanceOf[Corr] ||
             aggregateExpression.aggregateFunction.isInstanceOf[VariancePop] ||
             aggregateExpression.aggregateFunction.isInstanceOf[VarianceSamp] ||
             aggregateExpression.aggregateFunction.isInstanceOf[StddevSamp] ||
             aggregateExpression.aggregateFunction.isInstanceOf[StddevPop] ||
             aggregateExpression.aggregateFunction.isInstanceOf[CovSample] ||
             aggregateExpression.aggregateFunction.isInstanceOf[Skewness] ||
             aggregateExpression.aggregateFunction.isInstanceOf[Kurtosis] ||
             aggregateExpression.aggregateFunction.isInstanceOf[CovPopulation] =>
          val uFun = Sum(output2)
          Alias(aggregateExpression.copy(aggregateFunction = uFun),
            output1.name)(exprId = output1.exprId)
        case _ =>
          if (output1.name != output2.name) {
            Alias(output2, output1.name)(exprId = output1.exprId)
          } else {
            output2
          }
      }
    }
    val updatedPredicates = groupBy.predicateList.map {
      predicate =>
        outputListMapping.find {
          case (output1, _) =>
            output1 match {
              case alias: Alias if predicate.isInstanceOf[Alias] =>
                alias.child.semanticEquals(predicate.children.head)
              case alias: Alias =>
                alias.child.semanticEquals(predicate)
              case other =>
                other.semanticEquals(predicate)
            }
        } match {
          case Some((_, output2)) => output2
          case _ => predicate
        }
    }
    (outputList, updatedPredicates)
  }

  /**
   * Updates the flagspec of given select plan with attributes of relation select plan
   */
  private def updateFlagSpec(
      select: Select,
      relation: Select,
      aliasMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[Seq[Any]] = {
    val updatedFlagSpec = select.flagSpec.map {
      flagSpec =>
        flagSpec.map {
          case list: ArrayBuffer[_] =>
            list.map {
              case sortOrder: SortOrder =>
                val expressions =
                  updateOutputList(
                    Seq(sortOrder.child.asInstanceOf[Attribute]),
                    relation,
                    aliasMap,
                    keepAlias = false)
                SortOrder(expressions.head, sortOrder.direction)
            }
          // In case of limit it goes to other.
          case other => other
        }
    }
    updatedFlagSpec
  }

  private def updateOutputList(
      outputList: Seq[NamedExpression],
      select: Select,
      aliasMap: Map[AttributeKey, NamedExpression],
      keepAlias: Boolean): Seq[NamedExpression] = {
    val updatedOutputList = updateSubsumeAttributes(
      outputList,
      aliasMap,
      Some(select.aliasMap.values.head),
      keepAlias).asInstanceOf[Seq[NamedExpression]]
    updatedOutputList.zip(outputList).map {
      case (updatedOutput, output) =>
        updatedOutput match {
          case reference: AttributeReference =>
            Alias(reference, output.name)(output.exprId)
          case Alias(reference: AttributeReference, _) =>
            Alias(reference, output.name)(output.exprId)
          case other => other
        }
    }
  }

  /**
   * Updates the expressions as per the subsumer output expressions. It is needed to update the
   * expressions as per the mv table relation
   *
   * @param expressions expressions which are needed to update
   * @param aliasName   table alias name
   * @return Updated expressions
   */
  private def updateSubsumeAttributes(
      expressions: Seq[Expression],
      attributeMap: Map[AttributeKey, NamedExpression],
      aliasName: Option[String],
      keepAlias: Boolean = false): Seq[Expression] = {
    def getAttribute(expression: Expression) = {
      expression match {
        case Alias(aggregate: AggregateExpression, _) =>
          aggregate.aggregateFunction.collect {
            case reference: AttributeReference =>
              ExpressionHelper.createReference(reference.name,
                reference.dataType,
                reference.nullable,
                reference.metadata,
                reference.exprId,
                aliasName)
          }.head
        case Alias(child, _) => child
        case other => other
      }
    }

    expressions.map {
      case alias@Alias(agg: AggregateExpression, name) =>
        attributeMap.get(AttributeKey(agg)).map {
          expression =>
            Alias(getAttribute(expression), name)(
              alias.exprId,
              alias.qualifier,
              alias.explicitMetadata)
        }.getOrElse(alias)
      case reference: AttributeReference =>
        attributeMap.get(AttributeKey(reference)).map {
          expression =>
            if (keepAlias) {
              AttributeReference(
                name = expression.name,
                dataType = expression.dataType,
                nullable = expression.nullable,
                metadata = expression.metadata)(
                exprId = expression.exprId,
                qualifier = reference.qualifier)
            } else {
              expression
            }
        }.getOrElse(reference)
      case alias@Alias(expression: Expression, name) =>
        attributeMap.get(AttributeKey(expression)).map {
          expression =>
            Alias(getAttribute(expression), name)(alias.exprId, alias.qualifier,
              alias.explicitMetadata)
        }.getOrElse(alias)
      case expression: Expression =>
        attributeMap.getOrElse(AttributeKey(expression), expression)
    }
  }

  private val subqueryNameGenerator: SubqueryNameGenerator = new SubqueryNameGenerator()

  private val rewrittenPlans: java.util.Set[ModularPlan] = new java.util.HashSet[ModularPlan]()

  lazy val optimizedPlan: LogicalPlan = BirdcageOptimizer.execute(logicalPlan)

  lazy val modularPlan: ModularPlan = SimpleModularizer.modularize(optimizedPlan).next()

  lazy val harmonizedPlan: ModularPlan = modularPlan.harmonized

  lazy val rewrittenPlan: ModularPlan = rewrite(rewriteWithSchemaWrapper(harmonizedPlan))

  lazy val toCompactSQL: String = rewriteWithSchemaWrapper(modularPlan).asCompactSQL

  lazy val toOneLineSQL: String = rewriteWithSchemaWrapper(modularPlan).asOneLineSQL

  private case class AttributeKey(expression: Expression) {

    override def equals(other: Any): Boolean = {
      other match {
        case that: AttributeKey =>
          expression.semanticEquals(that.expression)
        case _ => false
      }
    }

    // Basically we want to use it as simple linked list so hashcode is hardcoded.
    override def hashCode: Int = {
      1
    }

  }

}

private class SubqueryNameGenerator {

  private val nextSubqueryId = new AtomicLong(0)

  def newSubsumerName(): String = s"gen_subsumer_${nextSubqueryId.getAndIncrement()}"

}
