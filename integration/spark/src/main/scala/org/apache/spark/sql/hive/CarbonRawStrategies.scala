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

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, _}
import org.apache.spark.sql.catalyst.planning.{PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.logical.{Filter => LogicalFilter, LogicalPlan}
import org.apache.spark.sql.execution.{Filter, Project, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.optimizer.{CarbonAliasDecoderRelation, CarbonDecoderRelation}

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable

class CarbonRawStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonRawStrategies")

  def getStrategies: Seq[Strategy] = {
    val total = sqlContext.planner.strategies :+ CarbonRawTableScans
    total
  }

  /**
   * Carbon strategies for Carbon cube scanning
   */
  private[sql] object CarbonRawTableScans extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
          if (isStarQuery(plan)) {
            carbonRawScanForStarQuery(projectList, predicates, carbonRelation, l)(sqlContext) :: Nil
          } else {
            carbonRawScan(projectList,
              predicates,
              carbonRelation,
              l,
              None,
              detailQuery = true,
              useBinaryAggregation = false)(sqlContext)._1 :: Nil
          }

        case catalyst.planning.PartialAggregation(
        namedGroupingAttributes,
        rewrittenAggregateExpressions,
        groupingExpressions,
        partialComputation,
        PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
          handleRawAggregation(plan, plan, projectList, predicates, carbonRelation,
            l, partialComputation, groupingExpressions, namedGroupingAttributes,
            rewrittenAggregateExpressions)
        case CarbonDictionaryCatalystDecoder(relations, profile,
               aliasMap, _, child) =>
          CarbonDictionaryDecoder(relations,
            profile,
            aliasMap,
            planLater(child))(sqlContext) :: Nil
        case _ =>
          Nil
      }
    }


    def handleRawAggregation(plan: LogicalPlan,
        aggPlan: LogicalPlan,
        projectList: Seq[NamedExpression],
        predicates: Seq[Expression],
        carbonRelation: CarbonDatasourceRelation,
        logicalRelation: LogicalRelation,
        partialComputation: Seq[NamedExpression],
        groupingExpressions: Seq[Expression],
        namedGroupingAttributes: Seq[Attribute],
        rewrittenAggregateExpressions: Seq[NamedExpression]):
    Seq[SparkPlan] = {
      val groupByPresentOnMsr = isGroupByPresentOnMeasures(groupingExpressions,
        carbonRelation.carbonRelation.metaData.carbonTable)
      if(!groupByPresentOnMsr) {
        val s = carbonRawScan(projectList,
          predicates,
          carbonRelation,
          logicalRelation,
          Some(partialComputation),
          detailQuery = false,
          useBinaryAggregation = true)(sqlContext)
        // If any aggregate function present on dimnesions then don't use this plan.
        if (!s._2) {
          CarbonAggregate(
            partial = false,
            namedGroupingAttributes,
            rewrittenAggregateExpressions,
            CarbonRawAggregate(
              partial = true,
              groupingExpressions,
              partialComputation,
              s._1))(sqlContext) :: Nil
        } else {
          Nil
        }
      } else {
        Nil
      }
    }

    /**
     * Create carbon scan
     */
    private def carbonRawScan(projectList: Seq[NamedExpression],
        predicates: Seq[Expression],
        relation: CarbonDatasourceRelation,
        logicalRelation: LogicalRelation,
        groupExprs: Option[Seq[Expression]],
        detailQuery: Boolean,
        useBinaryAggregation: Boolean)(sc: SQLContext): (SparkPlan, Boolean) = {

      val tableName: String =
        relation.carbonRelation.metaData.carbonTable.getFactTableName.toLowerCase
      // Check out any expressions are there in project list. if they are present then we need to
      // decode them as well.
      val projectExprsNeedToDecode = new java.util.HashSet[Attribute]()
      projectList.map {
        case attr: AttributeReference =>
        case Alias(attr: AttributeReference, _) =>
        case others =>
          others.references.map(f => projectExprsNeedToDecode.add(f))
      }
      val projectSet = AttributeSet(projectList.flatMap(_.references))
      val scan = CarbonRawTableScan(projectSet.toSeq,
        relation.carbonRelation,
        predicates,
        groupExprs,
        useBinaryAggregation)(sqlContext)
      val dimAggrsPresence: Boolean = scan.buildCarbonPlan.getDimAggregatorInfos.size() > 0
      projectExprsNeedToDecode.addAll(scan.attributesNeedToDecode)
      if (!detailQuery) {
        if (projectExprsNeedToDecode.size > 0) {
          val decoder = getCarbonDecoder(logicalRelation,
            sc,
            tableName,
            projectExprsNeedToDecode.asScala.toSeq,
            scan)
          if (scan.unprocessedExprs.nonEmpty) {
            val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
            (Project(projectList, filterCondToAdd.map(Filter(_, decoder)).getOrElse(decoder)), true)
          } else {
            (Project(projectList, decoder), true)
          }
        } else {
          (scan, dimAggrsPresence)
        }
      } else {
        if (projectExprsNeedToDecode.size() > 0) {
          val decoder = getCarbonDecoder(logicalRelation,
            sc,
            tableName,
            projectExprsNeedToDecode.asScala.toSeq,
            scan)
          if (scan.unprocessedExprs.nonEmpty) {
            val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
            (Project(projectList, filterCondToAdd.map(Filter(_, decoder)).getOrElse(decoder)), true)
          } else {
            (Project(projectList, decoder), true)
          }
        } else {
          (Project(projectList, scan), dimAggrsPresence)
        }
      }
    }

    /**
     * Create carbon scan for star query
     */
    private def carbonRawScanForStarQuery(projectList: Seq[NamedExpression],
        predicates: Seq[Expression],
        relation: CarbonDatasourceRelation,
        logicalRelation: LogicalRelation)(sc: SQLContext): SparkPlan = {

      val tableName: String =
        relation.carbonRelation.metaData.carbonTable.getFactTableName.toLowerCase
      // Check out any expressions are there in project list. if they are present then we need to
      // decode them as well.
      val projectExprsNeedToDecode = new java.util.HashSet[Attribute]()
      val scan = CarbonRawTableScan(projectList.map(_.toAttribute),
        relation.carbonRelation,
        predicates,
        None,
        useBinaryAggregator = false)(sqlContext)
      projectExprsNeedToDecode.addAll(scan.attributesNeedToDecode)
      if (projectExprsNeedToDecode.size() > 0) {
        val decoder = getCarbonDecoder(logicalRelation,
          sc,
          tableName,
          projectExprsNeedToDecode.asScala.toSeq,
          scan)
        if (scan.unprocessedExprs.nonEmpty) {
          val filterCondToAdd = scan.unprocessedExprs.reduceLeftOption(expressions.And)
          filterCondToAdd.map(Filter(_, decoder)).getOrElse(decoder)
        } else {
          decoder
        }
      } else {
        scan
      }
    }

    def getCarbonDecoder(logicalRelation: LogicalRelation,
        sc: SQLContext,
        tableName: String,
        projectExprsNeedToDecode: Seq[Attribute],
        scan: CarbonRawTableScan): CarbonDictionaryDecoder = {
      val relation = CarbonDecoderRelation(logicalRelation.attributeMap,
        logicalRelation.relation.asInstanceOf[CarbonDatasourceRelation])
      val attrs = projectExprsNeedToDecode.map { attr =>
        val newAttr = AttributeReference(attr.name,
          attr.dataType,
          attr.nullable,
          attr.metadata)(attr.exprId, Seq(tableName))
        relation.addAttribute(newAttr)
        newAttr
      }
      CarbonDictionaryDecoder(Seq(relation), IncludeProfile(attrs),
        CarbonAliasDecoderRelation(), scan)(sc)
    }

    private def isStarQuery(plan: LogicalPlan) = {
      plan match {
        case LogicalFilter(condition,
        LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) => true
        case LogicalRelation(carbonRelation: CarbonDatasourceRelation, _) => true
        case _ => false
      }
    }

    private def isGroupByPresentOnMeasures(groupingExpressions: Seq[Expression],
        carbonTable: CarbonTable): Boolean = {
      groupingExpressions.map { g =>
       g.collect {
         case attr: AttributeReference
           if carbonTable.getMeasureByName(carbonTable.getFactTableName, attr.name) != null =>
           return true
       }
      }
      false
    }
  }

}
