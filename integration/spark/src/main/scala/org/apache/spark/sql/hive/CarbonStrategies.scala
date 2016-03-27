/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.CollectHashSet
import org.apache.spark.sql.catalyst.expressions.CombineSetsAndCount
import org.apache.spark.sql.catalyst.expressions.Count
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.Max
import org.apache.spark.sql.catalyst.expressions.Sum
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, LogicalPlan}
import org.apache.spark.sql.cubemodel.{ShowAllCubesInSchema, ShowAllCubes, ShowCreateCube, ShowAggregateTables, ShowAllTablesDetail,
ShowLoads, SuggestAggregates}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.catalyst.plans.logical.Limit
import org.apache.spark.sql.catalyst.plans.logical.Sort
import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.planning.{QueryPlanner, PhysicalOperation}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.types.{IntegerType, LongType}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.execution.joins.{BuildRight, BuildLeft}
import org.apache.spark.sql.execution.{ExecutedCommand}
import org.apache.spark.sql.{CarbonAggregate, CarbonContext, CarbonRelation}
import org.apache.spark.sql.CarbonRelation
import org.apache.spark.sql.CarbonCubeScan
import org.apache.spark.sql.PartialAggregation
import org.apache.spark.sql.PhysicalOperation1
import org.apache.spark.sql.execution.datasources.{DescribeCommand => LogicalDescribeCommand}
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand}
import org.apache.spark.sql.{ShowAggregateTablesCommand, ShowCubeCommand, ShowAllCubeCommand, ShowCreateCubeCommand, SuggestAggregateCommand, ShowLoadsCommand,
ShowSchemaCommand, ShowTablesDetailedCommand}
import org.apache.spark.sql.Strategy
import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent
import scala.math.BigInt.int2bigInt
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.execution.DescribeHiveTableCommand
import org.apache.spark.sql.execution.Project
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.execution.SparkStrategies
import org.apache.spark.sql.catalyst.planning.QueryPlanner
import org.apache.spark.sql.CarbonDatasourceRelation
import org.apache.spark.sql.sources.HadoopFsRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.DescribeFormattedCommand
import org.apache.spark.sql.cubemodel.DescribeNativeCommand
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.cubemodel.DescribeCommandFormatted
import org.apache.spark.sql.execution.joins.FilterPushJoin
import org.apache.spark.sql.execution.Filter
import org.apache.spark.sql.cubemodel.LoadCube
import org.apache.spark.sql.QueryStatsLogicalPlan
import org.apache.spark.sql.QueryStatsSparkPlan
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.AnalysisException

class CarbonStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonStrategies")

  def getStrategies: Seq[Strategy] = {
    /*OlapCubeScans :: Nil*/
    val total = sqlContext.planner.strategies :+ OlapCubeScans
    total
  }

  /** Olap strategies for Carbon cube scanning
    */
  private[sql] object OlapCubeScans extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
        olapScan(projectList, predicates, carbonRelation.olapRelation, None, None, None, false, true) :: Nil

      case Limit(IntegerLiteral(limit),
      Sort(order, _, p@ PartialAggregation(
      namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))))) =>
      val aggPlan = handleAggregation(plan, p, projectList, predicates, carbonRelation,
            partialComputation, groupingExpressions, namedGroupingAttributes, rewrittenAggregateExpressions)
      org.apache.spark.sql.execution.TakeOrderedAndProject(limit,
          order,
          None,
          aggPlan(0)) :: Nil
          
      case Limit(IntegerLiteral(limit), p@ PartialAggregation(
      namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))) =>
      val aggPlan = handleAggregation(plan, p, projectList, predicates, carbonRelation,
            partialComputation, groupingExpressions, namedGroupingAttributes, rewrittenAggregateExpressions)
      org.apache.spark.sql.execution.Limit(limit, aggPlan(0)) :: Nil
        
      case PartialAggregation(
      namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
        handleAggregation(plan, plan, projectList, predicates, carbonRelation,
            partialComputation, groupingExpressions, namedGroupingAttributes, rewrittenAggregateExpressions)

      case Limit(IntegerLiteral(limit),
      PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
        val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)
        val s = olapScan(projectList, predicates, carbonRelation.olapRelation, groupExprs, substitutesortExprs, limitExpr, false, true)
        org.apache.spark.sql.execution.Limit(limit, s) :: Nil


      case Limit(IntegerLiteral(limit),
      Sort(order, _,
      PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))) =>
        val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)
        val s = olapScan(projectList, predicates, carbonRelation.olapRelation, groupExprs, substitutesortExprs, limitExpr, false, true)
        org.apache.spark.sql.execution.TakeOrderedAndProject(limit,
          order,
          None,
          s) :: Nil

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)), right)
        if (canPushDownJoin(right, condition)) =>
        LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"pushing down for ExtractEquiJoinKeys:right")
        val olap = olapScan(projectList, predicates, carbonRelation.olapRelation, None, None, None, false, true)
        val pushedDownJoin = FilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          BuildRight,
          olap,
          planLater(right),
          condition)

        condition.map(Filter(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))
        if (canPushDownJoin(left, condition)) =>
        LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, s"pushing down for ExtractEquiJoinKeys:left")
        val olap = olapScan(projectList, predicates, carbonRelation.olapRelation, None, None, None, false, true)

        val pushedDownJoin = FilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          BuildLeft,
          planLater(left),
          olap,
          condition)
        condition.map(Filter(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil

      case QueryStatsLogicalPlan(child) =>
        QueryStatsSparkPlan(planLater(child)) :: Nil
      case ShowCubeCommand(schemaName) =>
        ExecutedCommand(ShowAllCubesInSchema(schemaName, plan.output)) :: Nil
      case c@ShowAllCubeCommand() =>
        ExecutedCommand(ShowAllCubes(plan.output)) :: Nil
      case ShowCreateCubeCommand(cm) =>
        ExecutedCommand(ShowCreateCube(cm, plan.output)) :: Nil
      case ShowTablesDetailedCommand(schemaName) =>
        ExecutedCommand(ShowAllTablesDetail(schemaName, plan.output)) :: Nil
      case SuggestAggregateCommand(script, sugType, schemaName, cubeName) =>
        ExecutedCommand(SuggestAggregates(script, sugType, schemaName, cubeName, plan.output)) :: Nil
      case ShowAggregateTablesCommand(schemaName) =>
        ExecutedCommand(ShowAggregateTables(schemaName, plan.output)) :: Nil
      case ShowLoadsCommand(schemaName, cube, limit) =>
        ExecutedCommand(ShowLoads(schemaName, cube, limit, plan.output)) :: Nil
      case DescribeFormattedCommand(sql, tblIdentifier) =>
        val isCube = CarbonEnv.getInstance(sqlContext).carbonCatalog.cubeExists(tblIdentifier)(sqlContext);
        if (isCube) {
          val describe = LogicalDescribeCommand(UnresolvedRelation(tblIdentifier, None), false)
          val resolvedTable = sqlContext.executePlan(describe.table).analyzed
          val resultPlan = sqlContext.executePlan(resolvedTable).executedPlan
          ExecutedCommand(DescribeCommandFormatted(resultPlan, plan.output, tblIdentifier)) :: Nil
        }
        else {
          ExecutedCommand(DescribeNativeCommand(sql, plan.output)) :: Nil
        }
      case describe@LogicalDescribeCommand(table, isExtended) =>
        val resolvedTable = sqlContext.executePlan(describe.table).analyzed
        resolvedTable match {
          case t: MetastoreRelation =>
            ExecutedCommand(DescribeHiveTableCommand(t, describe.output, describe.isExtended)) :: Nil
          case o: LogicalPlan =>
            val resultPlan = sqlContext.executePlan(o).executedPlan
            ExecutedCommand(RunnableDescribeCommand(resultPlan, describe.output, describe.isExtended)) :: Nil
        }
      case _ =>
        Nil
    }

    def handleAggregation(plan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
        aggPlan: org.apache.spark.sql.catalyst.plans.logical.LogicalPlan,
        projectList: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression],
        predicates: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
        carbonRelation: org.apache.spark.sql.CarbonDatasourceRelation,
        partialComputation: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression],
        groupingExpressions: Seq[org.apache.spark.sql.catalyst.expressions.Expression],
        namedGroupingAttributes: Seq[org.apache.spark.sql.catalyst.expressions.Attribute],
        rewrittenAggregateExpressions: Seq[org.apache.spark.sql.catalyst.expressions.NamedExpression]):
         Seq[SparkPlan] = {
      val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)

      val s =
        try {
          olapScan(projectList, predicates, carbonRelation.olapRelation, Some(partialComputation), substitutesortExprs, limitExpr, !groupingExpressions.isEmpty)
        } catch {
          case _ => null
        }

      if (s != null) {
        CarbonAggregate(
          partial = false,
          namedGroupingAttributes,
          rewrittenAggregateExpressions,
          CarbonAggregate(
            partial = true,
            groupingExpressions,
            partialComputation,
            s)(sqlContext))(sqlContext) :: Nil

      } else {
        (aggPlan, true) match {
          case PartialAggregation(
          namedGroupingAttributes,
          rewrittenAggregateExpressions,
          groupingExpressions,
          partialComputation,
          PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
            val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)


            val s = olapScan(projectList, predicates, carbonRelation.olapRelation, Some(partialComputation), substitutesortExprs, limitExpr, !groupingExpressions.isEmpty, true)

            CarbonAggregate(
              partial = false,
              namedGroupingAttributes,
              rewrittenAggregateExpressions,
              CarbonAggregate(
                partial = true,
                groupingExpressions,
                partialComputation,
                s)(sqlContext))(sqlContext) :: Nil
        }
      }
    }

    def canBeCodeGened(aggs: Seq[AggregateExpression]) = !aggs.exists {
      case _: Sum | _: Count | _: Max | _: CombineSetsAndCount => false
      // The generated set implementation is pretty limited ATM.
      case CollectHashSet(exprs) if exprs.size == 1 &&
        Seq(IntegerType, LongType).contains(exprs.head.dataType) => false
      case _ => true
    }

    def allAggregates(exprs: Seq[Expression]) =
      exprs.flatMap(_.collect { case a: AggregateExpression => a })

    def canPushDownJoin(otherRDDPlan: LogicalPlan, joinCondition: Option[Expression]): Boolean = {
      val pushdowmJoinEnabled = sqlContext.sparkContext.conf.getBoolean("spark.carbon.pushdown.join.as.filter", true)

      if (!pushdowmJoinEnabled) return false

      val isJoinOnCarbonCube = otherRDDPlan match {
        case other@PhysicalOperation(projectList, predicates, l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) => true
        case _ => false
      }

      //TODO remove the isJoinOnCarbonCube check
      if (isJoinOnCarbonCube) {
        println("For now If both left & right are carbon cubes, let's not join")
        return false //For now If both left & right are carbon cubes, let's not join
      }

      otherRDDPlan match {
        case BroadcastHint(p) => true
        case p if sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
          p.statistics.sizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold => {
          LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG, "canPushDownJoin statistics:" + p.statistics.sizeInBytes)
          true
        }
        case _ => false
      }
    }

    /**
      * Create olap scan
      */
    def olapScan(projectList: Seq[NamedExpression],
                 predicates: Seq[Expression],
                 relation: CarbonRelation,
                 groupExprs: Option[Seq[Expression]],
                 substitutesortExprs: Option[Seq[SortOrder]],
                 limitExpr: Option[Expression],
                 isGroupByPresent: Boolean,
                 detailQuery: Boolean = false) = {

      if (detailQuery == false) {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        CarbonCubeScan(
          projectSet.toSeq,
          relation,
          predicates,
          groupExprs,
          substitutesortExprs,
          limitExpr,
          isGroupByPresent,
          detailQuery)(sqlContext)
      }
      else {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        Project(projectList,
          CarbonCubeScan(projectSet.toSeq,
            relation,
            predicates,
            groupExprs,
            substitutesortExprs,
            limitExpr,
            isGroupByPresent,
            detailQuery)(sqlContext))

      }
    }

    def extractPlan(plan: LogicalPlan) = {
      val (a, b, c, aliases, groupExprs, sortExprs, limitExpr) =
        PhysicalOperation1.collectProjectsAndFilters(plan)
      val substitutesortExprs = sortExprs match {
        case Some(sort) =>
          Some(sort.map {
            case SortOrder(a: Alias, direction) =>
              val ref = aliases.getOrElse(a.toAttribute, a) match {
                case Alias(ref, name) => ref
                case others => others
              }
              SortOrder(ref, direction)
            case others => others
          })
        case others => others
      }
      (a, b, c, aliases, groupExprs, substitutesortExprs, limitExpr)
    }
  }

}
