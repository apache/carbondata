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

import scala.math.BigInt.int2bigInt

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation, QueryPlanner}
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical.{BroadcastHint, Limit, LogicalPlan, Sort}
import org.apache.spark.sql.cubemodel._
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand, ExecutedCommand, Filter, Project, SparkPlan}
import org.apache.spark.sql.execution.datasources.{DescribeCommand => LogicalDescribeCommand, LogicalRelation}
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, FilterPushJoin}
import org.apache.spark.sql.hive.execution.{DescribeHiveTableCommand, DropTable, HiveNativeCommand}
import org.apache.spark.sql.types.{IntegerType, LongType}

import org.carbondata.common.logging.LogServiceFactory

class CarbonStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonStrategies")

  def getStrategies: Seq[Strategy] = {
    val total = sqlContext.planner.strategies :+ CarbonCubeScans
    total
  }

  /**
   * Carbon strategies for Carbon cube scanning
   */
  private[sql] object CarbonCubeScans extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
        carbonScan(projectList, predicates, carbonRelation.carbonRelation, None, None, None, false,
          true) :: Nil

      case Limit(IntegerLiteral(limit),
      Sort(order, _, p@PartialAggregation(
      namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))))) =>
        val aggPlan = handleAggregation(plan, p, projectList, predicates, carbonRelation,
          partialComputation, groupingExpressions, namedGroupingAttributes,
          rewrittenAggregateExpressions)
        org.apache.spark.sql.execution.TakeOrderedAndProject(limit,
          order,
          None,
          aggPlan(0)) :: Nil

      case Limit(IntegerLiteral(limit), p@PartialAggregation(
      namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))) =>
        val aggPlan = handleAggregation(plan, p, projectList, predicates, carbonRelation,
          partialComputation, groupingExpressions, namedGroupingAttributes,
          rewrittenAggregateExpressions)
        org.apache.spark.sql.execution.Limit(limit, aggPlan(0)) :: Nil

      case PartialAggregation(
      namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
        handleAggregation(plan, plan, projectList, predicates, carbonRelation,
          partialComputation, groupingExpressions, namedGroupingAttributes,
          rewrittenAggregateExpressions)

      case Limit(IntegerLiteral(limit),
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
        val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)
        val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation, groupExprs,
          substitutesortExprs, limitExpr, false, true)
        org.apache.spark.sql.execution.Limit(limit, s) :: Nil


      case Limit(IntegerLiteral(limit),
      Sort(order, _,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))) =>
        val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)
        val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation, groupExprs,
          substitutesortExprs, limitExpr, false, true)
        org.apache.spark.sql.execution.TakeOrderedAndProject(limit,
          order,
          None,
          s) :: Nil

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)), right)
        if (canPushDownJoin(right, condition)) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:right")
        val carbon = carbonScan(projectList, predicates, carbonRelation.carbonRelation, None, None,
          None, false, true)
        val pushedDownJoin = FilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          BuildRight,
          carbon,
          planLater(right),
          condition)

        condition.map(Filter(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))
        if (canPushDownJoin(left, condition)) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:left")
        val carbon = carbonScan(projectList, predicates, carbonRelation.carbonRelation, None, None,
          None, false, true)

        val pushedDownJoin = FilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          BuildLeft,
          planLater(left),
          carbon,
          condition)
        condition.map(Filter(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil

      case ShowCubeCommand(schemaName) =>
        ExecutedCommand(ShowAllCubesInSchema(schemaName, plan.output)) :: Nil
      case c@ShowAllCubeCommand() =>
        ExecutedCommand(ShowAllCubes(plan.output)) :: Nil
      case ShowCreateCubeCommand(cm) =>
        ExecutedCommand(ShowCreateCube(cm, plan.output)) :: Nil
      case ShowTablesDetailedCommand(schemaName) =>
        ExecutedCommand(ShowAllTablesDetail(schemaName, plan.output)) :: Nil
      case DropTable(tableName, ifNotExists)
        if (CarbonEnv.getInstance(sqlContext).carbonCatalog.cubeExists(Seq(tableName))
        (sqlContext)) =>
        ExecutedCommand(DropCubeCommand(ifNotExists, None, tableName)) :: Nil
      case ShowAggregateTablesCommand(schemaName) =>
        ExecutedCommand(ShowAggregateTables(schemaName, plan.output)) :: Nil
      case ShowLoadsCommand(schemaName, cube, limit) =>
        ExecutedCommand(ShowLoads(schemaName, cube, limit, plan.output)) :: Nil
      case LoadCube(schemaNameOp, cubeName, factPathFromUser, dimFilesPath,
        partionValues, isOverwriteExist, inputSqlString) =>
        val isCarbonTable = CarbonEnv.getInstance(sqlContext).carbonCatalog
          .cubeExists(schemaNameOp, cubeName)(sqlContext);
        if (isCarbonTable) {
          ExecutedCommand(LoadCube(schemaNameOp, cubeName, factPathFromUser,
            dimFilesPath, partionValues, isOverwriteExist, inputSqlString)) :: Nil
        } else {
          ExecutedCommand(HiveNativeCommand(inputSqlString)) :: Nil
        }
      case DescribeFormattedCommand(sql, tblIdentifier) =>
        val isCube = CarbonEnv.getInstance(sqlContext).carbonCatalog
          .cubeExists(tblIdentifier)(sqlContext);
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
            ExecutedCommand(DescribeHiveTableCommand(t, describe.output, describe.isExtended)) ::
              Nil
          case o: LogicalPlan =>
            val resultPlan = sqlContext.executePlan(o).executedPlan
            ExecutedCommand(
              RunnableDescribeCommand(resultPlan, describe.output, describe.isExtended)) :: Nil
        }
      case _ =>
        Nil
    }

    def handleAggregation(plan: LogicalPlan,
                          aggPlan: LogicalPlan,
                          projectList: Seq[NamedExpression],
                          predicates: Seq[Expression],
                          carbonRelation: CarbonDatasourceRelation,
                          partialComputation: Seq[NamedExpression],
                          groupingExpressions: Seq[Expression],
                          namedGroupingAttributes: Seq[Attribute],
                          rewrittenAggregateExpressions: Seq[NamedExpression]):
    Seq[SparkPlan] = {
      val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)

      val s =
        try {
          carbonScan(projectList, predicates, carbonRelation.carbonRelation,
            Some(partialComputation), substitutesortExprs, limitExpr, !groupingExpressions.isEmpty)
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
          PhysicalOperation(projectList, predicates,
          l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))) =>
            val (_, _, _, aliases, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)


            val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation,
              Some(partialComputation), substitutesortExprs, limitExpr,
              !groupingExpressions.isEmpty, true)

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

    private def canBeCodeGened(aggs: Seq[AggregateExpression]) = !aggs.exists {
      case _: Sum | _: Count | _: Max | _: CombineSetsAndCount => false
      // The generated set implementation is pretty limited ATM.
      case CollectHashSet(exprs) if exprs.size == 1 &&
        Seq(IntegerType, LongType).contains(exprs.head.dataType) => false
      case _ => true
    }

    private def allAggregates(exprs: Seq[Expression]) =
      exprs.flatMap(_.collect { case a: AggregateExpression => a })

    private def canPushDownJoin(otherRDDPlan: LogicalPlan,
                                joinCondition: Option[Expression]): Boolean = {
      val pushdowmJoinEnabled = sqlContext.sparkContext.conf
        .getBoolean("spark.carbon.pushdown.join.as.filter", true)

      if (!pushdowmJoinEnabled) return false

      val isJoinOnCarbonCube = otherRDDPlan match {
        case other@PhysicalOperation(projectList, predicates,
        l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) => true
        case _ => false
      }

      // TODO remove the isJoinOnCarbonCube check
      if (isJoinOnCarbonCube) {
        return false // For now If both left & right are carbon cubes, let's not join
      }

      otherRDDPlan match {
        case BroadcastHint(p) => true
        case p if sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
          p.statistics.sizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold => {
          LOGGER.info("canPushDownJoin statistics:" + p.statistics.sizeInBytes)
          true
        }
        case _ => false
      }
    }

    /**
     * Create carbon scan
     */
    private def carbonScan(projectList: Seq[NamedExpression],
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

    private def extractPlan(plan: LogicalPlan) = {
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
