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
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand, ExecutedCommand, Filter, Project, SparkPlan}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.{DescribeCommand => LogicalDescribeCommand, LogicalRelation}
import org.apache.spark.sql.execution.joins.{BroadCastFilterPushJoin, BuildLeft, BuildRight}
import org.apache.spark.sql.hive.execution.{DescribeHiveTableCommand, DropTable, HiveNativeCommand}

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.spark.exception.MalformedCarbonCommandException

object CarbonHiveSyntax {

  @transient
  protected val sqlParser = new CarbonSqlParser

  def parse(sqlText: String): LogicalPlan = {
    sqlParser.parse(sqlText)
  }
}

class CarbonStrategies(sqlContext: SQLContext) extends QueryPlanner[SparkPlan] {

  override def strategies: Seq[Strategy] = getStrategies

  val LOGGER = LogServiceFactory.getLogService("CarbonStrategies")

  def getStrategies: Seq[Strategy] = {
    val total = sqlContext.planner.strategies :+ CarbonTableScans :+ DDLStrategies
    total
  }

  /**
   * Carbon strategies for Carbon cube scanning
   */
  private[sql] object CarbonTableScans extends Strategy {

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)) =>
        carbonScan(projectList,
          predicates,
          carbonRelation.carbonRelation,
          None,
          None,
          None,
          isGroupByPresent = false,
          detailQuery = true) :: Nil

      case Limit(IntegerLiteral(limit),
      Sort(order, _,
      p@PartialAggregation(namedGroupingAttributes,
      rewrittenAggregateExpressions,
      groupingExpressions,
      partialComputation,
      PhysicalOperation(
      projectList,
      predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _))))) =>
        val aggPlan = handleAggregation(plan, p, projectList, predicates, carbonRelation,
          partialComputation, groupingExpressions, namedGroupingAttributes,
          rewrittenAggregateExpressions)
        org.apache.spark.sql.execution.TakeOrderedAndProject(limit,
          order,
          None,
          aggPlan.head) :: Nil

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
        org.apache.spark.sql.execution.Limit(limit, aggPlan.head) :: Nil

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
        val (_, _, _, _, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)
        val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation, groupExprs,
          substitutesortExprs, limitExpr, isGroupByPresent = false, detailQuery = true)
        org.apache.spark.sql.execution.Limit(limit, s) :: Nil

      case Limit(IntegerLiteral(limit),
      Sort(order, _,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)))) =>
        val (_, _, _, _, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)
        val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation, groupExprs,
          substitutesortExprs, limitExpr, isGroupByPresent = false, detailQuery = true)
        org.apache.spark.sql.execution.TakeOrderedAndProject(limit,
          order,
          None,
          s) :: Nil

      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition,
      PhysicalOperation(projectList, predicates,
      l@LogicalRelation(carbonRelation: CarbonDatasourceRelation, _)), right)
        if canPushDownJoin(right, condition) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:right")
        val carbon = carbonScan(projectList,
          predicates,
          carbonRelation.carbonRelation,
          None,
          None,
          None,
          isGroupByPresent = false,
          detailQuery = true)
        val pushedDownJoin = BroadCastFilterPushJoin(
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
        if canPushDownJoin(left, condition) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:left")
        val carbon = carbonScan(projectList,
          predicates,
          carbonRelation.carbonRelation,
          None,
          None,
          None,
          isGroupByPresent = false,
          detailQuery = true)

        val pushedDownJoin = BroadCastFilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          BuildLeft,
          planLater(left),
          carbon,
          condition)
        condition.map(Filter(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil

      case _ => Nil
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
      val (_, _, _, _, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)

      val s =
        try {
          carbonScan(projectList, predicates, carbonRelation.carbonRelation,
            Some(partialComputation), substitutesortExprs, limitExpr, groupingExpressions.nonEmpty)
        } catch {
          case e: Exception => null
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
            val (_, _, _, _, groupExprs, substitutesortExprs, limitExpr) = extractPlan(plan)


            val s = carbonScan(projectList, predicates, carbonRelation.carbonRelation,
              Some(partialComputation), substitutesortExprs, limitExpr,
              groupingExpressions.nonEmpty, detailQuery = true)

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

    private def canPushDownJoin(otherRDDPlan: LogicalPlan,
        joinCondition: Option[Expression]): Boolean = {
      val pushdowmJoinEnabled = sqlContext.sparkContext.conf
        .getBoolean("spark.carbon.pushdown.join.as.filter", defaultValue = true)

      if (!pushdowmJoinEnabled) {
        return false
      }

      otherRDDPlan match {
        case BroadcastHint(p) => true
        case p if sqlContext.conf.autoBroadcastJoinThreshold > 0 &&
                  p.statistics.sizeInBytes <= sqlContext.conf.autoBroadcastJoinThreshold =>
          LOGGER.info("canPushDownJoin statistics:" + p.statistics.sizeInBytes)
          true
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

      if (!detailQuery) {
        val projectSet = AttributeSet(projectList.flatMap(_.references))
        CarbonTableScan(
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
          CarbonTableScan(projectSet.toSeq,
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
                case Alias(reference, name) => reference
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

  object DDLStrategies extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ShowCubeCommand(schemaName) =>
        ExecutedCommand(ShowAllTablesInSchema(schemaName, plan.output)) :: Nil
      case c@ShowAllCubeCommand() =>
        ExecutedCommand(ShowAllTables(plan.output)) :: Nil
      case ShowCreateCubeCommand(cm) =>
        ExecutedCommand(ShowCreateTable(cm, plan.output)) :: Nil
      case ShowTablesDetailedCommand(schemaName) =>
        ExecutedCommand(ShowAllTablesDetail(schemaName, plan.output)) :: Nil
      case DropTable(tableName, ifNotExists)
        if CarbonEnv.getInstance(sqlContext).carbonCatalog
          .cubeExists(Seq(tableName.toLowerCase()))(sqlContext) =>
        ExecutedCommand(DropCubeCommand(ifNotExists, None, tableName.toLowerCase())) :: Nil
      case ShowAggregateTablesCommand(schemaName) =>
        ExecutedCommand(ShowAggregateTables(schemaName, plan.output)) :: Nil
      case ShowLoadsCommand(schemaName, cube, limit) =>
        ExecutedCommand(ShowLoads(schemaName, cube, limit, plan.output)) :: Nil
      case LoadCube(schemaNameOp, cubeName, factPathFromUser, dimFilesPath,
      partionValues, isOverwriteExist, inputSqlString) =>
        val isCarbonTable = CarbonEnv.getInstance(sqlContext).carbonCatalog
          .cubeExists(schemaNameOp, cubeName)(sqlContext)
        if (isCarbonTable || partionValues.nonEmpty) {
          ExecutedCommand(LoadCube(schemaNameOp, cubeName, factPathFromUser,
            dimFilesPath, partionValues, isOverwriteExist, inputSqlString)) :: Nil
        } else {
          ExecutedCommand(HiveNativeCommand(inputSqlString)) :: Nil
        }
      case d: HiveNativeCommand =>
        try {
          val resolvedTable = sqlContext.executePlan(CarbonHiveSyntax.parse(d.sql)).optimizedPlan
          planLater(resolvedTable) :: Nil
        } catch {
          case ce: MalformedCarbonCommandException =>
            throw ce
          case ae: AnalysisException =>
            throw ae
          case e: Exception => ExecutedCommand(d) :: Nil
        }
      case DescribeFormattedCommand(sql, tblIdentifier) =>
        val isCube = CarbonEnv.getInstance(sqlContext).carbonCatalog
          .cubeExists(tblIdentifier)(sqlContext)
        if (isCube) {
          val describe =
            LogicalDescribeCommand(UnresolvedRelation(tblIdentifier, None), isExtended = false)
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
            ExecutedCommand(
              DescribeHiveTableCommand(t, describe.output, describe.isExtended)) :: Nil
          case o: LogicalPlan =>
            val resultPlan = sqlContext.executePlan(o).executedPlan
            ExecutedCommand(
              RunnableDescribeCommand(resultPlan, describe.output, describe.isExtended)) :: Nil
        }
      case _ =>
        Nil
    }
  }

}
