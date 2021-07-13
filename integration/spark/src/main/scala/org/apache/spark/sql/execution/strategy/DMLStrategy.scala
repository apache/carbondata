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

package org.apache.spark.sql.execution.strategy

import java.util.Locale

import scala.collection.mutable

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{CarbonCountStar, CarbonDatasourceHadoopRelation, CarbonToSparkAdapter, CountStarPlan, InsertIntoCarbonTable, SparkSession, SparkVersionAdapter}
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAlias
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, Cast, Descending, Expression, IntegerLiteral, Literal, NamedExpression, ScalaUDF, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, PhysicalOperation}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, Limit, LogicalPlan, Project, ReturnAnswer, Sort}
import org.apache.spark.sql.execution.{CarbonTakeOrderedAndProjectExec, FilterExec, LeafExecNode, PlanLater, ProjectExec, SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec, LoadDataCommand, RunnableCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.joins.BroadCastPolygonFilterPushJoin
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper.isCarbonTable
import org.apache.spark.sql.hive.MatchLogicalRelation
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, StringType}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.geo.{InPolygonJoinRangeListUDF, InPolygonJoinUDF, ToRangeListAsStringUDF}
import org.apache.carbondata.spark.rdd.CarbonScanRDD

object DMLStrategy extends SparkStrategy {
  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      // load data / insert into
      case loadData: LoadDataCommand if isCarbonTable(loadData.table) =>
        ExecutedCommandExec(DMLHelper.loadData(loadData)) :: Nil
      case insert: InsertIntoCarbonTable =>
        if (insert.containsMultipleInserts) {
          // Successful insert in carbon will return segment ID in a row.
          // In-case of this specific multiple inserts scenario the Union node executes in the
          // physical plan phase of the command, so the rows should be of unsafe row object.
          // So we should override the sideEffectResult to prepare the content of command's
          // corresponding rdd from physical plan of insert into command.
          UnionCommandExec(CarbonPlanHelper.insertInto(insert)) :: Nil
        } else {
          ExecutedCommandExec(CarbonPlanHelper.insertInto(insert)) :: Nil
        }
      case insert: InsertIntoHadoopFsRelationCommand
        if insert.catalogTable.isDefined && isCarbonTable(insert.catalogTable.get.identifier) =>
        DataWritingCommandExec(DMLHelper.insertInto(insert), planLater(insert.query)) :: Nil
      case CountStarPlan(colAttr, PhysicalOperation(_, _, l: LogicalRelation))
        if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] && driverSideCountStar(l) =>
        val relation = l.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
        CarbonCountStar(colAttr, relation.carbonTable, SparkSession.getActiveSession.get) :: Nil
      case join: Join if join.condition.isDefined && join.condition.get.isInstanceOf[ScalaUDF] &&
                         isPolygonJoinUdfFilter(join.condition) =>
        val condition = join.condition
        if (join.joinType != Inner) {
          throw new UnsupportedOperationException("Unsupported query")
        }
        val carbon = CarbonSourceStrategy.apply(join.left).head
        val leftKeys = Seq(condition.get.asInstanceOf[ScalaUDF].children.head)
        val rightKeys = Seq(condition.get.asInstanceOf[ScalaUDF].children.last)
        if (condition.get.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonJoinUDF]) {
          // If join condition is IN_POLYGON_JOIN udf, then add a implicit projection to the
          // polygon table logical plan
          val tableInfo = carbon.collectFirst {
            case scan: CarbonDataSourceScan => scan.inputRDDs().head
          }.get.asInstanceOf[CarbonScanRDD[InternalRow]].getTableInfo
          // create ToRangeListAsString udf as implicit projection with the required fields
          val toRangeListUDF = new ToRangeListAsStringUDF
          val dataType = StringType
          var children: Seq[Expression] = mutable.Seq.empty
          val geoHashColumn = condition.get.children.head match {
            case Cast(attr: AttributeReference, _, _) =>
              attr
            case attr: AttributeReference =>
              attr
          }
          // get origin Latitude and gridSize from spatial table properties
          val commonKey = CarbonCommonConstants.SPATIAL_INDEX +
                          CarbonCommonConstants.POINT +
                          geoHashColumn.name +
                          CarbonCommonConstants.POINT
          val originLatitude = tableInfo.getFactTable
            .getTableProperties
            .get(commonKey + "originlatitude")
          val gridSize = tableInfo.getFactTable.getTableProperties.get(commonKey + "gridsize")
          if (originLatitude == null || gridSize == null) {
            throw new UnsupportedOperationException(
              s"Join condition having left column ${ geoHashColumn.name } is not GeoId column")
          }
          // join condition right side will be the polygon column
          children = children :+ condition.get.children.last
          children = children :+ Literal(originLatitude.toDouble)
          children = children :+ Literal(gridSize.toInt)

          var inputTypes: Seq[DataType] = Seq.empty
          inputTypes = inputTypes :+ StringType
          inputTypes = inputTypes :+ DoubleType
          inputTypes = inputTypes :+ IntegerType
          val rangeListScalaUdf = CarbonToSparkAdapter.createRangeListScalaUDF(toRangeListUDF,
            dataType, children, inputTypes)
          // add ToRangeListAsString udf column to the polygon table plan projection list
          val rightSide = join.right transform {
            case Project(projectList, child) =>
              val positionId = UnresolvedAlias(rangeListScalaUdf)
              val newProjectList = projectList :+ positionId
              Project(newProjectList, child)
          }
          val sparkSession = SparkSQLUtil.getSparkSession
          lazy val analyzer = sparkSession.sessionState.analyzer
          lazy val optimizer = sparkSession.sessionState.optimizer
          val analyzedPlan = CarbonToSparkAdapter.invokeAnalyzerExecute(
            analyzer, rightSide)
          val polygonTablePlan = optimizer.execute(analyzedPlan)
          // transform join condition by replacing polygon column with ToRangeListAsString udf
          // column output
          val newCondition = condition.get transform {
            case scalaUdf: ScalaUDF if scalaUdf.function.isInstanceOf[InPolygonJoinUDF] =>
              var udfChildren: Seq[Expression] = Seq.empty
              udfChildren = udfChildren :+ scalaUdf.children.head
              udfChildren = udfChildren :+ polygonTablePlan.output.last
              val polygonJoinUdf = new InPolygonJoinUDF
              CarbonToSparkAdapter.getTransformedPolygonJoinUdf(scalaUdf,
                udfChildren, polygonJoinUdf)
          }
          // push down in_polygon join filter to carbon
          val pushedDownJoin = BroadCastPolygonFilterPushJoin(
            leftKeys,
            rightKeys,
            join.joinType,
            Some(newCondition),
            carbon,
            PlanLater(polygonTablePlan)
          )
          Some(newCondition).map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
        } else {
          // push down in_polygon join filter to carbon
          val pushedDownJoin = BroadCastPolygonFilterPushJoin(
            leftKeys,
            rightKeys,
            join.joinType,
            condition,
            carbon,
            PlanLater(join.right)
          )
          condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
        }
      case CarbonExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
        if isCarbonPlan(left) && CarbonIndexUtil.checkIsIndexTable(right) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:right")
        val carbon = CarbonSourceStrategy.apply(left).head
        // in case of SI Filter push join remove projection list from the physical plan
        // no need to have the project list in the main table physical plan execution
        // only join uses the projection list
        var carbonChild = carbon match {
          case projectExec: ProjectExec =>
            projectExec.child
          case _ =>
            carbon
        }
        // check if the outer and the inner project are matching, only then remove project
        if (left.isInstanceOf[Project]) {
          val leftOutput = left.output
            .filterNot(_.name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID))
            .map(c => (c.name.toLowerCase, c.dataType))
          val childOutput = carbonChild.output
            .filterNot(_.name.equalsIgnoreCase(CarbonCommonConstants.POSITION_ID))
            .map(c => (c.name.toLowerCase, c.dataType))
          if (!leftOutput.equals(childOutput)) {
            // if the projection list and the scan list are different(in case of alias)
            // we should not skip the project, so we are taking the original plan with project
            carbonChild = carbon
          }
        }
        val pushedDownJoin = BroadCastSIFilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          Inner,
          CarbonToSparkAdapter.getBuildRight,
          carbonChild,
          planLater(right),
          condition)
        condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
      case CarbonExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left,
      right)
        if isCarbonPlan(right) && CarbonIndexUtil.checkIsIndexTable(left) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeys:left")
        val carbon = CarbonSourceStrategy.apply(right).head
        val pushedDownJoin =
          BroadCastSIFilterPushJoin(
            leftKeys: Seq[Expression],
            rightKeys: Seq[Expression],
            Inner,
            CarbonToSparkAdapter.getBuildLeft,
            planLater(left),
            carbon,
            condition)
        condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
      case CarbonExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition,
      left, right)
        if isLeftSemiExistPushDownEnabled &&
          isAllCarbonPlan(left) && isAllCarbonPlan(right) =>
        LOGGER.info(s"pushing down for ExtractEquiJoinKeysLeftSemiExist:right")
        val pushedDownJoin = BroadCastSIFilterPushJoin(
          leftKeys: Seq[Expression],
          rightKeys: Seq[Expression],
          LeftSemi,
          CarbonToSparkAdapter.getBuildRight,
          planLater(left),
          planLater(right),
          condition)
        condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin) :: Nil
      case ExtractTakeOrderedAndProjectExec(carbonTakeOrderedAndProjectExec) =>
        carbonTakeOrderedAndProjectExec :: Nil
      case _ => Nil
    }
  }

  private def isPolygonJoinUdfFilter(condition: Option[Expression]) = {
    condition.get.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonJoinUDF] ||
    condition.get.asInstanceOf[ScalaUDF].function.isInstanceOf[InPolygonJoinRangeListUDF]
  }

  object CarbonExtractEquiJoinKeys {
    def unapply(plan: LogicalPlan): Option[(JoinType, Seq[Expression], Seq[Expression],
      Option[Expression], LogicalPlan, LogicalPlan)] = {
      plan match {
        case join: Join =>
          ExtractEquiJoinKeys.unapply(join) match {
              // TODO: Spark is using hints now, carbon also should use join hints
            case Some(x) => Some(x._1, x._2, x._3, x._4, x._5, x._6)
            case None => None
          }
        case _ => None
      }
    }
  }

  /**
   * Return true if driver-side count star optimization can be used.
   * Following case can't use driver-side count star:
   * 1. There is data update and delete
   * 2. It is streaming table
   */
  private def driverSideCountStar(logicalRelation: LogicalRelation): Boolean = {
    val relation = logicalRelation.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
    val segmentUpdateStatusManager = new SegmentUpdateStatusManager(
      relation.carbonRelation.carbonTable)
    val updateDeltaMetadata = segmentUpdateStatusManager.readLoadMetadata()
    val hasNonCarbonSegment =
      segmentUpdateStatusManager.getLoadMetadataDetails.exists(!_.isCarbonFormat)
    if (hasNonCarbonSegment || updateDeltaMetadata != null && updateDeltaMetadata.nonEmpty) {
      false
    } else if (relation.carbonTable.isStreamingSink) {
      false
    } else {
      true
    }
  }

  private def isCarbonPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(_, _,
      MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case Filter(_, MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case _ => false
    }
  }

  private def isLeftSemiExistPushDownEnabled: Boolean = {
    CarbonProperties.getInstance.getProperty(
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER,
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER_DEFAULT).toBoolean
  }

  private def isAllCarbonPlan(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    allRelations.forall(x => x.relation.isInstanceOf[CarbonDatasourceHadoopRelation])
  }


  object ExtractTakeOrderedAndProjectExec {

    def unapply(plan: LogicalPlan): Option[CarbonTakeOrderedAndProjectExec] = {
      val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
      // push down order by limit to carbon map task,
      // only when there are only one CarbonDatasourceHadoopRelation
      if (allRelations.size != 1 ||
        allRelations.exists(x => !x.relation.isInstanceOf[CarbonDatasourceHadoopRelation])) {
        return None
      }
      //  check and Replace TakeOrderedAndProject (physical plan node for order by + limit)
      //  with CarbonTakeOrderedAndProjectExec.
      val relation = allRelations.head.relation.asInstanceOf[CarbonDatasourceHadoopRelation]
      plan match {
        case ReturnAnswer(rootPlan) => rootPlan match {
          case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
            carbonTakeOrder(relation, limit,
              order,
              child.output,
              planLater(pushLimit(limit, child)))
          case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
            carbonTakeOrder(relation, limit, order, projectList, planLater(pushLimit(limit, child)))
          case _ => None
        }
        case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
          carbonTakeOrder(relation, limit, order, child.output, planLater(pushLimit(limit, child)))
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
          carbonTakeOrder(relation, limit, order, projectList, planLater(pushLimit(limit, child)))
        case _ => None
      }
    }

    private def carbonTakeOrder(relation: CarbonDatasourceHadoopRelation,
        limit: Int,
        orders: Seq[SortOrder],
        projectList: Seq[NamedExpression],
        child: SparkPlan): Option[CarbonTakeOrderedAndProjectExec] = {
      val latestOrder = orders.last
      val fromHead: Boolean = latestOrder.direction match {
        case Ascending => true
        case Descending => false
      }
      val (columnName, canPushDown) = latestOrder.child match {
        case attr: AttributeReference => (attr.name, true)
        case Alias(AttributeReference(name, _, _, _), _) => (name, true)
        case _ => (null, false)
      }
      val mapOrderPushDown = CarbonProperties.getInstance.getProperty(
        CarbonCommonConstants.CARBON_MAP_ORDER_PUSHDOWN + "." +
          s"${ relation.carbonTable.getTableUniqueName.toLowerCase(Locale.ROOT) }.column")
      // when this property is enabled and order by column is in sort column,
      // enable limit push down to map task, row scanner can use this limit.
      val sortColumns = relation.carbonTable.getSortColumns
      if (mapOrderPushDown != null && canPushDown && sortColumns.size() > 0
        && sortColumns.get(0).equalsIgnoreCase(columnName)
        && mapOrderPushDown.equalsIgnoreCase(columnName)) {
        // Replace TakeOrderedAndProject (which comes after physical plan with limit and order by)
        // with CarbonTakeOrderedAndProjectExec.
        // which will skip the order at map task as column data is already sorted
        Some(CarbonTakeOrderedAndProjectExec(limit, orders, projectList, child, skipMapOrder =
          true, readFromHead = fromHead))
      } else {
        None
      }
    }

    def pushLimit(limit: Int, plan: LogicalPlan): LogicalPlan = {
      val newPlan = plan transform {
        case lr: LogicalRelation =>
          val relation = lr.relation
            .asInstanceOf[CarbonDatasourceHadoopRelation]
          relation.setLimit(limit)
          val newRelation = lr.copy(relation = relation)
          newRelation
        case other => other
      }
      newPlan
    }
  }
}

/**
 * This class will be used when Union node is present in plan with multiple inserts.
 * It is a physical operator that executes the run method of a RunnableCommand and
 * saves the result to prevent multiple executions.
 */
case class UnionCommandExec(cmd: RunnableCommand) extends LeafExecNode {

  protected[sql] lazy val sideEffectResult: Seq[InternalRow] = {
    val converter = CatalystTypeConverters.createToCatalystConverter(schema)
    val internalRow = cmd.run(sqlContext.sparkSession).map(converter(_).asInstanceOf[InternalRow])
    val unsafeProjection = UnsafeProjection.create(output.map(_.dataType).toArray)
    // To make GenericInternalRow to UnsafeRow
    val row = unsafeProjection(internalRow.head)
    Seq(row)
  }

  override def output: Seq[Attribute] = cmd.output

  protected override def doExecute(): RDD[InternalRow] = {
    sqlContext.sparkContext.parallelize(sideEffectResult, 1)
  }
}

