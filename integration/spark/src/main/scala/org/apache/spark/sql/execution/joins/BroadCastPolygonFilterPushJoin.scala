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

package org.apache.spark.sql.execution.joins

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, JoinedRow, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{BinaryExecNode, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadCastPolygonFilterPushJoin.addPolygonRangeListFilterToPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.{IndexFilter, Segment}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.geo.{GeoConstants, GeoUtils}
import org.apache.carbondata.geo.scan.expression.PolygonRangeListExpression
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class BroadCastPolygonFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan
) extends BinaryExecNode with HashJoin {

  override protected lazy val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private lazy val inputCopy: Array[InternalRow] = {
    getBuildPlan.map(_.copy()).collect().clone()
  }

  lazy val spatialTableRDD: Option[RDD[InternalRow]] = streamedPlan.collectFirst {
    case scan: CarbonDataSourceScan => scan.inputRDDs().head
  }

  @transient private lazy val boundCondition: InternalRow => Boolean = {
    if (condition.isDefined) {
      newPredicate(condition.get, streamedPlan.output ++ buildPlan.output).eval _
    } else {
      (_: InternalRow) => true
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    // get polygon rangeList from polygon table and add as IN_POLYGON_RANGE_LIST filter to the
    // spatial table
    addPolygonRangeListFilterToPlan(buildPlan, streamedPlan, inputCopy, condition)
    // inner join spatial and polygon plan by applying in_polygon join filter
    streamedPlan.execute().mapPartitionsInternal {
      streamedIter =>
        // get polygon table data rows
        val buildRows = inputCopy
        val joinedRow = new JoinedRow

        streamedIter.flatMap { streamedRow =>
          val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
          // apply in_polygon_join filter
          if (condition.isDefined) {
            joinedRows.filter(boundCondition)
          } else {
            joinedRows
          }
        }
    }
  }

  protected def getBuildPlan: RDD[InternalRow] = {
    buildPlan match {
      case c@CarbonBroadCastExchangeExec(_, _) =>
        c.asInstanceOf[BroadcastExchangeExec].child.execute()
      case ReusedExchangeExec(_, c@CarbonBroadCastExchangeExec(_, _)) =>
        c.asInstanceOf[BroadcastExchangeExec].child.execute()
      case _ => buildPlan.children.head match {
        case c@CarbonBroadCastExchangeExec(_, _) =>
          c.asInstanceOf[BroadcastExchangeExec].child.execute()
        case ReusedExchangeExec(_, c@CarbonBroadCastExchangeExec(_, _)) =>
          c.asInstanceOf[BroadcastExchangeExec].child.execute()
        case _ => buildPlan.execute()
      }
    }
  }
}

object BroadCastPolygonFilterPushJoin {

  def addPolygonRangeListFilterToPlan(buildPlan: SparkPlan,
      streamedPlan: SparkPlan,
      inputCopy: Array[InternalRow],
      condition: Option[Expression]): Unit = {

    val children = condition.get.asInstanceOf[ScalaUDF].children
    val polygonExpression = children(1)

    val keys = polygonExpression.map { a =>
      BindReferences.bindReference(a, buildPlan.output)
    }.toArray

    val filters = keys.map {
      k =>
        inputCopy.map(
          r => {
            val curr = k.eval(r)
            curr match {
              case _: UTF8String => Literal(curr.toString).asInstanceOf[Expression]
              case _ => Literal(curr).asInstanceOf[Expression]
            }
          })
    }

    val tableScan = streamedPlan.collectFirst {
      case ProjectExec(_, batchData: CarbonDataSourceScan) =>
        batchData
      case ProjectExec(_, rowData: RowDataSourceScanExec) =>
        rowData
      case batchData: CarbonDataSourceScan =>
        batchData
      case rowData: RowDataSourceScanExec =>
        rowData
    }
    val configuredFilterRecordSize = CarbonProperties.getInstance.getProperty(
      CarbonCommonConstants.BROADCAST_RECORD_SIZE,
      CarbonCommonConstants.DEFAULT_BROADCAST_RECORD_SIZE)

    if (tableScan.isDefined && null != filters
        && filters.length > 0
        && (filters(0).length > 0 && filters(0).length <= configuredFilterRecordSize.toInt)) {
      tableScan.get match {
        case scan: CarbonDataSourceScan =>
          addPushDownToCarbonRDD(scan.inputRDDs().head, filters(0), scan.relation.carbonTable)
        case _ =>
      }
    }
  }

  private def addPushDownToCarbonRDD(rdd: RDD[InternalRow],
      polygonFilter: Array[Expression], table: CarbonTable): Unit = {
    rdd match {
      case value: CarbonScanRDD[InternalRow] =>
        if (polygonFilter.nonEmpty) {
          val (columnName, instance) = GeoUtils.getGeoHashHandler(table
            .getTableInfo.getFactTable.getTableProperties.asScala)
          var inputPolygonRanges = new ArrayBuffer[String]
          polygonFilter.map { expression =>
            val range: String = expression.asInstanceOf[Literal].value.toString
            inputPolygonRanges += range
            expression
          }
          inputPolygonRanges = inputPolygonRanges.filterNot(range =>
            range.equalsIgnoreCase("NULL") ||
            range.equalsIgnoreCase("'null'"))
          val polygonOrRanges = inputPolygonRanges.mkString("\\,")
          val expressionVal = if (polygonOrRanges.toLowerCase.startsWith(GeoConstants.RANGE_LIST)) {
            new PolygonRangeListExpression(polygonOrRanges,
              "OR",
              columnName,
              instance)
          } else {
            new PolygonRangeListExpression(polygonOrRanges,
              "OR",
              columnName,
              instance,
              false,
              inputPolygonRanges.asJava)
          }
          if (null != expressionVal) {
            val filter = new IndexFilter(table, expressionVal)
            value.indexFilter = filter
          }
        }
      case _ =>
    }
  }
}

object CarbonBroadCastExchangeExec {
  def unapply(plan: SparkPlan): Option[(BroadcastMode, SparkPlan)] = {
    plan match {
      case cExe: BroadcastExchangeExec =>
        Some(cExe.mode, cExe.child)
      case _ => None
    }
  }
}
