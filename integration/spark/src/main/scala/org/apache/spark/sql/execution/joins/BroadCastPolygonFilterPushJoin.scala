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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, BindReferences, Expression, JoinedRow, Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.{BinaryExecNode, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.BroadCastPolygonFilterPushJoin.addPolygonRangeListFilterToPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.index.IndexFilter
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.geo.{GeoConstants, GeoUtils}
import org.apache.carbondata.geo.scan.expression.PolygonRangeListExpression
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class BroadCastPolygonFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan
) extends BinaryExecNode {

  // BuildSide will be BuildRight
  protected lazy val (buildPlan, streamedPlan) = (right, left)

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  // execute the polygon table sparkPlan and collect data rows
  private lazy val inputCopy: Array[InternalRow] = {
    getBuildPlan.map(_.copy()).collect().clone()
  }

  @transient private lazy val boundCondition: InternalRow => Boolean = {
    // get the join condition
    if (condition.isDefined) {
      CarbonToSparkAdapter.getPredicate(streamedPlan.output ++ buildPlan.output, condition)
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
    // buildPlan will be the polygon table plan. Match the buildPlan and return the result of
    // this query as an RDD[InternalRow]
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

  override def output: Seq[Attribute] = left.output ++ right.output
}

object BroadCastPolygonFilterPushJoin {

  /**
   * This method will add the polygon range list filter to the spatial table scan
   * @param buildPlan polygon table spark plan
   * @param streamedPlan spatial table spark plan
   * @param inputCopy polygon table data rows
   * @param condition in_polygon_join expression
   */
  def addPolygonRangeListFilterToPlan(buildPlan: SparkPlan,
      streamedPlan: SparkPlan,
      inputCopy: Array[InternalRow],
      condition: Option[Expression]): Unit = {

    // get the polygon column from the in_polygon_join join condition
    val children = condition.get.asInstanceOf[ScalaUDF].children
    val polygonExpression = children(1)

    // evaluate and get the polygon data rows from polygon table InternalRows
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

    // get the spatial table scan
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

    // add filter to spatial table scan
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
        // prepare Polygon Range List filter
        if (polygonFilter.nonEmpty) {
          // get the GeoHandler custom instance
          val (columnName, instance) = GeoUtils.getGeoHashHandler(table
            .getTableInfo.getFactTable.getTableProperties.asScala)
          var inputPolygonRanges = new ArrayBuffer[String]
          // remove NULL values in the polygon range list and convert list of ranges to string
          polygonFilter.map { expression =>
            val range: String = expression.asInstanceOf[Literal].value.toString
            inputPolygonRanges += range
            expression
          }
          inputPolygonRanges = inputPolygonRanges.filterNot(range =>
            range.equalsIgnoreCase("NULL") ||
            range.equalsIgnoreCase("'null'"))
          val polygonRanges = inputPolygonRanges.mkString("\\,")
          // get the PolygonRangeListExpression for input polygon range
          val expressionVal = if (polygonRanges.toLowerCase.startsWith(GeoConstants.RANGE_LIST)) {
            new PolygonRangeListExpression(polygonRanges,
              "OR",
              columnName,
              instance)
          } else {
            new PolygonRangeListExpression(polygonRanges,
              "OR",
              columnName,
              instance,
              false,
              inputPolygonRanges.asJava)
          }
          // set the filter as PolygonRangeListExpression
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
    // if plan contains BroadcastExchange, get the sparkPlan
    plan match {
      case cExe: BroadcastExchangeExec =>
        Some(cExe.mode, cExe.child)
      case _ => None
    }
  }
}
