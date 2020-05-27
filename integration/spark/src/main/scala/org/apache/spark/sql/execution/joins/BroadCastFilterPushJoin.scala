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

import java.util

import scala.Array.canBuildFrom
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, BindReferences, BoundReference, Expression, In, Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, BroadcastMode, Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.strategy.CarbonDataSourceScan
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.types.{LongType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.rdd.CarbonScanRDD

case class BroadCastFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends CarbonBroadCastFilterPushJoin {

  override lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private lazy val (input: Array[InternalRow], inputCopy: Array[InternalRow]) = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }
    // Here for CarbonPlan, 2 spark plans are wrapped so that it can be broadcasted
    // 1. BroadcastExchange
    // 2. BroadcastExchangeExec
    // Both the relations to be removed to execute and get the output
    val buildPlanOutput = getBuildPlan

    val input: Array[InternalRow] = buildPlanOutput.map(_.copy()).collect()
    val inputCopy: Array[InternalRow] = input.clone()
    (input, inputCopy)
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(buildKeys)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  private lazy val carbonScan = buildSide match {
    case BuildLeft => right
    case BuildRight => left
  }

  override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }
    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    BroadCastFilterPushJoin.addInFilterToPlan(buildPlan,
      carbonScan,
      inputCopy,
      leftKeys,
      rightKeys,
      buildSide)
    val streamedPlanOutput = streamedPlan.execute()
    // scalastyle:off
    // scalastyle:on
    performJoinOperation(sparkContext, streamedPlanOutput, broadcastRelation, numOutputRows)
  }
}

object BroadCastFilterPushJoin {

  def addInFilterToPlan(buildPlan: SparkPlan,
      carbonScan: SparkPlan,
      inputCopy: Array[InternalRow],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      buildSide: BuildSide,
      isIndexTable: Boolean = false): Unit = {
    val LOGGER = if (isIndexTable) {
      LogServiceFactory.getLogService(BroadCastSIFilterPushJoin.getClass.getName)
    } else {
      LogServiceFactory.getLogService(BroadCastFilterPushJoin.getClass.getName)
    }
    val keys = {
      buildSide match {
        case BuildLeft => (leftKeys)
        case BuildRight => (rightKeys)
      }
    }.map { a =>
      BindReferences.bindReference(a, buildPlan.output)
    }.toArray

    val filters = keys.map {
      k =>
        inputCopy.map(
          r => {
            val curr = k.eval(r)
            curr match {
              case _: UTF8String => Literal(curr.toString).asInstanceOf[Expression]
              case _: Long if k.dataType.isInstanceOf[TimestampType] =>
                Literal(curr, TimestampType).asInstanceOf[Expression]
              case _ => Literal(curr).asInstanceOf[Expression]
            }
          })
    }

    val filterKey = (buildSide match {
      case BuildLeft => rightKeys
      case BuildRight => leftKeys
    }).collectFirst { case a: Attribute => a }

    def resolveAlias(expressions: Seq[Expression]) = {
      val aliasMap = new mutable.HashMap[Attribute, Expression]()
      carbonScan.transformExpressions {
        case alias: Alias =>
          aliasMap.put(alias.toAttribute, alias.child)
          alias
      }
      expressions.map {
        case at: AttributeReference =>
          // cannot use Map.get() as qualifier is different.
          aliasMap.find(_._1.semanticEquals(at)) match {
            case Some(child) => child._2
            case _ => at
          }
        case others => others
      }
    }

    val filterKeys = buildSide match {
      case BuildLeft =>
        resolveAlias(rightKeys)
      case BuildRight =>
        resolveAlias(leftKeys)
    }

    val tableScan = carbonScan.collectFirst {
      case ProjectExec(projectList, batchData: CarbonDataSourceScan)
        if (filterKey.isDefined && (isIndexTable || projectList.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        batchData
      case ProjectExec(projectList, rowData: RowDataSourceScanExec)
        if (filterKey.isDefined && (isIndexTable || projectList.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        rowData
      case batchData: CarbonDataSourceScan
        if (filterKey.isDefined && (isIndexTable || batchData.output.attrs.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        batchData
      case rowData: RowDataSourceScanExec
        if (filterKey.isDefined && (isIndexTable || rowData.output.exists(x =>
          x.name.equalsIgnoreCase(filterKey.get.name) &&
          x.exprId.id == filterKey.get.exprId.id &&
          x.exprId.jvmId.equals(filterKey.get.exprId.jvmId)))) =>
        rowData
    }
    val configuredFilterRecordSize = CarbonProperties.getInstance.getProperty(
      CarbonCommonConstants.BROADCAST_RECORD_SIZE,
      CarbonCommonConstants.DEFAULT_BROADCAST_RECORD_SIZE)

    if (tableScan.isDefined && null != filters
        && filters.length > 0
        && ((filters(0).length > 0 && filters(0).length <= configuredFilterRecordSize.toInt) ||
            isIndexTable)) {
      LOGGER.info("Pushing down filter for broadcast join. Filter size:" + filters(0).length)
      tableScan.get match {
        case scan: CarbonDataSourceScan =>
          pushdownFilterToCarbonRDD(scan.rdd,
            addPushdownFilters(filterKeys, filters))
        case _ =>
          pushdownFilterToCarbonRDD(tableScan.get.asInstanceOf[RowDataSourceScanExec].rdd,
            addPushdownFilters(filterKeys, filters))
      }
    } else {
      // in filter has no values, set an empty splits list to CarbonScanRDD
      if (tableScan.isDefined &&
          null != filters &&
          filters.length > 0 &&
          filters(0).length == 0) {
        tableScan.get match {
          case scan: CarbonDataSourceScan =>
            setCarbonRDDWithEmptySplits(scan.rdd)
          case _ =>
            setCarbonRDDWithEmptySplits(tableScan.get.asInstanceOf[RowDataSourceScanExec].rdd)
        }
      } else {
        LOGGER.info("Not push down filter for broadcast join.")
      }
    }
  }

  private def pushdownFilterToCarbonRDD(rdd: RDD[InternalRow],
      expressions: Seq[Expression]): Unit = {
    rdd match {
      case value: CarbonScanRDD[InternalRow] =>
        if (expressions.nonEmpty) {
          val expressionVal = CarbonFilters
            .transformExpression(CarbonFilters.preProcessExpressions(expressions).head)
          if (null != expressionVal) {
            value.setFilterExpression(expressionVal)
          }
        }
      case _ =>
    }
  }

  private def setCarbonRDDWithEmptySplits(rdd: RDD[InternalRow]): Unit = {
    rdd match {
      case value: CarbonScanRDD[InternalRow] =>
          value.splits = new util.ArrayList[InputSplit](0)
      case _ =>
    }
  }

  private def addPushdownFilters(keys: Seq[Expression],
      filters: Array[Array[Expression]]): Seq[Expression] = {

    // TODO Values in the IN filter is duplicate. replace the list with set
    val buffer = new ArrayBuffer[Expression]
    keys.zipWithIndex.foreach { a =>
      buffer += In(a._1, filters(a._2)).asInstanceOf[Expression]
    }

    // Let's not pushdown condition. Only filter push down is sufficient.
    // Conditions can be applied on hash join result.
    val cond = if (buffer.size > 1) {
      val e = buffer.remove(0)
      buffer.fold(e)(And)
    } else {
      buffer.asJava.get(0)
    }
    Seq(cond)
  }
}

/**
 * unapply method of BroadcastExchangeExec
 */
object CarbonBroadcastExchangeExec {
  def unapply(plan: SparkPlan): Option[(BroadcastMode, SparkPlan)] = {
    plan match {
      case cExec: BroadcastExchangeExec =>
        Some(cExec.mode, cExec.child)
      case _ => None
    }
  }
}
