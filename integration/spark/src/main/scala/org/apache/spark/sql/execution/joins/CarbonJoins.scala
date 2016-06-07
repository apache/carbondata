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

import scala.concurrent._
import scala.concurrent.duration._
import scala.Array.canBuildFrom

import org.apache.spark.{InternalAccumulator, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonTableScan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, Literal}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.ThreadUtils

case class BroadCastFilterPushJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan,
    condition: Option[Expression]) extends BinaryNode with HashJoin {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }
  private lazy val (input: Array[InternalRow], inputCopy: Array[InternalRow]) = {
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }
    val buildPlanOutput = buildPlan.execute()
    val input: Array[InternalRow] = buildPlanOutput.map(_.copy()).collect()
    val inputCopy: Array[InternalRow] = buildPlanOutput.map(_.copy()).collect()
    (input, inputCopy)
  }
  // Use lazy so that we won't do broadcast when calling explain but still cache the broadcast value
  // for the same query.
  @transient
  private lazy val broadcastFuture = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // The following line doesn't run in a job so we cannot track the metric value. However, we
        // have already tracked it in the above lines. So here we can use
        // `SQLMetrics.nullLongMetric` to ignore it.
        val hashed = HashedRelation(
          input.iterator, SQLMetrics.nullLongMetric, buildSideKeyGenerator, input.size)
        sparkContext.broadcast(hashed)
      }
    }(BroadCastFilterPushJoin.broadcastHashJoinExecutionContext)
  }

  override def doExecute(): RDD[InternalRow] = {

    val numOutputRows = longMetric("numOutputRows")
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }

    val keys = buildKeys.map { a =>
      BindReferences.bindReference(a, buildPlan.output)
    }.toArray
    val filters = keys.map {
      k =>
        inputCopy.map(
          r => {
            val curr = k.eval(r)
            if (curr.isInstanceOf[UTF8String]) {
              Literal(curr.toString).asInstanceOf[Expression]
            } else {
              Literal(curr).asInstanceOf[Expression]
            }
          })
    }
    val carbonScan = buildSide match {
      case BuildLeft => right
      case BuildRight => left
    }

    val cubeScan = carbonScan.collectFirst { case a: CarbonTableScan => a }
    if (cubeScan.isDefined) {
      cubeScan.get.addPushdownFilters(streamedKeys, filters, condition)
    }

    val streamedPlanOutput = streamedPlan.execute()
    // scalastyle:off
    val broadcastRelation = Await.result(broadcastFuture, timeout)
    // scalastyle:on
    streamedPlanOutput.mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value
      hashedRelation match {
        case unsafe: UnsafeHashedRelation =>
          TaskContext.get().internalMetricsToAccumulators(
            InternalAccumulator.PEAK_EXECUTION_MEMORY).add(unsafe.getUnsafeSize)
        case _ =>
      }
      hashJoin(streamedIter, numStreamedRows, hashedRelation, numOutputRows)
    }

  }
}

object BroadCastFilterPushJoin {

  private[joins] val broadcastHashJoinExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("filterpushhash-join", 128))
}
