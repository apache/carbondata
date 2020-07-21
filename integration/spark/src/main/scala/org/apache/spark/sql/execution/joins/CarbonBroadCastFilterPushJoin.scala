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

import org.apache.spark.{SparkContext, TaskContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.BinaryExecNode
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ReusedExchangeExec}
import org.apache.spark.sql.execution.metric.SQLMetric

/**
 * CarbonBroadCastFilterPushJoin
 */
trait CarbonBroadCastFilterPushJoin extends BinaryExecNode with HashJoin {

  protected def performJoinOperation(sparkContext: SparkContext,
      streamedPlanOutput: RDD[InternalRow],
      broadcastRelation: Broadcast[HashedRelation],
      numOutputRows: SQLMetric): RDD[InternalRow] = {
    streamedPlanOutput.mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value.asReadOnlyCopy()
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashedRelation.estimatedSize)
      join(streamedIter, hashedRelation, numOutputRows)
    }
  }

  protected def getBuildPlan : RDD[InternalRow] = {
    buildPlan match {
      case b@CarbonBroadcastExchangeExec(_, _) => b.asInstanceOf[BroadcastExchangeExec].child
        .execute()
      case _ => buildPlan.children.head match {
        case a@CarbonBroadcastExchangeExec(_, _) => a.asInstanceOf[BroadcastExchangeExec].child
          .execute()
        case ReusedExchangeExec(_, broadcast@CarbonBroadcastExchangeExec(_, _)) =>
          broadcast.asInstanceOf[BroadcastExchangeExec].child.execute()
        case _ => buildPlan.execute
      }
    }
  }
}
