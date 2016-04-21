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

import scala.Array.canBuildFrom

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonCubeScan
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, Literal}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.unsafe.types.UTF8String

case class FilterPushJoin(
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

  override def doExecute(): RDD[InternalRow] = {

    val numOutputRows = longMetric("numOutputRows")
    val (numBuildRows, numStreamedRows) = buildSide match {
      case BuildLeft => (longMetric("numLeftRows"), longMetric("numRightRows"))
      case BuildRight => (longMetric("numRightRows"), longMetric("numLeftRows"))
    }

    // Referred the doExecute method from ShuffeldedHashJoin & BroadcastHashJoin
    // TODO Need to implement this join through broadcast like BroadcastHashJoin

    val buildPlanOutput = buildPlan.execute()
    val input: Array[InternalRow] = buildPlanOutput.map(_.copy()).collect()
    val input2: Array[InternalRow] = buildPlanOutput.map(_.copy()).collect()

    val keys = buildKeys.map { a =>
      BindReferences.bindReference(a, buildPlan.output)
    }.toArray

    val filters = keys.map {
      k =>
        input.map(
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

    val cubeScan = carbonScan.collectFirst { case a: CarbonCubeScan => a }
    if (cubeScan.isDefined) {
      cubeScan.get.addPushdownFilters(streamedKeys, filters, condition)
    }

    val streamedPlanOutput = streamedPlan.execute()

    streamedPlanOutput.mapPartitions { streamedIter =>
      val hashed = HashedRelation(input2.iterator, numBuildRows, buildSideKeyGenerator)
      hashJoin(streamedIter, numStreamedRows, hashed, numOutputRows)
    }

  }
}
