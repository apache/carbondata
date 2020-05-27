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

import org.apache.spark.sql.catalyst.plans.{ExistenceJoin, InnerLike, JoinType, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.joins.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.SparkSQLUtil

/**
 * Note: copy from spark org.apache.spark.sql.execution.SparkStrategies.JoinSelection
 */
object BroadcastJoinHelper {

  /**
   * Matches a plan whose output should be small enough to be used in broadcast join.
   */
  private def canBroadcast(plan: LogicalPlan): Boolean = {
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <=
        SparkSQLUtil.getSparkSession.conf.get(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD)
  }

  private def canBuildRight(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
    case _ => false
  }

  private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
    case _: InnerLike | RightOuter => true
    case _ => false
  }

  private def broadcastSide(
      canBuildLeft: Boolean,
      canBuildRight: Boolean,
      left: LogicalPlan,
      right: LogicalPlan): BuildSide = {

    def smallerSide =
      if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft

    if (canBuildRight && canBuildLeft) {
      // Broadcast smaller side base on its estimated physical size
      // if both sides have broadcast hint
      smallerSide
    } else if (canBuildRight) {
      BuildRight
    } else if (canBuildLeft) {
      BuildLeft
    } else {
      // for the last default broadcast nested loop join
      smallerSide
    }
  }

  def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : Boolean = {
    val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
    val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
    buildLeft || buildRight
  }

  def broadcastSideByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : BuildSide = {
    val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
    val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
    broadcastSide(buildLeft, buildRight, left, right)
  }

  def canBroadcastBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : Boolean = {
    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
    val buildRight = canBuildRight(joinType) && canBroadcast(right)
    buildLeft || buildRight
  }

  def broadcastSideBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan)
  : BuildSide = {
    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
    val buildRight = canBuildRight(joinType) && canBroadcast(right)
    broadcastSide(buildLeft, buildRight, left, right)
  }
}
