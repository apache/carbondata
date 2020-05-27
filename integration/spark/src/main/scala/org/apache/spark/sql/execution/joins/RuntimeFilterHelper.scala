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

import org.apache.log4j.Logger
import org.apache.spark.sql.CarbonDatasourceHadoopRelation
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.{Inner, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.strategy.BroadcastJoinHelper
import org.apache.spark.sql.hive.MatchLogicalRelation
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.joins.BroadCastSIFilterPushJoin
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Runtime filter util
 */
object RuntimeFilterHelper {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  /**
   * enable RuntimeFilter by default
   */
  lazy val enabledRuntimeFilter: Boolean = {
    val conf = SparkSQLUtil.getSparkSession.conf.get(
      "spark.carbon.pushdown.join.as.filter", "true")
    conf.trim.equalsIgnoreCase("true")
  }

  /**
   * disable LeftSemi join push down by default
   */
  lazy val enabledLeftSemiExistPushDown: Boolean = {
    CarbonProperties.getInstance.getProperty(
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER,
      CarbonCommonConstants.CARBON_PUSH_LEFTSEMIEXIST_JOIN_AS_IN_FILTER_DEFAULT).toBoolean
  }

  /**
   * whether it can push down si query as a filter of carbon table query or not
   */
  def canPushDownSIAsFilter(carbonPlan: LogicalPlan, indexTable: LogicalPlan): Boolean = {
    enabledRuntimeFilter &&
    isCarbonPlan(carbonPlan) &&
    CarbonIndexUtil.checkIsIndexTable(indexTable)
  }

  /**
   * the left side of join is carbon table query
   * the right side of join is si table query
   * push si table query result as the filter of left side
   */
  def pushDownRSIAsFilter(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      left: LogicalPlan,
      carbon: SparkPlan,
      rightPlan: SparkPlan
  ): SparkPlan = {
    LOGGER.info(s"try to push down right side SI query as a filter for inner equi-join")
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
        .filterNot(attr => attr.name
          .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID))
        .map(c => (c.name.toLowerCase, c.dataType))
      val childOutput = carbonChild.output
        .filterNot(attr => attr.name
          .equalsIgnoreCase(CarbonCommonConstants.POSITION_ID))
        .map(c => (c.name.toLowerCase, c.dataType))
      if (!leftOutput.equals(childOutput)) {
        // if the projection list and the scan list are different(in case of alias)
        // we should not skip the project, so we are taking the original plan with project
        carbonChild = carbon
      }
    }
    val pushedDownJoin =
      BroadCastSIFilterPushJoin(
        leftKeys,
        rightKeys,
        Inner,
        BuildRight,
        carbonChild,
        rightPlan,
        condition)
    condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin)
  }

  /**
   * the left side of join is si table query
   * the right side of join is carbon table query
   * push si table query result as the filter of right side
   */
  def pushDownLSIAsFilter(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      leftPlan: SparkPlan,
      carbon: SparkPlan
  ): SparkPlan = {
    LOGGER.info(s"try to push down left side SI query as a filter for inner equi-join")
    val pushedDownJoin =
      BroadCastSIFilterPushJoin(
        leftKeys,
        rightKeys,
        Inner,
        BuildLeft,
        leftPlan,
        carbon,
        condition)
    condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin)
  }

  /**
   * whether the join can broadcast one side by hints
   */
  def canBroadcastByHints(left: LogicalPlan, right: LogicalPlan): Boolean = {
    enabledRuntimeFilter &&
    BroadcastJoinHelper.canBroadcastByHints(Inner, left, right)
  }

  /**
   * get broadcast side by hints
   */
  def broadcastSideByHints(left: LogicalPlan, right: LogicalPlan): Option[BuildSide] = {
    BroadcastJoinHelper.broadcastSideByHints(Inner, left, right) match {
      case BuildLeft if RuntimeFilterHelper.isCarbonPlan(right) =>
        Option(BuildLeft)
      case BuildRight if RuntimeFilterHelper.isCarbonPlan(left) =>
        Option(BuildRight)
      case _ => None
    }
  }

  /**
   * whether the join can broadcast one side by sizes
   */
  def canBroadcastBySizes(left: LogicalPlan, right: LogicalPlan): Boolean = {
    enabledRuntimeFilter &&
    BroadcastJoinHelper.canBroadcastBySizes(Inner, left, right)
  }

  /**
   * get broadcast side by sizes
   */
  def broadcastSideBySizes(left: LogicalPlan, right: LogicalPlan): Option[BuildSide] = {
    BroadcastJoinHelper.broadcastSideBySizes(Inner, left, right) match {
      case BuildLeft if RuntimeFilterHelper.isCarbonPlan(right) =>
        Option(BuildLeft)
      case BuildRight if RuntimeFilterHelper.isCarbonPlan(left) =>
        Option(BuildRight)
      case _ => None
    }
  }

  /**
   * push down build side as a filter of another side
   */
  def pushDownInnerJoin(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      buildSide: BuildSide,
      left: SparkPlan,
      right: SparkPlan): SparkPlan = {
    LOGGER.info(s"try to push down ${buildSide.getClass.getSimpleName} side query " +
                s"as a filter for inner equi-join")
    val pushedDownJoin = BroadCastFilterPushJoin(
      leftKeys,
      rightKeys,
      Inner,
      buildSide,
      left,
      right,
      condition)
    condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin)
  }

  /**
   * whether the join can push down right side as a filter of left side
   */
  def canPushDownLeftSemi(left: LogicalPlan, right: LogicalPlan): Boolean = {
    enabledLeftSemiExistPushDown && isAllCarbonPlan(left) && isAllCarbonPlan(right)
  }

  /**
   * push down right side as a filter of
   */
  def pushDownLeftSemiJoin(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan
  ): SparkPlan = {
    LOGGER.info(s"try to push down right side query as a filter for LeftSemi equi-join")
    val pushedDownJoin = BroadCastFilterPushJoin(
      leftKeys,
      rightKeys,
      LeftSemi,
      BuildRight,
      left,
      right,
      condition)
    condition.map(FilterExec(_, pushedDownJoin)).getOrElse(pushedDownJoin)
  }

  def isCarbonPlan(plan: LogicalPlan): Boolean = {
    plan match {
      case PhysicalOperation(_, _,
      MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case Filter(_, MatchLogicalRelation(_: CarbonDatasourceHadoopRelation, _, _)) =>
        true
      case _ => false
    }
  }

  def isAllCarbonPlan(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    allRelations.forall(x => x.relation.isInstanceOf[CarbonDatasourceHadoopRelation])
  }
}


