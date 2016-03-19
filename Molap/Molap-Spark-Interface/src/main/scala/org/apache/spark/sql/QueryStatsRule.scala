/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, Limit, LogicalPlan, Project, Sort, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule

/**
  * This rule will get registed to unified context. This will be parrent for all logical plan related to select queries
  */
class QueryStatsRule extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {

    /**
      * aggregate sub queries need not be transformed as its already included
      */
    if ((plan.isInstanceOf[Aggregate]) && needToExclude(plan)) {
      plan
    }
    else {
      plan match {
        case Project(fields, child) => QueryStatsLogicalPlan(transformPlan(plan))
        case Filter(condition, child) => QueryStatsLogicalPlan(transformPlan(plan))
        case Aggregate(groupingExpressions, aggregateExpressions, child) => QueryStatsLogicalPlan(transformPlan(plan))
        case Sort(order, global, child) => QueryStatsLogicalPlan(transformPlan(plan))
        case Limit(limitExpr, child) => QueryStatsLogicalPlan(transformPlan(plan))
        case _ => plan
      }
    }
  }

  /**
    * ways to identify for aggregate query if they are already included is, if its child is filter,subquery or join
    */
  def needToExclude(plan: LogicalPlan): Boolean = {
    val children: Seq[LogicalPlan] = plan.children
    if (null != children && !children.isEmpty) {
      val child = children(0)
      child match {
        case Filter(condition, child) => true
        case Subquery(alias, child) => true
        case Join(left, right, joinType, condition) => true
        case Aggregate(groupingExpressions, aggregateExpressions, child) => true
        case _ => false
      }
    } else {
      false
    }
  }

  def transformPlan(plan: LogicalPlan): LogicalPlan = {

    val children: Seq[LogicalPlan] = plan.children
    plan transform {
      case QueryStatsLogicalPlan(child) => child
    }
  }
}

case class QueryStatsLogicalPlan(plan: LogicalPlan) extends LogicalPlan {
  override def children: Seq[LogicalPlan] = Seq(plan)

  override def output = plan.output

  def child = plan
}