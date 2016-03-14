package org.apache.spark.sql

import org.apache.hadoop.hive.ql.exec.spark.SparkPlan

import org.apache.spark.sql.catalyst.expressions.IntegerLiteral
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, Filter, Aggregate, Subquery, Limit, Sort, Join}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.hive.HiveContext


/**
  * This rule will get registed to unified context. This will be parrent for all logical plan related to select queries
  *
  * @author A00902717
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
    }
    else {
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