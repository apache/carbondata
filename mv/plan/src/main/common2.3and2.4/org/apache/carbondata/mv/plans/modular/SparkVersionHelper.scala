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

package org.apache.carbondata.mv.plans.modular

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSeq, Expression, ExprId, NamedExpression, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, CollapseProject, CollapseRepartition, CollapseWindow, ColumnPruning, CombineFilters, CombineLimits, CombineUnions, ConstantFolding, EliminateOuterJoin, EliminateSerialization, EliminateSorts, FoldablePropagation, NullPropagation, PushDownPredicate, PushPredicateThroughJoin, PushProjectionThroughUnion, RemoveDispensableExpressions, RemoveRedundantAliases, RemoveRedundantProject, ReorderAssociativeOperator, ReorderJoin, RewriteCorrelatedScalarSubquery, SimplifyBinaryComparison, SimplifyCaseConversionExpressions, SimplifyCasts, SimplifyConditionals}
import org.apache.spark.sql.catalyst.plans.{logical, JoinType, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, LogicalPlan, Statistics, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, Metadata}

import org.apache.carbondata.mv.plans.util.BirdcageOptimizer


object SparkVersionHelper {

  def getStatisticsObj(outputList: Seq[NamedExpression],
      plan: LogicalPlan, stats: Statistics,
      aliasMap: Option[AttributeMap[Attribute]] = None): Statistics = {
    val output = outputList.map(_.toAttribute)
    val mapSeq = plan.collect { case n: logical.LeafNode => n }.map {
      table => AttributeMap(table.output.zip(output))
    }
    val rewrites = mapSeq.head
    val attributes: AttributeMap[ColumnStat] = stats.attributeStats
    var attributeStats = AttributeMap(attributes.iterator
      .map { pair => (rewrites(pair._1), pair._2) }.toSeq)
    if (aliasMap.isDefined) {
      attributeStats = AttributeMap(
        attributeStats.map(pair => (aliasMap.get(pair._1), pair._2)).toSeq)
    }
    Statistics(stats.sizeInBytes, stats.rowCount, attributeStats, stats.hints)
  }

  def getOptimizedPlan(s: SubqueryExpression): LogicalPlan = {
    val Subquery(newPlan) = BirdcageOptimizer.execute(Subquery(s.plan))
    newPlan
  }

  def normalizeExpressions(r: NamedExpression, attrs: AttributeSeq): NamedExpression = {
    QueryPlan.normalizeExprId(r, attrs)
  }

  def attributeMap(rAliasMap: AttributeMap[Attribute]) : AttributeMap[Expression] = {
    rAliasMap.asInstanceOf[AttributeMap[Expression]]
  }

  def seqOfRules : Seq[Rule[LogicalPlan]] = {
    Seq(
      // Operator push down
      PushProjectionThroughUnion,
      ReorderJoin,
      EliminateOuterJoin,
      PushPredicateThroughJoin,
      PushDownPredicate,
      ColumnPruning,
      // Operator combine
      CollapseRepartition,
      CollapseProject,
      CollapseWindow,
      CombineFilters,
      CombineLimits,
      CombineUnions,
      // Constant folding and strength reduction
      NullPropagation,
      FoldablePropagation,
      ConstantFolding,
      ReorderAssociativeOperator,
      // No need to apply LikeSimplification rule while creating MV
      // as modular plan asCompactSql will be set in schema
      //        LikeSimplification,
      BooleanSimplification,
      SimplifyConditionals,
      RemoveDispensableExpressions,
      SimplifyBinaryComparison,
      EliminateSorts,
      SimplifyCasts,
      SimplifyCaseConversionExpressions,
      RewriteCorrelatedScalarSubquery,
      EliminateSerialization,
      RemoveRedundantAliases,
      RemoveRedundantProject)
  }
}

trait GetVerboseString extends LeafNode {
}

trait GroupByUnaryNode extends UnaryNode {
}

trait SelectModularPlan extends ModularPlan {
}

trait UnionModularPlan extends ModularPlan {
}

trait OneRowTableLeafNode extends LeafNode {
}

object MatchJoin {
  def unapply(plan : LogicalPlan): Option[(LogicalPlan, LogicalPlan, JoinType, Option[Expression],
    Option[Any])] = {
    plan match {
      case j@Join(left, right, joinType, condition) =>
        Some(left, right, joinType, condition, None)
      case _ => None
    }
  }
}

object MatchAggregateExpression {
  def unapply(expr : AggregateExpression): Option[(AggregateFunction, AggregateMode, Boolean,
    Option[Expression], ExprId)] = {
    expr match {
      case j@AggregateExpression(aggregateFunction, mode, isDistinct, resultId) =>
        Some(aggregateFunction, mode, isDistinct, None, resultId)
      case _ => None
    }
  }
}
