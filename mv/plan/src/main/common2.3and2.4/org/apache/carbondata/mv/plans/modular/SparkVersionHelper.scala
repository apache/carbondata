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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSeq, Exists, Expression, ExprId, ListQuery, NamedExpression, Predicate, SubqueryExpression, Unevaluable}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, CollapseProject, CollapseRepartition, CollapseWindow, ColumnPruning, CombineFilters, CombineLimits, CombineUnions, ConstantFolding, EliminateOuterJoin, EliminateSerialization, EliminateSorts, FoldablePropagation, NullPropagation, PushDownPredicate, PushPredicateThroughJoin, PushProjectionThroughUnion, RemoveDispensableExpressions, RemoveRedundantAliases, RemoveRedundantProject, ReorderAssociativeOperator, ReorderJoin, RewriteCorrelatedScalarSubquery, SimplifyBinaryComparison, SimplifyCaseConversionExpressions, SimplifyCasts, SimplifyConditionals}
import org.apache.spark.sql.catalyst.plans.{logical, JoinType, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Join, LogicalPlan, Statistics, Subquery}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, Metadata}

import org.apache.carbondata.mv.expressions.modular.ModularSubquery
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

trait ModularRelationHarmonizedRelation extends GetVerboseString {
}

trait MVModularRelation extends GetVerboseString {
}

trait MVModularizeLater extends LeafNode {
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

/**
 * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id = a.id)
 * }}}
 */
case class ModularExists(
                          plan: ModularPlan,
                          children: Seq[Expression] = Seq.empty,
                          exprId: ExprId = NamedExpression.newExprId)
  extends ModularSubquery(plan, children, exprId) with Predicate with Unevaluable {
  override def nullable: Boolean = false

  override def withNewPlan(plan: ModularPlan): ModularExists = copy(plan = plan)

  override def toString: String = s"modular-exists#${exprId.id} $conditionString"

  override lazy val canonicalized: Expression = {
    ModularExists(
      plan.canonicalizedDef,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

/**
 * A [[ListQuery]] expression defines the query which we want to search in an IN subquery
 * expression. It should and can only be used in conjunction with an IN expression.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 */
case class ModularListQuery(
                             plan: ModularPlan,
                             children: Seq[Expression] = Seq.empty,
                             exprId: ExprId = NamedExpression.newExprId)
  extends ModularSubquery(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = plan.schema.fields.head.dataType

  override def nullable: Boolean = false

  override def withNewPlan(plan: ModularPlan): ModularListQuery = copy(plan = plan)

  override def toString: String = s"modular-list#${exprId.id} $conditionString"

  override lazy val canonicalized: Expression = {
    ModularListQuery(
      plan.canonicalizedDef,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

/**
 * A subquery that will return only one row and one column. This will be converted into a physical
 * scalar subquery during planning.
 *
 * Note: `exprId` is used to have a unique name in explain string output.
 */
case class ScalarModularSubquery(
                                  plan: ModularPlan,
                                  children: Seq[Expression] = Seq.empty,
                                  exprId: ExprId = NamedExpression.newExprId)
  extends ModularSubquery(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = plan.schema.fields.head.dataType

  override def nullable: Boolean = true

  override def withNewPlan(plan: ModularPlan): ScalarModularSubquery = copy(plan = plan)

  override def toString: String = s"scalar-modular-subquery#${ exprId.id } $conditionString"

  override lazy val canonicalized: Expression = {
    ScalarModularSubquery(
      plan.canonicalizedDef,
      children.map(_.canonicalized),
      ExprId(0))
  }

  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case s: ScalarModularSubquery => s.children.nonEmpty
      case _ => false
    }.isDefined
  }
}
