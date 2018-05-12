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

package org.apache.carbondata.mv.expressions.modular

import org.apache.spark.sql.catalyst.expressions.{AttributeSeq, AttributeSet, Expression, ExprId, LeafExpression, NamedExpression, OuterReference, PlanExpression, Predicate, Unevaluable}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.types._

import org.apache.carbondata.mv.plans.modular.ModularPlan

/**
 * A base interface for expressions that contain a [[ModularPlan]].
 */
abstract class ModularSubquery(
    plan: ModularPlan,
    children: Seq[Expression],
    exprId: ExprId) extends PlanExpression[ModularPlan] {
  override lazy val resolved: Boolean = childrenResolved && plan.resolved
  override lazy val references: AttributeSet =
    if (plan.resolved) {
      super.references -- plan.outputSet
    } else {
      super.references
    }

  override def withNewPlan(plan: ModularPlan): ModularSubquery

  override def semanticEquals(o: Expression): Boolean = {
    o match {
      case p: ModularSubquery =>
        this.getClass.getName.equals(p.getClass.getName) && plan.sameResult(p.plan) &&
        children.length == p.children.length &&
        children.zip(p.children).forall(p => p._1.semanticEquals(p._2))
      case _ => false
    }
  }

  def canonicalize(attrs: AttributeSeq): ModularSubquery = {
    // Normalize the outer references in the subquery plan.
    val normalizedPlan = plan.transformAllExpressions {
      case OuterReference(r) => OuterReference(QueryPlan.normalizeExprId(r, attrs))
    }
    withNewPlan(normalizedPlan).canonicalized.asInstanceOf[ModularSubquery]
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
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

object ScalarModularSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case s: ScalarModularSubquery => s.children.nonEmpty
      case _ => false
    }.isDefined
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

  override def toString: String = s"modular-list#${ exprId.id } $conditionString"

  override lazy val canonicalized: Expression = {
    ModularListQuery(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
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

  override def toString: String = s"modular-exists#${ exprId.id } $conditionString"

  override lazy val canonicalized: Expression = {
    ModularExists(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}

/**
 * A place holder for generated SQL for subquery expression.
 */
case class SubqueryHolder(override val sql: String) extends LeafExpression with Unevaluable {
  override def dataType: DataType = NullType

  override def nullable: Boolean = true
}
