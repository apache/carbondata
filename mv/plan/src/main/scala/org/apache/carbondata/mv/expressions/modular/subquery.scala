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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import org.apache.carbondata.mv.plans.modular.{ModularPlan, SparkVersionHelper}

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

  def canonicalize(attrs: AttributeSeq): ModularSubquery = {
    // Normalize the outer references in the subquery plan.
    val normalizedPlan = plan.transformAllExpressions {
      case OuterReference(r) => OuterReference(
        SparkVersionHelper.normalizeExpressions(r, attrs))
    }
    withNewPlan(normalizedPlan).canonicalized.asInstanceOf[ModularSubquery]
  }
}


/**
 * A place holder for generated SQL for subquery expression.
 */
case class SubqueryHolder(override val sql: String) extends LeafExpression with Unevaluable {
  override def dataType: DataType = NullType

  override def nullable: Boolean = true
}
