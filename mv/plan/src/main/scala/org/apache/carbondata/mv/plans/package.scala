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

package org.apache.carbondata.mv

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions.{AttributeSet, Expression, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.plans.util.{CheckSPJG, LogicalPlanSignatureGenerator, Signature}

/**
 * A a collection of common abstractions for query plans as well as
 * a base semantic plan representation.
 */
package object plans {
  @DeveloperApi
  type Pattern = org.apache.carbondata.mv.plans.modular.ModularPattern

  implicit class LogicalPlanUtils(val plan: LogicalPlan) {
    lazy val isSPJG: Boolean = CheckSPJG.isSPJG(plan)
    lazy val signature: Option[Signature] = LogicalPlanSignatureGenerator.generate(plan)
  }

  implicit class MorePredicateHelper(p: PredicateHelper) {
    def canEvaluate(expr: Expression, plan: ModularPlan): Boolean = {
      expr.references.subsetOf(plan.outputSet)
    }


    /**
     * If exp is a ScalaUDF, then for each of it's children, we have to check if
     * children can be derived from another scala UDF children from exprList
     * @param exp scalaUDF
     * @param exprList predicate and rejoin output list
     * @return if udf can be derived from another udf
     */
    def canEvaluate(exp: ScalaUDF, exprList: Seq[Expression]): Boolean = {
      var canBeDerived = false
      exprList.forall {
        case udf: ScalaUDF =>
          if (udf.children.length == exp.children.length) {
            if (udf.children.zip(exp.children).forall(e => e._1.sql.equalsIgnoreCase(e._2.sql))) {
              canBeDerived = true
            }
          }
          canBeDerived
        case _ =>
          canBeDerived
      }
    }

    def canEvaluate(expr: Expression, exprList: Seq[Expression]): Boolean = {
      expr match {
        case exp: ScalaUDF =>
          canEvaluate(exp, exprList)
        case _ =>
          expr.references.subsetOf(AttributeSet(exprList))
      }
    }
  }

  def supports(supported: Boolean, message: Any) {
    if (!supported) {
      throw new UnsupportedOperationException(s"unsupported operation: $message")
    }
  }
}
