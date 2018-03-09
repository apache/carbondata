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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Exists, ListQuery, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode

import org.apache.carbondata.mv.expressions.modular._
import org.apache.carbondata.mv.plans._

abstract class GenericPattern[TreeType <: TreeNode[TreeType]] extends Logging {
  protected def modularizeLater(plan: LogicalPlan): TreeType

  def apply(plan: LogicalPlan): Seq[TreeType]
}

abstract class Modularizer[TreeType <: TreeNode[TreeType]] {
  def patterns: Seq[GenericPattern[TreeType]]

  // protected def modularizeLater(plan: LogicalPlan) = this.modularize(plan).next()

  def modularize(plan: LogicalPlan): Iterator[TreeType] = {
    val replaced = plan.transformAllExpressions {
      case s: ScalarSubquery =>
        if (s.children.isEmpty) {
          ScalarModularSubquery(
            modularize(s.plan).next.asInstanceOf[ModularPlan],
            s.children,
            s.exprId)
        } else {
          throw new UnsupportedOperationException(s"Expression $s doesn't canonicalized")
        }
      case l: ListQuery =>
        if (l.children.isEmpty) {
          ModularListQuery(modularize(l.plan).next.asInstanceOf[ModularPlan], l.children, l.exprId)
        } else {
          throw new UnsupportedOperationException(s"Expression $l doesn't canonicalized")
        }
      case e: Exists =>
        if (e.children.isEmpty) {
          ModularExists(modularize(e.plan).next.asInstanceOf[ModularPlan], e.children, e.exprId)
        } else {
          throw new UnsupportedOperationException(s"Expression $e doesn't canonicalized")
        }
      case o => o
    }
    //    val replaced = plan
    val mplans = modularizeCore(replaced)
    makeupAliasMappings(mplans)
  }

  private def modularizeCore(plan: LogicalPlan): Iterator[TreeType] = {
    // Collect modular plan candidates.
    val candidates = patterns.iterator.flatMap(_ (plan))

    // The candidates may contain placeholders marked as [[modularizeLater]],
    // so try to replace them by their child plans.
    val plans = candidates.flatMap { candidate =>
      val placeholders = collectPlaceholders(candidate)

      if (placeholders.isEmpty) {
        // Take the candidate as is because it does not contain placeholders.
        Iterator(candidate)
      } else {
        // Plan the logical plan marked as [[modularizeLater]] and replace the placeholders.
        placeholders.iterator.foldLeft(Iterator(candidate)) {
          case (candidatesWithPlaceholders, (placeholder, logicalPlan)) =>
            // Modularize the logical plan for the placeholder
            val childPlans = this.modularizeCore(logicalPlan)

            candidatesWithPlaceholders.flatMap { candidateWithPlaceholder =>
              childPlans.map { childPlan =>
                // Replace the placeholder by the child plan
                candidateWithPlaceholder.transformUp {
                  case p if p == placeholder => childPlan
                }
              }
            }
        }
      }
    }

    val pruned = prunePlans(plans)
    // val iter = patterns.view.flatMap(_(plan)).toIterator
    supports(
      pruned.hasNext,
      s"Modular plan not supported (e.g. has subquery expression) for \n$plan")
    //    makeupAliasMappings(pruned)
    pruned
  }

  /** Collects placeholders marked as [[modularizeLater]] by pattern and its [[LogicalPlan]]s */
  protected def collectPlaceholders(plan: TreeType): Seq[(TreeType, LogicalPlan)]

  /** Prunes bad plans to prevent combinatorial explosion. */
  protected def prunePlans(plans: Iterator[TreeType]): Iterator[TreeType]

  protected def makeupAliasMappings(plans: Iterator[TreeType]): Iterator[TreeType]

}
