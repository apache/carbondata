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

package org.apache.carbondata.mv.testutil

import java.io.File

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.test.util.{PlanTest, QueryTest}

import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.{ModularPlan, OneRowTable, Select}
import org.apache.carbondata.mv.plans.modular.Flags._

/**
 * Provides helper methods for comparing plans.
 */
abstract class ModularPlanTest extends QueryTest with PredicateHelper {

  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: ModularPlan): plan.type = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(exprId = ExprId(0))
      case e: Exists =>
        e.copy(exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
    }
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   * ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   * - Join conditions will be resorted by hashCode.
   */
  protected def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case filter@Filter(condition: Expression, child: LogicalPlan) =>
        Filter(
          splitConjunctivePredicates(condition).map(rewriteEqual(_)).sortBy(_.hashCode())
            .reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)(true)
      case join@Join(left, right, joinType, condition) if condition.isDefined =>
        val newCondition =
          splitConjunctivePredicates(condition.get).map(rewriteEqual(_)).sortBy(_.hashCode())
            .reduce(And)
        Join(left, right, joinType, Some(newCondition))
    }
  }

  /**
   * Rewrite [[EqualTo]] and [[EqualNullSafe]] operator to keep order. The following cases will be
   * equivalent:
   * 1. (a = b), (b = a);
   * 2. (a <=> b), (b <=> a).
   */
  private def rewriteEqual(condition: Expression): Expression = {
    condition match {
      case eq@EqualTo(l: Expression, r: Expression) =>
        Seq(l, r).sortBy(_.hashCode()).reduce(EqualTo)
      case eq@EqualNullSafe(l: Expression, r: Expression) =>
        Seq(l, r).sortBy(_.hashCode()).reduce(EqualNullSafe)
      case _ => condition // Don't reorder.
    }
  }

  //
  //  /** Fails the test if the two plans do not match */
  //  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
  //    val normalized1 = normalizePlan(normalizeExprIds(plan1))
  //    val normalized2 = normalizePlan(normalizeExprIds(plan2))
  //    if (normalized1 != normalized2) {
  //      fail(
  //        s"""
  //          |== FAIL: Plans do not match ===
  //          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
  //         """.stripMargin)
  //    }
  //  }
  //
  //  /** Fails the test if the two expressions do not match */
  //  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
  //    comparePlans(Filter(e1, OneRowRelation), Filter(e2, OneRowRelation))
  //  }
  //
  //  /** Fails the test if the join order in the two plans do not match */
  //  protected def compareJoinOrder(plan1: LogicalPlan, plan2: LogicalPlan) {
  //    val normalized1 = normalizePlan(normalizeExprIds(plan1))
  //    val normalized2 = normalizePlan(normalizeExprIds(plan2))
  //    if (!sameJoinPlan(normalized1, normalized2)) {
  //      fail(
  //        s"""
  //           |== FAIL: Plans do not match ===
  //           |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
  //         """.stripMargin)
  //    }
  //  }
  //
  //  /** Consider symmetry for joins when comparing plans. */
  //  private def sameJoinPlan(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
  //    (plan1, plan2) match {
  //      case (j1: Join, j2: Join) =>
  //        (sameJoinPlan(j1.left, j2.left) && sameJoinPlan(j1.right, j2.right)) ||
  //          (sameJoinPlan(j1.left, j2.right) && sameJoinPlan(j1.right, j2.left))
  //      case (p1: Project, p2: Project) =>
  //        p1.projectList == p2.projectList && sameJoinPlan(p1.child, p2.child)
  //      case _ =>
  //        plan1 == plan2
  //    }
  //  }
  /** Fails the test if the corresponding pairs of plans do not match */
  protected def comparePlanCollections(planSet1: Seq[String], planSet2: Seq[String]) {
    for ((plan1, plan2) <- planSet1 zip planSet2) {
      compareMessages(plan1, plan2)
    }
  }

  /** Fails the test if the two plans do not match */
  /** Only expressionIds are normalized.  This is enough for our test cases */
  /** For more general normalization, see Spark PlanTest.scala for Logical Plan */
  protected def comparePlans(plan1: ModularPlan, plan2: ModularPlan) {
    val normalized1 = normalizeExprIds(plan1)
    val normalized2 = normalizeExprIds(plan2)
    if (normalized1 != normalized2) {
      fail(
        s"""
           |== FAIL: Plans do not match ===
           |${ sideBySide(normalized1.treeString, normalized1.treeString).mkString("\n") }
         """.stripMargin)
    }
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Seq[Expression], e2: Seq[Expression]): Unit = {
    comparePlans(
      Select(Nil, Nil, e1, Map.empty, Nil, Seq(OneRowTable), NoFlags, Seq.empty, Seq.empty), modular
        .Select(Nil, Nil, e2, Map.empty, Nil, Seq(OneRowTable), NoFlags, Seq.empty, Seq.empty))
  }

  protected def compareMessages(msg1: String, msg2: String) {
    if (msg1 != msg2) {
      fail(
        s"""
           |== FAIL: Messages do not match ==
           |${ sideBySide(msg1, msg2).mkString("\n") }
           """.stripMargin)
    }
  }
}
