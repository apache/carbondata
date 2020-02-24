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

import scala.collection._
import scala.collection.mutable.{HashMap, MultiMap}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{JoinType, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.trees.TreeNode

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.util.{Printers, Signature, SQLBuilder}

abstract class ModularPlan
  extends QueryPlan[ModularPlan]
    with AggregatePushDown
    with Logging
    with Serializable
    with PredicateHelper with Printers {

  /**
   * the first two are to support sub-query expressions
   */

  lazy val resolved: Boolean = expressions.forall(_.resolved) && childrenResolved

  def canonicalizedDef: ModularPlan = {
    canonicalized
  }

  def childrenResolved: Boolean = children.forall(_.resolved)

  private var statsCache: Option[Statistics] = None

  final def stats(spark: SparkSession): Statistics = {
    statsCache.getOrElse {
      statsCache = Some(computeStats(spark))
      statsCache.get
    }
  }

  final def invalidateStatsCache(): Unit = {
    statsCache = None
    children.foreach(_.invalidateStatsCache())
  }

  protected def computeStats(spark: SparkSession): Statistics = {
    //    spark.conf.set("spark.sql.cbo.enabled", true)
    val sqlStmt = asOneLineSQL
    val plan = spark.sql(sqlStmt).queryExecution.optimizedPlan
    plan.stats
  }

  override def fastEquals(other: TreeNode[_]): Boolean = {
    this.eq(other)
  }

  private var _rewritten: Boolean = false

  /**
   * Marks this plan as already rewritten.
   */
  private[mv] def setRewritten(): ModularPlan = {
    _rewritten = true
    children.foreach(_.setRewritten())
    this
  }

  /**
   * Returns true if this node and its children have already been gone through query rewrite.
   * Note this this is only an optimization used to avoid rewriting trees that have already
   * been rewritten, and can be reset by transformations.
   */
  def rewritten: Boolean = {
    _rewritten
  }

  private var rolledUp: Boolean = false

  /**
   * Marks this plan as rolledup plan
   */
  private[mv] def setRolledUp(): ModularPlan = {
    rolledUp = true
    children.foreach(_.setRolledUp())
    this
  }

  /**
   * Returns true if plan is rolledup
   */
  def isRolledUp: Boolean = {
    rolledUp
  }

  private var _skip: Boolean = false

  private[mv] def setSkip(): ModularPlan = {
    _skip = true
    children.foreach(_.setSkip())
    this
  }

  private[mv] def resetSkip(): ModularPlan = {
    _skip = false
    children.foreach(_.resetSkip())
    this
  }

  def skip: Boolean = _skip

  def isSPJGH: Boolean = {
    this match {
      case modular.Select(_, _, _, _, _,
        Seq(modular.GroupBy(_, _, _, _,
        sel_c2@modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)), _, _, _, _)
        if sel_c2.children.forall(_.isInstanceOf[modular.LeafNode]) => true

      case modular.GroupBy(_, _, _, _, sel_c2@modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)
        if sel_c2.children.forall(_.isInstanceOf[modular.LeafNode]) => true

      case modular.Select(_, _, _, _, _, children, _, _, _, _)
        if children.forall(_.isInstanceOf[modular.LeafNode]) => true

      case modular.GroupBy(_, _, _, _, modular.ModularRelation(_, _, _, _, _), _, _, _) => true

      case _ => false
    }
  }

  def signature: Option[Signature] = ModularPlanSignatureGenerator.generate(this)

  def createMutableAdjacencyList(
      edges: Seq[JoinEdge]
  ): mutable.HashMap[Int, mutable.Set[(Int, JoinType)]] with mutable.MultiMap[Int, (Int, JoinType)]
  = {
    val mm = new HashMap[Int, mutable.Set[(Int, JoinType)]] with MultiMap[Int, (Int, JoinType)]
    for (edge <- edges) { mm.addBinding(edge.left, (edge.right, edge.joinType)) }
    mm
  }

  def createImmutableAdjacencyList(edges: Seq[JoinEdge]): Predef.Map[Int, Seq[(Int, JoinType)]] = {
    edges.groupBy { _.left }.map { case (k, v) => (k, v.map(e => (e.right, e.joinType))) }
  }

  def adjacencyList: Map[Int, Seq[(Int, JoinType)]] = Map.empty

  def extractJoinConditions(left: ModularPlan, right: ModularPlan): Seq[Expression] = Seq.empty

  def extractRightEvaluableConditions(left: ModularPlan, right: ModularPlan): Seq[Expression] =
    Seq.empty

  def extractEvaluableConditions(plan: ModularPlan): Seq[Expression] = Seq.empty

  def asCompactSQL: String = asCompactString(new SQLBuilder(this).fragmentExtract)

  def asOneLineSQL: String = asOneLineString(new SQLBuilder(this).fragmentExtract)

  // for plan without sub-query expression only
  def asOneLineSQL(subqueryPrefix: String): String = {
    asOneLineString(new SQLBuilder(
      this,
      subqueryPrefix).fragmentExtract)
  }

  /**
   * Returns a plan where a best effort attempt has been made to transform `this` in a way
   * that preserves the result but replaces harmonized dimension table with HarmonizedRelation
   * and fact table with sub-plan that pre-aggregates the table before join with dimension table
   *
   * Some nodes should overwrite this to provide proper harmonization logic.
   */
  lazy val harmonized: ModularPlan = FullHarmonizer.execute(preHarmonized)

  lazy val semiHarmonized: ModularPlan = SemiHarmonizer.execute(preHarmonized)

  /**
   * Do some simple transformation on this plan before harmonizing. Implementations can override
   * this method to provide customized harmonize logic without rewriting the whole logic.
   *
   * We assume queries need to be harmonized are of the form:
   *
   * FACT (left) join (harmonized) DIM1 (left) join (harmonized) DIM2 ...
   *
   * For queries of not this form, customize this method for them to conform this form.
   */
  protected def preHarmonized: ModularPlan = {
    this
  }
}

object ModularPlan extends PredicateHelper {

}

abstract class LeafNode extends ModularPlan {
  override def children: Seq[ModularPlan] = Nil
}

abstract class UnaryNode extends ModularPlan {
  def child: ModularPlan

  override def children: Seq[ModularPlan] = child :: Nil
}

abstract class BinaryNode extends ModularPlan {
  def left: ModularPlan

  def right: ModularPlan

  override def children: Seq[ModularPlan] = Seq(left, right)
}
