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

import org.apache.spark.sql.catalyst.expressions.{Attribute, _}
import org.apache.spark.sql.catalyst.plans.JoinType

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.Flags._

private[mv] trait Matchable extends ModularPlan {
  def outputList: Seq[NamedExpression]

  def predicateList: Seq[Expression]
}

case class GroupBy(
    outputList: Seq[NamedExpression],
    inputList: Seq[Expression],
    predicateList: Seq[Expression],
    alias: Option[String],
    child: ModularPlan,
    flags: FlagSet,
    flagSpec: Seq[Seq[Any]]) extends UnaryNode with Matchable {
  override def output: Seq[Attribute] = outputList.map(_.toAttribute)
}

case class Select(
    outputList: Seq[NamedExpression],
    inputList: Seq[Expression],
    predicateList: Seq[Expression],
    aliasMap: Map[Int, String],
    joinEdges: Seq[JoinEdge],
    children: Seq[ModularPlan],
    flags: FlagSet,
    flagSpec: Seq[Seq[Any]],
    windowSpec: Seq[Seq[Any]]) extends ModularPlan with Matchable {
  override def output: Seq[Attribute] = outputList.map(_.toAttribute)

  override def adjacencyList: scala.collection.immutable.Map[Int, Seq[(Int, JoinType)]] = {
    joinEdges.groupBy { _.left }.map { case (k, v) => (k, v.map(e => (e.right, e.joinType))) }
  }

  override def extractJoinConditions(
      left: ModularPlan, right: ModularPlan): Seq[Expression] = {
    predicateList.filter(p => p.references.intersect(left.outputSet).nonEmpty &&
                              p.references.intersect(right.outputSet).nonEmpty &&
                              p.references.subsetOf(left.outputSet ++ right.outputSet))
  }

  override def extractRightEvaluableConditions(
      left: ModularPlan, right: ModularPlan): Seq[Expression] = {
    predicateList.filter(p => p.references.subsetOf(left.outputSet ++ right.outputSet) &&
                              p.references.intersect(right.outputSet).nonEmpty)
  }

  override def extractEvaluableConditions(plan: ModularPlan): Seq[Expression] = {
    predicateList.filter(p => canEvaluate(p, plan))
  }
}

case class Union(children: Seq[ModularPlan], flags: FlagSet, flagSpec: Seq[Seq[Any]])
  extends ModularPlan {
  override def output: Seq[Attribute] = children.head.output
}

case object OneRowTable extends LeafNode {
  override def output: Seq[Attribute] = Nil
}
