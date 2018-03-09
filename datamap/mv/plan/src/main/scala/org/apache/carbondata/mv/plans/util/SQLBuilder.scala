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

package org.apache.carbondata.mv.plans.util

import java.util.concurrent.atomic.AtomicLong

import scala.collection.immutable

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}

import org.apache.carbondata.mv.expressions.modular._
import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.{Select, _}
import org.apache.carbondata.mv.plans.util.SQLBuildDSL.Fragment

// TODO: generalize this using SQLBuild to handle nested sub-query expression, using the
// correspondence
//      SQLBuild <--> QueryRewrite
class SQLBuilder private(
    modularPlan: ModularPlan,
    nextSubqueryId: AtomicLong,
    subqueryPrefix: String) {

  def this(modularPlan: ModularPlan) = {
    this(
      SQLBuilder.subqueryPreprocess(modularPlan, new AtomicLong()),
      new AtomicLong(0),
      "gen_subquery_")
  }

  // only allow one level scalar subquery
  def this(modularPlan: ModularPlan, subqueryPrefix: String) = {
    this(SQLBuilder.screenOutPlanWithSubquery(modularPlan), new AtomicLong(0), subqueryPrefix)
  }

  private def newSubqueryName(): String = {
    s"${ subqueryPrefix }${
      nextSubqueryId.getAndIncrement
      ()
    }"
  }

  def fragmentExtract: Fragment = {
    val finalPlan: ModularPlan = SQLizer.execute(modularPlan)
    SQLBuildDSL.fragmentExtract(finalPlan, None)
  }

  object SQLizer extends RuleExecutor[ModularPlan] {
    override protected def batches: Seq[Batch] = {
      Seq(
        Batch(
          "Recover Scoping Info", Once,
          // Clean up qualifiers due to erasure of sub-query aliases to use base tables as
          // qualifiers
          // This complements AddSubquery to handle corner case qMV3 in Tpcds_1_4_Querybatch.scala
          CleanupQualifier,
          // Insert sub queries on top of operators that need to appear after FROM clause.
          AddSubquery
        )
      )
    }

    object CleanupQualifier extends Rule[ModularPlan] {
      override def apply(tree: ModularPlan): ModularPlan = {
        tree transformDown {
          case s@modular.Select(_, _, _, _, _,
            Seq(g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
            _, _, _) if !s.rewritten && s2.children.forall { _.isInstanceOf[modular.LeafNode] } =>
            val attrMap = AttributeMap(s2.outputList
              .collect { case a: Alias if a.child.isInstanceOf[Attribute] => (a.toAttribute, a) })
            cleanupQualifier(s, s2.aliasMap, s2.children, attrMap)

          case g@modular.GroupBy(_, _, _, _,
            s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)
            if !g.rewritten && s2.children.forall { _.isInstanceOf[modular.LeafNode] } =>
            val attrMap = AttributeMap(s2.outputList
              .collect { case a: Alias if a.child.isInstanceOf[Attribute] => (a.toAttribute, a) })
            cleanupQualifier(g, s2.aliasMap, s2.children, attrMap)

          case s@modular.Select(_, _, _, _, _, _, _, _, _)
            if !s.rewritten && s.children.forall { _.isInstanceOf[modular.LeafNode] } =>
            cleanupQualifier(s, s.aliasMap, s.children, AttributeMap(Seq.empty[(Attribute, Alias)]))

        }
      }

      private def cleanupQualifier(node: ModularPlan,
          aliasMap: Map[Int, String],
          children: Seq[ModularPlan],
          attrMap: AttributeMap[Alias]): ModularPlan = {
        node transform {
          case plan if !plan.rewritten && !plan.isInstanceOf[modular.LeafNode] =>
            plan.transformExpressions {
              case ref: Attribute =>
                val i = children.indexWhere(_.outputSet.contains(ref))
                if (i > -1) {
                  // this is a walk around for mystery of spark qualifier
                  if (aliasMap.nonEmpty && aliasMap(i).nonEmpty) {
                    AttributeReference(
                      ref.name,
                      ref.dataType)(exprId = ref.exprId, qualifier = Option(aliasMap(i)))
                  } else {
                    ref
                  }
                } else {
                  attrMap.get(ref) match {
                    case Some(alias) => Alias(alias.child, alias.name)(exprId = alias.exprId)
                    case None => ref
                  }
                }
            }
        }
      }
    }

    object AddSubquery extends Rule[ModularPlan] {
      override def apply(tree: ModularPlan): ModularPlan = {
        tree transformUp {
          case s@modular.Select(_, _, _, _, _,
            Seq(g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
            _, _, _) if s2.children.forall { _.isInstanceOf[modular.LeafNode] } => s

          case g@modular.GroupBy(_, _, _, _, s2@modular.Select(_, _, _, _, _, _, _, _, _), _, _)
            if s2.children.forall { _.isInstanceOf[modular.LeafNode] } => g

          case s@modular.Select(_, _, _, _, _, _, _, _, _)
            if !s.rewritten && !s.children.forall { _.isInstanceOf[modular.LeafNode] } =>
            var list: List[(Int, String)] = List()
            var newS = s.copy()
            s.children.zipWithIndex.filterNot { _._1.isInstanceOf[modular.LeafNode] }.foreach {
              case (child: ModularPlan, index) if (!s.aliasMap.contains(index)) =>
                val subqueryName = newSubqueryName()
                val windowAttributeSet = if (child.isInstanceOf[Select]) {
                  val windowExprs = child.asInstanceOf[Select].windowSpec
                    .map { case Seq(expr) => expr.asInstanceOf[Seq[NamedExpression]] }
                    .foldLeft(Seq.empty.asInstanceOf[Seq[NamedExpression]])(_ ++ _)
                  SQLBuilder.collectAttributeSet(windowExprs)
                } else {
                  AttributeSet.empty
                }
                val subqueryAttributeSet = child.outputSet ++ windowAttributeSet
                //              TODO: how to use alias to avoid duplicate names with distinct
                // exprIds
                //              val duplicateNames = collectDuplicateNames(subqueryAttributeSet)
                // .toSet
                if (SQLBuilder.collectDuplicateNames(subqueryAttributeSet).nonEmpty) {
                  new UnsupportedOperationException(s"duplicate name(s): ${
                    child.output
                      .map(_.toString + ", ")
                  }")
                }
                list = list :+ ((index, subqueryName))
                newS = newS.transformExpressions {
                  case ref: Attribute if (subqueryAttributeSet.contains(ref)) =>
                    AttributeReference(ref.name, ref.dataType)(
                      exprId = ref.exprId,
                      qualifier = Some(subqueryName))
                  case alias: Alias if (subqueryAttributeSet.contains(alias.toAttribute)) =>
                    Alias(alias.child, alias.name)(
                      exprId = alias.exprId,
                      qualifier = Some(subqueryName))
                }

              case _ =>
            }
            if (list.nonEmpty) {
              list = list ++ s.aliasMap.toSeq
              newS.copy(aliasMap = list.toMap)
            } else {
              newS
            }

          case g@modular.GroupBy(_, _, _, _, _, _, _) if (!g.rewritten && g.alias.isEmpty) =>
            val newG = if (g.outputList.isEmpty) {
              val ol = g.predicateList.map { case a: Attribute => a }
              g.copy(outputList = ol)
            } else {
              g
            }
            val subqueryName = newSubqueryName()
            val subqueryAttributeSet = newG.child.outputSet
            if (SQLBuilder.collectDuplicateNames(subqueryAttributeSet).nonEmpty) {
              new UnsupportedOperationException(s"duplicate name(s): ${
                newG.child.output.map(_.toString + ", ")
              }")
            }
            newG.transformExpressions {
              case ref: AttributeReference if (subqueryAttributeSet.contains(ref)) =>
                AttributeReference(ref.name, ref.dataType)(
                  exprId = ref.exprId,
                  qualifier = Some(subqueryName))
              case alias: Alias if (subqueryAttributeSet.contains(alias.toAttribute)) =>
                Alias(alias.child, alias.name)(
                  exprId = alias.exprId,
                  qualifier = Some(subqueryName))
            }.copy(alias = Some(subqueryName))
        }
      }
    }
  }

}

object SQLBuilder {
  def collectAttributeSet(outputList: Seq[Expression]): AttributeSet = {
    AttributeSet(outputList
      .collect {
        case a@Alias(_, _) => a.toAttribute
        case a: Attribute => a
      })
  }

  def collectDuplicateNames(s: AttributeSet): immutable.Iterable[String] = {
    s.baseSet.map(_.a).groupBy(_.name).collect { case (x, ys) if ys.size > 1 => x }
  }

  def subqueryPreprocess(plan: ModularPlan, nextScalarSubqueryId: AtomicLong): ModularPlan = {
    def newSubqueryName(nextScalarSubqueryId: AtomicLong): String = {
      s"gen_expression_${
        nextScalarSubqueryId
          .getAndIncrement()
      }_"
    }

    plan.transformAllExpressions {
      case s: ModularSubquery =>
        if (s.children.isEmpty) {
          val subqueryName = newSubqueryName(nextScalarSubqueryId)
          SubqueryHolder(s"(${ s.plan.asOneLineSQL(subqueryName) })")
        } else {
          throw new UnsupportedOperationException(s"Expression $s can't converted to SQL")
        }
      case o => o
    }
  }

  def screenOutPlanWithSubquery(plan: ModularPlan): ModularPlan = {
    plan.transformAllExpressions {
      case s: ModularSubquery =>
        throw new UnsupportedOperationException(s"Nested scala subquery $s doesn't supported")
      case e => e
    }
  }
}
