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

package org.apache.carbondata.mv.tool.manager

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, NamedExpression, Or, _}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter, RightOuter}
import org.apache.spark.sql.internal._

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.{HarmonizedRelation, JoinEdge, ModularPlan, ModularRelation}
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util.{Signature, TableCluster}

abstract class CommonSubexpressionManager(spark: SparkSession, conf: SQLConf)
  extends CommonSubexpressionRuleEngine[ModularPlan] {

  override val rules = Seq(
    UnifyAttributeReferenceID,
    EliminateNonStarNCanonicalizeSPJGs(conf),
    FindPromisingTrivialCandidates(spark, conf),
    CreateCandidateCSEs(spark, conf),
    DiscardCheapCSEs,
    ExcludeCandidateWithHugeResults,
    KeepWhenBeneficial
  )
}

object UnifyAttributeReferenceID extends CoveringRule[ModularPlan] with PredicateHelper {
  def apply(inBatch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    val outputBatch = new ArrayBuffer[(ModularPlan, Int)]()

    // ==============================================================
    // Resolving the AttributeReference#ID problem. Refer to method
    // `transformToSameDatasource` for more information
    // ==============================================================

    val seenLeafNodeRelations = mutable.Set[modular.LeafNode]()

    def getSameRelation(table: modular.LeafNode): Option[modular.LeafNode] = {
      for (r <- seenLeafNodeRelations) {
        if (r == table) {
          return Some(r)
        }
      }
      None
    }

    inBatch.indices.foreach { iPlan =>
      var plan = inBatch(iPlan)._1
      for (fromRelation <- getLeafNodeRelations(inBatch(iPlan)._1)) {
        getSameRelation(fromRelation) match {
          case Some(toRelation: modular.LeafNode) =>
            plan = transformToSameDatasource(plan, fromRelation, toRelation)
          case None =>
            seenLeafNodeRelations.add(fromRelation)
        }
      }
      outputBatch += ((plan, inBatch(iPlan)._2))
    }
    collection.immutable.Seq(outputBatch: _*)
  }

  private def transformToSameDatasource(
      plan: ModularPlan,
      fromRelation: modular.LeafNode,
      toRelation: modular.LeafNode): ModularPlan = {

    val outPlan = plan.transform { case `fromRelation` => toRelation }

    val qualifier =
      toRelation match {
        case m: ModularRelation => Some(m.tableName)
        case h: HarmonizedRelation => Some("harmonized_" + h.tableName)
      }
    val attributeSet = AttributeSet(fromRelation.output)
    val rewrites = AttributeMap(fromRelation.output.zip(toRelation.output))
    val transformedPlan = outPlan.transformUp {
      case p => p.transformExpressions {
        case a: Attribute if attributeSet contains a =>
          rewrites(a).withQualifier(qualifier)
      }
    }
    transformedPlan
  }

  def getLeafNodeRelations(plan: ModularPlan): Seq[modular.LeafNode] = {
    plan.collect {
      case n: modular.LeafNode => n
    }
  }
}

case class EliminateNonStarNCanonicalizeSPJGs(conf: SQLConf)
  extends CoveringRule[ModularPlan] with PredicateHelper {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    val signature = batch match { case Nil => None; case head :: tail => head._1.signature }
    val mapper = new ObjectMapper().registerModule(DefaultScalaModule)
    val tableClusterString = conf.getConfString("spark.mv.tableCluster")
    val tableCluster = mapper.readValue(tableClusterString, classOf[TableCluster])
    val fTables: Set[String] = tableCluster.getFact

    val factTables = signature map { s => s.datasets.toList.filter {x => fTables.contains(x)}}

    factTables match {
      case Some(factTable::Nil) =>
        batch flatMap { case (subplan, freq) =>
          subplan match {
            case modular.GroupBy(_, _, _, _, sel @ modular.Select(_, _, _, _, _, _, _, _, _), _, _)
              if sel.children.forall(_.isInstanceOf[modular.LeafNode]) =>
              collectCanonicalizedStarSPJG(signature, Some(factTable), subplan, sel)
                .map(plan => (plan, freq))
            case modular.Select(
              _, _, _, _, _,
              Seq(modular.GroupBy(
                _, _, _, _, sel @ modular.Select(_, _, _, _, _, _, _, _, _), _, _)),
              _, _, _) if sel.children.forall(_.isInstanceOf[modular.LeafNode]) =>
              collectCanonicalizedStarSPJG(signature, Some(factTable), subplan, sel)
                .map(plan => (plan, freq))
            case _ => Seq.empty
          }
        }

      case _ => Seq.empty
    }
  }

  def collectCanonicalizedStarSPJG(
      signature: Option[Signature],
      factTable: Option[String],
      subplan: ModularPlan,
      select: modular.Select): Seq[ModularPlan] = {
    (signature, factTable) match {
      case (Some(sig), Some(fact)) =>
        val p = canonicalizeJoinOrderIfFeasible(select, sig, fact)
        p match {
          case Some(canonicalizedSel) =>
            val adjList = canonicalizedSel.adjacencyList
            if (adjList.isEmpty || adjList.keySet.size == 1) {
              Seq(subplan transform { case `select` => canonicalizedSel })
            } else {
              Seq.empty
            }
          case _ => Seq.empty
        }

      case _ => Seq.empty
    }
  }

  def canonicalizeJoinOrderIfFeasible(
      select: modular.Select,
      signature: Signature,
      factTable: String): Option[modular.Select] = {
    val rest = signature.datasets - factTable
    val dimTables = rest.toSeq.sorted

//    if (dimTables.toSet.size != dimTables.size) {
//      None
//    }

    val childrenMap = select.children.zipWithIndex.collect {
      case (table @ modular.ModularRelation(_, _, _, _, _), i) =>
        if (table.databaseName != null && table.tableName != null) {
          (s"${ table.databaseName }.${ table.tableName }", (table, i))
        } else {
          (table.output.toString, (table, i))
        }
      case (table @ modular.HarmonizedRelation(_), i) =>
        if (table.databaseName != null && table.tableName != null) {
          (s"${ table.databaseName }.${ table.tableName }", (table, i))
        } else {
          (table.output.toString, (table, i))
        }
    }.toMap

    val old2new = (dimTables.zipWithIndex.flatMap {
      case (tbl, idx) =>
        childrenMap.get(tbl).map { entry => (entry._2, idx + 1) }
    } ++ childrenMap.get(factTable).map { entry => (entry._2, 0) }).toMap

    val tChildren = Seq.empty ++
                    childrenMap.get(factTable).map(_._1) ++
                    dimTables.flatMap(s => childrenMap.get(s).map(_._1))
    val tJoinEdges: Seq[JoinEdge] = select.joinEdges.flatMap {
      case JoinEdge(l, r, t) =>
        (old2new.get(l), old2new.get(r)) match {
          case (Some(left), Some(right)) =>
            if (left <= right) Some(JoinEdge(left, right, t))
            else {
              if (t == RightOuter) Some(JoinEdge(right, left, LeftOuter))
              else if (t == LeftOuter) Some(JoinEdge(right, left, RightOuter))
              else if (t == Inner) Some(JoinEdge(right, left, Inner))
              else if (t == FullOuter) Some(JoinEdge(right, left, FullOuter))
              else Some(JoinEdge(left, right, t))
            }
          case _ => None
        }
    }
    val tAliasMap = select.aliasMap.flatMap { case (i, a) => old2new.get(i).map((_, a)) }

    assert(
      tJoinEdges.length == select.joinEdges.length && tAliasMap.size == select.aliasMap.size,
      "canonicalizing plan went wrong")

    Some(select.copy(joinEdges = tJoinEdges, aliasMap = tAliasMap, children = tChildren))
  }
}

case class FindPromisingTrivialCandidates(spark: SparkSession, conf: SQLConf)
  extends CoveringRule[ModularPlan] {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    val distinctBatch = batch.map { pp =>
      pp._1 match {
        case plan@modular.GroupBy(outputList, _, _, _, _, _, _) =>
          val distinctExprGroups = outputList
            .groupBy { case a: Alias => a.child.semanticHash(); case e => e.semanticHash() }
          val distinctNamedExprs = for (exprs <- distinctExprGroups.values if !(
            exprs.head.isInstanceOf[Alias] &&
            exprs.head.asInstanceOf[Alias].child.isInstanceOf[Literal])) yield {
            exprs.head match {
              case a: Alias if a.child.isInstanceOf[Attribute] =>
                a.child.asInstanceOf[NamedExpression]
              case other => other.asInstanceOf[NamedExpression]
            }
          }
          (plan.copy(outputList = distinctNamedExprs.toSeq), pp._2)

      case _ => pp
    }}
    val distinctTrivalCandidates = distinctBatch.groupBy(p => p._1.semanticHash())
    val promisingCandidates = for (candidates <- distinctTrivalCandidates
      .values; if isEstimableAndPromising(candidates.head._1)) yield {
      (candidates.head._1, candidates.map(_._2).sum)
    }
    promisingCandidates.toSeq
  }

  def isEstimableAndPromising(subplan: ModularPlan): Boolean = {
    if (subplan.subqueries.nonEmpty) {
      false
    } else {
      true
    }
  }
}

case class CreateCandidateCSEs(spark: SparkSession, conf: SQLConf)
  extends CoveringRule[ModularPlan] with PredicateHelper {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    CostBasedMVRecommendation(spark, conf).findCostEffectiveCandidates(batch)
  }

  /**
   * combines 2 modular plans to a common covering plan
   * This is a recursive method.
   * planR and planM must be the common subtrees in order to be able to be combined
   *
   * @param planR: the first modular plan
   * @param planM: the second modular plan
   * @return (isJoinCompatible, covering plan).
   *         isJoinCompatible = true means there is at least one equijoin predicate shared by
   *         planR and planM
   */
  def combinePlans(
      planR: ModularPlan,
      planM: ModularPlan,
      isFirst: Boolean): (Boolean, Option[ModularPlan], Seq[Expression]) = {
    if (planR.eq(planM) || planR == planM) {
      return (true, Option(planR), Seq.empty)
    }

    (planR, planM) match {
      // TODO: add handling for grouping set
      case (
        r @ modular.GroupBy(outputListR, inputListR, predicateListR, _, childR, _, _),
        m @ modular.GroupBy(outputListM, inputListM, predicateListM, _, childM, _, _)) =>
        val combinedResult = combinePlans(r.child, m.child, isFirst)

        val combinedOutputGroups =
          (outputListR ++ outputListM ++ predicateListR ++ predicateListM ++ combinedResult._3)
            .groupBy {
              case a: Alias => a.child.semanticHash()
              case e => e.semanticHash()
            }

        // TODO: how to handle Substring, which can not be converted to NamedExpression
        val combinedOutputList = (for (exprs <- combinedOutputGroups.values) yield {
          exprs.head match {
            case expression: NamedExpression =>
              expression
            case _ =>
              Alias(exprs.head, exprs.head.toString)()
          }
        }).toSeq

        val combinedInputGroups = (inputListR ++ inputListM ++ combinedResult._3)
          .groupBy(_.semanticHash())
        val combinedInputList = (for (attrs <- combinedInputGroups.values) yield {
          attrs.head.asInstanceOf[NamedExpression]
        }).toSeq

        val combinedPredicateGroups = (predicateListR ++ predicateListM ++ combinedResult._3)
          .groupBy { _.semanticHash }
        val combinedPredicateList = (for (exprs <- combinedPredicateGroups.values) yield {
          exprs.head
        }).toSeq

        combinedResult match {
          case (isJoinCompatible, Some(combinedChild), _) =>
            (isJoinCompatible,
              Option(modular.GroupBy(
                combinedOutputList,
                combinedInputList,
                combinedPredicateList,
                None,
                combinedChild,
                NoFlags,
                Seq.empty)),
              Seq.empty[Expression])
          case (isJoinCompatible, _, _) =>
            (isJoinCompatible, None, Seq.empty)
        }

      case (
        r @ modular.Select(outputListR, inputListR, _, _, _, childrenR, _, _, _),
        m @ modular.Select(outputListM, inputListM, _, _, _, childrenM, _, _, _)) =>
        val combinedChildren =
          r.children.zip(m.children).map {
            case (childR, childM) => combinePlans(childR, childM, isFirst)
          }.flatMap {
            _ match {
              case (true, Some(combinedChild), _) => Seq(combinedChild)
              case _ => Seq.empty
            }
          }

        val combinedOutputGroups = (outputListR ++ outputListM).groupBy {
          case a: Alias => a.child.semanticHash()
          case e => e.semanticHash()
        }
        val combinedOutputList =
          (for (exprs <- combinedOutputGroups.values) yield exprs.head)
            .toSeq
        val combinedInputGroups = (inputListR ++ inputListM).groupBy(_.semanticHash())
        val combinedInputList =
          (for (attrs <- combinedInputGroups.values) yield attrs.head)
            .toSeq

        val combinedPredicates = mutable.ArrayBuffer[Expression]()

        for {
          joinEdgeR <- r.joinEdges
          joinEdgeM <- m.joinEdges
          if isJoinCompatible(joinEdgeR, r, joinEdgeM, m)
          conditionR <- r.extractJoinConditions(
            r.children(joinEdgeR.left), r.children(joinEdgeR.right)).reduceOption(And)
          conditionM <- m.extractJoinConditions(
            m.children(joinEdgeM.left), m.children(joinEdgeM.right)).reduceOption(And)
        } {
          (conditionR, conditionM) match {
            case (l, r) if l fastEquals r => combinedPredicates += l
            case _ =>
              val lhs = splitConjunctivePredicates(conditionR)
              val rhs = splitConjunctivePredicates(conditionM)
              val common = lhs.filter(e => rhs.exists(e.semanticEquals(_)))
              if (common.isEmpty) {
                // No common factors
                combinedPredicates += Or(conditionR, conditionM)
              } else {
                val ldiff = lhs.filterNot(e => common.exists(e.semanticEquals(_)))
                val rdiff = rhs.filterNot(e => common.exists(e.semanticEquals(_)))
                if (ldiff.isEmpty || rdiff.isEmpty) {
                  combinedPredicates += common.reduce(And)
                } else {
                  combinedPredicates +=
                  (common :+ Or(ldiff.reduce(And), rdiff.reduce(And)))
                    .reduce(And)
                }
              }
          }
        }

        val combinedPredicateList = collection.immutable.Seq(combinedPredicates: _*)

        var extendedAttributeSet = AttributeSet.empty

        // add all attributes in filters to grouping attributes
        if (isFirst) {
          for (childR <- childrenR) {
            extendedAttributeSet = extendedAttributeSet ++
                                   r.extractEvaluableConditions(childR)
                                     .foldLeft(AttributeSet.empty)((s, p) => s ++ p.references)
          }
        }

        for (childM <- childrenM) {
          extendedAttributeSet = extendedAttributeSet ++
                                 m.extractEvaluableConditions(childM)
                                   .foldLeft(AttributeSet.empty)((s, p) => s ++ p.references)
        }

        val aliasMap = childrenM.zipWithIndex.collect {
          case (child: modular.ModularRelation, i) =>
            (i, child.tableName)
          case (child: modular.HarmonizedRelation, i) =>
            (i, child.tableName)
        }.toMap

        if (combinedPredicateList.size == r.joinEdges.size &&
            combinedChildren.size == r.children.size) {
          (true,
            Some(modular.Select(
              combinedOutputList, combinedInputList, combinedPredicateList,
              aliasMap, r.joinEdges, combinedChildren, NoFlags, Seq.empty, Seq.empty)),
            extendedAttributeSet.toSeq)
        } else {
          (false, None, Seq.empty)
        }

      case (r: modular.LeafNode, m: modular.LeafNode) =>
        if (r == m) {
          (true, Some(r), Seq.empty)
        } else {
          (false, None, Seq.empty)
        }

      case _ => (false, None, Seq.empty)
    }
  }

  def isJoinCompatible(
      jr: JoinEdge,
      selr: modular.Select,
      jm: JoinEdge,
      selm: modular.Select): Boolean = {
    def joinKeys(j: JoinEdge, sel: modular.Select): Set[(Int, Int)] = {
      val condition = sel.extractJoinConditions(sel.children(j.left), sel.children(j.right))
      condition.flatMap {
        case EqualTo(l, r)
          if canEvaluate(l, sel.children(j.left)) && canEvaluate(r, sel.children(j.right)) =>
          Some((l.semanticHash, r.semanticHash))
        case EqualTo(l, r)
          if canEvaluate(l, sel.children(j.right)) && canEvaluate(r, sel.children(j.left)) =>
          Some((r.semanticHash, l.semanticHash))
        case _ => None
      }.toSet
    }
    if (jr == jm) {
      val common = joinKeys(jr, selr).intersect(joinKeys(jm, selm))
      if (common.nonEmpty) true else false
    } else {
      false
    }
  }

}

object DiscardCheapCSEs extends CoveringRule[ModularPlan] {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    // TODO: implement me
    batch
  }
}

object ExcludeCandidateWithHugeResults extends CoveringRule[ModularPlan] {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    // TODO: implement me
    batch
  }
}

object KeepWhenBeneficial extends CoveringRule[ModularPlan] {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    // TODO: implement me
    batch
  }
}
