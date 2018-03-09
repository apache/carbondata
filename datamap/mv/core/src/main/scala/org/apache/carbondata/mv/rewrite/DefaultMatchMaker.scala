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

package org.apache.carbondata.mv.rewrite

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, PredicateHelper, _}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner}

import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.{GroupBy, JoinEdge, Matchable, ModularPlan, Select}
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util.SQLBuilder

abstract class DefaultMatchMaker extends MatchMaker[ModularPlan]

abstract class DefaultMatchPattern extends MatchPattern[ModularPlan] {

  /** Name for this pattern, automatically inferred based on class name. */
  val patternName: String = {
    val className = getClass.getName
    if (className endsWith "$") className.dropRight(1) else className
  }

  def factorOutSubsumer(
      compensation: ModularPlan,
      subsumer: Matchable,
      subsumerName: Option[String]): ModularPlan = {
    val aliasMap = AttributeMap(
        subsumer.outputList.collect {
          case a: Alias if a.child.isInstanceOf[Attribute] =>
            (a.child.asInstanceOf[Attribute], a.toAttribute)
          })

    val compensation1 = compensation.transform {
      case plan if !plan.skip =>
        plan.transformExpressions {
          case a: AttributeReference =>
            aliasMap
              .get(a)
              .map { ref =>
                AttributeReference(
                  ref.name, ref.dataType)(
                  exprId = ref.exprId,
                  qualifier = a.qualifier)
              }.getOrElse(a)
          }
    }

    val subqueryAttributeSet = SQLBuilder.collectAttributeSet(subsumer.outputList)
    if (SQLBuilder.collectDuplicateNames(subqueryAttributeSet).nonEmpty) {
      new UnsupportedOperationException(
        s"duplicate name(s): ${ subsumer.output.map(_.toString + ", ") }")
    }
    val compensationFinal = compensation1.transformExpressions {
      case ref: Attribute if subqueryAttributeSet.contains(ref) =>
        AttributeReference(ref.name, ref.dataType)(exprId = ref.exprId, qualifier = subsumerName)
      case alias: Alias if subqueryAttributeSet.contains(alias.toAttribute) =>
        Alias(alias.child, alias.name)(exprId = alias.exprId, qualifier = subsumerName)
    }
    compensationFinal
  }
}

object DefaultMatchMaker extends DefaultMatchMaker {
  lazy val patterns =
    SelectSelectNoChildDelta ::
    GroupbyGroupbyNoChildDelta ::
    GroupbyGroupbySelectOnlyChildDelta ::
    GroupbyGroupbyGroupbyChildDelta ::
    SelectSelectSelectChildDelta ::
    SelectSelectGroupbyChildDelta :: Nil
}

/**
 * Convention:
 * EmR: each subsumee's expression match some of subsumer's expression
 * EdR: each subsumee's expression derive from some of subsumer's expression
 * RmE: each subsumer's expression match some of subsumee's expression
 * RdE: each subsumer's expression derive from some of subsumee's expression
 */

object SelectSelectNoChildDelta extends DefaultMatchPattern with PredicateHelper {
  private def isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]): Boolean = {
    if (subsumee.asInstanceOf[Select].predicateList.contains(exprE)) {
      subsumer.asInstanceOf[Select].predicateList.exists(_.semanticEquals(exprE)) ||
      canEvaluate(exprE, subsumer)
    } else if (subsumee.asInstanceOf[Select].outputList.contains(exprE)) {
      exprE match {
        case a@Alias(_, _) =>
          exprListR.exists(a1 => a1.isInstanceOf[Alias] &&
                                 a1.asInstanceOf[Alias].child.semanticEquals(a.child))
        case exp => exprListR.exists(_.semanticEquals(exp) || canEvaluate(exp, subsumer))
      }
    } else {
      false
    }
  }

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {

    (subsumer, subsumee, compensation) match {
      case (
          sel_1a @ modular.Select(_, _, _, _, _, _, _, _, _),
          sel_1q @ modular.Select(_, _, _, _, _, _, _, _, _), None
        ) if sel_1a.children.forall { _.isInstanceOf[modular.LeafNode] } &&
             sel_1q.children.forall { _.isInstanceOf[modular.LeafNode] } =>

        // assume children (including harmonized relation) of subsumer and subsumee
        // are 1-1 correspondence.
        // Change the following two conditions to more complicated ones if we want to
        // consider things that combine extrajoin, rejoin, and harmonized relations
        val isUniqueRmE = subsumer.children.filter { x => subsumee.children.count(_ == x) != 1 }
        val isUniqueEmR = subsumee.children.filter { x => subsumer.children.count(_ == x) != 1 }

        val extrajoin = sel_1a.children.filterNot { child => sel_1q.children.contains(child) }
        val rejoin = sel_1q.children.filterNot { child => sel_1a.children.contains(child) }
        val rejoinOutputList = rejoin.flatMap(_.output)

        val isPredicateRmE = sel_1a.predicateList.forall(expr =>
          sel_1q.predicateList.exists(_.semanticEquals(expr)))
        val isPredicateEmdR = sel_1q.predicateList.forall(expr =>
          isDerivable(expr, sel_1a.outputList ++ rejoinOutputList, sel_1q, sel_1a, None))
        val isOutputEdR = sel_1q.outputList.forall(expr =>
          isDerivable(expr, sel_1a.outputList ++ rejoinOutputList, sel_1q, sel_1a, None))

        if (isUniqueRmE.isEmpty && isUniqueEmR.isEmpty && extrajoin.isEmpty && isPredicateRmE &&
            isPredicateEmdR && isOutputEdR) {
          val mappings = sel_1a.children.zipWithIndex.map {
            case (childr, fromIdx) if sel_1q.children.contains(childr) =>
              val toIndx = sel_1q.children.indexWhere(_ == childr)
              (toIndx -> fromIdx)

          }
          val e2r = mappings.toMap
          val r2e = e2r.map(_.swap)
          val r2eJoinsMatch = sel_1a.joinEdges.forall { x =>
              (r2e.get(x.left), r2e.get(x.right)) match {
                case (Some(l), Some(r)) =>
                  val mappedEdge = JoinEdge(l, r, x.joinType)
                  val joinTypeEquivalent =
                    if (sel_1q.joinEdges.contains(mappedEdge)) true
                    else {
                      x.joinType match {
                        case Inner | FullOuter =>
                          sel_1q.joinEdges.contains(JoinEdge(r, l, x.joinType))
                        case _ => false
                      }
                    }
                  if (joinTypeEquivalent) {
                    val sel_1a_join = sel_1a.extractJoinConditions(
                      sel_1a.children(x.left),
                      sel_1a.children(x.right))
                    val sel_1q_join = sel_1q.extractJoinConditions(
                      sel_1q.children(mappedEdge.left),
                      sel_1q.children(mappedEdge.right))
                    sel_1a_join.forall(e => sel_1q_join.exists(e.semanticEquals(_))) &&
                    sel_1q_join.forall(e => sel_1a_join.exists(e.semanticEquals(_)))
                  } else false
                case _ => false
              }
          }

          val isPredicateEmR = sel_1q.predicateList.forall(expr =>
            sel_1a.predicateList.exists(_.semanticEquals(expr)))
          val isOutputEmR = sel_1q.outputList.forall(expr =>
            sel_1a.outputList.exists(_.semanticEquals(expr)))
          val isOutputRmE = sel_1a.outputList.forall(expr =>
            sel_1q.outputList.exists(_.semanticEquals(expr)))

          if (r2eJoinsMatch) {
            if (isPredicateEmR && isOutputEmR && isOutputRmE && rejoin.isEmpty) {
              Seq(sel_1a) // no compensation needed
            } else {
              val tChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
              val tAliasMap = new collection.mutable.HashMap[Int, String]()

              tChildren += sel_1a
              tAliasMap += (tChildren.indexOf(sel_1a) -> rewrite.newSubsumerName())

              sel_1q.children.zipWithIndex.foreach {
                case (childe, idx) =>
                  if (e2r.get(idx).isEmpty) {
                    tChildren += childe
                    sel_1q.aliasMap.get(idx).map(x => tAliasMap += (tChildren.indexOf(childe) -> x))
                  }
              }

              val tJoinEdges = sel_1q.joinEdges.collect {
                case JoinEdge(le, re, joinType) =>
                  (e2r.get(le), e2r.get(re)) match {
                    case (Some(lr), None) =>
                      JoinEdge(
                        0,
                        tChildren.indexOf(sel_1q.children(re)),
                        joinType)
                    case (None, None) =>
                      JoinEdge(
                        tChildren.indexOf(sel_1q.children(le)),
                        tChildren.indexOf(sel_1q.children(re)),
                        joinType)
                    case (None, Some(rr)) =>
                      JoinEdge(
                        tChildren.indexOf(sel_1q.children(le)),
                        0,
                        joinType)
                    case _ =>
                      null.asInstanceOf[JoinEdge]
                  }
              }
              val tPredicateList = sel_1q.predicateList.filter { p =>
                !sel_1a.predicateList.exists(_.semanticEquals(p)) }
              val wip = sel_1q.copy(
                predicateList = tPredicateList,
                children = tChildren,
                joinEdges = tJoinEdges.filter(_ != null),
                aliasMap = tAliasMap.toMap)
              val subsumerName = wip.aliasMap.get(0)
              val done = factorOutSubsumer(wip, sel_1a, subsumerName)
              Seq(done)
            }
          } else Nil
        } else Nil

      case (
        sel_3a @ modular.Select(_, _, _, _, _, _, _, _, _),
        sel_3q @ modular.Select(_, _, _, _, _, _, _, _, _), None)
        if sel_3a.children.forall(_.isInstanceOf[GroupBy]) &&
           sel_3q.children.forall(_.isInstanceOf[GroupBy]) =>
        val isPredicateRmE = sel_3a.predicateList.isEmpty ||
                             sel_3a.predicateList.forall(expr =>
                               sel_3q.predicateList.exists(_.semanticEquals(expr)))
        val isPredicateEmdR = sel_3q.predicateList.isEmpty ||
                              sel_3q.predicateList.forall(expr =>
                                sel_3a.predicateList.exists(_.semanticEquals(expr)) ||
                                isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))
        val isOutputEdR = sel_3q.outputList.forall(expr =>
          isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))
        val isSingleChild = sel_3a.children.length == 1 && sel_3q.children.length == 1

        if (isPredicateRmE && isPredicateEmdR && isOutputEdR && isSingleChild) {
          val isPredicateEmR = sel_3q.predicateList.isEmpty ||
                               sel_3q.predicateList.forall(expr =>
                                 sel_3a.predicateList.exists(_.semanticEquals(expr)))
          val isOutputRmE = sel_3a.outputList.forall(expr =>
            isDerivable(expr, sel_3q.outputList, sel_3a, sel_3q, None))
          val isOutputEmR = sel_3q.outputList.forall(expr =>
            isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))

          if (isPredicateEmR && isOutputEmR && isOutputRmE) {
            Seq(sel_3a)
          } else if (isPredicateEmR && isOutputEmR) {
            // no compensation needed
            val sel_3q_exp = sel_3q.transformExpressions({
              case a: Alias => sel_3a.outputList
                .find { a1 =>
                  a1.isInstanceOf[Alias] &&
                  a1.asInstanceOf[Alias].child.semanticEquals(a.child)
                }.map(_.toAttribute).get
            })
            val wip = sel_3q_exp.copy(
              children = Seq(sel_3a),
              aliasMap = Seq(0 -> rewrite.newSubsumerName()).toMap)
            val subsumerName = wip.aliasMap.get(0)
            val done = factorOutSubsumer(wip, sel_3a, subsumerName)
            Seq(done)
          } else {
            Nil
          }
        } else Nil

      case _ => Nil
    }
  }
}

object GroupbyGroupbyNoChildDelta extends DefaultMatchPattern {
  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {
    (subsumer, subsumee, compensation) match {
      case (
        gb_2a @ modular.GroupBy(_, _, _, _, _, _, _),
        gb_2q @ modular.GroupBy(_, _, _, _, _, _, _),
        None) =>
        val isGroupingEmR = gb_2q.predicateList.forall(expr =>
          gb_2a.predicateList.exists(_.semanticEquals(expr)))
        val isGroupingRmE = gb_2a.predicateList.forall(expr =>
          gb_2q.predicateList.exists(_.semanticEquals(expr)))
        if (isGroupingEmR && isGroupingRmE) {
          val isOutputEmR = gb_2q.outputList.forall {
            case a @ Alias(_, _) => gb_2a.outputList.exists(a1 => a1.isInstanceOf[Alias] &&
                                                                  a1.asInstanceOf[Alias].child
                                                                    .semanticEquals(a.child))
            case exp => gb_2a.outputList.exists(_.semanticEquals(exp))
          }

          if (isOutputEmR) {
            Seq(gb_2a)
          } else {
            Nil
          }
        } else {
          val aliasMap = AttributeMap(gb_2a.outputList.collect { case a: Alias =>
            (a.toAttribute, a)})
          if (isGroupingEmR) {
            Utils.tryMatch(
              gb_2a, gb_2q, aliasMap).flatMap {
              case g: GroupBy =>
                Some(g.copy(child = g.child.withNewChildren(
                  g.child.children.map {
                    case modular.Select(_, _, _, _, _, _, _, _, _) => gb_2a;
                    case other => other
                  })));
              case _ => None}.map(Seq(_)).getOrElse(Nil)
          } else {
            Nil
          }
        }

      case _ => Nil
    }
  }
}

object GroupbyGroupbySelectOnlyChildDelta extends DefaultMatchPattern with PredicateHelper {
  private def isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]) = {
    if (subsumee.asInstanceOf[GroupBy].predicateList.contains(exprE)) {
      if (exprListR.exists(_.semanticEquals(exprE)) || canEvaluate(exprE, exprListR)) true
      else false
    } else if (compensation.getOrElse(throw new RuntimeException("compensation cannot be None"))
      .asInstanceOf[Select].predicateList.contains(exprE)) {
      if (canEvaluate(exprE, exprListR) || exprListR.exists(_.semanticEquals(exprE))) true
      else false
    } else {
      false
    }
  }

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {
    val aggInputEinR = subsumee.expressions
      .collect { case agg: aggregate.AggregateExpression => AttributeSet(Seq(agg))
        .subsetOf(subsumer.outputSet)
      }.forall(identity)
    val compensationSelectOnly = !compensation.map { _.collect { case n => n.getClass } }
      .exists(_.contains(modular.GroupBy))

    (subsumer, subsumee, compensation, aggInputEinR, compensationSelectOnly) match {
      case (
        gb_2a @ modular.GroupBy(_, _, _, _, _, _, _),
        gb_2q @ modular.GroupBy(_, _, _, _, _, _, _),
        Some(sel_1c1 @ modular.Select(_, _, _, _, _, _, _, _, _)),
        true,
        true)
        if !gb_2q.flags.hasFlag(EXPAND) && !gb_2a.flags.hasFlag(EXPAND) =>

        val rejoinOutputList = sel_1c1.children.tail.flatMap(_.output)
        val isGroupingEdR = gb_2q.predicateList.forall(expr =>
          isDerivable(expr, gb_2a.predicateList ++ rejoinOutputList, gb_2q, gb_2a, compensation))
        val needRegrouping = !gb_2a.predicateList.forall(gb_2q.predicateList.contains)
        val canPullup = sel_1c1.predicateList.forall(expr =>
          isDerivable(expr, gb_2a.predicateList ++ rejoinOutputList, gb_2q, gb_2a, compensation))
        val isAggEmR = gb_2q.outputList.collect {
          case agg: aggregate.AggregateExpression =>
            gb_2a.outputList.exists(_.semanticEquals(agg))
        }.forall(identity)

        if (isGroupingEdR && ((!needRegrouping && isAggEmR) || needRegrouping) && canPullup) {
          // pull up
          val pullupOutputList = gb_2a.outputList.map(_.toAttribute) ++ rejoinOutputList
          val sel_2c1 = sel_1c1.copy(
            outputList = pullupOutputList,
            inputList = pullupOutputList,
            children = sel_1c1.children.map {
              case s: Select => gb_2a
              case other => other })

          if (needRegrouping && rejoinOutputList.isEmpty) {
            val aliasMap = AttributeMap(gb_2a.outputList.collect {
              case a: Alias => (a.toAttribute, a) })
            Utils.tryMatch(gb_2a, gb_2q, aliasMap).flatMap {
              case g: GroupBy => Some(g.copy(child = sel_2c1));
              case _ => None
            }.map { wip =>
              factorOutSubsumer(wip, gb_2a, sel_1c1.aliasMap.get(0))
            }.map(Seq(_))
             .getOrElse(Nil)
          }
          // TODO: implement regrouping with 1:N rejoin (rejoin tables being the "1" side)
          // via catalog service
          else if (!needRegrouping && isAggEmR) {
            Seq(sel_2c1).map(wip => factorOutSubsumer(wip, gb_2a, sel_1c1.aliasMap.get(0)))
          } else Nil
        } else Nil

      case _ => Nil
    }
  }
}

object GroupbyGroupbyGroupbyChildDelta extends DefaultMatchPattern {
  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {
    val groupbys = compensation.map { _.collect { case g: GroupBy => g } }.getOrElse(Nil).toSet

    (subsumer, subsumee, groupbys.nonEmpty) match {
      case (
        modular.Select(_, _, _, _, _, _, _, _, _),
        modular.Select(_, _, _, _, _, _, _, _, _),
        true) =>
        // TODO: implement me
        Nil

      case _ => Nil
    }
  }
}


object SelectSelectSelectChildDelta extends DefaultMatchPattern {
  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {
    val compensationSelectOnly =
      !compensation
        .map { _.collect { case n => n.getClass } }
        .exists(_.contains(modular.GroupBy))

    (subsumer, subsumee, compensationSelectOnly) match {
      case (
        modular.Select(_, _, _, _, _, _, _, _, _),
        modular.Select(_, _, _, _, _, _, _, _, _),
        true) =>
        // TODO: implement me
        Nil
      case _ => Nil
    }
  }
}

object SelectSelectGroupbyChildDelta extends DefaultMatchPattern with PredicateHelper {
  private def isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]) = {
    Utils.isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan])
  }

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {
    (subsumer, subsumee, compensation, subsumer.children, subsumee.children) match {
      case (
        sel_3a@modular.Select(
        _, _, Nil, _, _,
        Seq(gb_2a@modular.GroupBy(_, _, _, _, _, _, _)), _, _, _),
        sel_3q@modular.Select(
        _, _, _, _, _,
        Seq(gb_2q@modular.GroupBy(_, _, _, _, _, _, _)), _, _, _),
        Some(gb_2c@modular.GroupBy(_, _, _, _, _, _, _)),
        rchild :: Nil,
        echild :: Nil) =>
        val tbls_sel_3a = sel_3a.collect { case tbl: modular.LeafNode => tbl }
        val tbls_sel_3q = sel_3q.collect { case tbl: modular.LeafNode => tbl }

        val extrajoin = tbls_sel_3a.filterNot(tbls_sel_3q.contains)
        val rejoin = tbls_sel_3q.filterNot(tbls_sel_3a.contains)
        val rejoinOutputList = rejoin.flatMap(_.output)

        val isPredicateRmE = sel_3a.predicateList.forall(expr =>
          sel_3q.predicateList.exists(_.semanticEquals(expr)) ||
          gb_2c.predicateList.exists(_.semanticEquals(expr)))
        val isPredicateEmdR = sel_3q.predicateList
          .forall(expr =>
            sel_3a.predicateList.exists(_.semanticEquals(expr)) ||
            isDerivable(
              expr,
              sel_3a.outputList ++ rejoinOutputList,
              sel_3q,
              sel_3a,
              compensation))
        val isOutputEdR = sel_3q.outputList
          .forall(expr =>
            isDerivable(
              expr,
              sel_3a.outputList ++ rejoinOutputList,
              sel_3q,
              sel_3a,
              compensation))

        val canSELPullup = gb_2c.child.isInstanceOf[Select] &&
                           gb_2c.child.asInstanceOf[Select].predicateList
                             .forall(expr =>
                               isDerivable(
                                 expr,
                                 sel_3a.outputList ++ rejoinOutputList,
                                 sel_3q,
                                 sel_3a,
                                 compensation))
        val canGBPullup = gb_2c.predicateList
          .forall(expr =>
            isDerivable(
              expr,
              sel_3a.outputList ++ rejoinOutputList,
              sel_3q,
              sel_3a,
              compensation))

        if (extrajoin.isEmpty && isPredicateRmE &&
            isPredicateEmdR &&
            isOutputEdR &&
            canSELPullup &&
            canGBPullup) {
          gb_2c.child match {
            case s: Select =>
              val sel_3c1 = s.withNewChildren(
                s.children.map {
                  case gb: GroupBy => sel_3a.setSkip()
                  case other => other })
              val gb_3c2 = gb_2c.copy(child = sel_3c1)

              val aliasMap_exp = AttributeMap(
                gb_2c.outputList.collect {
                  case a: Alias => (a.toAttribute, a) })
              val sel_3q_exp = sel_3q.transformExpressions({
                case attr: Attribute if aliasMap_exp.contains(attr) => aliasMap_exp(attr)
              })

              val mappings = sel_3q_exp.outputList zip gb_2c.outputList

              val oList = for ((o1, o2) <- mappings) yield {
                if (o1.name != o2.name) Alias(o2, o1.name)(exprId = o1.exprId) else o2
              }

              val wip = sel_3q_exp.copy(outputList = oList, children = Seq(gb_3c2))
              val sel_3c3 = Some(factorOutSubsumer(wip, sel_3a, s.aliasMap.get(0)))
              sel_3c3.map(Seq(_)).getOrElse(Nil)

            case _ => Nil
          }
        } else {
          Nil
        }

      case _ => Nil
    }
  }
}


