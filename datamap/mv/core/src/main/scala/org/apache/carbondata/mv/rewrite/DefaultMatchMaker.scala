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

import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, PredicateHelper, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter}
import org.apache.spark.sql.types.{DataType, Metadata}

import org.apache.carbondata.mv.plans.modular.{JoinEdge, Matchable, ModularPlan, _}
import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util.SQLBuilder
import org.apache.carbondata.mv.timeseries.TimeSeriesFunction


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
      aliasMapMain: Map[Int, String]): ModularPlan = {

    // Create aliasMap with attribute to alias reference attribute
    val aliasMap = AttributeMap(
        subsumer.outputList.collect {
          case a: Alias if a.child.isInstanceOf[Attribute] =>
            (a.child.asInstanceOf[Attribute], a.toAttribute)
          })

    // Create aliasMap with Expression to alias reference attribute
    val aliasMapExp =
      subsumer.outputList.collect {
        case a: Alias if a.child.isInstanceOf[Expression] &&
                         !a.child.isInstanceOf[AggregateExpression] =>
          a.child match {
            case s: ScalaUDF if s.function.isInstanceOf[TimeSeriesFunction] =>
              Utils.getTransformedTimeSeriesUDF(s) -> a.toAttribute
            case _ =>
              a.child -> a.toAttribute
          }
      }.toMap

    // Check and replace all alias references with subsumer alias map references.
    val compensation1 = compensation.transform {
      case plan if !plan.skip && plan != subsumer =>
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
          case a: Expression =>
            var attribute = aliasMapExp
              .get(a)
            // attribute will be empty, if attribute name is of different case. If empty, change
            // case of scalaUDF present in expression and get updated expression from aliasMap
            if (attribute.isEmpty) {
              val newExp = a transform {
                case s: ScalaUDF if s.function.isInstanceOf[TimeSeriesFunction] =>
                  Utils.getTransformedTimeSeriesUDF(s)
              }
              attribute = aliasMapExp.get(newExp)
            }
            attribute
              .map { ref =>
                AttributeReference(
                  ref.name, ref.dataType)(
                  exprId = ref.exprId)
              }.getOrElse(a)
          }
    }

    val subqueryAttributeSet = SQLBuilder.collectAttributeSet(subsumer.outputList)
    if (SQLBuilder.collectDuplicateNames(subqueryAttributeSet).nonEmpty) {
      new UnsupportedOperationException(
        s"duplicate name(s): ${ subsumer.output.map(_.toString + ", ") }")
    }
    if (aliasMapMain.size == 1) {
      val subsumerName: Option[String] = aliasMapMain.get(0)
      // Replace all compensation1 attributes with refrences of subsumer attributeset
      val compensationFinal = compensation1.transformExpressions {
        case ref: Attribute if subqueryAttributeSet.contains(ref) =>
          CarbonToSparkAdapter.createAttributeReference(
            ref.name, ref.dataType, nullable = true, metadata = Metadata.empty,
            exprId = ref.exprId, qualifier = subsumerName)
        case alias: Alias if subqueryAttributeSet.contains(alias.toAttribute) =>
          CarbonToSparkAdapter.createAliasRef(
            alias.child, alias.name, alias.exprId, subsumerName)
      }
      compensationFinal
    } else {
      compensation1
    }
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
      canEvaluate(exprE, subsumer) ||
      Utils.isExpressionMatchesUDF(exprE, subsumer.asInstanceOf[Select].predicateList)
    } else if (subsumee.asInstanceOf[Select].outputList.contains(exprE)) {
      exprE match {
        case a@Alias(_, _) =>
          exprListR.exists(a1 => a1.isInstanceOf[Alias] &&
                                 a1.asInstanceOf[Alias].child.semanticEquals(a.child)) ||
          exprListR.exists(_.semanticEquals(exprE) || canEvaluate(exprE, subsumer)) ||
          Utils.isExpressionMatchesUDF(exprE, exprListR)
        case exp =>
          exprListR.exists(a1 => a1.isInstanceOf[Alias] &&
                                 a1.asInstanceOf[Alias].child.semanticEquals(exp)) ||
          exprListR.exists(_.semanticEquals(exprE) || canEvaluate(exprE, subsumer)) ||
          Utils.isExpressionMatchesUDF(exprE, exprListR)
      }
    } else {
      false
    }
  }

  private def isLeftJoinView(subsumer: ModularPlan): Boolean = {
    subsumer match {
      case sel: Select
        if sel.joinEdges.length == 1 &&
           sel.joinEdges.head.joinType == LeftOuter &&
           sel.children(1).isInstanceOf[HarmonizedRelation] =>
        val hDim = sel.children(1).asInstanceOf[HarmonizedRelation]
        hDim.tag match {
          case Some(tag) => sel.outputList.contains(tag)
          case None => false
        }
      case _ => false
    }
  }

  def apply(subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      rewrite: QueryRewrite): Seq[ModularPlan] = {

    (subsumer, subsumee, compensation) match {
      case (
          sel_1a @ modular.Select(_, _, _, _, _, _, _, _, _, _),
          sel_1q @ modular.Select(_, _, _, _, _, _, _, _, _, _), None
        ) if sel_1a.children.forall { _.isInstanceOf[modular.LeafNode] } &&
             sel_1q.children.forall { _.isInstanceOf[modular.LeafNode] } =>

        // assume children (including harmonized relation) of subsumer and subsumee
        // are 1-1 correspondence.
        // Change the following two conditions to more complicated ones if we want to
        // consider things that combine extrajoin, rejoin, and harmonized relations
        val isUniqueRmE = subsumer.children.filter { x => subsumee.children.count{
          case relation: ModularRelation => relation.fineEquals(x)
          case other => other == x
        } != 1 }
        val isUniqueEmR = subsumee.children.filter { x => subsumer.children.count{
          case relation: ModularRelation => relation.fineEquals(x)
          case other => other == x
        } != 1 }

        val extrajoin = sel_1a.children.filterNot { child => sel_1q.children.contains(child) }
        val rejoin = sel_1q.children.filterNot { child => sel_1a.children.contains(child) }
        val rejoinOutputList = rejoin.flatMap(_.output)

        val isPredicateRmE = sel_1a.predicateList.forall(expr =>
          sel_1q.predicateList.exists(_.semanticEquals(expr)) ||
          Utils.isExpressionMatchesUDF(expr, sel_1q.predicateList))
        val isPredicateEmdR = sel_1q.predicateList.forall(expr =>
          isDerivable(expr, sel_1a.outputList ++ rejoinOutputList, sel_1q, sel_1a, None))
        // Check if sel_1q.outputList is non empty and then check whether
        // it can be derivable with sel_1a otherwise for empty cases it returns true.
        val isOutputEdR = sel_1q.outputList.nonEmpty && sel_1q.outputList.forall(expr =>
          isDerivable(expr, sel_1a.outputList ++ rejoinOutputList, sel_1q, sel_1a, None))

        if (isUniqueRmE.isEmpty && isUniqueEmR.isEmpty && extrajoin.isEmpty && isPredicateRmE &&
            isPredicateEmdR && isOutputEdR) {
          val mappings = sel_1a.children.zipWithIndex.map {
            case (childr, fromIdx) if sel_1q.children.contains(childr) =>
              val toIndx = sel_1q.children.indexWhere{
                case relation: ModularRelation => relation.fineEquals(childr)
                case other => other == childr
              }
              (toIndx -> fromIdx)

          }
          val e2r = mappings.toMap
          val r2e = e2r.map(_.swap)
          val r2eJoinsMatch = sel_1a.joinEdges.forall { x =>
              (r2e.get(x.left), r2e.get(x.right)) match {
                case (Some(l), Some(r)) =>
                  val mappedEdge = JoinEdge(l, r, x.joinType)
                  val joinTypeEquivalent =
                    if (sel_1q.joinEdges.contains(mappedEdge)) {
                      true
                    } else {
                      x.joinType match {
                        case Inner | FullOuter =>
                          sel_1q.joinEdges.contains(JoinEdge(r, l, x.joinType))
                        case LeftOuter if isLeftJoinView(sel_1a) =>
                          sel_1q.joinEdges.contains(JoinEdge(l, r, Inner)) ||
                          sel_1q.joinEdges.contains(JoinEdge(r, l, Inner))
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
                    sel_1a_join.forall(e => sel_1q_join.exists(e.semanticEquals(_)) ||
                    Utils.isExpressionMatchesUDF(e, sel_1q_join)) &&
                    sel_1q_join.forall(e => sel_1a_join.exists(e.semanticEquals(_)) ||
                    Utils.isExpressionMatchesUDF(e, sel_1a_join))
                  } else false
                case _ => false
              }
          }

          val isPredicateEmR = sel_1q.predicateList.forall(expr =>
            sel_1a.predicateList.exists(_.semanticEquals(expr)) ||
            Utils.isExpressionMatchesUDF(expr, sel_1a.predicateList))
          val isOutputEmR = sel_1q.outputList.forall(expr =>
            sel_1a.outputList.exists(_.semanticEquals(expr)) ||
            Utils.isExpressionMatchesUDF(expr, sel_1a.outputList))
          val isOutputRmE = sel_1a.outputList.forall(expr =>
            sel_1q.outputList.exists(_.semanticEquals(expr)) ||
            Utils.isExpressionMatchesUDF(expr, sel_1q.outputList))
          val isLOEmLOR = !(isLeftJoinView(sel_1a) && sel_1q.joinEdges.head.joinType == Inner)

          if (r2eJoinsMatch) {
            if (isPredicateEmR && isOutputEmR && isOutputRmE && rejoin.isEmpty && isLOEmLOR) {
              Seq(sel_1a)
            } else {
              // no compensation needed
              val tChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
              val tAliasMap = new collection.mutable.HashMap[Int, String]()

              val usel_1a = sel_1a.copy(outputList = sel_1a.outputList)
              tChildren += usel_1a
              tAliasMap += (tChildren.indexOf(usel_1a) -> rewrite.newSubsumerName())

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
                !sel_1a.predicateList.exists(_.semanticEquals(p))
              } ++ (if (isLeftJoinView(sel_1a) &&
                        sel_1q.joinEdges.head.joinType == Inner) {
                sel_1a.children(1)
                  .asInstanceOf[HarmonizedRelation].tag.map(IsNotNull(_)).toSeq
              } else {
                Seq.empty
              })
              val sel_1q_temp = sel_1q.copy(
                predicateList = tPredicateList,
                children = tChildren,
                joinEdges = tJoinEdges.filter(_ != null),
                aliasMap = tAliasMap.toMap)

              val done = factorOutSubsumer(sel_1q_temp, usel_1a, sel_1q_temp.aliasMap)
              Seq(done)
            }
          } else Nil
        } else Nil

      case (
        sel_3a @ modular.Select(_, _, _, _, _, _, _, _, _, _),
        sel_3q @ modular.Select(_, _, _, _, _, _, _, _, _, _), None)
        if sel_3a.children.forall(_.isInstanceOf[GroupBy]) &&
           sel_3q.children.forall(_.isInstanceOf[GroupBy]) =>
        val isPredicateRmE = sel_3a.predicateList.isEmpty ||
                             sel_3a.predicateList.forall(expr =>
                               sel_3q.predicateList.exists(_.semanticEquals(expr)) ||
                               Utils.isExpressionMatchesUDF(expr, sel_3q.predicateList))
        val isPredicateEmdR = sel_3q.predicateList.isEmpty ||
                              sel_3q.predicateList.forall(expr =>
                                sel_3a.predicateList.exists(_.semanticEquals(expr) ||
                                                            Utils.isExpressionMatchesUDF(expr,
                                                              sel_3a.predicateList)) ||
                                isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))
        val isOutputEdR = sel_3q.outputList.forall(expr =>
          isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))
        val isSingleChild = sel_3a.children.length == 1 && sel_3q.children.length == 1

        if (isPredicateRmE && isPredicateEmdR && isOutputEdR && isSingleChild) {
          val isPredicateEmR = sel_3q.predicateList.isEmpty ||
                               sel_3q.predicateList.forall(expr =>
                                 sel_3a.predicateList.exists(_.semanticEquals(expr)) ||
                                 Utils.isExpressionMatchesUDF(expr, sel_3a.predicateList))
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
                  (a1.asInstanceOf[Alias].child.semanticEquals(a.child) ||
                    Utils.isExpressionMatchesUDF(a1.asInstanceOf[Alias].child, a.child))
                }.map(_.toAttribute).get
            })
            val wip = sel_3q_exp.copy(
              children = Seq(sel_3a),
              aliasMap = Seq(0 -> rewrite.newSubsumerName()).toMap)
            val done = factorOutSubsumer(wip, sel_3a, wip.aliasMap)
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
        gb_2a @ modular.GroupBy(_, _, _, _, _, _, _, _),
        gb_2q @ modular.GroupBy(_, _, _, _, _, _, _, _),
        None) =>
        val isGroupingEmR = gb_2q.predicateList.forall(expr =>
          gb_2a.predicateList.exists(_.semanticEquals(expr)) ||
          Utils.isExpressionMatchesUDF(expr, gb_2a.predicateList))
        val isGroupingRmE = gb_2a.predicateList.forall(expr =>
          gb_2q.predicateList.exists(_.semanticEquals(expr)) ||
          Utils.isExpressionMatchesUDF(expr, gb_2q.predicateList))
        val isOutputEmR = gb_2q.outputList.forall {
          case a @ Alias(_, _) =>
            gb_2a.outputList.exists{
              case a1: Alias => a1.child.semanticEquals(a.child)
              case exp => exp.semanticEquals(a.child)
            }
          case exp => gb_2a.outputList.exists(_.semanticEquals(exp))
        }
        if (isGroupingEmR && isGroupingRmE) {
          if (isOutputEmR) {
            // Mappings of output of two plans by checking semantic equals.
            val mappings = gb_2a.outputList.zipWithIndex.map { case(exp, index) =>
              (exp, gb_2q.outputList.find {
                case a: Alias if exp.isInstanceOf[Alias] =>
                  a.child.semanticEquals(exp.children.head) ||
                  Utils.isExpressionMatchesUDF(a.child, exp.children.head)
                case a: Alias => a.child.semanticEquals(exp)
                case other => other.semanticEquals(exp)
              }.getOrElse(gb_2a.outputList(index)))
            }

            val oList = mappings.map{case (out1, out2) =>
              if (out1.name != out2.name) out1 match {
                case alias: Alias => Alias(alias.child, out2.name)(exprId = alias.exprId)
                case _ => Alias(out1, out2.name)(exprId = out2.exprId)
              } else out1
            }

            Seq(gb_2a.copy(outputList = oList))
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
                // Check any agg function exists on outputlist, in case of expressions like
                // sum(a), then create new alias and copy to group by node
                val aggFunExists = g.outputList.exists { f =>
                  f.find {
                    case _: AggregateExpression => true
                    case _ => false
                  }.isDefined
                }
                if (aggFunExists && !isGroupingRmE && isOutputEmR) {
                  val tChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
                  val sel_1a = g.child.asInstanceOf[Select]

                  val usel_1a = sel_1a.copy(outputList = sel_1a.outputList)
                  tChildren += gb_2a
                  val sel_1q_temp = sel_1a.copy(
                    predicateList = sel_1a.predicateList,
                    children = tChildren,
                    joinEdges = sel_1a.joinEdges,
                    aliasMap = Seq(0 -> rewrite.newSubsumerName()).toMap)

                  val res = factorOutSubsumer(sel_1q_temp, usel_1a, sel_1q_temp.aliasMap)
                  Some(g.copy(child = res))
                } else {
                  Some(g.copy(child = g.child.withNewChildren(
                    g.child.children.map {
                      case modular.Select(_, _, _, _, _, _, _, _, _, _) => gb_2a;
                      case other => other
                    })));
                }
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
      if (exprListR.exists(_.semanticEquals(exprE)) || canEvaluate(exprE, exprListR) ||
          isDerivableForUDF(exprE, exprListR) || Utils.isExpressionMatchesUDF(exprE, exprListR)) {
        true
      } else {
        false
      }
    } else if (compensation.getOrElse(throw new RuntimeException("compensation cannot be None"))
      .asInstanceOf[Select].predicateList.contains(exprE)) {
      if (canEvaluate(exprE, exprListR) || exprListR.exists(_.semanticEquals(exprE)) ||
          isDerivableForUDF(exprE, exprListR) || Utils.isExpressionMatchesUDF(exprE, exprListR)) {
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  /**
   * org.apache.carbondata.mv.plans.MorePredicateHelper#canEvaluate will be checking the
   * exprE.references as subset of AttibuteSet(exprListR), which will just take the column name from
   * UDF, so it will be always false for ScalaUDF.
   * This method takes care of checking whether the exprE references can be derived form list
   *
   * Example:
   * exprE has attribute reference for timeseries column like UDF:timeseries(projectjoindate, month)
   * So exprE.references will give UDF:timeseries(projectjoindate, month)
   * exprListR has ScalaUDF also, so AttributeSet(exprListR) contains just column name like
   * projectjoindate, so canEvaluate method returns false.
   *
   * Here checking whether the exprListR(ScalaUDF) .sql gives UDF:timeseries(projectjoindate, month)
   * which can be checked with exprE.references
   */
  private def isDerivableForUDF(exprE: Expression, exprListR: Seq[Expression]): Boolean = {
    var canBeDerived = false
    exprE match {
      case f: ScalaUDF =>
        canEvaluate(f, exprListR)
      case _ =>
        exprListR.forall {
          case a: ScalaUDF =>
            a.references.foreach { a =>
              canBeDerived = exprE.sql.contains(a.name)
            }
            canBeDerived
          case _ =>
            canBeDerived
        }
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
        gb_2a @ modular.GroupBy(_, _, _, _, _, _, _, _),
        gb_2q @ modular.GroupBy(_, _, _, _, _, _, _, _),
        Some(sel_1c1 @ modular.Select(_, _, _, _, _, _, _, _, _, _)),
        true,
        true)
        if !gb_2q.flags.hasFlag(EXPAND) && !gb_2a.flags.hasFlag(EXPAND) =>

        val rejoinOutputList = sel_1c1.children.tail.flatMap(_.output)
        val isGroupingEdR = gb_2q.predicateList.forall(expr =>
          isDerivable(expr, gb_2a.predicateList ++ rejoinOutputList, gb_2q, gb_2a, compensation))
        val needRegrouping = !gb_2a.predicateList
          .forall(f => gb_2q.predicateList.contains(f) ||
                       Utils.isExpressionMatchesUDF(f, gb_2q.predicateList))
        val canPullup = sel_1c1.predicateList.forall(expr =>
          isDerivable(expr, gb_2a.predicateList ++ rejoinOutputList, gb_2q, gb_2a, compensation))
        val isAggEmR = gb_2q.outputList.collect {
          case agg: aggregate.AggregateExpression =>
            gb_2a.outputList.exists(_.semanticEquals(agg))
        }.forall(identity)

        if (isGroupingEdR && ((!needRegrouping && isAggEmR) || needRegrouping) && canPullup) {
          // pull up
          val pullupOutputList = gb_2a.outputList.map(_.toAttribute) ++ rejoinOutputList
          val myOutputList = gb_2a.outputList.filter {
            case alias: Alias =>
              val aliasList = gb_2q.outputList.filter(_.isInstanceOf[Alias])
              aliasList.exists(_.asInstanceOf[Alias].child.semanticEquals(alias.child)) ||
              Utils.isExpressionMatchesUDF(alias.child, aliasList)
            case attr: Attribute =>
              gb_2q.outputList.exists(_.semanticEquals(attr))
          }.map(_.toAttribute) ++ rejoinOutputList
          // TODO: find out if we really need to check needRegrouping or just use myOutputList
          val sel_2c1 = if (needRegrouping) {
            sel_1c1
              .copy(outputList = pullupOutputList,
                inputList = pullupOutputList,
                children = sel_1c1.children
                  .map { _ match { case s: modular.Select => gb_2a; case other => other } })
          } else {
            sel_1c1
              .copy(outputList = myOutputList,
                inputList = pullupOutputList,
                children = sel_1c1.children
                  .map { _ match { case s: modular.Select => gb_2a; case other => other } })
          }
          // sel_1c1.copy(outputList = pullupOutputList, inputList = pullupOutputList, children =
          // sel_1c1.children.map { _ match { case s: modular.Select => gb_2a; case other =>
          // other } })

          if (rejoinOutputList.isEmpty) {
            val aliasMap = AttributeMap(gb_2a.outputList.collect {
              case a: Alias => (a.toAttribute, a)
            })
            val res =
            Utils.tryMatch(gb_2a, gb_2q, aliasMap).flatMap {
              case g: GroupBy =>

                // Check any agg function exists on outputlist, in case of expressions like
                // sum(a)+sum(b) , outputlist directly replaces with alias with in place of function
                // so we should remove the groupby clause in those cases.
                val aggFunExists = g.outputList.exists { f =>
                  f.find {
                    case ag: AggregateExpression => true
                    case _ => false
                  }.isDefined
                }
                if (aggFunExists) {
                  Some(g.copy(child = sel_2c1))
                } else {
                  // Remove group by clause.
                  Some(g.copy(child = sel_2c1, predicateList = Seq.empty))
                }
              case _ => None
            }.map { wip =>
              factorOutSubsumer(wip, gb_2a, sel_1c1.aliasMap)
            }.map(Seq(_))
              .getOrElse(Nil)
            res
          }
          // TODO: implement regrouping with 1:N rejoin (rejoin tables being the "1" side)
          // via catalog service
          else if (!needRegrouping && isAggEmR) {
            Seq(sel_2c1).map(wip => factorOutSubsumer(wip, gb_2a, sel_1c1.aliasMap))
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
        modular.Select(_, _, _, _, _, _, _, _, _, _),
        modular.Select(_, _, _, _, _, _, _, _, _, _),
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
        modular.Select(_, _, _, _, _, _, _, _, _, _),
        modular.Select(_, _, _, _, _, _, _, _, _, _),
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
        Seq(gb_2a@modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _),
        sel_3q@modular.Select(
        _, _, _, _, _,
        Seq(gb_2q@modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _),
        Some(gb_2c@modular.GroupBy(_, _, _, _, _, _, _, _)),
        rchild :: Nil,
        echild :: Nil) =>
        val tbls_sel_3a = sel_3a.collect { case tbl: modular.LeafNode => tbl }
        val tbls_sel_3q = sel_3q.collect { case tbl: modular.LeafNode => tbl }

        val extrajoin = tbls_sel_3a.filterNot(tbls_sel_3q.contains)
        val rejoin = tbls_sel_3q.filterNot(tbls_sel_3a.contains)
        val rejoinOutputList = rejoin.flatMap(_.output)

        val isPredicateRmE = sel_3a.predicateList.forall(expr =>
          sel_3q.predicateList.exists(_.semanticEquals(expr)) ||
          Utils.isExpressionMatchesUDF(expr, sel_3q.predicateList) ||
          gb_2c.predicateList.exists(_.semanticEquals(expr)) ||
          Utils.isExpressionMatchesUDF(expr, gb_2c.predicateList))
        val isPredicateEmdR = sel_3q.predicateList
          .forall(expr =>
            sel_3a.predicateList.exists(_.semanticEquals(expr)) ||
            Utils.isExpressionMatchesUDF(expr, sel_3a.predicateList) ||
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
                  case a: Alias => (a.toAttribute, AliasWrapper(a)) })
              val sel_3q_exp = sel_3q.transformExpressions({
                case attr: Attribute if aliasMap_exp.contains(attr) => aliasMap_exp(attr)
              }).transformExpressions {
                case AliasWrapper(alias: Alias) => alias
              }
              // Mappings of output of two plans by checking semantic equals.
              val mappings = sel_3q_exp.outputList.zipWithIndex.map { case(exp, index) =>
                (exp, gb_2c.outputList.find {
                  case a: Alias if exp.isInstanceOf[Alias] =>
                    a.child.semanticEquals(exp.children.head) ||
                    Utils.isExpressionMatchesUDF(a.child, exp.children.head)
                  case a: Alias => a.child.semanticEquals(exp)
                  case other => other.semanticEquals(exp)
                }.getOrElse(gb_2c.outputList(index)))
              }

              val oList = CarbonToSparkAdapter.createAliases(mappings)

              val wip = sel_3q_exp.copy(outputList = oList, children = Seq(gb_3c2))
              val sel_3c3 = Some(factorOutSubsumer(wip, sel_3a, s.aliasMap))
              sel_3c3.map(Seq(_)).getOrElse(Nil)

            case _ => Nil
          }
        } else {
          Nil
        }

      case _ => Nil
    }
  }


  case class AliasWrapper(alias: Alias) extends UnaryExpression {
    override def child: Expression = null

    override protected def doGenCode(ctx: CodegenContext,
        ev: ExprCode): ExprCode = ev

    override def dataType: DataType = alias.dataType
  }

}


