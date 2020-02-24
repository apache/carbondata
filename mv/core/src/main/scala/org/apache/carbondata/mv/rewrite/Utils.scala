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

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, Cast, Divide, Expression, Literal, Multiply, NamedExpression, PredicateHelper, ScalaUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}

import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.{ModularPlan, Select}
import org.apache.carbondata.mv.timeseries.TimeSeriesFunction

/**
 * Utility functions used by mqo matcher to convert our plan to new aggregation code path
 */
object Utils extends PredicateHelper {

  // use for match qb_2a, qb_2q and sel_3a, sel_3q
  private def doMatch(
      operator_a: modular.Matchable,
      operator_q: modular.Matchable,
      alias_m: AttributeMap[Alias]): Option[modular.Matchable] = {
    var matchable = true
    val matched = operator_q.transformExpressions {
      case cnt_q@AggregateExpression(Count(exprs_q), _, false, _) =>
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Count] =>
            // case for groupby
            val cnt_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val exprs_a = cnt_a.aggregateFunction.asInstanceOf[Count].children
            if (cnt_a.isDistinct != cnt_q.isDistinct || exprs_q.length != exprs_a.length) {
              false
            } else {
              exprs_a.sortBy(_.hashCode()).zip(exprs_q.sortBy(_.hashCode()))
                .forall(p => p._1.semanticEquals(p._2))
            }

          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Count] =>
            val cnt_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val exprs_a = cnt_a.aggregateFunction.asInstanceOf[Count].children
            if (cnt_a.isDistinct != cnt_q.isDistinct || exprs_q.length != exprs_a.length) {
              false
            } else {
              exprs_a.sortBy(_.hashCode()).zip(exprs_q.sortBy(_.hashCode()))
                .forall(p => p._1.semanticEquals(p._2))
            }

          case _ => false
        }.map { cnt => AggregateExpression(
            Sum(cnt.toAttribute),
            cnt_q.mode,
            isDistinct = false,
            cnt_q.resultId)
        }.getOrElse { matchable = false; cnt_q }

      case sum_q@AggregateExpression(Sum(expr_q), _, false, _) =>
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Sum] =>
            val sum_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val expr_a = sum_a.aggregateFunction.asInstanceOf[Sum].child
            if (sum_a.isDistinct != sum_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }

          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Sum] =>
            val sum_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val expr_a = sum_a.aggregateFunction.asInstanceOf[Sum].child
            if (sum_a.isDistinct != sum_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }

          case _ => false
        }.map { sum => AggregateExpression(
            Sum(sum.toAttribute),
            sum_q.mode,
            isDistinct = false,
            sum_q.resultId)
        }.getOrElse { matchable = false; sum_q }

      case max_q@AggregateExpression(Max(expr_q), _, false, _) =>
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Max] =>
            val max_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val expr_a = max_a.aggregateFunction.asInstanceOf[Max].child
            if (max_a.isDistinct != max_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }

          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Max] =>
            val max_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val expr_a = max_a.aggregateFunction.asInstanceOf[Max].child
            if (max_a.isDistinct != max_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }

          case _ => false
        }.map { max => AggregateExpression(
            Max(max.toAttribute),
            max_q.mode,
            isDistinct = false,
            max_q.resultId)
        }.getOrElse { matchable = false; max_q }

      case min_q@AggregateExpression(Min(expr_q), _, false, _) =>
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Min] => {
            val min_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val expr_a = min_a.aggregateFunction.asInstanceOf[Min].child
            if (min_a.isDistinct != min_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }
          }
          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Min] => {
            val min_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val expr_a = min_a.aggregateFunction.asInstanceOf[Min].child
            if (min_a.isDistinct != min_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }
          }
          case _ => false
        }.map { min => AggregateExpression(
            Min(min.toAttribute),
            min_q.mode,
            isDistinct = false,
            min_q.resultId)
        }.getOrElse { matchable = false; min_q }


      case avg_q@AggregateExpression(Average(expr_q), _, false, _) =>
        val cnt_q = operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Count] => { // case for groupby
            val cnt_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val exprs_a = cnt_a.aggregateFunction.asInstanceOf[Count].children
            if (!cnt_a.isDistinct && exprs_a.sameElements(Set(expr_q))) {
              true
            } else {
              false
            }
          }
          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Count] => {
            val cnt_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val exprs_a = cnt_a.aggregateFunction.asInstanceOf[Count].children
            if (!cnt_a.isDistinct && exprs_a.sameElements(Set(expr_q))) {
              true
            } else {
              false
            }
          }
          case _ => false
        }.map { cnt => Sum(cnt.toAttribute) }
          .getOrElse { matchable = false; NoOp }

        val derivative = if (matchable) {
          operator_a.outputList.find {
            case alias: Alias if alias_m.contains(alias.toAttribute) &&
                                 alias_m(alias.toAttribute).child
                                   .isInstanceOf[AggregateExpression] &&
                                 alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                   .aggregateFunction.isInstanceOf[Sum] => {
              val sum_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
              val expr_a = sum_a.aggregateFunction.asInstanceOf[Sum].child
              if (sum_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            }
            case attr: Attribute if alias_m.contains(attr) &&
                                    alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                    alias_m(attr).child.asInstanceOf[AggregateExpression]
                                      .aggregateFunction.isInstanceOf[Sum] => {
              val sum_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
              val expr_a = sum_a.aggregateFunction.asInstanceOf[Sum].child
              if (sum_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            }
            case alias: Alias if alias_m.contains(alias.toAttribute) &&
                                 alias_m(alias.toAttribute).child
                                   .isInstanceOf[AggregateExpression] &&
                                 alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                   .aggregateFunction.isInstanceOf[Average] => {
              val avg_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            }
            case attr: Attribute if alias_m.contains(attr) &&
                                    alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                    alias_m(attr).child.asInstanceOf[AggregateExpression]
                                      .aggregateFunction.isInstanceOf[Average] => {
              val avg_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            }
            case _ => false
          }.map { sum_or_avg =>
            val fun = alias_m(sum_or_avg.toAttribute).child.asInstanceOf[AggregateExpression]
              .aggregateFunction
            if (fun.isInstanceOf[Sum]) {
              val accu = Sum(sum_or_avg.toAttribute)
              Divide(accu, Cast(cnt_q, accu.dataType))
            } else {
              val accu = Sum(Multiply(sum_or_avg.toAttribute, Cast(cnt_q, sum_or_avg.dataType)))
              Divide(accu, Cast(cnt_q, accu.dataType))
            }
          }
        } else {
          matchable = false
          None
        }
        // If derivative is empty, check if subsumer contains aggregateFunction instance of Average
        // function and form an Average expression
        if (derivative.isEmpty) {
          matchable = true
          operator_a.outputList.find {
            case alias: Alias if alias_m.contains(alias.toAttribute) &&
                                 alias_m(alias.toAttribute).child
                                   .isInstanceOf[AggregateExpression] &&
                                 alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                   .aggregateFunction.isInstanceOf[Average] => {
              val avg_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            }
            case attr: Attribute if alias_m.contains(attr) &&
                                    alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                    alias_m(attr).child.asInstanceOf[AggregateExpression]
                                      .aggregateFunction.isInstanceOf[Average] => {
              val avg_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            }
            case _ => false
          }.map { avg => AggregateExpression(
            Average(avg.toAttribute),
            avg_q.mode,
            isDistinct = false,
            avg_q.resultId)
          }.getOrElse { matchable = false; avg_q }
        } else {
          derivative.getOrElse { matchable = false; avg_q }
        }

      case other: AggregateExpression =>
        matchable = false
        other

      case expr: Expression if !expr.isInstanceOf[AggregateFunction] =>
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               (alias_m(alias.toAttribute).child.semanticEquals(expr) ||
                                Utils.isExpressionMatchesUDF(alias_m(alias.toAttribute), expr)) &&
                               !alias_m(alias.toAttribute).child
                                 .isInstanceOf[AggregateExpression] => true
          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.semanticEquals(expr) &&
                                  !alias_m(attr).child.isInstanceOf[AggregateExpression] => true
          case _ => false
        }.map(_.toAttribute)
         .getOrElse { expr }
    }

    if (matchable) {
      Some(matched)
    } else {
      None
    }
  }

  def tryMatch(a: modular.Matchable,
      q: modular.Matchable,
      m: AttributeMap[Alias]): Option[modular.Matchable] = {
    if (a.getClass == q.getClass) {
      doMatch(a, q, m)
    } else {
      None
    }
  }

  /**
   * (Subsumee) expression translation:
   *
   * The translation begins by creating a copy of the whole expression (step 1).  Then each input
   * column is translated in turn.
   * To translate an input column, we first find the child block that produces the input column
   * and replace the input column with the
   * associated output column expression (step 2).  The next step is to replace the translated
   * expression with its equivalent output
   * expression at the top of the child compensation (step 3).  Then, We recursively translate
   * each new input column(except input
   * columns produced by rejoin children) until we reach the bottom of the child compensation
   * (step 4).  Finally, we find an
   * equivalent output expression in subsumer (step 5).
   *
   * So given a subsumee expr, the translation follows the following path:
   *
   * top of subsumee --> child of subsumee --> top of compensation --> bottom of compensation -->
   * top of subsumer
   *
   * To simplify this we assume in subsumer outputList of top select 1-1 corresponds to the
   * outputList of groupby
   * note that subsumer outputList is list of attributes and that of groupby is list of aliases
   *
   */
  private def doTopSelectTranslation(exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]): Option[Expression] = {
    (subsumer, subsumee, compensation) match {
      // top selects whose children do not match exactly
      // for simplicity, we assume outputList of subsumer is 1-1 corresponding to that of its
      // immediately groupby child
      case (
        sel_3a@modular.Select(
          _, _, _, _, _,
          Seq(gb_2a@modular.GroupBy(
            _, _, _, _, sel_2a@modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)),
          _, _, _, _),
        sel_3q@modular.Select(
          _, _, _, _, _, Seq(gb_2q@modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _),
        Some(gb_2c@modular.GroupBy(
          _, _, _, _, sel_2c@modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _))
        ) =>
        if (sel_3q.predicateList.contains(exprE)) {
          val expr1E = exprE.transform {
            case attr: Attribute =>
              gb_2c.outputList.lift(
                gb_2q.outputList.indexWhere {
                  case alias: Alias if alias.toAttribute.semanticEquals(attr) => true;
                  case other => false
                  }).getOrElse { attr }
          }
          if (expr1E.eq(exprE)) {
            None
          } else {
            Some(expr1E)
          }
        }
        else if (sel_3q.outputList.contains(exprE)) {
          exprE match {
            case attr: Attribute => // this subexpression must in subsumee select output list
              gb_2c.outputList.lift(
                gb_2q.outputList.indexWhere {
                  case a if a.toAttribute.semanticEquals(attr) => true;
                  case other => false
                  })

            case alias: Alias =>
              gb_2c.outputList.lift(
                gb_2q.outputList.indexWhere {
                  case a if a.toAttribute.semanticEquals(alias.toAttribute) => true;
                  case other => false
                  })

            case _ => None
          }
        } else if (sel_2c.predicateList.contains(exprE)) {
          if (sel_2a.predicateList.exists(_.semanticEquals(exprE)) ||
              canEvaluate(exprE, subsumer) || canBeDerived(subsumer, exprE)) {
            Some(exprE)
          } else {
            None
          }
        } else if (gb_2c.predicateList.contains(exprE)) {
          if (gb_2a.outputList.exists {
                case a: Alias if a.child.semanticEquals(exprE) => true;
                case _ => false
              } || canEvaluate(exprE, subsumer)) {
            Some(exprE)
          } else {
            None
          }
        } else if (sel_2a.predicateList.exists(_.semanticEquals(exprE)) ||
                   canEvaluate(exprE, subsumer)) {
          Some(exprE)
        } else {
          None
        }

      case _ => None // TODO: implement this
    }
  }

  private def canBeDerived(subsumer: ModularPlan, expE: Expression): Boolean = {
    var canBeDerived = false
    subsumer.asInstanceOf[Select].outputList.forall {
      case Alias(s: ScalaUDF, _) =>
        expE.children.foreach { expr =>
          if (s.semanticEquals(expr)|| isExpressionMatchesUDF(s, expr)) {
            canBeDerived = true
          }
          // It is because when expression is like between filter, the expr will be as Cast
          // expression and its child will be ScalaUDF(timeseries), So compare the child also.
          if (!canBeDerived && null != expr.children) {
            expr.children.foreach { expC =>
              if (s.semanticEquals(expC) || isExpressionMatchesUDF(s, expC)) {
                canBeDerived = true
              }
            }
          }
        }
        canBeDerived
      case _ =>
        canBeDerived
    }
  }

  private def isSemanticEquivalent(translatedExpr: Expression, subsumer: ModularPlan) = {
    subsumer match {
      // if subsumer has where clause, even if expr can be translated into new expr based on
      // subsumer, the two may not be semantic equivalent
      // TODO: refine this
      case modular.Select(
        _, _, predicateList, _, _,
        Seq(modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _)
        if predicateList.nonEmpty => false
      case _ => true
    }
  }

  /**
   * derivable = translatable + semantic equivalent
   *
   * The translation method described above is also the first step in deriving a subsumee
   * expression Eexp from the subsumer's output columns.  After translating
   * Eexp to E'exp, deriavability can be established by making sure that the subsumer
   * computes at its output certain necessary subexpressions of E'exp (or even the entire
   * E'exp).  The problem that arises, however, is to determine the parts of E'exp that
   * can/should be computed by the subsumer.
   *
   * In general, translation causes an expression to expand by replacing individual input
   * columns with equivalent subexpressions.  Derivation is the reverse operation, where
   * pieces of the translated expression are collapsed as they are computed along the
   * derivation path.
   */

  def isDerivable(exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]): Boolean = {
    val exprE1 = doTopSelectTranslation(exprE, exprListR, subsumee, subsumer, compensation)
    exprE1 match {
      case Some(e) => isSemanticEquivalent(e, subsumer)
      case _ => false
    }
  }

  /**
   * Check's if timeseries udf function exists. If exists, compare literal with case insensitive
   * value
   */
  def isExpressionMatchesUDF(subsumeExp: Expression, subsumerExprList: Seq[Expression]): Boolean = {
    // Check if expression has a ScalaUDF of timeSeries function and verify it's children
    // irrespective of case. The structure of scalaUDF function will look like,
    //                ScalaUDF
    //                    |
    //             TimeSeriesFunction
    //                 /    \
    //                /      \
    //   AttributeReference   Literal
    subsumeExp match {
      case Alias(udf: ScalaUDF, _) if udf.function.isInstanceOf[TimeSeriesFunction] =>
        val children = udf.children
        val subsumerTimeSeriesExp = subsumerExprList.filter(a1 =>
          a1.isInstanceOf[Alias] && a1.asInstanceOf[Alias].child.isInstanceOf[ScalaUDF] &&
          a1.asInstanceOf[Alias].child.asInstanceOf[ScalaUDF].function.
            isInstanceOf[TimeSeriesFunction])
        subsumerTimeSeriesExp.exists(f => {
          val childExprsOfTimeSeriesUDF = f.asInstanceOf[Alias].child
            .asInstanceOf[ScalaUDF].children
          childExprsOfTimeSeriesUDF.head.semanticEquals(children.head) &&
          childExprsOfTimeSeriesUDF.last.asInstanceOf[Literal].toString().equalsIgnoreCase(
            children.last.asInstanceOf[Literal].toString())
        })
      case udf: ScalaUDF if udf.function.isInstanceOf[TimeSeriesFunction] =>
        val children = udf.children
        var subsumerTimeSeriesExprList: Seq[Expression] = Seq.empty
        subsumerExprList foreach {
          case s: ScalaUDF if s.function.isInstanceOf[TimeSeriesFunction] =>
            subsumerTimeSeriesExprList = subsumerTimeSeriesExprList.+:(s)
          case Alias(s: ScalaUDF, _) if s.function.isInstanceOf[TimeSeriesFunction] =>
            subsumerTimeSeriesExprList = subsumerTimeSeriesExprList.+:(s.asInstanceOf[Expression])
          case _ =>
        }
        subsumerTimeSeriesExprList.exists(f => {
          val childExprsOfTimeSeriesUDF = f.asInstanceOf[ScalaUDF].children
          childExprsOfTimeSeriesUDF.head.semanticEquals(children.head) &&
          childExprsOfTimeSeriesUDF.last.asInstanceOf[Literal].toString().equalsIgnoreCase(
            children.last.asInstanceOf[Literal].toString())
        })
      case exp: Expression =>
        val transformedExpwithLowerCase = exp.transform {
          case s: ScalaUDF if s.function.isInstanceOf[TimeSeriesFunction] =>
            getTransformedTimeSeriesUDF(s)
          case other => other
        }
        val transformedExprListWithLowerCase = subsumerExprList map { expr =>
          expr.transform {
            case s: ScalaUDF if s.function.isInstanceOf[TimeSeriesFunction] =>
              getTransformedTimeSeriesUDF(s)
            case other => other
          }
        }
        transformedExprListWithLowerCase.exists(_.semanticEquals(transformedExpwithLowerCase))
      case _ => false
    }
  }

  def getTransformedTimeSeriesUDF(s: ScalaUDF): Expression = {
    s.transform {
      case l: Literal =>
        Literal(l.toString().toLowerCase, l.dataType)
    }
  }

  /**
   * Check if expr1 and expr2 matches TimeSeriesUDF function. If both expressions are
   * timeseries udf functions, then check it's childrens are same irrespective of case.
   */
  def isExpressionMatchesUDF(expr1: Expression, expr2: Expression): Boolean = {
    (expr1, expr2) match {
      case (s1: ScalaUDF, s2: ScalaUDF) if s1.function.isInstanceOf[TimeSeriesFunction] &&
                                           s2.function.isInstanceOf[TimeSeriesFunction] =>
        s1.children.head.semanticEquals(s2.children.head) &&
        s1.children.last.asInstanceOf[Literal].toString()
          .equalsIgnoreCase(s2.children.last.asInstanceOf[Literal].toString())
      case _ => false
    }
  }
}
