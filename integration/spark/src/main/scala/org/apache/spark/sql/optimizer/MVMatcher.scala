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

package org.apache.spark.sql.optimizer

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeMap, AttributeReference, AttributeSet, Expression, PredicateHelper, _}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, AggregateFunction, Average, Count, Max, Min, NoOp, Sum}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter}
import org.apache.spark.sql.types.{DataType, Metadata}
import org.apache.spark.unsafe.types.UTF8String

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.mv.plans.modular.{JoinEdge, Matchable, ModularPlan, _}
import org.apache.carbondata.mv.plans.modular
import org.apache.carbondata.mv.plans.modular.Flags._
import org.apache.carbondata.mv.plans.util.SQLBuilder
import org.apache.carbondata.view.TimeSeriesFunction

private abstract class MVMatchMaker {

  /** Define a sequence of rules, to be overridden by the implementation. */
  protected val patterns: Seq[MVMatchPattern]

  def execute(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      subqueryNameGenerator: SubqueryNameGenerator): Iterator[ModularPlan] = {
    patterns.view.flatMap(
      _ (subsumer, subsumee, compensation, subqueryNameGenerator)
    ).toIterator
  }

}

private object MVMatchMaker extends MVMatchMaker {
  lazy val patterns: List[MVMatchPattern] =
    SelectSelectNoChildDelta ::
    GroupbyGroupbyNoChildDelta ::
    GroupbyGroupbySelectOnlyChildDelta ::
    GroupbyGroupbyGroupbyChildDelta ::
    SelectSelectSelectChildDelta ::
    SelectSelectGroupbyChildDelta :: Nil
}

private abstract class MVMatchPattern extends Logging {

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      generator: SubqueryNameGenerator): Seq[ModularPlan]

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
        case alias: Alias if alias.child.isInstanceOf[Expression] &&
          !alias.child.isInstanceOf[AggregateExpression] =>
          alias.child match {
            case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
              getTransformedTimeSeriesFunction(function) -> alias.toAttribute
            case cast: Cast if cast.child.isInstanceOf[AttributeReference] =>
              getTransformedCastOrImplicitCastExpression(cast) -> alias.toAttribute
            case implicitCastInputTypeExp: ImplicitCastInputTypes =>
              getTransformedCastOrImplicitCastExpression(implicitCastInputTypeExp) ->
              alias.toAttribute
            case _ =>
              alias.child -> alias.toAttribute
          }
      }.toMap

    // Check and replace all alias references with subsumer alias map references.
    val compensation1 = compensation.transform {
      case plan if !plan.skip && plan != subsumer =>
        plan.transformExpressions {
          case reference: AttributeReference =>
            aliasMap.get(reference).map {
              attribute =>
                AttributeReference(
                  attribute.name, attribute.dataType)(
                  exprId = attribute.exprId,
                  qualifier = reference.qualifier)
              }.getOrElse(reference)
          case expression: Expression =>
            var attribute = aliasMapExp.get(expression)
            // attribute will be empty, if attribute name is of different case. If empty, change
            // case of scalaUDF present in expression and get updated expression from aliasMap
            if (attribute.isEmpty) {
              val newExp = expression transform {
                case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
                  getTransformedTimeSeriesFunction(function)
                case cast: Cast if cast.child.isInstanceOf[AttributeReference] =>
                  getTransformedCastOrImplicitCastExpression(cast)
                case implicitCastInputTypeExp: ImplicitCastInputTypes =>
                  getTransformedCastOrImplicitCastExpression(implicitCastInputTypeExp)
              }
              attribute = aliasMapExp.get(newExp)
            }
            attribute.map {
              reference =>
                AttributeReference(reference.name, reference.dataType)(exprId = reference.exprId)
            }.getOrElse(expression)
        }
    }

    val subqueryAttributeSet = SQLBuilder.collectAttributeSet(subsumer.outputList)
    if (SQLBuilder.collectDuplicateNames(subqueryAttributeSet).nonEmpty) {
      new UnsupportedOperationException(
        s"duplicate name(s): ${ subsumer.output.map(_.toString + ", ") }")
    }
    if (aliasMapMain.size == 1) {
      val subsumerName: Option[String] = aliasMapMain.get(0)
      // Replace all compensation1 attributes with refrences of subsumer attribute set
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

  /**
   * Check's if timeseries udf function exists. If exists, compare literal with case insensitive
   * value
   */
  protected def isExpressionMatches(subsume: Expression,
      subsumerList: Seq[Expression]): Boolean = {
    // Check if expression has a ScalaUDF of timeSeries function and verify it's children
    // irrespective of case. The structure of scalaUDF function will look like,
    //                ScalaUDF
    //                    |
    //             TimeSeriesFunction
    //                 /    \
    //                /      \
    //   AttributeReference   Literal
    subsume match {
      case Alias(function: ScalaUDF, _) if function.function.isInstanceOf[TimeSeriesFunction] =>
        val children = function.children
        val subsumerTimeSeriesFunctions = subsumerList.filter(
          expression =>
            expression.isInstanceOf[Alias] &&
              expression.asInstanceOf[Alias].child.isInstanceOf[ScalaUDF] &&
              expression.asInstanceOf[Alias].child.asInstanceOf[ScalaUDF].function.
                isInstanceOf[TimeSeriesFunction])
        subsumerTimeSeriesFunctions.exists(
          function => {
            val functionChildren = function.asInstanceOf[Alias].child
              .asInstanceOf[ScalaUDF].children
            functionChildren.head.semanticEquals(children.head) &&
              functionChildren.last.asInstanceOf[Literal].toString().equalsIgnoreCase(
                children.last.asInstanceOf[Literal].toString())
          }
        )
      case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
        val children = function.children
        var subsumerTimeSeriesFunctions: Seq[Expression] = Seq.empty
        subsumerList foreach {
          case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
            subsumerTimeSeriesFunctions = subsumerTimeSeriesFunctions.+:(function)
          case Alias(s: ScalaUDF, _) if s.function.isInstanceOf[TimeSeriesFunction] =>
            subsumerTimeSeriesFunctions = subsumerTimeSeriesFunctions.+:(s.asInstanceOf[Expression])
          case _ =>
        }
        subsumerTimeSeriesFunctions.exists(
          function => {
            val functionChildren = function.asInstanceOf[ScalaUDF].children
            functionChildren.head.semanticEquals(children.head) &&
              functionChildren.last.asInstanceOf[Literal].toString().equalsIgnoreCase(
                children.last.asInstanceOf[Literal].toString())
          }
        )
      case expression: Expression =>
        val transformedExpWithLowerCase = expression.transform {
          case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
            getTransformedTimeSeriesFunction(function)
          case other => other
        }
        val transformedExprListWithLowerCase = subsumerList map {
          expression =>
            expression.transform {
              case function: ScalaUDF if function.function.isInstanceOf[TimeSeriesFunction] =>
                getTransformedTimeSeriesFunction(function)
              case other => other
            }
        }
        transformedExprListWithLowerCase.exists(_.semanticEquals(transformedExpWithLowerCase))
      case _ => false
    }
  }

  /**
   * Compares the output list of subsumer/subsumee with/without alias. In case if, expression
   * is instance of Alias, then compare it's child expression.
   */
  protected def compareOutputList(subsumerOutputList: Seq[NamedExpression],
      subsumeeOutputList: Seq[NamedExpression]): Boolean = {
    subsumerOutputList.forall {
      case a@Alias(cast: Cast, _) =>
        subsumeeOutputList.exists {
          case Alias(castExp: Cast, _) => castExp.child.semanticEquals(cast.child)
          case alias: Alias => alias.child.semanticEquals(cast.child)
          case exp => exp.semanticEquals(cast.child)
        } || isExpressionMatches(a, subsumeeOutputList)
      case a@Alias(_, _) =>
        subsumeeOutputList.exists {
          case Alias(cast: Cast, _) => cast.child.semanticEquals(a.child)
          case alias: Alias => alias.child.semanticEquals(a.child) ||
                               alias.sql.equalsIgnoreCase(a.sql)
          case exp => exp.semanticEquals(a.child)
        } || isExpressionMatches(a, subsumeeOutputList)
      case ex@exp =>
        subsumeeOutputList.exists {
          case alias: Alias => alias.child.semanticEquals(exp)
          case expr => expr.semanticEquals(exp)
        } || isExpressionMatches(ex, subsumeeOutputList)
    }
  }

  /**
   * Check if expr1 and expr2 matches TimeSeriesUDF function. If both expressions are
   * timeseries udf functions, then check it's children are same irrespective of case.
   */
  protected def isExpressionMatches(expression1: Expression, expression2: Expression): Boolean = {
    (expression1, expression2) match {
      case (function1: ScalaUDF, function2: ScalaUDF)
        if function1.function.isInstanceOf[TimeSeriesFunction] &&
        function2.function.isInstanceOf[TimeSeriesFunction] =>
        function1.children.head.semanticEquals(function2.children.head) &&
          function1.children.last.asInstanceOf[Literal].toString()
            .equalsIgnoreCase(function2.children.last.asInstanceOf[Literal].toString())
      case _ => false
    }
  }

  protected def getTransformedTimeSeriesFunction(function: ScalaUDF): Expression = {
    function.transform {
      case literal: Literal =>
        Literal(UTF8String.fromString(literal.toString().toLowerCase), literal.dataType)
    }
  }

  /**
   * transform castOrImplicitCastExp expression to change it's child attribute reference name to
   * lower case
   */
  protected def getTransformedCastOrImplicitCastExpression(
      castOrImplicitCastExp: Expression): Expression = {
    CarbonToSparkAdapter.lowerCaseAttribute(castOrImplicitCastExp)
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

  // use for match qb_2a, qb_2q and sel_3a, sel_3q
  private def doMatch(
      operator_a: modular.Matchable,
      operator_q: modular.Matchable,
      alias_m: AttributeMap[Alias]): Option[modular.Matchable] = {
    var matchable = true
    val matched = operator_q.transformExpressions {
      case cnt_q: AggregateExpression if cnt_q.aggregateFunction.isInstanceOf[Count] =>
        val exprs_q = cnt_q.aggregateFunction.children
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Count] =>
            // case for group by
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
        }.map { cnt => cnt_q.copy(
          Sum(cnt.toAttribute),
          isDistinct = false)
        }.getOrElse { matchable = false; cnt_q }

      case sum_q: AggregateExpression if sum_q.aggregateFunction.isInstanceOf[Sum] =>
        val expr_q = sum_q.aggregateFunction.children.head
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
        }.map { sum => sum_q.copy(
          Sum(sum.toAttribute),
          isDistinct = false)
        }.getOrElse { matchable = false; sum_q }

      case max_q: AggregateExpression if max_q.aggregateFunction.isInstanceOf[Max] =>
        val expr_q = max_q.aggregateFunction.children.head
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
        }.map { max => max_q.copy(
          Max(max.toAttribute),
          isDistinct = false)
        }.getOrElse { matchable = false; max_q }

      case min_q: AggregateExpression if min_q.aggregateFunction.isInstanceOf[Min] =>
        val expr_q = min_q.aggregateFunction.children.head
        operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Min] =>
            val min_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val expr_a = min_a.aggregateFunction.asInstanceOf[Min].child
            if (min_a.isDistinct != min_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }
          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Min] =>
            val min_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val expr_a = min_a.aggregateFunction.asInstanceOf[Min].child
            if (min_a.isDistinct != min_q.isDistinct) {
              false
            } else {
              expr_a.semanticEquals(expr_q)
            }
          case _ => false
        }.map { min => min_q.copy(
          Min(min.toAttribute),
          min_q.mode,
          isDistinct = false,
          resultId = min_q.resultId)
        }.getOrElse { matchable = false; min_q }


      case avg_q: AggregateExpression if avg_q.aggregateFunction.isInstanceOf[Average] =>
        val expr_q = avg_q.aggregateFunction.children.head
        val cnt_q = operator_a.outputList.find {
          case alias: Alias if alias_m.contains(alias.toAttribute) &&
                               alias_m(alias.toAttribute).child.isInstanceOf[AggregateExpression] &&
                               alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                 .aggregateFunction.isInstanceOf[Count] => // case for group by
            val cnt_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
            val exprs_a = cnt_a.aggregateFunction.asInstanceOf[Count].children
            if (!cnt_a.isDistinct && exprs_a.sameElements(Set(expr_q))) {
              true
            } else {
              false
            }
          case attr: Attribute if alias_m.contains(attr) &&
                                  alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                  alias_m(attr).child.asInstanceOf[AggregateExpression]
                                    .aggregateFunction.isInstanceOf[Count] =>
            val cnt_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
            val exprs_a = cnt_a.aggregateFunction.asInstanceOf[Count].children
            if (!cnt_a.isDistinct && exprs_a.sameElements(Set(expr_q))) {
              true
            } else {
              false
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
                                   .aggregateFunction.isInstanceOf[Sum] =>
              val sum_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
              val expr_a = sum_a.aggregateFunction.asInstanceOf[Sum].child
              if (sum_a.isDistinct != avg_q.isDistinct) {
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
              if (sum_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            case alias: Alias if alias_m.contains(alias.toAttribute) &&
                                 alias_m(alias.toAttribute).child
                                   .isInstanceOf[AggregateExpression] &&
                                 alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
                                   .aggregateFunction.isInstanceOf[Average] =>
              val avg_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            case attr: Attribute if alias_m.contains(attr) &&
                                    alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                    alias_m(attr).child.asInstanceOf[AggregateExpression]
                                      .aggregateFunction.isInstanceOf[Average] =>
              val avg_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
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
                                   .aggregateFunction.isInstanceOf[Average] =>
              val avg_a = alias_m(alias.toAttribute).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            case attr: Attribute if alias_m.contains(attr) &&
                                    alias_m(attr).child.isInstanceOf[AggregateExpression] &&
                                    alias_m(attr).child.asInstanceOf[AggregateExpression]
                                      .aggregateFunction.isInstanceOf[Average] =>
              val avg_a = alias_m(attr).child.asInstanceOf[AggregateExpression]
              val expr_a = avg_a.aggregateFunction.asInstanceOf[Average].child
              if (avg_a.isDistinct != avg_q.isDistinct) {
                false
              } else {
                expr_a.semanticEquals(expr_q)
              }
            case _ => false
          }.map { avg => avg_q.copy(
            Average(avg.toAttribute),
            isDistinct = false)
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
                                isExpressionMatches(alias_m(alias.toAttribute), expr)) &&
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

}

/**
 * Convention:
 * EmR: each subsumee's expression match some of subsumer's expression
 * EdR: each subsumee's expression derive from some of subsumer's expression
 * RmE: each subsumer's expression match some of subsumee's expression
 * RdE: each subsumer's expression derive from some of subsumee's expression
 */

private object SelectSelectNoChildDelta extends MVMatchPattern with PredicateHelper {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  private def isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]): Boolean = {
    if (subsumee.asInstanceOf[Select].predicateList.contains(exprE)) {
      subsumer.asInstanceOf[Select].predicateList.exists(_.semanticEquals(exprE)) ||
      canEvaluate(exprE, subsumer) ||
      isExpressionMatches(exprE, subsumer.asInstanceOf[Select].predicateList)
    } else if (subsumee.asInstanceOf[Select].outputList.contains(exprE)) {
      exprE match {
        case a@Alias(_, _) =>
          exprListR.exists(a1 => a1.isInstanceOf[Alias] &&
                                 a1.asInstanceOf[Alias].child.semanticEquals(a.child)) ||
          exprListR.exists(_.semanticEquals(exprE) || canEvaluate(exprE, subsumer)) ||
          isExpressionMatches(exprE, exprListR)
        case exp =>
          exprListR.exists(a1 => a1.isInstanceOf[Alias] &&
                                 a1.asInstanceOf[Alias].child.semanticEquals(exp)) ||
          exprListR.exists(_.semanticEquals(exprE) || canEvaluate(exprE, subsumer)) ||
          isExpressionMatches(exprE, exprListR)
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
      generator: SubqueryNameGenerator): Seq[ModularPlan] = {

    (subsumer, subsumee, compensation) match {
      case (
        sel_1a @ modular.Select(_, _, _, _, _, _, _, _, _, _),
        sel_1q @ modular.Select(_, _, _, _, _, _, _, _, _, _), None
        ) if sel_1a.children.forall { _.isInstanceOf[modular.LeafNode] } &&
             sel_1q.children.forall { _.isInstanceOf[modular.LeafNode] } =>

        LOGGER.debug(s"Applying pattern: {SelectSelectNoChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }. ")

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
          isExpressionMatches(expr, sel_1q.predicateList))
        val isPredicateEmdR = sel_1q.predicateList.forall(expr =>
          isDerivable(expr, sel_1a.outputList ++ rejoinOutputList, sel_1q, sel_1a, None))
        // Check if sel_1q.outputList is non empty and then check whether
        // it can be derivable with sel_1a otherwise for empty cases it returns true.
        val isOutputEdR = sel_1q.outputList.nonEmpty && sel_1q.outputList.forall(expr =>
          isDerivable(expr, sel_1a.outputList ++ rejoinOutputList, sel_1q, sel_1a, None))

        if (isUniqueRmE.isEmpty && isUniqueEmR.isEmpty && extrajoin.isEmpty && isPredicateRmE &&
            isPredicateEmdR && isOutputEdR) {
          val mappings = sel_1a.children.zipWithIndex.map {
            case (child, fromIndex) if sel_1q.children.contains(child) =>
              val toIndex = sel_1q.children.indexWhere{
                case relation: ModularRelation => relation.fineEquals(child)
                case other => other == child
              }
              (toIndex -> fromIndex)

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
                  sel_1a_join.forall(e => sel_1q_join.exists(e.semanticEquals) ||
                                          isExpressionMatches(e, sel_1q_join)) &&
                  sel_1q_join.forall(e => sel_1a_join.exists(e.semanticEquals) ||
                                          isExpressionMatches(e, sel_1a_join))
                } else false
              case _ => false
            }
          }

          val isPredicateEmR = sel_1q.predicateList.forall(expr =>
            sel_1a.predicateList.exists(_.semanticEquals(expr)) ||
            isExpressionMatches(expr, sel_1a.predicateList))
          val isOutputEmR = compareOutputList(sel_1q.outputList, sel_1a.outputList)
          val isOutputRmE = compareOutputList(sel_1a.outputList, sel_1q.outputList)
          val isLOEmLOR = !(isLeftJoinView(sel_1a) && sel_1q.joinEdges.head.joinType == Inner)

          if (r2eJoinsMatch) {
            if (isPredicateEmR && isOutputEmR && isOutputRmE && rejoin.isEmpty && isLOEmLOR) {
              if (sel_1q.flagSpec.isEmpty) {
                Seq(sel_1a)
              } else {
                Seq(sel_1a.copy(flags = sel_1q.flags, flagSpec = sel_1q.flagSpec))
              }
            } else {
              // no compensation needed
              val tChildren = new collection.mutable.ArrayBuffer[ModularPlan]()
              val tAliasMap = new collection.mutable.HashMap[Int, String]()

              val usel_1a = sel_1a.copy(outputList = sel_1a.outputList)
              tChildren += usel_1a
              tAliasMap += (tChildren.indexOf(usel_1a) -> generator.newSubsumerName())

              sel_1q.children.zipWithIndex.foreach {
                case (child, idx) =>
                  if (e2r.get(idx).isEmpty) {
                    tChildren += child
                    sel_1q.aliasMap.get(idx).map(x => tAliasMap += (tChildren.indexOf(child) -> x))
                  }
              }

              val tJoinEdges = sel_1q.joinEdges.collect {
                case JoinEdge(le, re, joinType) =>
                  (e2r.get(le), e2r.get(re)) match {
                    case (Some(_), None) =>
                      JoinEdge(
                        0,
                        tChildren.indexOf(sel_1q.children(re)),
                        joinType)
                    case (None, None) =>
                      JoinEdge(
                        tChildren.indexOf(sel_1q.children(le)),
                        tChildren.indexOf(sel_1q.children(re)),
                        joinType)
                    case (None, Some(_)) =>
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
        LOGGER.debug(s"Applying pattern: {SelectSelectNoChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }")
        val isPredicateRmE = sel_3a.predicateList.isEmpty ||
                             sel_3a.predicateList.forall(expr =>
                               sel_3q.predicateList.exists(_.semanticEquals(expr)) ||
                               isExpressionMatches(expr, sel_3q.predicateList))
        val isPredicateEmdR = sel_3q.predicateList.isEmpty ||
                              sel_3q.predicateList.forall(expr =>
                                sel_3a.predicateList.exists(_.semanticEquals(expr) ||
                                                            isExpressionMatches(expr,
                                                              sel_3a.predicateList)) ||
                                isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))
        val isOutputEdR = sel_3q.outputList.forall(expr =>
          isDerivable(expr, sel_3a.outputList, sel_3q, sel_3a, None))
        val isSingleChild = sel_3a.children.length == 1 && sel_3q.children.length == 1

        if (isPredicateRmE && isPredicateEmdR && isOutputEdR && isSingleChild) {
          val isPredicateEmR = sel_3q.predicateList.isEmpty ||
                               sel_3q.predicateList.forall(expr =>
                                 sel_3a.predicateList.exists(_.semanticEquals(expr)) ||
                                 isExpressionMatches(expr, sel_3a.predicateList))
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
                      isExpressionMatches(a1.asInstanceOf[Alias].child, a.child))
                }.map(_.toAttribute).get
            })
            val wip = sel_3q_exp.copy(
              children = Seq(sel_3a),
              aliasMap = Seq(0 -> generator.newSubsumerName()).toMap)
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

private object GroupbyGroupbyNoChildDelta extends MVMatchPattern {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      generator: SubqueryNameGenerator): Seq[ModularPlan] = {
    (subsumer, subsumee, compensation) match {
      case (
        gb_2a @ modular.GroupBy(_, _, _, _, _, _, _, _),
        gb_2q @ modular.GroupBy(_, _, _, _, _, _, _, _),
        None) =>
        LOGGER.debug(s"Applying pattern: {GroupbyGroupbyNoChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }")
        val isGroupingEmR = gb_2q.predicateList.forall(expr =>
          gb_2a.predicateList.exists(_.semanticEquals(expr)) ||
          isExpressionMatches(expr, gb_2a.predicateList))
        val isGroupingRmE = gb_2a.predicateList.forall(expr =>
          gb_2q.predicateList.exists(_.semanticEquals(expr)) ||
          isExpressionMatches(expr, gb_2q.predicateList))
        val isOutputEmR = compareOutputList(gb_2q.outputList, gb_2a.outputList)
        if (isGroupingEmR && isGroupingRmE) {
          if (isOutputEmR) {
            // Mappings of output of two plans by checking semantic equals.
            val mappings = gb_2a.outputList.zipWithIndex.map { case(exp, index) =>
              (exp, gb_2q.outputList.find {
                case a: Alias if exp.isInstanceOf[Alias] =>
                  a.child.semanticEquals(exp.children.head) ||
                  isExpressionMatches(a.child, exp.children.head) ||
                  a.sql.equalsIgnoreCase(exp.sql)
                case a: Alias => a.child.semanticEquals(exp)
                case other => exp match {
                  case alias: Alias =>
                    other.semanticEquals(alias.child)
                  case _ =>
                    other.semanticEquals(exp)
                }
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
            val subsumerName = Seq(0 -> generator.newSubsumerName()).toMap
            tryMatch(
              gb_2a, gb_2q, aliasMap).flatMap {
              case g: GroupBy =>
                // Check any agg function exists on output list, in case of expressions like
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

                  tChildren += gb_2a
                  val sel_1q_temp = sel_1a.copy(
                    predicateList = sel_1a.predicateList,
                    children = tChildren,
                    joinEdges = sel_1a.joinEdges,
                    aliasMap = subsumerName)
                  Some(g.copy(child = sel_1q_temp))
                } else {
                  Some(g.copy(child = g.child.withNewChildren(
                    g.child.children.map {
                      case modular.Select(_, _, _, _, _, _, _, _, _, _) => gb_2a;
                      case other => other
                    })));
                }
              case _ => None
            }.map { wip =>
              factorOutSubsumer(wip, gb_2a, subsumerName)
            }.map(Seq(_))
              .getOrElse(Nil)
          } else {
            Nil
          }
        }

      case _ => Nil
    }
  }
}

private object GroupbyGroupbySelectOnlyChildDelta
  extends MVMatchPattern with PredicateHelper {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  private def isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]) = {
    if (subsumee.asInstanceOf[GroupBy].predicateList.contains(exprE)) {
      if (exprListR.exists(_.semanticEquals(exprE)) || canEvaluate(exprE, exprListR) ||
          isDerivableForFunction(exprE, exprListR) || isExpressionMatches(exprE, exprListR)) {
        true
      } else {
        false
      }
    } else if (compensation.getOrElse(throw new RuntimeException("compensation cannot be None"))
      .asInstanceOf[Select].predicateList.contains(exprE)) {
      if (canEvaluate(exprE, exprListR) || exprListR.exists(_.semanticEquals(exprE)) ||
          isDerivableForFunction(exprE, exprListR) || isExpressionMatches(exprE, exprListR)) {
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
   * exprE.references as subset of AttributeSet(exprListR), which will just take the column name
   * from UDF, so it will be always false for ScalaUDF.
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
  private def isDerivableForFunction(expression: Expression,
      expressionList: Seq[Expression]): Boolean = {
    var canBeDerived = false
    expression match {
      case function: ScalaUDF =>
        canEvaluate(function, expressionList)
      case _ =>
        expressionList.forall {
          case function: ScalaUDF =>
            function.references.foreach {
              reference => canBeDerived = expression.sql.contains(reference.name)
            }
            canBeDerived
          case _ =>
            canBeDerived
        }
    }
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

  def canEvaluate(expression: Expression, expressionList: Seq[Expression]): Boolean = {
    expression match {
      case function: ScalaUDF =>
        canEvaluate(function, expressionList)
      case _ =>
        expression.references.subsetOf(AttributeSet(expressionList))
    }
  }

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      generator: SubqueryNameGenerator): Seq[ModularPlan] = {
    val aggInputEinR = subsumee.expressions
      .collect { case agg: aggregate.AggregateExpression => AttributeSet(Seq(agg))
        .subsetOf(subsumer.outputSet)
      }.forall(identity)
    val compensationSelectOnly = !compensation.map { _.collect { case n => n.getClass } }
      .exists(_.contains(GroupBy))

    (subsumer, subsumee, compensation, aggInputEinR, compensationSelectOnly) match {
      case (
        gb_2a @ GroupBy(_, _, _, _, _, _, _, _),
        gb_2q @ GroupBy(_, _, _, _, _, _, _, _),
        Some(sel_1c1 @ Select(_, _, _, _, _, _, _, _, _, _)),
        true,
        true)
        if !gb_2q.flags.hasFlag(EXPAND) && !gb_2a.flags.hasFlag(EXPAND) =>

        LOGGER.debug(s"Applying pattern: {GroupbyGroupbySelectOnlyChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }. " +
                     s"Compensation: { ${ sel_1c1.toString().trim } }")

        val rejoinOutputList = sel_1c1.children.tail.flatMap(_.output)
        val isGroupingEdR = gb_2q.predicateList.forall(expr =>
          isDerivable(expr, gb_2a.predicateList ++ rejoinOutputList, gb_2q, gb_2a, compensation))
        val needRegrouping = !gb_2a.predicateList
          .forall(f => gb_2q.predicateList.contains(f) ||
                       isExpressionMatches(f, gb_2q.predicateList))
        val canPullUp = sel_1c1.predicateList.forall(expr =>
          isDerivable(expr, gb_2a.predicateList ++ rejoinOutputList, gb_2q, gb_2a, compensation))
        val isAggEmR = gb_2q.outputList.collect {
          case agg: aggregate.AggregateExpression =>
            gb_2a.outputList.exists(_.semanticEquals(agg))
        }.forall(identity)

        if (isGroupingEdR && ((!needRegrouping && isAggEmR) || needRegrouping) && canPullUp) {
          // pull up
          val pullUpOutputList = gb_2a.outputList.map(_.toAttribute) ++ rejoinOutputList
          val myOutputList = gb_2a.outputList.filter {
            case alias: Alias =>
              val aliasList = gb_2q.outputList.filter(_.isInstanceOf[Alias])
              aliasList.exists(_.asInstanceOf[Alias].child.semanticEquals(alias.child)) ||
              isExpressionMatches(alias.child, aliasList)
            case attr: Attribute =>
              gb_2q.outputList.exists(_.semanticEquals(attr))
          }.map(_.toAttribute) ++ rejoinOutputList
          // TODO: find out if we really need to check needRegrouping or just use myOutputList
          val sel_2c1 = if (needRegrouping) {
            sel_1c1
              .copy(outputList = pullUpOutputList,
                inputList = pullUpOutputList,
                children = sel_1c1.children
                  .map { case _: modular.Select => gb_2a; case other => other })
          } else {
            sel_1c1
              .copy(outputList = myOutputList,
                inputList = pullUpOutputList,
                children = sel_1c1.children
                  .map { case _: modular.Select => gb_2a; case other => other })
          }
          // sel_1c1.copy(outputList = pullUpOutputList, inputList = pullUpOutputList, children =
          // sel_1c1.children.map { _ match { case s: modular.Select => gb_2a; case other =>
          // other } })

          if (rejoinOutputList.isEmpty) {
            val aliasMap = AttributeMap(gb_2a.outputList.collect {
              case a: Alias => (a.toAttribute, a)
            })
            val res =
              tryMatch(gb_2a, gb_2q, aliasMap).flatMap {
                case g: GroupBy =>

                  // Check any agg function exists on output list,
                  // in case of expressions like sum(a)+sum(b) ,
                  // output list directly replaces with alias with in place of function
                  // so we should remove the group by clause in those cases.
                  val aggFunExists = g.outputList.exists { f =>
                    f.find {
                      case _: AggregateExpression => true
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

private object GroupbyGroupbyGroupbyChildDelta extends MVMatchPattern {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      generator: SubqueryNameGenerator): Seq[ModularPlan] = {
    val groupbys = compensation.map { _.collect { case g: GroupBy => g } }.getOrElse(Nil).toSet

    (subsumer, subsumee, groupbys.nonEmpty) match {
      case (
        Select(_, _, _, _, _, _, _, _, _, _),
        Select(_, _, _, _, _, _, _, _, _, _),
        true) =>
        LOGGER.debug(s"Applying pattern: {GroupbyGroupbyGroupbyChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }")
        // TODO: implement me
        Nil

      case _ => Nil
    }
  }
}


private object SelectSelectSelectChildDelta extends MVMatchPattern {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      generator: SubqueryNameGenerator): Seq[ModularPlan] = {
    val compensationSelectOnly =
      !compensation
        .map { _.collect { case n => n.getClass } }
        .exists(_.contains(GroupBy))

    (subsumer, subsumee, compensationSelectOnly) match {
      case (
        modular.Select(_, _, _, _, _, _, _, _, _, _),
        modular.Select(_, _, _, _, _, _, _, _, _, _),
        true) =>
        LOGGER.debug(s"Applying pattern: {SelectSelectSelectChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }")
        // TODO: implement me
        Nil
      case _ => Nil
    }
  }
}

private object SelectSelectGroupbyChildDelta
  extends MVMatchPattern with PredicateHelper {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  private def isDerivable(
      exprE: Expression,
      exprListR: Seq[Expression],
      subsumee: ModularPlan,
      subsumer: ModularPlan,
      compensation: Option[ModularPlan]) = {
    val exprE1 = doTopSelectTranslation(exprE, exprListR, subsumee, subsumer, compensation)
    exprE1 match {
      case Some(e) => isSemanticEquivalent(e, subsumer)
      case _ => false
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
   * outputList of group by
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
      // immediately group by child
      case (
        _@modular.Select(
        _, _, _, _, _,
        Seq(gb_2a@modular.GroupBy(
        _, _, _, _, sel_2a@modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)),
        _, _, _, _),
        sel_3q@modular.Select(
        _, _, _, _, _, Seq(gb_2q@modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _),
        Some(gb_2c@modular.GroupBy(
        _, _, _, _, sel_2c@modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _))
        ) =>
        val distinctGrpByOList = getDistinctOutputList(gb_2q.outputList)
        if (sel_3q.predicateList.contains(exprE)) {
          val expr1E = exprE.transform {
            case attr: Attribute =>
              gb_2c.outputList.lift(
                distinctGrpByOList.indexWhere {
                  case alias: Alias if alias.toAttribute.semanticEquals(attr) => true;
                  case _ => false
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
                distinctGrpByOList.indexWhere {
                  case a if a.toAttribute.semanticEquals(attr) => true;
                  case _ => false
                })

            case alias: Alias =>
              gb_2c.outputList.lift(
                distinctGrpByOList.indexWhere {
                  case a if a.toAttribute.semanticEquals(alias.toAttribute) => true;
                  case _ => false
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
          if (s.semanticEquals(expr) || isExpressionMatches(s, expr)) {
            canBeDerived = true
          }
          // It is because when expression is like between filter, the expr will be as Cast
          // expression and its child will be ScalaUDF(timeseries), So compare the child also.
          if (!canBeDerived && null != expr.children) {
            expr.children.foreach { expC =>
              if (s.semanticEquals(expC) || isExpressionMatches(s, expC)) {
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

  /**
   * Removes duplicate projection in the output list for query matching
   */
  def getDistinctOutputList(outputList: Seq[NamedExpression]): Seq[NamedExpression] = {
    var distinctOList: Seq[NamedExpression] = Seq.empty
    outputList.foreach { output =>
      if (distinctOList.isEmpty) {
        distinctOList = distinctOList :+ output
      } else {
        // get output name
        var outputName = output.name
        if (output.isInstanceOf[Alias]) {
          // In case of queries with join on more than one table and projection list having
          // aggregation of same column name on join tables like sum(t1.column), sum(t2.column),
          // in that case, compare alias name with column id, as alias name will be same for
          // both output(sum(t1))
          val projectName = output.toString()
          outputName = projectName.substring(0, projectName.indexOf(" AS"))
        }
        if (!distinctOList.exists(distinctOutput =>
          if (distinctOutput.isInstanceOf[Alias]) {
            val projectName = distinctOutput.toString()
            val aliasName = projectName.substring(0, projectName.indexOf(" AS"))
            aliasName.equalsIgnoreCase(outputName)
          } else {
            distinctOutput.qualifiedName.equalsIgnoreCase(output.qualifiedName)
          })) {
          distinctOList = distinctOList :+ output
        }
      }
    }
    distinctOList
  }

  def apply(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      compensation: Option[ModularPlan],
      generator: SubqueryNameGenerator): Seq[ModularPlan] = {
    (subsumer, subsumee, compensation, subsumer.children, subsumee.children) match {
      case (
        sel_3a@modular.Select(
        _, _, Nil, _, _,
        Seq(_@modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _),
        sel_3q_dup@modular.Select(
        _, _, _, _, _,
        Seq(_@modular.GroupBy(_, _, _, _, _, _, _, _)), _, _, _, _),
        Some(gb_2c@modular.GroupBy(_, _, _, _, _, _, _, _)),
        _ :: Nil,
        _ :: Nil) =>
        LOGGER.debug(s"Applying pattern: {SelectSelectGroupbyChildDelta} for the plan: " +
                     s"{ ${ subsumee.toString().trim } }. " +
                     s"Current Subsumer: { ${ subsumer.toString().trim } }. " +
                     s"Compensation: { ${ gb_2c.toString().trim } }")
        val tbls_sel_3a = sel_3a.collect { case tbl: modular.LeafNode => tbl }
        val tbls_sel_3q = sel_3q_dup.collect { case tbl: modular.LeafNode => tbl }
        val distinctSelOList = getDistinctOutputList(sel_3q_dup.outputList)
        val sel_3q = sel_3q_dup.copy(outputList = distinctSelOList)

        val extrajoin = tbls_sel_3a.filterNot(tbls_sel_3q.contains)
        val rejoin = tbls_sel_3q.filterNot(tbls_sel_3a.contains)
        val rejoinOutputList = rejoin.flatMap(_.output)

        val isPredicateRmE = sel_3a.predicateList.forall(expr =>
          sel_3q.predicateList.exists(_.semanticEquals(expr)) ||
          isExpressionMatches(expr, sel_3q.predicateList) ||
          gb_2c.predicateList.exists(_.semanticEquals(expr)) ||
          isExpressionMatches(expr, gb_2c.predicateList))
        val isPredicateEmdR = sel_3q.predicateList
          .forall(expr =>
            sel_3a.predicateList.exists(_.semanticEquals(expr)) ||
            isExpressionMatches(expr, sel_3a.predicateList) ||
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

        val canSELPullUp = gb_2c.child.isInstanceOf[Select] &&
                           gb_2c.child.asInstanceOf[Select].predicateList
                             .forall(expr =>
                               isDerivable(
                                 expr,
                                 sel_3a.outputList ++ rejoinOutputList,
                                 sel_3q,
                                 sel_3a,
                                 compensation))
        val canGBPullUp = gb_2c.predicateList
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
            canSELPullUp &&
            canGBPullUp) {
          gb_2c.child match {
            case s: Select =>
              val sel_3c1 = s.withNewChildren(
                s.children.map {
                  case _: GroupBy => sel_3a.setSkip()
                  case other => other })
              val gb_3c2 = gb_2c.copy(child = sel_3c1)

              val aliasMap_exp = AttributeMap(
                gb_2c.outputList.collect {
                  case a: Alias => (a.toAttribute, AliasWrapper(a)) })
              val sel_3q_exp = sel_3q_dup.transformExpressions({
                case attr: Attribute if aliasMap_exp.contains(attr) => aliasMap_exp(attr)
              }).transformExpressions {
                case AliasWrapper(alias: Alias) => alias
              }
              // Mappings of output of two plans by checking semantic equals.
              val mappings = sel_3q_exp.outputList.zipWithIndex.map { case(exp, index) =>
                (exp, gb_2c.outputList.find {
                  case a: Alias if exp.isInstanceOf[Alias] =>
                    a.child.semanticEquals(exp.children.head) ||
                      isExpressionMatches(a.child, exp.children.head)
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
