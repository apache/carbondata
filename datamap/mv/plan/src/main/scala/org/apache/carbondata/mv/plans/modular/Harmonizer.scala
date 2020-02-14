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

import org.apache.spark.sql.CarbonToSparkAdapter
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.Metadata

import org.apache.carbondata.mv.plans
import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.Flags._

abstract class Harmonizer(conf: SQLConf)
  extends RuleExecutor[ModularPlan] {

  //  protected val fixedPoint = FixedPoint(conf.getConfString("spark.mv.harmonizer
  // .maxIterations").toInt)
  protected val fixedPoint = FixedPoint(conf.optimizerMaxIterations)

  def batches: Seq[Batch] = {
    Batch(
      "Data Harmonizations", fixedPoint,
      Seq( HarmonizeDimensionTable) ++
      extendedOperatorHarmonizationRules: _*) :: Nil
  }

  /**
   * Override to provide additional rules for the modular operator harmonization batch.
   */
  def extendedOperatorHarmonizationRules: Seq[Rule[ModularPlan]] = Nil
}

/**
 * A full Harmonizer - harmonize both fact and dimension tables
 */
object FullHarmonizer extends FullHarmonizer

class FullHarmonizer extends Harmonizer(new SQLConf()) {
  override def extendedOperatorHarmonizationRules: Seq[Rule[ModularPlan]] =
    super.extendedOperatorHarmonizationRules ++ (HarmonizeFactTable :: Nil)
}

/**
 * A semi Harmonizer - it harmonizes dimension tables only
 */
object SemiHarmonizer extends SemiHarmonizer

class SemiHarmonizer extends Harmonizer(new SQLConf())

object HarmonizeDimensionTable extends Rule[ModularPlan] with PredicateHelper {

  def apply(plan: ModularPlan): ModularPlan = {
    plan transform {
      case s@Select(_, _, _, _, jedges, fact :: dims, _, _, _, _) if
      jedges.forall(e => e.joinType == LeftOuter || e.joinType == Inner) &&
      fact.isInstanceOf[ModularRelation] &&
      dims.filterNot(_.isInstanceOf[modular.LeafNode]).nonEmpty &&
      dims.forall(d => (d.isInstanceOf[ModularRelation] || HarmonizedRelation.canHarmonize(d))) => {
        var tPullUpPredicates = Seq.empty[Expression]
        val tChildren = fact :: dims.map {
          case m: ModularRelation => m
          case h@GroupBy(
          _,
          _,
          _,
          _,
          s1@Select(_, _, _, _, _, dim :: Nil, NoFlags, Nil, Nil, _),
          NoFlags,
          Nil, _) if (dim.isInstanceOf[ModularRelation]) => {
            val rAliasMap = AttributeMap(h.outputList.collect {
                case a: Alias  if a.child.isInstanceOf[Attribute] =>
                (a.child.asInstanceOf[Attribute], a.toAttribute) })
            val pullUpPredicates = s1.predicateList
              .map(replaceAlias(_, rAliasMap.asInstanceOf[AttributeMap[Expression]]))
            if (pullUpPredicates.forall(cond => canEvaluate(cond, h))) {
              tPullUpPredicates = tPullUpPredicates ++ pullUpPredicates
              plans.modular.HarmonizedRelation(h.copy(child = s1.copy(predicateList = Nil)))
            } else {
              h
            }
          }
          // case _ =>
        }
        if (tChildren.forall(_.isInstanceOf[modular.LeafNode])) {
          s.copy(predicateList = s.predicateList ++ tPullUpPredicates, children = tChildren)
        } else {
          s
        }
      }
      //        s.withNewChildren(fact :: dims.map { case m: modular.ModularRelation => m; case h
      // => HarmonizedRelation(h) })}
      //        s.copy(predicateList = predicateList ++ moveUpPredicates, children = tChildren)}
      // fact :: dims.map { case m: modular.ModularRelation => m; case h => HarmonizedRelation(h)
      // })}
    }
  }

}

object HarmonizeFactTable extends Rule[ModularPlan] with PredicateHelper with AggregatePushDown {

  def apply(plan: ModularPlan): ModularPlan = {
    plan transform {
      case g@GroupBy(_, _, _, _,
        s@Select(_, _, _, aliasm, jedges, fact :: dims, _, _, _, _), _, _, _)
        if s.adjacencyList.keySet.size <= 1 &&
           jedges.forall(e => e.joinType == Inner) && // !s.flags.hasFlag(DISTINCT) &&
           fact.isInstanceOf[ModularRelation] &&
           (fact :: dims).forall(_.isInstanceOf[modular.LeafNode]) &&
           dims.nonEmpty => {
        val selAliasMap = AttributeMap(s.outputList.collect {
          case a: Alias if (a.child.isInstanceOf[Attribute]) => (a.toAttribute, a.child
            .asInstanceOf[Attribute])
        })
        val aggTransMap = findPushThroughAggregates(
          g.outputList,
          selAliasMap,
          fact.asInstanceOf[ModularRelation])

        val constraintsAttributeSet = dims.flatMap(s.extractEvaluableConditions(_))
          .map(_.references)
          .foldLeft(AttributeSet.empty)(_ ++ _)
        val groupingAttributeSet = g.predicateList.map(_.references)
          .foldLeft(AttributeSet.empty)(_ ++ _)
        if (aggTransMap.isEmpty ||
            // TODO: the following condition is too pessimistic, more work needed using methods
            // similar to those in trait
            //      QueryPlanConstraints
            !constraintsAttributeSet.subsetOf(groupingAttributeSet)) {
          g
        } else {
          val starJExprs = dims.flatMap(dim => s.extractJoinConditions(fact, dim)).toSeq
          val gJAttributes = starJExprs.map(expr => expr.references)
            .foldLeft(AttributeSet.empty)(_ ++ _).filter(fact.outputSet.contains(_))
          val fExprs = s.extractEvaluableConditions(fact)
          val gFAttributes = fExprs.map(expr => expr.references)
            .foldLeft(AttributeSet.empty)(_ ++ _)
            .filter(fact.outputSet.contains(_))
          val gGAttributes = g.predicateList.map(expr => expr.references)
            .foldLeft(AttributeSet.empty)(_ ++ _).filter(fact.outputSet.contains(_))
          val gAttributes = (gJAttributes ++ gFAttributes ++ gGAttributes).toSeq

          val oAggregates = aggTransMap.map(_._2).flatMap(_._2).toSeq

          val tAliasMap = (aliasm.get(0) match {
            case Some(name) => Seq((0, name));
            case _ => Seq.empty
          }).toMap
          val sOutput = (oAggregates.map(_.references).foldLeft(AttributeSet.empty)(_ ++ _) ++
                         AttributeSet(gAttributes)).toSeq
          val hFactSel = plans.modular
            .Select(
              sOutput,
              fact.output,
              Seq.empty,
              tAliasMap,
              Seq.empty,
              fact :: Nil,
              NoFlags,
              Seq.empty,
              Seq.empty)
          val hFact = plans.modular
            .GroupBy(
              gAttributes ++ oAggregates,
              sOutput,
              gAttributes,
              None,
              hFactSel,
              NoFlags,
              Seq.empty)
          val hFactName = s"gen_harmonized_${
            fact.asInstanceOf[ModularRelation]
              .databaseName
          }_${ fact.asInstanceOf[ModularRelation].tableName }"
          val hAliasMap = (aliasm - 0) + (0 -> hFactName)
          val hInputList = gAttributes ++ oAggregates.map(_.toAttribute) ++
                           dims.flatMap(_.asInstanceOf[modular.LeafNode].output).toSeq
          // val hPredicateList = s.predicateList
          val attrOutputList = s.outputList.filter(expr => (expr.isInstanceOf[Attribute]) ||
                                                           (expr.isInstanceOf[Alias] &&
                                                            expr.asInstanceOf[Alias].child
                                                              .isInstanceOf[Attribute]))
          val aggOutputList = aggTransMap.values.flatMap(t => t._2)
            .map { ref =>
              CarbonToSparkAdapter.createAttributeReference(
                ref.name, ref.dataType, nullable = true, Metadata.empty,
                ref.exprId, Some(hFactName))
            }
          val hFactOutputSet = hFact.outputSet
          // Update the outputlist qualifier
          val hOutputList = (attrOutputList ++ aggOutputList).map {attr =>
            attr.transform {
              case ref: Attribute if hFactOutputSet.contains(ref) =>
                CarbonToSparkAdapter.createAttributeReference(
                  ref.name, ref.dataType, nullable = true, Metadata.empty,
                  ref.exprId, Some(hFactName))
            }
          }.asInstanceOf[Seq[NamedExpression]]

          // Update the predicate qualifier
          val hPredList = s.predicateList.map{ pred =>
            pred.transform {
              case ref: Attribute if hFactOutputSet.contains(ref) =>
                CarbonToSparkAdapter.createAttributeReference(
                  ref.name, ref.dataType, nullable = true, Metadata.empty,
                  ref.exprId, Some(hFactName))
            }
          }
          val hSel = s.copy(
              outputList = hOutputList,
              inputList = hInputList,
              aliasMap = hAliasMap,
              predicateList = hPredList,
              children = hFact :: dims)
          val gOutputList = g.outputList.zipWithIndex
            .map { case (expr, index) =>
              if (aggTransMap.keySet.contains(index)) {
                aggTransMap(index)
                  ._1
              } else {
                expr
              }
            }

          val wip = g.copy(outputList = gOutputList, inputList = hInputList, child = hSel)
          wip.transformExpressions {
            case ref: Attribute if hFactOutputSet.contains(ref) =>
              CarbonToSparkAdapter.createAttributeReference(
                ref.name, ref.dataType, nullable = true, Metadata.empty,
                ref.exprId, Some(hFactName))
          }
        }
      }
    }
  }
}
