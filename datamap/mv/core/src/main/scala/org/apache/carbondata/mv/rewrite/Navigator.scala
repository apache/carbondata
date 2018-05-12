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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeSet}

import org.apache.carbondata.mv.datamap.{MVHelper, MVState}
import org.apache.carbondata.mv.expressions.modular._
import org.apache.carbondata.mv.plans.modular.{GroupBy, ModularPlan, Select}
import org.apache.carbondata.mv.plans.modular

private[mv] class Navigator(catalog: SummaryDatasetCatalog, session: MVState) {

  def rewriteWithSummaryDatasets(plan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    val replaced = plan.transformAllExpressions {
      case s: ModularSubquery =>
        if (s.children.isEmpty) {
          ScalarModularSubquery(
            rewriteWithSummaryDatasetsCore(s.plan, rewrite), s.children, s.exprId)
        }
        else throw new UnsupportedOperationException(s"Rewrite expression $s isn't supported")
      case o => o
    }
    rewriteWithSummaryDatasetsCore(replaced, rewrite)
  }

  def rewriteWithSummaryDatasetsCore(plan: ModularPlan, rewrite: QueryRewrite): ModularPlan = {
    val rewrittenPlan = plan transformDown {
      case currentFragment =>
        if (currentFragment.rewritten || !currentFragment.isSPJGH) currentFragment
        else {
          val compensation =
            (for { dataset <- catalog.lookupFeasibleSummaryDatasets(currentFragment).toStream
                   subsumer <- session.modularizer.modularize(
                     session.optimizer.execute(dataset.plan)).map(_.harmonized)
                   subsumee <- unifySubsumee(currentFragment)
                   comp <- subsume(
                     unifySubsumer2(
                       unifySubsumer1(
                         subsumer,
                         subsumee,
                         dataset.relation),
                       subsumee),
                     subsumee, rewrite)
                 } yield comp).headOption
          compensation.map(_.setRewritten).getOrElse(currentFragment)
        }
    }
    // In case it is rewritten plan and the datamap table is not updated then update the datamap
    // table in plan.
    if (rewrittenPlan.find(_.rewritten).isDefined) {
      val updatedDataMapTablePlan = rewrittenPlan transform {
        case s: Select =>
          MVHelper.updateDataMap(s, rewrite)
        case g: GroupBy =>
          MVHelper.updateDataMap(g, rewrite)
      }
      // TODO Find a better way to set the rewritten flag, it may fail in some conditions.
      val mapping =
        rewrittenPlan.collect {case m: ModularPlan => m } zip
        updatedDataMapTablePlan.collect {case m: ModularPlan => m}
      mapping.foreach(f => if (f._1.rewritten) f._2.setRewritten())

      updatedDataMapTablePlan

    } else {
      rewrittenPlan
    }
  }

  def subsume(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      rewrite: QueryRewrite): Option[ModularPlan] = {
    if (subsumer.getClass == subsumee.getClass) {
      (subsumer.children, subsumee.children) match {
        case (Nil, Nil) => None
        case (r, e) if r.forall(_.isInstanceOf[modular.LeafNode]) &&
                       e.forall(_.isInstanceOf[modular.LeafNode]) =>
          val iter = session.matcher.execute(subsumer, subsumee, None, rewrite)
          if (iter.hasNext) Some(iter.next)
          else None

        case (rchild :: Nil, echild :: Nil) =>
          val compensation = subsume(rchild, echild, rewrite)
          val oiter = compensation.map {
            case comp if comp.eq(rchild) =>
              session.matcher.execute(subsumer, subsumee, None, rewrite)
            case _ =>
              session.matcher.execute(subsumer, subsumee, compensation, rewrite)
          }
          oiter.flatMap { case iter if iter.hasNext => Some(iter.next)
                          case _ => None }

        case _ => None
      }
    } else None
  }

  private def updateDatamap(rchild: ModularPlan, subsume: ModularPlan) = {
    val update = rchild match {
      case s: Select if s.dataMapTableRelation.isDefined =>
        true
      case g: GroupBy if g.dataMapTableRelation.isDefined =>
        true
      case _ => false
    }

    if (update) {
      subsume match {
        case s: Select =>
          s.copy(children = Seq(rchild))

        case g: GroupBy =>
          g.copy(child = rchild)
        case _ => subsume
      }
    } else {
      subsume
    }
  }

  // add Select operator as placeholder on top of subsumee to facilitate matching
  def unifySubsumee(subsumee: ModularPlan): Option[ModularPlan] = {
    subsumee match {
      case gb @ modular.GroupBy(_, _, _, _,
        modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _) =>
        Some(
          Select(gb.outputList, gb.outputList, Nil, Map.empty, Nil, gb :: Nil, gb.flags,
            gb.flagSpec, Seq.empty))
      case other => Some(other)
    }
  }

  // add Select operator as placeholder on top of subsumer to facilitate matching
  def unifySubsumer1(
      subsumer: ModularPlan,
      subsumee: ModularPlan,
      dataMapRelation: ModularPlan): ModularPlan = {
    // Update datamap table relation to the subsumer modular plan
    val updatedSubsumer = subsumer match {
      case s: Select => s.copy(dataMapTableRelation = Some(dataMapRelation))
      case g: GroupBy => g.copy(dataMapTableRelation = Some(dataMapRelation))
      case other => other
    }
    (updatedSubsumer, subsumee) match {
      case (r @
        modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _),
        modular.Select(_, _, _, _, _,
          Seq(modular.GroupBy(_, _, _, _, modular.Select(_, _, _, _, _, _, _, _, _, _), _, _, _)),
          _, _, _, _)
        ) =>
        modular.Select(
          r.outputList, r.outputList, Nil, Map.empty, Nil, r :: Nil, r.flags,
          r.flagSpec, Seq.empty).setSkip()
      case _ => updatedSubsumer.setSkip()
    }
  }

  def unifySubsumer2(subsumer: ModularPlan, subsumee: ModularPlan): ModularPlan = {
    val rtables = subsumer.collect { case n: modular.LeafNode => n }
    val etables = subsumee.collect { case n: modular.LeafNode => n }
    val pairs = for {
      rtable <- rtables
      etable <- etables
      if (rtable == etable)
    } yield (rtable, etable)

    pairs.foldLeft(subsumer) {
      case (curSubsumer, pair) =>
        val nxtSubsumer = curSubsumer.transform { case pair._1 => pair._2 }
        val attributeSet = AttributeSet(pair._1.output)
        val rewrites = AttributeMap(pair._1.output.zip(pair._2.output))
        nxtSubsumer.transformUp {
          case p => p.transformExpressions {
            case a: Attribute if attributeSet contains a => rewrites(a).withQualifier(a.qualifier)
          }
        }
    }
  }
}
