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

package org.apache.carbondata.mv.tool.preprocessor

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._

import org.apache.carbondata.mv.plans._
import org.apache.carbondata.mv.plans.modular.ModularPlan

abstract class QueryBatchPreprocessor extends QueryBatchRuleEngine[ModularPlan] {
  lazy val phases = Seq(
      DedupPlans,
      FindSPJGs
  )
}

object DedupPlans extends Phase[ModularPlan] with PredicateHelper {
  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    val distinctPlanMap = batch.groupBy(p => p._1.semanticHash())
    val distinctPlans = for (candidates <- distinctPlanMap.values) yield
      (candidates.head._1, candidates.size)
    distinctPlans.toSeq
  }
}

object FindSPJGs extends Phase[ModularPlan] with PredicateHelper  {

  def apply(batch: Seq[(ModularPlan, Int)]): Seq[(ModularPlan, Int)] = {
    val spjgCandidates = mutable.ArrayBuffer[(ModularPlan, Int)]()

    batch.indices.foreach(iSubplan => {
      val plan = batch(iSubplan)._1
      plan.subqueries.foreach { subquery =>
        for (groupby <- getGroupBys(subquery)) {
          if (groupby.isSPJGH) {
            spjgCandidates += ((groupby, batch(iSubplan)._2))
          }
        }
      }

      for (groupby <- getGroupBys(plan)) {
        if (groupby.isSPJGH) {
          spjgCandidates += ((groupby, batch(iSubplan)._2))
        }
      }
    })

    collection.immutable.Seq(spjgCandidates: _*)
  }

  def getGroupBys(subplan: ModularPlan): Seq[modular.GroupBy] = {
    subplan.collect {
      case n: modular.GroupBy =>
//        mqoContext.consumersMap.addBinding(CSEUtil.computeMerkleTreeHash(n), subplan)
        n
    }
  }
}
