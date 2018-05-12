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

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.datamap.MVState
import org.apache.carbondata.mv.plans.modular.ModularPlan

/**
 * The primary workflow for rewriting relational queries using Spark libraries.
 */
class QueryRewrite private (
    state: MVState,
    logical: LogicalPlan,
    nextSubqueryId: AtomicLong) {
  self =>

  def this(state: MVState, logical: LogicalPlan) =
    this(state, logical, new AtomicLong(0))

  def newSubsumerName(): String = s"gen_subsumer_${nextSubqueryId.getAndIncrement()}"

  lazy val optimizedPlan: LogicalPlan =
    state.optimizer.execute(logical)

  lazy val modularPlan: ModularPlan =
    state.modularizer.modularize(optimizedPlan).next().harmonized

  lazy val withSummaryData: ModularPlan =
    state.navigator.rewriteWithSummaryDatasets(modularPlan, self)

  lazy val toCompactSQL: String = withSummaryData.asCompactSQL

  lazy val toOneLineSQL: String = withSummaryData.asOneLineSQL
}
