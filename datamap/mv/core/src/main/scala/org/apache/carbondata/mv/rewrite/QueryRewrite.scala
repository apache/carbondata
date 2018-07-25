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

import org.apache.carbondata.mv.datamap.MVHelper
import org.apache.carbondata.mv.plans.modular.ModularPlan
import org.apache.carbondata.mv.session.MVSession

/**
 * The primary workflow for rewriting relational queries using Spark libraries.
 * Designed to allow easy access to the intermediate phases of query rewrite for developers.
 *
 * While this is not a public class, we should avoid changing the function names for the sake of
 * changing them, because a lot of developers use the feature for debugging.
 */
class QueryRewrite private (
    state: MVSession,
    logical: LogicalPlan,
    nextSubqueryId: AtomicLong) {
  self =>

  def this(state: MVSession, logical: LogicalPlan) =
    this(state, logical, new AtomicLong(0))

  def newSubsumerName(): String = s"gen_subsumer_${nextSubqueryId.getAndIncrement()}"

  lazy val optimizedPlan: LogicalPlan =
    state.sessionState.optimizer.execute(logical)

  lazy val modularPlan: ModularPlan =
    state.sessionState.modularizer.modularize(optimizedPlan).next().harmonized

  lazy val withSummaryData: ModularPlan =
    state.sessionState.navigator.rewriteWithSummaryDatasets(modularPlan, self)

  lazy val withMVTable: ModularPlan = MVHelper.rewriteWithMVTable(withSummaryData, this)

  lazy val toCompactSQL: String = withSummaryData.asCompactSQL

  lazy val toOneLineSQL: String = withSummaryData.asOneLineSQL
}
