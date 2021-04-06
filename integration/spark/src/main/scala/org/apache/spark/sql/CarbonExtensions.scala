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

package org.apache.spark.sql

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.strategy.{CarbonSourceStrategy, DDLStrategy, DMLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonLoadDataAnalyzeRule, CarbonPreInsertionCasts}
import org.apache.spark.sql.parser.CarbonExtensionSqlParser

/**
 * use SparkSessionExtensions to inject Carbon's extensions.
 * @since 2.0
 */
class CarbonExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Carbon internal parser
    extensions
      .injectParser((sparkSession: SparkSession, parser: ParserInterface) =>
        new CarbonExtensionSqlParser(new SQLConf, sparkSession, parser))

    // carbon analyzer rules
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonLoadDataAnalyzeRule(session))

    // carbon optimizer rules
    extensions.injectPostHocResolutionRule((session: SparkSession) => CarbonOptimizerRule(session))

    // carbon planner strategies
    extensions
      .injectPlannerStrategy(_ => StreamingTableStrategy)
    extensions
      .injectPlannerStrategy(_ => DMLStrategy)
    extensions
      .injectPlannerStrategy(_ => DDLStrategy)
    extensions
      .injectPlannerStrategy(_ => CarbonSourceStrategy)

    // init CarbonEnv
    CarbonEnv.init()
  }
}

case class CarbonOptimizerRule(session: SparkSession) extends Rule[LogicalPlan] {
  self =>

  var notAdded = true

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (notAdded) {
      self.synchronized {
        if (notAdded) {
          notAdded = false

          val sessionState = session.sessionState
          val field = sessionState.getClass.getDeclaredField("optimizer")
          field.setAccessible(true)
          field.set(sessionState,
            CarbonToSparkAdapter.getCarbonOptimizer(session, sessionState))
        }
      }
    }
    plan
  }
}

