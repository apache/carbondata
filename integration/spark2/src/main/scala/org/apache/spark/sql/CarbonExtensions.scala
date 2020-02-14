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
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonPreInsertionCasts}
import org.apache.spark.sql.parser.CarbonExtensionSqlParser

class CarbonExtensions extends ((SparkSessionExtensions) => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Try to initialize MV extension before carbon extension
    // It may fail if MVExtension class is not in class path
    try {
      val clazz = Class.forName("org.apache.carbondata.mv.extension.MVExtension")
      val method = clazz.getMethod("apply", classOf[SparkSessionExtensions])
      val mvExtension = clazz.newInstance()
      method.invoke(mvExtension, extensions)
    } catch {
      case _: Throwable =>
        // If MVExtension initialization failed, ignore it
    }

    // Carbon internal parser
    extensions
      .injectParser((sparkSession: SparkSession, parser: ParserInterface) =>
        new CarbonExtensionSqlParser(new SQLConf, sparkSession, parser))

    // carbon analyzer rules
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))

    // carbon optimizer rules
    extensions.injectPostHocResolutionRule((session: SparkSession) => CarbonOptimizerRule(session))

    // carbon planner strategies
    extensions
      .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))

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
            new CarbonOptimizer(session, sessionState.catalog, sessionState.optimizer))
        }
      }
    }
    plan
  }
}

