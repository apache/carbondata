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
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonPreAggregateDataLoadingRules, CarbonPreAggregateQueryRules, CarbonPreInsertionCasts}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonSparkSqlParser
import org.apache.spark.util.{CarbonReflectionUtils, Utils}

class CarbonExtensions extends ((SparkSessionExtensions) => Unit) {

  CarbonExtensions

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Carbon parser
    extensions
      .injectParser((sparkSession: SparkSession, _: ParserInterface) =>
        new CarbonSparkSqlParser(new SQLConf, sparkSession))

    // carbon analyzer rules
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonIUDAnalysisRule(session))
    extensions
      .injectResolutionRule((session: SparkSession) => CarbonPreInsertionCasts(session))


    // carbon post adhoc resolution rules
    extensions
      .injectPostHocResolutionRule(
        (session: SparkSession) => new CarbonPreAggregateDataLoadingRules(session))
    extensions
      .injectPostHocResolutionRule(
        (session: SparkSession) => new CarbonPreAggregateQueryRules(session))

    val mv = try {
      Utils.classForName("org.apache.carbondata.mv.datamap.MVAnalyzerRule")
    } catch {
      case e: Exception =>
        null
    }

    if(mv != null) {
      extensions
        .injectPostHocResolutionRule(
          (session: SparkSession) => {
            try {
              CarbonReflectionUtils.createObject(
                "org.apache.carbondata.mv.datamap.MVAnalyzerRule",
                session)._1.asInstanceOf[Rule[LogicalPlan]]
            } catch {
              case e: Exception =>
                null
            }
          })
    }

    // carbon extra optimizations
    extensions
      .injectOptimizerRule((_: SparkSession) => new CarbonIUDRule)
    extensions
      .injectOptimizerRule((_: SparkSession) => new CarbonUDFTransformRule)
    extensions
      .injectOptimizerRule((_: SparkSession) => new CarbonLateDecodeRule)

    // carbon planner strategies
    extensions
      .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))

  }
}

object CarbonExtensions {
  CarbonEnv.init
}
