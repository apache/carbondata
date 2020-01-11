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

import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.strategy.{CarbonLateDecodeStrategy, DDLStrategy, StreamingTableStrategy}
import org.apache.spark.sql.hive.{CarbonIUDAnalysisRule, CarbonMVRules, CarbonPreInsertionCasts, CarbonPreOptimizerRule}
import org.apache.spark.sql.optimizer.{CarbonIUDRule, CarbonLateDecodeRule, CarbonUDFTransformRule}
import org.apache.spark.sql.parser.CarbonExtensionSqlParser

class CarbonExtensions extends ((SparkSessionExtensions) => Unit) {

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

    // carbon optimizer rules
    extensions.injectPostHocResolutionRule((session: SparkSession) => OptimizerRule(session) )

    // carbon planner strategies
    extensions
      .injectPlannerStrategy((session: SparkSession) => new StreamingTableStrategy(session))
    extensions
      .injectPlannerStrategy((_: SparkSession) => new CarbonLateDecodeStrategy)
    extensions
      .injectPlannerStrategy((session: SparkSession) => new DDLStrategy(session))

    // init CarbonEnv
    CarbonEnv.init
  }
}

case class OptimizerRule(session: SparkSession) extends Rule[LogicalPlan] {
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
            new OptimizerProxy(session, sessionState.catalog, sessionState.optimizer))
        }
      }
    }
    plan
  }
}

class OptimizerProxy(
    session: SparkSession,
    catalog: SessionCatalog,
    optimizer: Optimizer) extends Optimizer(catalog) {

  private lazy val firstBatchRules = Seq(Batch("First Batch Optimizers", Once,
      Seq(CarbonMVRules(session), new CarbonPreOptimizerRule()): _*))

  private lazy val LastBatchRules = Batch("Last Batch Optimizers", fixedPoint,
    Seq(new CarbonIUDRule(), new CarbonUDFTransformRule(), new CarbonLateDecodeRule()): _*)

  override def batches: Seq[Batch] = {
    firstBatchRules ++ convertedBatch() :+ LastBatchRules
  }

  def convertedBatch(): Seq[Batch] = {
    optimizer.batches.map { batch =>
      Batch(
        batch.name,
        batch.strategy match {
          case optimizer.Once =>
            Once
          case _: optimizer.FixedPoint =>
            fixedPoint
        },
        batch.rules: _*
      )
    }
  }
}
