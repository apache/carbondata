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

package org.apache.carbondata.mv.extension

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions, SQLConf}
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import org.apache.carbondata.mv.rewrite.MVUdf
import org.apache.carbondata.mv.timeseries.TimeSeriesFunction

/**
 * Materialized View extension for Apache Spark
 *
 * Following SQL command are added:
 *   1. CREATE MATERIALIZED VIEW
 *   2. DROP MATERIALIZED VIEW
 *   3. SHOW MATERIALIZED VIEW
 *   4. REFRESH MATERIALIZED VIEW
 *
 * Following optimizer rules are added:
 *   1. Rewrite SQL statement by matching existing MV and
 *      select the lowest cost MV
 */
class MVExtension extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    // MV parser
    extensions.injectParser(
      (sparkSession: SparkSession, parser: ParserInterface) =>
        new MVExtensionSqlParser(new SQLConf, sparkSession, parser))

    // MV optimizer rules
    extensions.injectPostHocResolutionRule(
      (session: SparkSession) => MVOptimizerRule(session) )
  }
}

case class MVOptimizerRule(session: SparkSession) extends Rule[LogicalPlan] {
  self =>

  var initialized = false

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!initialized) {
      self.synchronized {
        if (!initialized) {
          initialized = true

          addMVUdf(session)

          val sessionState = session.sessionState
          val field = sessionState.getClass.getDeclaredField("optimizer")
          field.setAccessible(true)
          field.set(sessionState,
            new MVOptimizer(session, sessionState.catalog, sessionState.optimizer))
        }
      }
    }
    plan
  }

  private def addMVUdf(sparkSession: SparkSession) = {
    // added for handling MV table creation. when user will fire create ddl for
    // create table we are adding a udf so no need to apply MV rules.
    sparkSession.udf.register(MVUdf.MV_SKIP_RULE_UDF, () => "")

    // added for handling timeseries function like hour, minute, day , month , year
    sparkSession.udf.register("timeseries", new TimeSeriesFunction)
  }
}

