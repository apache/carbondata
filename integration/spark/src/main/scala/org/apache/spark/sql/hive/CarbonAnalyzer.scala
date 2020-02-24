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

package org.apache.spark.sql.hive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Analyzer
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.CarbonReflectionUtils

class CarbonAnalyzer(catalog: SessionCatalog,
    conf: SQLConf,
    sparkSession: SparkSession,
    analyzer: Analyzer) extends Analyzer(catalog, conf) {

  val mvPlan = try {
    CarbonReflectionUtils.createObject(
      "org.apache.carbondata.mv.extension.MVAnalyzerRule",
      sparkSession)._1.asInstanceOf[Rule[LogicalPlan]]
  } catch {
    case e: Exception =>
      null
  }

  override def execute(plan: LogicalPlan): LogicalPlan = {
    val logicalPlan = analyzer.execute(plan)
    if (mvPlan != null) {
      mvPlan.apply(logicalPlan)
    } else {
      logicalPlan
    }
  }
}
