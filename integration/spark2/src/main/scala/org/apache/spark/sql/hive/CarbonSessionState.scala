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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, ExperimentalMethods, SparkSession}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{PredicateSubquery, ScalarSubquery}
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.{CarbonLateDecodeStrategy, SparkOptimizer}
import org.apache.spark.sql.execution.command.DDLStrategy
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.optimizer.CarbonLateDecodeRule
import org.apache.spark.sql.parser.CarbonSparkSqlParser

/**
 * Session state implementation to override sql parser and adding strategies
 * @param sparkSession
 */
class CarbonSessionState(sparkSession: SparkSession) extends HiveSessionState(sparkSession) {

  override lazy val sqlParser: ParserInterface = new CarbonSparkSqlParser(conf)

  experimentalMethods.extraStrategies =
    Seq(new CarbonLateDecodeStrategy, new DDLStrategy(sparkSession))
  experimentalMethods.extraOptimizations = Seq(new CarbonLateDecodeRule)

  override lazy val optimizer: Optimizer = new CarbonOptimizer(catalog, conf, experimentalMethods)
}

class CarbonOptimizer(
    catalog: SessionCatalog,
    conf: SQLConf,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, conf, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    // In case scalar subquery add flag in relation to skip the decoder plan in optimizer rule, And
    // optimize whole plan at once.
    val transFormedPlan = plan.transform {
      case filter: Filter =>
        filter.transformExpressions {
          case s: ScalarSubquery =>
            val tPlan = s.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            ScalarSubquery(tPlan, s.children, s.exprId)
          case p: PredicateSubquery =>
            val tPlan = p.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            PredicateSubquery(tPlan, p.children, p.nullAware, p.exprId)
        }
    }
    super.execute(transFormedPlan)
  }
}
