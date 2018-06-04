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

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, ExperimentalMethods}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog
import org.apache.spark.sql.catalyst.expressions.{Exists, In, ListQuery, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.SparkOptimizer
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf


class CarbonOptimizer(
    catalog: SessionCatalog,
    experimentalMethods: ExperimentalMethods)
  extends SparkOptimizer(catalog, experimentalMethods) {

  override def execute(plan: LogicalPlan): LogicalPlan = {
    val transFormedPlan: LogicalPlan = CarbonOptimizerUtil.transformForScalarSubQuery(plan)
    super.execute(transFormedPlan)
  }
}

object CarbonOptimizerUtil {
  def transformForScalarSubQuery(plan: LogicalPlan): LogicalPlan = {
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
          case e: Exists =>
            val tPlan = e.plan.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            Exists(tPlan, e.children.map(_.canonicalized), e.exprId)

          case In(value, Seq(l@ListQuery(sub, _, exprId,_))) =>
            val tPlan = sub.transform {
              case lr: LogicalRelation
                if lr.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
                lr.relation.asInstanceOf[CarbonDatasourceHadoopRelation].isSubquery += true
                lr
            }
            In(value, Seq(ListQuery(tPlan, l.children, exprId)))
        }
    }
    transFormedPlan
  }
}