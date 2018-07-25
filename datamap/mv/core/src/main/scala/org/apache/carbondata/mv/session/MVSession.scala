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

package org.apache.carbondata.mv.session

import java.io.Closeable
import java.math.BigInteger

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import org.apache.carbondata.mv.rewrite.{QueryRewrite, SummaryDatasetCatalog}
import org.apache.carbondata.mv.session.internal.SessionState

/**
 * The entry point for working with multi-query optimization in Sparky. Allow the
 * creation of CSEs (covering subexpression) as well as query rewrite before
 * submitting to SparkSQL
 */
class MVSession(
    @transient val sparkSession: SparkSession,
    @transient val catalog: SummaryDatasetCatalog)
  extends Serializable with Closeable {

  self =>

  /* ----------------------- *
   |  Session-related state  |
   * ----------------------- */

  /**
   * State isolated across sessions, including SQL configurations, temporary tables, registered
   * functions, and everything else that accepts a [[org.apache.spark.sql.internal.SQLConf]].
   */
  @transient
  private[mv] lazy val sessionState: SessionState = new SessionState(self)

  @transient
  lazy val tableFrequencyMap = new mutable.HashMap[String, Int]

  @transient
  lazy val consumersMap = new mutable.HashMap[BigInteger, mutable.Set[LogicalPlan]] with mutable
  .MultiMap[BigInteger, LogicalPlan]

  def rewrite(analyzed: LogicalPlan): QueryRewrite = {
    sessionState.rewritePlan(analyzed)
  }

  def rewriteToSQL(analyzed: LogicalPlan): String = {
    val queryRewrite = rewrite(analyzed)
    Try(queryRewrite.withSummaryData) match {
      case Success(rewrittenPlan) =>
        if (rewrittenPlan.fastEquals(queryRewrite.modularPlan)) {
          ""
        } else {
          Try(rewrittenPlan.asCompactSQL) match {
            case Success(s) => s
            case Failure(e) => ""
          }
        }
      case Failure(e) => ""
    }
  }

  override def close(): Unit = sparkSession.close()

}
