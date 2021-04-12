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

package org.apache.spark.sql.execution.command.mutation

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.types.LongType


/**
 * IUD update delete and compaction framework.
 *
 */
private[sql] case class CarbonProjectForDeleteCommand(
                                                       plan: LogicalPlan,
                                                       databaseNameOp: Option[String],
                                                       tableName: String,
                                                       timestamp: String)
  extends DataCommand {

  override val output: Seq[Attribute] = {
    Seq(AttributeReference("Deleted Row Count", LongType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(databaseNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    setAuditInfo(Map("plan" -> plan.simpleString))
    IUDHelper.processDeleteOp(plan, carbonTable, timestamp, sparkSession)
  }

  override protected def opName: String = "DELETE DATA"
}
