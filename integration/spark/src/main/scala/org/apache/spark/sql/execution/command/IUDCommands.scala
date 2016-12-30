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

package org.apache.spark.sql.execution.command

import org.apache.carbondata.common.logging.LogServiceFactory

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.RunnableCommand

/**
 * IUD update delete and compaction framework.
 *
 */

private[sql] case class ProjectForDeleteCommand(
     plan: LogicalPlan,
     tableIdentifier: Seq[String],
     timestamp: String) extends RunnableCommand {

  val LOG = LogServiceFactory.getLogService(this.getClass.getName)
  var horizontalCompactionFailed = false

  override def run(sqlContext: SQLContext): Seq[Row] = {
    DataFrame(sqlContext, plan).show(truncate = false)
    Seq.empty
  }
}

private[sql] case class ProjectForUpdateCommand(
    plan: LogicalPlan, tableIdentifier: Seq[String]) extends RunnableCommand {
  val LOGGER = LogServiceFactory.getLogService(ProjectForUpdateCommand.getClass.getName)

  override def run(sqlContext: SQLContext): Seq[Row] = {
    sqlContext.sparkContext.setLocalProperty(org.apache.spark.sql.execution.SQLExecution
      .EXECUTION_ID_KEY, null)
    DataFrame(sqlContext, plan).show(truncate = false)
    Seq.empty
  }
}
