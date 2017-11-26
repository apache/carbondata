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

package org.apache.spark.sql.execution.strategy

import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command.AlterTableRenameCommand
import org.apache.spark.sql.execution.command.mutation.{DeleteExecution, ProjectForDeleteCommand, ProjectForUpdateCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

/**
 * Strategy for streaming table, like blocking unsupported operation
 */
private[sql] class StreamingTableStrategy(sparkSession: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case ProjectForUpdateCommand(_, tableIdentifier) =>
        rejectIfStreamingTable(
          DeleteExecution.getTableIdentifier(tableIdentifier),
          "Data update")
        Nil
      case ProjectForDeleteCommand(_, tableIdentifier, _) =>
        rejectIfStreamingTable(
          DeleteExecution.getTableIdentifier(tableIdentifier),
          "Date delete")
        Nil
      case CarbonAlterTableAddColumnCommand(model) =>
        rejectIfStreamingTable(
          new TableIdentifier(model.tableName, model.databaseName),
          "Alter table add column")
        Nil
      case CarbonAlterTableDropColumnCommand(model) =>
        rejectIfStreamingTable(
          new TableIdentifier(model.tableName, model.databaseName),
          "Alter table drop column")
        Nil
      case CarbonAlterTableDataTypeChangeCommand(model) =>
        rejectIfStreamingTable(
          new TableIdentifier(model.tableName, model.databaseName),
          "Alter table change datatype")
        Nil
      case AlterTableRenameCommand(oldTableIdentifier, _, _) =>
        rejectIfStreamingTable(
          oldTableIdentifier,
          "Alter rename table")
        Nil
      case _ => Nil
    }
  }

  /**
   * Validate whether Update operation is allowed for specified table in the command
   */
  private def rejectIfStreamingTable(tableIdentifier: TableIdentifier, operation: String): Unit = {
    val streaming = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tableIdentifier)(sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable
      .isStreamingTable
    if (streaming) {
      throw new MalformedCarbonCommandException(
        s"$operation is not allowed for streaming table")
    }
  }
}
