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
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableChangeColumnCommand, AlterTableRenameCommand}
import org.apache.spark.sql.execution.command.mutation.{CarbonProjectForDeleteCommand, CarbonProjectForUpdateCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException

/**
 * Strategy for streaming table, like blocking unsupported operation
 */
private[sql] class StreamingTableStrategy(sparkSession: SparkSession) extends SparkStrategy {

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case CarbonProjectForUpdateCommand(_, databaseNameOp, tableName, columns) =>
        rejectIfStreamingTable(
          TableIdentifier(tableName, databaseNameOp),
          "Data update")
        Nil
      case CarbonProjectForDeleteCommand(_, databaseNameOp, tableName, timestamp) =>
        rejectIfStreamingTable(
          TableIdentifier(tableName, databaseNameOp),
          "Data delete")
        Nil
      case addColumns: AlterTableAddColumnsCommand if isCarbonTable(addColumns.table) =>
        rejectIfStreamingTable(
          addColumns.table,
          "Alter table add column")
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
      case changeColumn: AlterTableChangeColumnCommand
        if isCarbonTable(changeColumn.tableName) =>
        val columnName = changeColumn.columnName
        val newColumn = changeColumn.newColumn
        var isColumnRename = false
        if (!columnName.equalsIgnoreCase(newColumn.name)) {
          isColumnRename = true
        }
        val operation = if (isColumnRename) {
          "Alter table column rename"
        } else {
          "Alter table change datatype"
        }
        rejectIfStreamingTable(changeColumn.tableName, operation)
        Nil
      case CarbonAlterTableColRenameDataTypeChangeCommand(model, _) =>
        val operation = if (model.isColumnRename) {
          "Alter table column rename"
        } else {
          "Alter table change datatype"
        }
        rejectIfStreamingTable(
          new TableIdentifier(model.tableName, model.databaseName), operation)
        Nil
      case AlterTableRenameCommand(oldTableIdentifier, _, _) =>
        rejectIfStreamingTable(
          oldTableIdentifier, "Alter rename table")
        Nil
      case _ => Nil
    }
  }

  /**
   * Validate whether Update operation is allowed for specified table in the command
   */
  private def rejectIfStreamingTable(tableIdentifier: TableIdentifier, operation: String): Unit = {
    var streaming = false
    try {
      streaming = CarbonEnv.getCarbonTable(
        tableIdentifier.database, tableIdentifier.table)(sparkSession)
        .isStreamingSink
    } catch {
      case e: Exception =>
        streaming = false
    }
    if (streaming) {
      throw new MalformedCarbonCommandException(
        s"$operation is not allowed for streaming table")
    }
  }

  def isCarbonTable(tableIdent: TableIdentifier): Boolean = {
    CarbonPlanHelper.isCarbonTable(tableIdent, sparkSession)
  }
}
