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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.execution.command.{ExecutedCommandExec, RunnableCommand}
import org.apache.spark.sql.execution.command.management.CarbonAlterTableCompactionCommand
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableDropColumnCommand}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructField
import org.apache.spark.util.{CarbonReflectionUtils, SparkUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.util.DataTypeUtil

object CarbonPlanHelper {

  def addColumn(
      addColumnCommand: CarbonAlterTableAddColumnCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val alterTableAddColumnsModel = addColumnCommand.alterTableAddColumnsModel
    if (isCarbonTable(
      TableIdentifier(
        alterTableAddColumnsModel.tableName,
        alterTableAddColumnsModel.databaseName),
      sparkSession)) {
      val carbonTable = CarbonEnv.getCarbonTable(alterTableAddColumnsModel.databaseName,
        alterTableAddColumnsModel.tableName)(sparkSession)
      if (carbonTable != null && carbonTable.isFileLevelFormat) {
        throw new MalformedCarbonCommandException(
          "Unsupported alter operation on Carbon external fileformat table")
      } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
        throw new MalformedCarbonCommandException(
          "Unsupported operation on non transactional table")
      } else {
        ExecutedCommandExec(addColumnCommand) :: Nil
      }
      // TODO: remove this else if check once the 2.1 version is unsupported by carbon
    } else if (SparkUtil.isSparkVersionXandAbove("2.2")) {
      val structField = (alterTableAddColumnsModel.dimCols ++ alterTableAddColumnsModel.msrCols)
        .map { f =>
          val structField = StructField(f.column,
            CarbonSparkDataSourceUtil
              .convertCarbonToSparkDataType(DataTypeUtil.valueOf(f.dataType.get)))
          if (StringUtils.isNotEmpty(f.columnComment)) {
            structField.withComment(f.columnComment)
          } else {
            structField
          }
        }
      val identifier = TableIdentifier(
        alterTableAddColumnsModel.tableName,
        alterTableAddColumnsModel.databaseName)
      ExecutedCommandExec(CarbonReflectionUtils
        .invokeAlterTableAddColumn(identifier, structField).asInstanceOf[RunnableCommand]) ::
      Nil
      // TODO: remove this else check once the 2.1 version is unsupported by carbon
    } else {
      throw new MalformedCarbonCommandException("Unsupported alter operation on hive table")
    }
  }

  def changeColumn(
      changeColumnCommand: CarbonAlterTableColRenameDataTypeChangeCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val model = changeColumnCommand.alterTableColRenameAndDataTypeChangeModel
    if (isCarbonTable(TableIdentifier(model.tableName, model.databaseName), sparkSession)) {
      val carbonTable =
        CarbonEnv.getCarbonTable(model.databaseName, model.tableName)(sparkSession)
      if (carbonTable != null && carbonTable.isFileLevelFormat) {
        throw new MalformedCarbonCommandException(
          "Unsupported alter operation on Carbon external fileformat table")
      } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
        throw new MalformedCarbonCommandException(
          "Unsupported operation on non transactional table")
      } else {
        ExecutedCommandExec(changeColumnCommand) :: Nil
      }
    } else {
      throw new MalformedCarbonCommandException(
        String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
          model.tableName,
          model.databaseName.getOrElse("default")))
    }
  }

  def dropColumn(
      dropColumnCommand: CarbonAlterTableDropColumnCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val alterTableDropColumnModel = dropColumnCommand.alterTableDropColumnModel
    val carbonTable = CarbonEnv.getCarbonTable(alterTableDropColumnModel.databaseName,
      alterTableDropColumnModel.tableName)(sparkSession)
    if (carbonTable != null && carbonTable.isFileLevelFormat) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on Carbon external fileformat table")
    } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    } else {
      ExecutedCommandExec(dropColumnCommand) :: Nil
    }
  }

  def compact(
      compactionCommand: CarbonAlterTableCompactionCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val alterTableModel = compactionCommand.alterTableModel
    if (isCarbonTable(TableIdentifier(alterTableModel.tableName, alterTableModel.dbName),
      sparkSession)) {
      ExecutedCommandExec(compactionCommand) :: Nil
    } else {
      throw new MalformedCarbonCommandException(
        String.format("Table or view '%s' not found in database '%s' or not carbon fileformat",
          alterTableModel.tableName,
          alterTableModel.dbName.getOrElse("default")))
    }
  }

  def isCarbonTable(tableIdent: TableIdentifier, sparkSession: SparkSession): Boolean = {
    val dbOption = tableIdent.database.map(_.toLowerCase)
    val tableIdentifier = TableIdentifier(tableIdent.table.toLowerCase(), dbOption)
    CarbonEnv
      .getInstance(sparkSession)
      .carbonMetaStore
      .tableExists(tableIdentifier)(sparkSession)
  }

  def isTableExists(tableIdent: TableIdentifier, sparkSession: SparkSession): Boolean = {
    val dbOption = tableIdent.database.map(_.toLowerCase)
    val tableIdentifier = TableIdentifier(tableIdent.table.toLowerCase(), dbOption)
    sparkSession.sessionState.catalog.tableExists(tableIdentifier)
  }

  def validateCarbonTable(
      tableIdentifier: TableIdentifier,
      sparkSession: SparkSession,
      message: String
  ): Unit = {
    if (!CarbonPlanHelper.isTableExists(tableIdentifier, sparkSession)) {
      throw new NoSuchTableException(
        tableIdentifier.database.getOrElse(
          CarbonEnv.getDatabaseName(tableIdentifier.database)(sparkSession)),
        tableIdentifier.table)
    }
    if (!CarbonPlanHelper.isCarbonTable(tableIdentifier, sparkSession)) {
      throw new UnsupportedOperationException(message)
    }
  }
}
