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

import org.apache.spark.sql._
import org.apache.spark.sql.CarbonToSparkAdapter.RefreshTables
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.index.{DropIndexCommand, ShowIndexesCommand}
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand}
import org.apache.spark.sql.execution.command.mutation.CarbonTruncateCommand
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableLikeCommand, CarbonDropTableCommand, CarbonShowCreateTableCommand, CarbonShowTablesCommand}
import org.apache.spark.sql.execution.datasources.RefreshResource
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper.isCarbonTable
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand, MatchResetCommand}
import org.apache.spark.sql.secondaryindex.command.CarbonCreateSecondaryIndexCommand
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory

/**
 * Carbon strategies for ddl commands
 * CreateDataSourceTableAsSelectCommand class has extra argument in
 * 2.3, so need to add wrapper to match the case
 */
object MatchCreateDataSourceTable {
  def unapply(plan: LogicalPlan): Option[(CatalogTable, SaveMode, LogicalPlan)] = {
    plan match {
      case t: CreateDataSourceTableAsSelectCommand => Some(t.table, t.mode, t.query)
      case _ => None
    }
  }
}

object DDLStrategy extends SparkStrategy {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    val sparkSession = SparkSQLUtil.getSparkSession
    plan match {
      case _: ReturnAnswer => Nil
      // alter table
      case renameTable: AlterTableRenameCommand
        if isCarbonTable(renameTable.oldName) =>
        ExecutedCommandExec(DDLHelper.renameTable(renameTable)) :: Nil
      case compaction: CarbonAlterTableCompactionCommand =>
        CarbonPlanHelper.compact(compaction, sparkSession)
      case changeColumn: AlterTableChangeColumnCommand
        if isCarbonTable(changeColumn.tableName) =>
        ExecutedCommandExec(DDLHelper.changeColumn(changeColumn, sparkSession)) :: Nil
      case colRenameDataTypeChange: CarbonAlterTableColRenameDataTypeChangeCommand =>
        CarbonPlanHelper.changeColumn(colRenameDataTypeChange, sparkSession)
      case addColumns: AlterTableAddColumnsCommand
        if isCarbonTable(addColumns.table) =>
        ExecutedCommandExec(DDLHelper.addColumns(addColumns, sparkSession)) :: Nil
      case addColumn: CarbonAlterTableAddColumnCommand =>
        CarbonPlanHelper.addColumn(addColumn, sparkSession)
      case dropColumn: CarbonAlterTableDropColumnCommand =>
        if (isCarbonTable(TableIdentifier(
          dropColumn.alterTableDropColumnModel.tableName,
          dropColumn.alterTableDropColumnModel.databaseName))) {
          CarbonPlanHelper.dropColumn(dropColumn, sparkSession)
        } else {
          throw new UnsupportedOperationException("Only carbondata table support drop column")
        }
      case AlterTableSetLocationCommand(tableName, _, _)
        if isCarbonTable(tableName) =>
        throw new UnsupportedOperationException("Set partition location is not supported")
      // partition
      case showPartitions: ShowPartitionsCommand
        if isCarbonTable(showPartitions.tableName) =>
        ExecutedCommandExec(DDLHelper.showPartitions(showPartitions, sparkSession)) :: Nil
      case dropPartition: AlterTableDropPartitionCommand
        if isCarbonTable(dropPartition.tableName) =>
        ExecutedCommandExec(DDLHelper.dropPartition(dropPartition, sparkSession)) :: Nil
      case renamePartition: AlterTableRenamePartitionCommand
        if isCarbonTable(renamePartition.tableName) =>
        throw new UnsupportedOperationException("Renaming partition on table is not supported")
      case addPartition: AlterTableAddPartitionCommand
        if isCarbonTable(addPartition.tableName) =>
        ExecutedCommandExec(DDLHelper.addPartition(addPartition)) :: Nil
      // set/unset/reset
      case set: SetCommand =>
        ExecutedCommandExec(CarbonSetCommand(set)) :: Nil
      case MatchResetCommand(_) =>
        ExecutedCommandExec(CarbonResetCommand()) :: Nil
      case setProperties: AlterTableSetPropertiesCommand
        if isCarbonTable(setProperties.tableName) =>
        ExecutedCommandExec(DDLHelper.setProperties(setProperties, sparkSession)) :: Nil
      case unsetProperties: AlterTableUnsetPropertiesCommand
        if isCarbonTable(unsetProperties.tableName) =>
        ExecutedCommandExec(DDLHelper.unsetProperties(unsetProperties)) :: Nil
      // create/describe/drop table
      case createTable: CreateTableCommand
        if isCarbonHiveTable(createTable.table) =>
        // CREATE TABLE STORED AS carbondata
        ExecutedCommandExec(DDLHelper.createHiveTable(createTable)) :: Nil
      case createTable: CreateTableCommand
        if isCarbonFileHiveTable(createTable.table) =>
        // CREATE TABLE STORED AS carbon
        if (EnvHelper.isLegacy(sparkSession)) {
          Nil
        } else {
          ExecutedCommandExec(DDLHelper.createCarbonFileHiveTable(createTable)) :: Nil
        }
      case ctas: CreateHiveTableAsSelectCommand
        if isCarbonHiveTable(ctas.tableDesc) =>
        // CREATE TABLE STORED AS carbondata AS SELECT
        ExecutedCommandExec(DDLHelper.createHiveTableAsSelect(ctas, sparkSession)) :: Nil
      case ctas: CreateHiveTableAsSelectCommand
        if isCarbonFileHiveTable(ctas.tableDesc) =>
        // CREATE TABLE STORED AS carbon AS SELECT
        if (EnvHelper.isLegacy(sparkSession)) {
          Nil
        } else {
          DataWritingCommandExec(
            DDLHelper.createCarbonFileHiveTableAsSelect(ctas),
            planLater(ctas.query)) :: Nil
        }
      case showCreateTable: ShowCreateTableCommand
        if isCarbonTable(showCreateTable.table) =>
        ExecutedCommandExec(CarbonShowCreateTableCommand(showCreateTable)) :: Nil
      case createLikeTable: CreateTableLikeCommand
        if isCarbonTable(createLikeTable.sourceTable) =>
        ExecutedCommandExec(CarbonCreateTableLikeCommand(createLikeTable.sourceTable,
          createLikeTable.targetTable, createLikeTable.ifNotExists)) :: Nil
      case truncateTable: TruncateTableCommand
        if isCarbonTable(truncateTable.tableName) =>
        ExecutedCommandExec(CarbonTruncateCommand(truncateTable)) :: Nil
      case createTable@org.apache.spark.sql.execution.datasources.CreateTable(_, _, None)
        if CarbonSource.isCarbonDataSource(createTable.tableDesc) =>
        // CREATE TABLE USING carbondata
        ExecutedCommandExec(DDLHelper.createDataSourceTable(createTable)) :: Nil
      case MatchCreateDataSourceTable(tableDesc, mode, query)
        if CarbonSource.isCarbonDataSource(tableDesc) =>
        // CREATE TABLE USING carbondata AS SELECT
        ExecutedCommandExec(
          DDLHelper.createDataSourceTableAsSelect(tableDesc, query, mode, sparkSession)
        ) :: Nil
      case org.apache.spark.sql.execution.datasources.CreateTable(tableDesc, mode, query)
        if CarbonSource.isCarbonDataSource(tableDesc) =>
        // CREATE TABLE USING carbondata AS SELECT
        ExecutedCommandExec(
          DDLHelper.createDataSourceTableAsSelect(tableDesc, query.get, mode, sparkSession)
        ) :: Nil
      case createTable@CreateDataSourceTableCommand(table, _)
        if CarbonSource.isCarbonDataSource(table) =>
        // CREATE TABLE USING carbondata
        ExecutedCommandExec(DDLHelper.createDataSourceTable(createTable)) :: Nil
      case desc: DescribeTableCommand if isCarbonTable(desc.table) =>
        ExecutedCommandExec(DDLHelper.describeTable(desc, sparkSession)) :: Nil
      case DropTableCommand(identifier, ifNotExists, _, _)
        if isCarbonTable(identifier) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database, identifier.table.toLowerCase)
        ) :: Nil
      // refresh
      case refreshTable: RefreshTables =>
        ExecutedCommandExec(DDLHelper.refreshTable(refreshTable)) :: Nil
      case refreshResource: RefreshResource =>
        DDLHelper.refreshResource(refreshResource)
      // database
      case createDb: CreateDatabaseCommand =>
        ExecutedCommandExec(DDLHelper.createDatabase(createDb, sparkSession)) :: Nil
      case drop@DropDatabaseCommand(dbName, ifExists, _)
        if CarbonEnv.databaseLocationExists(dbName, sparkSession, ifExists) =>
        ExecutedCommandExec(CarbonDropDatabaseCommand(drop)) :: Nil
      // explain
      case explain: ExplainCommand =>
        DDLHelper.explain(explain, sparkSession)
      case showTables: ShowTablesCommand =>
        ExecutedCommandExec(CarbonShowTablesCommand(showTables)) :: Nil
      case CarbonCreateSecondaryIndexCommand(
      indexModel, tableProperties, ifNotExists, isDeferredRefresh, isCreateSIndex) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(indexModel.tableName, indexModel.dbName))(
            sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(CarbonCreateSecondaryIndexCommand(
            indexModel, tableProperties, ifNotExists, isDeferredRefresh, isCreateSIndex)) :: Nil
        } else {
          sys.error(s"Operation not allowed because either table " +
            s"${indexModel.tableName} doesn't exist or not a carbon table.")
        }
      case showIndex@ShowIndexesCommand(_, _) =>
        try {
          ExecutedCommandExec(showIndex) :: Nil
        } catch {
          case c: Exception =>
            sys.error("Operation not allowed on non-carbon table")
        }
      case dropIndex@DropIndexCommand(ifExistsSet, databaseNameOp, parentTableName, tableName, _) =>
        val tableIdentifier = TableIdentifier(parentTableName, databaseNameOp)
        val isParentTableExists = sparkSession.sessionState.catalog.tableExists(tableIdentifier)
        if (!isParentTableExists) {
          if (!ifExistsSet) {
            sys.error(s"Table $tableIdentifier does not exist")
          } else {
            Nil
          }
        } else {
          ExecutedCommandExec(dropIndex) :: Nil
        }
      case _ => Nil
    }
  }

  private def isCarbonHiveTable(table: CatalogTable): Boolean = {
    table.provider.isDefined &&
    DDLUtils.HIVE_PROVIDER == table.provider.get &&
    table.storage.serde.get == "org.apache.carbondata.hive.CarbonHiveSerDe"
  }

  private def isCarbonFileHiveTable(table: CatalogTable): Boolean = {
    table.provider.isDefined &&
    DDLUtils.HIVE_PROVIDER == table.provider.get &&
    table.storage.serde.get == "org.apache.carbondata.hive.CarbonFileHiveSerDe"
  }
}
