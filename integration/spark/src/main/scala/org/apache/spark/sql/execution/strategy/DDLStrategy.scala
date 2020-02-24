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
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, ReturnAnswer}
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.command.management.{CarbonAlterTableCompactionCommand, CarbonInsertIntoCommand}
import org.apache.spark.sql.execution.command.mutation.CarbonTruncateCommand
import org.apache.spark.sql.execution.command.schema._
import org.apache.spark.sql.execution.command.table.{CarbonCreateTableLikeCommand, CarbonDropTableCommand, CarbonShowCreateTableCommand}
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, RefreshResource, RefreshTable}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.hive.execution.command.{CarbonDropDatabaseCommand, CarbonResetCommand, CarbonSetCommand, MatchResetCommand}
import org.apache.spark.sql.secondaryindex.command.{CreateIndexTable, DropIndexCommand, RegisterIndexTableCommand, ShowIndexesCommand}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
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

class DDLStrategy(sparkSession: SparkSession) extends SparkStrategy {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def apply(plan: LogicalPlan): Seq[SparkPlan] = {
    plan match {
      case _: ReturnAnswer => Nil
      // load data / insert into
      case loadData: LoadDataCommand
        if isCarbonTable(loadData.table) =>
        ExecutedCommandExec(DMLHelper.loadData(loadData, sparkSession)) :: Nil
      case InsertIntoCarbonTable(
      relation: CarbonDatasourceHadoopRelation, partition, child: LogicalPlan, overwrite, _) =>
        ExecutedCommandExec(
          CarbonInsertIntoCommand(
            databaseNameOp = Some(relation.carbonRelation.databaseName),
            tableName = relation.carbonRelation.tableName,
            options = scala.collection.immutable
              .Map("fileheader" -> relation.tableSchema.get.fields.map(_.name).mkString(",")),
            isOverwriteTable = overwrite,
            logicalPlan = child,
            tableInfo = relation.carbonRelation.carbonTable.getTableInfo,
            partition = partition)) :: Nil
      case insert: InsertIntoHadoopFsRelationCommand
        if insert.catalogTable.isDefined && isCarbonTable(insert.catalogTable.get.identifier) =>
        DataWritingCommandExec(DMLHelper.insertInto(insert), planLater(insert.query)) :: Nil
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
        ExecutedCommandExec(DDLHelper.createHiveTable(createTable, sparkSession)) :: Nil
      case createTable: CreateTableCommand
        if isCarbonFileHiveTable(createTable.table) =>
        // CREATE TABLE STORED AS carbon
        if (EnvHelper.isCloud(sparkSession)) {
          Nil
        } else {
          ExecutedCommandExec(DDLHelper.createCarbonFileHiveTable(createTable, sparkSession)) :: Nil
        }
      case ctas: CreateHiveTableAsSelectCommand
        if isCarbonHiveTable(ctas.tableDesc) =>
        // CREATE TABLE STORED AS carbondata AS SELECT
        ExecutedCommandExec(
          DDLHelper.createHiveTableAsSelect(ctas, sparkSession)
        ) :: Nil
      case ctas: CreateHiveTableAsSelectCommand
        if isCarbonFileHiveTable(ctas.tableDesc) =>
        // CREATE TABLE STORED AS carbon AS SELECT
        if (EnvHelper.isCloud(sparkSession)) {
          Nil
        } else {
          DataWritingCommandExec(
            DDLHelper.createCarbonFileHiveTableAsSelect(ctas, sparkSession),
            planLater(ctas.query)
          ) :: Nil
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
        ExecutedCommandExec(DDLHelper.createDataSourceTable(createTable, sparkSession)) :: Nil
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
        ExecutedCommandExec(
          DDLHelper.createDataSourceTable(createTable, sparkSession)
        ) :: Nil
      case desc: DescribeTableCommand if isCarbonTable(desc.table) =>
        ExecutedCommandExec(DDLHelper.describeTable(desc, sparkSession)) :: Nil
      case DropTableCommand(identifier, ifNotExists, _, _)
        if isCarbonTable(identifier) =>
        ExecutedCommandExec(
          CarbonDropTableCommand(ifNotExists, identifier.database, identifier.table.toLowerCase)
        ) :: Nil
      // refresh
      case refreshTable: RefreshTable =>
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
        DDLHelper.showTables(showTables, sparkSession)
      case CreateIndexTable(indexModel, tableProperties, isCreateSIndex) =>
        val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          .tableExists(TableIdentifier(indexModel.tableName, indexModel.databaseName))(
            sparkSession)
        if (isCarbonTable) {
          ExecutedCommandExec(CreateIndexTable(indexModel, tableProperties,
            isCreateSIndex)) :: Nil
        } else {
          sys.error("Operation not allowed on non-carbon table")
        }
      case showIndex@ShowIndexesCommand(_, _) =>
        try {
          ExecutedCommandExec(showIndex) ::
          Nil
        } catch {
          case c: Exception =>
            sys.error("Operation not allowed on non-carbon table")
        }
      case dropIndex@DropIndexCommand(ifExistsSet, databaseNameOp,
      tableName, parentTableName) =>
        val tableIdentifier = TableIdentifier(parentTableName, databaseNameOp)
        val isParentTableExists = sparkSession.sessionState.catalog.tableExists(tableIdentifier)
        if (!isParentTableExists) {
          if (!ifExistsSet) {
            sys.error("Table does not exist on non-carbon table")
          } else {
            Nil
          }
        } else {
          val isCarbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
            .tableExists(tableIdentifier)(sparkSession)
          if (isCarbonTable) {
            val isIndexTableExist = CarbonEnv.getInstance(sparkSession).carbonMetaStore
              .tableExists(TableIdentifier(tableName, databaseNameOp))(sparkSession)
            if (!isIndexTableExist && !ifExistsSet) {
              val dbName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
              throw new MalformedCarbonCommandException(
                s"Index table [$dbName.$tableName] does not exist on " +
                s"parent table [$dbName.$parentTableName]")
            }
            ExecutedCommandExec(dropIndex) :: Nil
          } else {
            sys.error("Operation not allowed on non-carbon table")
          }
        }
      case _ => Nil
    }
  }

  private def isCarbonTable(tableIdent: TableIdentifier): Boolean = {
    CarbonPlanHelper.isCarbonTable(tableIdent, sparkSession)
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
