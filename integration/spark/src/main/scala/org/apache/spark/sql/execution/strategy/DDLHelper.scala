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

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.hive.metastore.api.InvalidOperationException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableChangeColumnCommand, AlterTableDropPartitionCommand, AlterTableUnsetPropertiesCommand, DescribeTableCommand, ShowPartitionsCommand, _}
import org.apache.spark.sql.execution.command.management.RefreshCarbonTableCommand
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableAddHivePartitionCommand, CarbonAlterTableDropHivePartitionCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableRenameCommand, CarbonAlterTableSetCommand, CarbonAlterTableUnsetCommand}
import org.apache.spark.sql.execution.command.table._
import org.apache.spark.sql.execution.datasources.{LogicalRelation, RefreshResource}
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.parser.{CarbonSpark2SqlParser, CarbonSparkSqlParserUtil}
import org.apache.spark.sql.types.{DecimalType, StructField}
import org.apache.spark.sql.util.SparkSQLUtil
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.view.MVManagerInSpark

object DDLHelper {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def createDatabase(
      createDatabaseCommand: CreateDatabaseCommand,
      sparkSession: SparkSession): CreateDatabaseCommand = {
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    if (!EnvHelper.isLegacy(sparkSession) && !createDatabaseCommand.path.isDefined) {
      val carbonStorePath = CarbonProperties.getStorePath()
      val sparkWarehouse = sparkSession.conf.get("spark.sql.warehouse.dir")
      if (StringUtils.isNotEmpty(carbonStorePath) && StringUtils.isNotEmpty(sparkWarehouse)
        && !carbonStorePath.equals(sparkWarehouse)) {
        throw new InvalidOperationException("Create database is prohibited when" +
          " database locaton is inconsistent, please don't configure " +
          " carbon.storelocation and spark.sql.warehouse.dir to different values," +
          s" carbon.storelocation is $carbonStorePath," +
          s" while spark.sql.warehouse.dir is $sparkWarehouse")
      }
    }
    createDatabaseCommand
  }

  private def createDataSourceTable(
      table: CatalogTable,
      ignoreIfExists: Boolean): CarbonCreateDataSourceTableCommand = {
    CarbonCreateDataSourceTableCommand(table, ignoreIfExists)
  }

  /**
   * create table stored as carbondata
   */
  def createHiveTable(
      createTableCommand: CreateTableCommand): CarbonCreateDataSourceTableCommand = {
    createDataSourceTable(
      createTableCommand.table.copy(provider = Option("carbondata")),
      createTableCommand.ignoreIfExists)
  }

  /**
   * create table using carbondata
   */
  def createDataSourceTable(
      createTable: org.apache.spark.sql.execution.datasources.CreateTable
  ): CarbonCreateDataSourceTableCommand = {
    createDataSourceTable(
      createTable.tableDesc,
      createTable.mode == SaveMode.Ignore)
  }

  /**
   * create table using carbondata
   */
  def createDataSourceTable(
      createTable: CreateDataSourceTableCommand): CarbonCreateDataSourceTableCommand = {
    createDataSourceTable(
      createTable.table,
      createTable.ignoreIfExists)
  }

  def createTableAsSelect(
      tableDesc: CatalogTable,
      query: LogicalPlan,
      mode: SaveMode,
      sparkSession: SparkSession): CarbonCreateTableAsSelectCommand = {
    val tableInfo: TableInfo =
      CarbonSparkSqlParserUtil
        .buildTableInfoFromCatalogTable(
          tableDesc,
          mode == SaveMode.Ignore,
          sparkSession,
          Some(query))
    CarbonCreateTableAsSelectCommand(
      tableInfo,
      query,
      mode == SaveMode.Ignore,
      tableDesc.storage.locationUri.map(CatalogUtils.URIToString))
  }

  /**
   * create table stored as carbondata as select from table
   */
  def createHiveTableAsSelect(
      ctas: CreateHiveTableAsSelectCommand,
      sparkSession: SparkSession): CarbonCreateTableAsSelectCommand = {
    createTableAsSelect(
      ctas.tableDesc,
      ctas.query,
      ctas.mode,
      sparkSession)
  }

  def createCarbonFileHiveTableAsSelect(
      ctas: CreateHiveTableAsSelectCommand): CreateDataSourceTableAsSelectCommand = {
    CreateDataSourceTableAsSelectCommand(
      ctas.tableDesc.copy(provider = Option("carbon")),
      ctas.mode,
      ctas.query,
      ctas.outputColumnNames
    )
  }

  /**
   * create table using carbondata as select from table
   */
  def createDataSourceTableAsSelect(
      table: CatalogTable,
      query: LogicalPlan,
      mode: SaveMode,
      sparkSession: SparkSession): CarbonCreateTableAsSelectCommand = {
    createTableAsSelect(
      table,
      query,
      mode,
      sparkSession
    )
  }

  def renameTable(renameTableCommand: AlterTableRenameCommand): CarbonAlterTableRenameCommand = {
    val dbOption = renameTableCommand.oldName.database.map(_.toLowerCase)
    val tableIdentifier =
      TableIdentifier(renameTableCommand.oldName.table.toLowerCase(), dbOption)
    val renameModel = AlterTableRenameModel(tableIdentifier, renameTableCommand.newName)
    CarbonAlterTableRenameCommand(renameModel)
  }

  def addColumns(
      addColumnsCommand: AlterTableAddColumnsCommand,
      sparkSession: SparkSession): CarbonAlterTableAddColumnCommand = {
    val table = addColumnsCommand.table
    val carbonTable = CarbonEnv.getCarbonTable(
      CarbonParserUtil.convertDbNameToLowerCase(table.database),
      table.table.toLowerCase)(sparkSession)
    if (carbonTable != null && carbonTable.isFileLevelFormat) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on Carbon external file format table")
    } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    } else {
      prepareAlterTableAddColsCommand(table.database, addColumnsCommand.colsToAdd,
        table.table.toLowerCase)
    }
  }

  def prepareAlterTableAddColsCommand(dbName: Option[String],
      colsToAdd: Seq[StructField],
      tableName: String): CarbonAlterTableAddColumnCommand = {
    val fields = new CarbonSpark2SqlParser().getFields(colsToAdd)
    val tableModel = CarbonParserUtil.prepareTableModel(ifNotExistPresent = false,
      CarbonParserUtil.convertDbNameToLowerCase(dbName),
      tableName,
      fields.map(CarbonParserUtil.convertFieldNamesToLowercase),
      Seq.empty,
      scala.collection.mutable.Map.empty[String, String],
      None,
      isAlterFlow = true)
    val alterTableAddColumnsModel = AlterTableAddColumnsModel(
      CarbonParserUtil.convertDbNameToLowerCase(dbName),
      tableName,
      Map.empty[String, String],
      tableModel.dimCols,
      tableModel.msrCols,
      tableModel.highCardinalityDims.getOrElse(Seq.empty))
    CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
  }

  def changeColumn(
      changeColumnCommand: AlterTableChangeColumnCommand,
      sparkSession: SparkSession): CarbonAlterTableColRenameDataTypeChangeCommand = {
    val tableName = changeColumnCommand.tableName
    val carbonTable = CarbonEnv.getCarbonTable(
      CarbonParserUtil.convertDbNameToLowerCase(tableName.database),
      tableName.table.toLowerCase)(sparkSession)
    if (carbonTable != null && carbonTable.isFileLevelFormat) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on Carbon external file format table")
    } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    } else {
      val columnName = changeColumnCommand.columnName
      val newColumn = changeColumnCommand.newColumn
      val newColumnMetaData = newColumn.metadata
      val isColumnRename = !columnName.equalsIgnoreCase(newColumn.name)
      var newColumnComment: Option[String] = Option.empty
      if (newColumnMetaData != null &&
          newColumnMetaData.contains(CarbonCommonConstants.COLUMN_COMMENT)) {
        newColumnComment =
          Some(newColumnMetaData.getString(CarbonCommonConstants.COLUMN_COMMENT))
      }

      prepareAlterTableColRenameDataTypeChangeCommand(newColumn,
        tableName.database.map(_.toLowerCase),
        tableName.table.toLowerCase,
        columnName.toLowerCase,
        isColumnRename,
        newColumnComment)
    }
  }

  def prepareAlterTableColRenameDataTypeChangeCommand(
      newColumn: StructField,
      dbName: Option[String],
      tableName: String,
      columnName: String,
      isColumnRename: Boolean,
      newColumnComment: Option[String]
  ): CarbonAlterTableColRenameDataTypeChangeCommand = {
    val values = newColumn.dataType match {
      case d: DecimalType => Some(List((d.precision, d.scale)))
      case _ => None
    }
    val dataTypeInfo = CarbonParserUtil.parseColumn(newColumn.name, newColumn.dataType, values)
    val alterTableColRenameAndDataTypeChangeModel =
      AlterTableDataTypeChangeModel(
        dataTypeInfo,
        dbName,
        tableName,
        columnName,
        newColumn.name.toLowerCase,
        isColumnRename,
        newColumnComment)

    CarbonAlterTableColRenameDataTypeChangeCommand(
      alterTableColRenameAndDataTypeChangeModel
    )
  }

  def describeTable(
      describeCommand: DescribeTableCommand,
      sparkSession: SparkSession): RunnableCommand = {
    val isFormatted: Boolean = if (sparkSession.version.startsWith("2.1")) {
      CarbonReflectionUtils.getDescribeTableFormattedField(describeCommand)
    } else {
      false
    }
    if (describeCommand.isExtended || isFormatted) {
      val resolvedTable =
        sparkSession.sessionState.executePlan(UnresolvedRelation(describeCommand.table)).analyzed
      CarbonDescribeFormattedCommand(
        sparkSession.sessionState.executePlan(resolvedTable).executedPlan,
        describeCommand.output,
        describeCommand.partitionSpec,
        describeCommand.table)
    } else {
      describeCommand
    }
  }

  def refreshTable(refreshTable: CarbonToSparkAdapter.RefreshTables): RefreshCarbonTableCommand = {
    RefreshCarbonTableCommand(
      refreshTable.tableIdent.database,
      refreshTable.tableIdent.table)
  }

  private def getTable(tableName: TableIdentifier, sparkSession: SparkSession): CarbonTable = {
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(tableName)(sparkSession).carbonTable
    if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    }
    carbonTable
  }

  def showPartitions(
      showPartitionsCommand: ShowPartitionsCommand,
      sparkSession: SparkSession): RunnableCommand = {
    val tableName = showPartitionsCommand.tableName
    val cols = showPartitionsCommand.spec
    val carbonTable = getTable(tableName, sparkSession)
    if (!carbonTable.isHivePartitionTable) {
      showPartitionsCommand
    } else {
      if (cols.isDefined) {
        ShowPartitionsCommand(tableName,
          Option(CarbonSparkSqlParserUtil.copyTablePartition(cols.get)))
      } else {
        ShowPartitionsCommand(tableName, None)
      }
    }
  }

  def addPartition(
      addPartitionCommand: AlterTableAddPartitionCommand
  ): CarbonAlterTableAddHivePartitionCommand = {
    CarbonAlterTableAddHivePartitionCommand(
      addPartitionCommand.tableName,
      addPartitionCommand.partitionSpecsAndLocs,
      addPartitionCommand.ifNotExists)
  }

  def dropPartition(
      dropPartitionCommand: AlterTableDropPartitionCommand,
      sparkSession: SparkSession): CarbonAlterTableDropHivePartitionCommand = {
    CarbonAlterTableDropHivePartitionCommand(
      dropPartitionCommand.tableName,
      dropPartitionCommand.specs,
      dropPartitionCommand.ifExists,
      dropPartitionCommand.purge,
      EnvHelper.isRetainData(sparkSession, dropPartitionCommand.retainData))
  }

  def setProperties(
      setProperties: AlterTableSetPropertiesCommand,
      sparkSession: SparkSession): CarbonAlterTableSetCommand = {
    val tableName = setProperties.tableName
    val carbonTable = getTable(tableName, sparkSession)

    // TODO remove this limitation later
    val properties = setProperties.properties
    val property = properties.find(_._1.equalsIgnoreCase("streaming"))
    if (property.isDefined) {
      if (carbonTable.getTablePath.startsWith("s3") && property.get._2.equalsIgnoreCase("s3")) {
        throw new UnsupportedOperationException("streaming is not supported with s3 store")
      }
      if (carbonTable.isStreamingSink) {
        throw new MalformedCarbonCommandException(
          "Streaming property can not be changed once it is 'true'")
      } else {
        if (!property.get._2.trim.equalsIgnoreCase("true")) {
          throw new MalformedCarbonCommandException(
            "Streaming property value is incorrect")
        }
        if (carbonTable.isMV) {
          throw new MalformedCarbonCommandException(
            "MV table does not support set streaming property")
        }
        if (MVManagerInSpark.get(sparkSession).hasSchemaOnTable(carbonTable)) {
          throw new MalformedCarbonCommandException(
            "The table which has materialized view does not support set streaming property")
        }
      }
    }
    CarbonAlterTableSetCommand(tableName, properties, setProperties.isView)
  }

  def unsetProperties(
      unsetPropertiesCommand: AlterTableUnsetPropertiesCommand): CarbonAlterTableUnsetCommand = {
    // TODO remove this limitation later
    if (unsetPropertiesCommand.propKeys.exists(_.equalsIgnoreCase("streaming"))) {
      throw new MalformedCarbonCommandException(
        "Streaming property can not be removed")
    }
    CarbonAlterTableUnsetCommand(
      unsetPropertiesCommand.tableName,
      unsetPropertiesCommand.propKeys,
      unsetPropertiesCommand.ifExists,
      unsetPropertiesCommand.isView)
  }

  def refreshResource(refreshResource: RefreshResource): Seq[SparkPlan] = {
    try {
      val plan = new CarbonSpark2SqlParser().parse(s"REFRESH ${ refreshResource.path }")
      ExecutedCommandExec(plan.asInstanceOf[RunnableCommand]) :: Nil
    } catch {
      case e: Exception =>
        LOGGER.error(e.getMessage)
        Nil
    }
  }

  def explain(explainCommand: ExplainCommand, sparkSession: SparkSession): Seq[SparkPlan] = {
    val isCommand = SparkSQLUtil.isCommand(explainCommand.logicalPlan)
    if (explainCommand.logicalPlan.isStreaming || isCommand) {
      Nil
    } else {
      val qe = sparkSession.sessionState.executePlan(explainCommand.logicalPlan)
      val hasCarbonTable = qe.analyzed collectFirst {
        case l: LogicalRelation if l.relation.isInstanceOf[CarbonDatasourceHadoopRelation] =>
          true
      }
      if (hasCarbonTable.isDefined) {
        ExecutedCommandExec(CarbonInternalExplainCommand(explainCommand)) :: Nil
      } else {
        Nil
      }
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // carbon file
  /////////////////////////////////////////////////////////////////////////////////
  def createCarbonFileHiveTable(
      createTableCommand: CreateTableCommand): CreateDataSourceTableCommand = {
    CreateDataSourceTableCommand(
      createTableCommand.table.copy(provider = Option("carbon")),
      createTableCommand.ignoreIfExists)
  }
}
