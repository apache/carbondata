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
import org.apache.spark.sql.catalyst.{CarbonParserUtil, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, UnresolvedRelation}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, Union}
import org.apache.spark.sql.execution.command.{AlterTableAddPartitionCommand, AlterTableChangeColumnCommand, AlterTableDropPartitionCommand, AlterTableUnsetPropertiesCommand, DescribeTableCommand, ShowPartitionsCommand, _}
import org.apache.spark.sql.execution.command.table._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.management.RefreshCarbonTableCommand
import org.apache.spark.sql.execution.command.partition.{CarbonAlterTableAddHivePartitionCommand, CarbonAlterTableDropHivePartitionCommand}
import org.apache.spark.sql.execution.command.schema.{CarbonAlterTableAddColumnCommand, CarbonAlterTableColRenameDataTypeChangeCommand, CarbonAlterTableRenameCommand, CarbonAlterTableSetCommand, CarbonAlterTableUnsetCommand}
import org.apache.spark.sql.execution.datasources.{LogicalRelation, RefreshResource, RefreshTable}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.hive.execution.CreateHiveTableAsSelectCommand
import org.apache.spark.sql.parser.{CarbonSpark2SqlParser, CarbonSparkSqlParserUtil}
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.util.{CarbonReflectionUtils, FileUtils}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, ThreadLocalSessionInfo}
import org.apache.carbondata.spark.util.DataTypeConverterUtil

object DDLHelper {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def createDatabase(
      createDatabaseCommand: CreateDatabaseCommand,
      sparkSession: SparkSession
  ): CreateDatabaseCommand = {
    ThreadLocalSessionInfo
      .setConfigurationToCurrentThread(sparkSession.sessionState.newHadoopConf())
    if (!EnvHelper.isCloud(sparkSession)) {
      val databaseName = createDatabaseCommand.databaseName
      val dbLocation = try {
        CarbonEnv.getDatabaseLocation(databaseName, sparkSession)
      } catch {
        case _: NoSuchDatabaseException =>
          CarbonProperties.getStorePath
      }
      FileUtils
        .createDatabaseDirectory(databaseName, dbLocation, sparkSession.sparkContext)
    }
    createDatabaseCommand
  }

  private def createDataSourceTable(
      table: CatalogTable,
      ignoreIfExists: Boolean,
      sparkSession: SparkSession
  ): CarbonCreateDataSourceTableCommand = {
    CarbonCreateDataSourceTableCommand(table, ignoreIfExists)
  }

  /**
   * create table stored as carbondata
   */
  def createHiveTable(
      createTableCommand: CreateTableCommand,
      sparkSession: SparkSession
  ): CarbonCreateDataSourceTableCommand = {
    createDataSourceTable(
      createTableCommand.table.copy(provider = Option("carbondata")),
      createTableCommand.ignoreIfExists,
      sparkSession
    )
  }

  /**
   * create table using carbondata
   */
  def createDataSourceTable(
      createTable: org.apache.spark.sql.execution.datasources.CreateTable,
      sparkSession: SparkSession
  ): CarbonCreateDataSourceTableCommand = {
    createDataSourceTable(
      createTable.tableDesc,
      createTable.mode == SaveMode.Ignore,
      sparkSession
    )
  }

  /**
   * create table using carbondata
   */
  def createDataSourceTable(
      createTable: CreateDataSourceTableCommand,
      sparkSession: SparkSession
  ): CarbonCreateDataSourceTableCommand = {
    createDataSourceTable(
      createTable.table,
      createTable.ignoreIfExists,
      sparkSession
    )
  }

  def createTableAsSelect(
      tableDesc: CatalogTable,
      query: LogicalPlan,
      mode: SaveMode,
      sparkSession: SparkSession
  ): CarbonCreateTableAsSelectCommand = {
    val tableInfo: TableInfo =
      CarbonSparkSqlParserUtil
        .buildTableInfoFromCatalogTable(
          tableDesc,
          mode == SaveMode.Ignore,
          sparkSession,
          Some(query)
        )
    CarbonCreateTableAsSelectCommand(
      tableInfo,
      query,
      mode == SaveMode.Ignore,
      tableDesc.storage.locationUri.map(CatalogUtils.URIToString)
    )
  }

  /**
   * create table stored as carbondata as select from table
   */
  def createHiveTableAsSelect(
      ctas: CreateHiveTableAsSelectCommand,
      sparkSession: SparkSession
  ): CarbonCreateTableAsSelectCommand = {
    createTableAsSelect(
      ctas.tableDesc,
      ctas.query,
      ctas.mode,
      sparkSession
    )
  }

  def createCarbonFileHiveTableAsSelect(
      ctas: CreateHiveTableAsSelectCommand,
      sparkSession: SparkSession
  ): CreateDataSourceTableAsSelectCommand = {
    CreateDataSourceTableAsSelectCommand(
      ctas.tableDesc.copy(
        provider = Option("carbon")
      ),
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
      sparkSession: SparkSession
  ) : CarbonCreateTableAsSelectCommand = {
    createTableAsSelect(
      table,
      query,
      mode,
      sparkSession
    )
  }

  def renameTable(
      renameTableCommand: AlterTableRenameCommand
  ): CarbonAlterTableRenameCommand = {
    val dbOption = renameTableCommand.oldName.database.map(_.toLowerCase)
    val tableIdentifier =
      TableIdentifier(renameTableCommand.oldName.table.toLowerCase(), dbOption)
    val renameModel = AlterTableRenameModel(tableIdentifier, renameTableCommand.newName)
    CarbonAlterTableRenameCommand(renameModel)
  }

  def addColumns(
      addColumnsCommand: AlterTableAddColumnsCommand,
      sparkSession: SparkSession
  ): CarbonAlterTableAddColumnCommand = {
    val table = addColumnsCommand.table
    val carbonTable = CarbonEnv.getCarbonTable(
      CarbonParserUtil.convertDbNameToLowerCase(table.database),
      table.table.toLowerCase)(sparkSession)
    if (carbonTable != null && carbonTable.isFileLevelFormat) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on Carbon external fileformat table")
    } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    } else {
      val fields = new CarbonSpark2SqlParser().getFields(addColumnsCommand.colsToAdd)
      val tableModel = CarbonParserUtil.prepareTableModel(false,
        CarbonParserUtil.convertDbNameToLowerCase(table.database),
        table.table.toLowerCase,
        fields.map(CarbonParserUtil.convertFieldNamesToLowercase),
        Seq.empty,
        scala.collection.mutable.Map.empty[String, String],
        None,
        true)
      val alterTableAddColumnsModel = AlterTableAddColumnsModel(
        CarbonParserUtil.convertDbNameToLowerCase(table.database),
        table.table.toLowerCase,
        Map.empty[String, String],
        tableModel.dimCols,
        tableModel.msrCols,
        tableModel.highcardinalitydims.getOrElse(Seq.empty))
      CarbonAlterTableAddColumnCommand(alterTableAddColumnsModel)
    }
  }

  def changeColumn(
      changeColumnCommand: AlterTableChangeColumnCommand,
      sparkSession: SparkSession
  ): CarbonAlterTableColRenameDataTypeChangeCommand = {
    val tableName = changeColumnCommand.tableName
    val carbonTable = CarbonEnv.getCarbonTable(
      CarbonParserUtil.convertDbNameToLowerCase(tableName.database),
      tableName.table.toLowerCase)(sparkSession)
    if (carbonTable != null && carbonTable.isFileLevelFormat) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on Carbon external fileformat table")
    } else if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    } else {
      val columnName = changeColumnCommand.columnName
      val newColumn = changeColumnCommand.newColumn
      var isColumnRename = false
      // If both the column name are not same, then its a call for column rename
      if (!columnName.equalsIgnoreCase(newColumn.name)) {
        isColumnRename = true
      }
      val values = newColumn.dataType match {
        case d: DecimalType => Some(List((d.precision, d.scale)))
        case _ => None
      }
      val dataTypeInfo = CarbonParserUtil.parseDataType(
        DataTypeConverterUtil
          .convertToCarbonType(newColumn.dataType.typeName)
          .getName
          .toLowerCase,
        values,
        isColumnRename)
      val alterTableColRenameAndDataTypeChangeModel =
        AlterTableDataTypeChangeModel(
          dataTypeInfo,
          tableName.database.map(_.toLowerCase),
          tableName.table.toLowerCase,
          columnName.toLowerCase,
          newColumn.name.toLowerCase,
          isColumnRename)

      CarbonAlterTableColRenameDataTypeChangeCommand(
        alterTableColRenameAndDataTypeChangeModel
      )
    }
  }

  def describeTable(
      describeCommand: DescribeTableCommand,
      sparkSession: SparkSession
  ): RunnableCommand = {
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

  def refreshTable(
      refreshTable: RefreshTable
  ): RefreshCarbonTableCommand = {
    RefreshCarbonTableCommand(
      refreshTable.tableIdent.database,
      refreshTable.tableIdent.table)
  }

  def showPartitions(
      showPartitionsCommand: ShowPartitionsCommand,
      sparkSession: SparkSession
  ): RunnableCommand = {
    val tableName = showPartitionsCommand.tableName
    val cols = showPartitionsCommand.spec
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(tableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
    if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    }
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
      sparkSession: SparkSession
  ): CarbonAlterTableDropHivePartitionCommand = {
    CarbonAlterTableDropHivePartitionCommand(
      dropPartitionCommand.tableName,
      dropPartitionCommand.specs,
      dropPartitionCommand.ifExists,
      dropPartitionCommand.purge,
      EnvHelper.isRetainData(sparkSession, dropPartitionCommand.retainData))
  }

  def setProperties(
      setProperties: AlterTableSetPropertiesCommand,
      sparkSession: SparkSession
  ): CarbonAlterTableSetCommand = {
    val tableName = setProperties.tableName
    val carbonTable = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(tableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
    if (carbonTable != null && !carbonTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException(
        "Unsupported operation on non transactional table")
    }

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
        if (carbonTable.hasMVCreated) {
          throw new MalformedCarbonCommandException(
            "The table which has materialized view does not support set streaming property")
        }
        if (carbonTable.isChildTableForMV) {
          throw new MalformedCarbonCommandException(
            "Datamap table does not support set streaming property")
        }
      }
    }
    CarbonAlterTableSetCommand(tableName, properties, setProperties.isView)
  }

  def unsetProperties(
      unsetPropertiesCommand: AlterTableUnsetPropertiesCommand
  ): CarbonAlterTableUnsetCommand = {
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

  def refreshResource(
      refreshResource: RefreshResource
  ): Seq[SparkPlan] = {
    try {
      val plan = new CarbonSpark2SqlParser().parse(s"REFRESH ${ refreshResource.path }")
      ExecutedCommandExec(plan.asInstanceOf[RunnableCommand]) :: Nil
    } catch {
      case e: Exception =>
        LOGGER.error(e.getMessage)
        Nil
    }
  }

  def explain(
      explainCommand: ExplainCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    val isCommand = explainCommand.logicalPlan match {
      case _: Command => true
      case Union(childern) if childern.forall(_.isInstanceOf[Command]) => true
      case _ => false
    }
    if (explainCommand.logicalPlan.isStreaming || isCommand) {
      Nil
    } else {
      val qe = sparkSession.sessionState.executePlan(explainCommand.logicalPlan)
      val hasCarbonTable = qe.analyzed collectFirst {
        case l: LogicalRelation if (l.relation.isInstanceOf[CarbonDatasourceHadoopRelation]) =>
          true
      }
      if (hasCarbonTable.isDefined) {
        ExecutedCommandExec(CarbonInternalExplainCommand(explainCommand)) :: Nil
      } else {
        Nil
      }
    }
  }

  def showTables(
      showTablesCommand: ShowTablesCommand,
      sparkSession: SparkSession
  ): Seq[SparkPlan] = {
    if (CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_SHOW_DATAMAPS,
        CarbonCommonConstants.CARBON_SHOW_DATAMAPS_DEFAULT).toBoolean) {
      Nil
    } else {
      ExecutedCommandExec(CarbonShowTablesCommand(
        showTablesCommand.databaseName,
        showTablesCommand.tableIdentifierPattern
      )) :: Nil
    }
  }

  //////////////////////////////////////////////////////////////////////////////////
  // carbon file
  /////////////////////////////////////////////////////////////////////////////////
  def createCarbonFileHiveTable(
      createTableCommand: CreateTableCommand,
      sparkSession: SparkSession
  ): CreateDataSourceTableCommand = {
    CreateDataSourceTableCommand(
      createTableCommand.table.copy(
        provider = Option("carbon")
      ),
      createTableCommand.ignoreIfExists)
  }
}
