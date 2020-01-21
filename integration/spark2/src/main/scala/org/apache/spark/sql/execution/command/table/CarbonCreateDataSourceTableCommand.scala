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

package org.apache.spark.sql.execution.command.table

import org.apache.spark.sql.{AnalysisException, CarbonEnv, CarbonSource, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, DropTableCommand, MetadataCommand}
import org.apache.spark.sql.parser.CarbonSparkSqlParserUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.SchemaEvolutionEntry
import org.apache.carbondata.core.metadata.schema.table.TableInfo

/**
 * this command wrap schema generation and CreateDataSourceTableCommand
 * step 1: generate schema file
 * step 2: create table
 */
case class CarbonCreateDataSourceTableCommand(
    table: CatalogTable,
    ignoreIfExists: Boolean
) extends MetadataCommand {
  override protected def opName: String = "CREATE TABLE"

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val existingTables = sessionState.catalog.listTables(db)
    var tableExist = false
    existingTables.foreach { tid =>
      if (tid.table.equalsIgnoreCase(table.identifier.table)
          && tid.database.getOrElse("").equalsIgnoreCase(db)) {
        tableExist = true
      }
    }
    if (tableExist) {
      if (ignoreIfExists) {
        return Seq.empty[Row]
      } else {
        throw new AnalysisException(s"Table ${ table.identifier.unquotedString } already exists.")
      }
    }

    // Step1:
    // create carbon TableInfo object from the CatalogTable
    // TableInfo will be persisted in table path as schema file.
    //
    // Step2:
    // create a new CatalogTable containing an updated table properties.
    // We need to update the table properties since carbon needs to use extra information
    // when creating Relation in CarbonSource, but spark is not passing them, like table name,
    // database name, table path, etc.
    //
    // Step3:
    // Persist the TableInfo object in table path as schema file.

    val tableInfo = createTableInfo(sparkSession, table)
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore

    val rows = try {
      val updatedCatalogTable = CarbonSource.updateCatalogTableWithCarbonSchema(
        table, tableInfo, metaStore)
      CreateDataSourceTableCommand(
        updatedCatalogTable,
        ignoreIfExists
      ).run(sparkSession)
    } catch {
      case ex: TableAlreadyExistsException if ignoreIfExists =>
        LOGGER.error(ex)
        return Seq.empty[Row]
      case ex =>
        throw ex
    }

    try {
      if (!metaStore.isReadFromHiveMetaStore && tableInfo.isTransactionalTable) {
        CarbonSource.saveToDisk(metaStore, ignoreIfExists, tableInfo)
      }
    } catch {
      case ex: Throwable =>
        // drop the table if anything goes wrong
        LOGGER.error(s"save carbon table schema file failed for table " +
                     s"${table.database}.${table.identifier.table}, dropping the table")
        DropTableCommand(
          table.identifier,
          ifExists = true,
          isView = false,
          purge = false
        ).run(sparkSession)
        throw ex
    }
    rows
  }

  private def createTableInfo(sparkSession: SparkSession, table: CatalogTable): TableInfo = {
    val tableInfo = CarbonSparkSqlParserUtil.buildTableInfoFromCatalogTable(
      table,
      ifNotExists = true,
      sparkSession)
    val tableLocation = if (table.storage.locationUri.isDefined) {
      Some(table.storage.locationUri.get.toString)
    } else {
      None
    }
    val tablePath = CarbonEnv.createTablePath(
      Some(tableInfo.getDatabaseName),
      tableInfo.getFactTable.getTableName,
      tableInfo.getFactTable.getTableId,
      tableLocation,
      table.tableType == CatalogTableType.EXTERNAL,
      tableInfo.isTransactionalTable)(sparkSession)
    tableInfo.setTablePath(tablePath)
    CarbonSparkSqlParserUtil.validateTableProperties(tableInfo)
    val schemaEvolutionEntry = new SchemaEvolutionEntry
    schemaEvolutionEntry.setTimeStamp(tableInfo.getLastUpdatedTime)
    tableInfo.getFactTable.getSchemaEvolution.getSchemaEvolutionEntryList.add(schemaEvolutionEntry)
    tableInfo
  }
}
