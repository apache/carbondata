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

import org.apache.carbondata.common.logging.LogServiceFactory

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
    // Create the table metadata required by carbondata table, including TableInfo object which
    // will be persist in step3 and a new CatalogTable with updated information need by
    // CarbonSource. (Because spark does not pass the information carbon needs when calling create
    // relation API, so we need to update the CatalogTable to add those information, like table
    // name, database name, table path, etc)
    //
    // Step2:
    // Create a new CatalogTable containing an updated table properties.
    // We need to update the table properties since carbon needs to use extra information
    // when creating Relation in CarbonSource, but spark is not passing them, like table name,
    // database name, table path, etc.
    //
    // Step3:
    // Persist the TableInfo object in table path as schema file.

    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val (tableInfo, catalogTable) = CarbonSource.createTableMeta(sparkSession, table, metaStore)

    val rows = try {
      CreateDataSourceTableCommand(
        catalogTable,
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
      CarbonSource.saveCarbonSchemaFile(metaStore, ignoreIfExists, tableInfo)
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

}
