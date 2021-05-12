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

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.spark.sql.{AnalysisException, CarbonEnv, CarbonSource, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, DropTableCommand, MetadataCommand}
import org.apache.spark.sql.execution.datasources.PartitioningUtils

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
    if (sparkSession.sessionState.catalog.tableExists(table.identifier)) {
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
    // Since we are creating as Hive table, if while creating hive compatible way, if it fails,then
    // it will fall back to save its metadata in the Spark SQL specific way, so partition validation
    // fails when we try to store in hive compatible way, so in retry it might pass, so doing the
    // partition validation here only.
    // Refer: org.apache.spark.sql.hive.HiveExternalCatalog.scala#createDataSourceTable

    val caseSensitiveAnalysis = sparkSession.sessionState.conf.caseSensitiveAnalysis
    PartitioningUtils.validatePartitionColumn(catalogTable.schema,
      catalogTable.partitionColumnNames,
      caseSensitiveAnalysis)
    val rows = try {
      org.apache.spark.sql.execution.CreateDataSourceTableCommand
        .createDataSource(catalogTable, ignoreIfExists, sparkSession)
    } catch {
      case ex: TableAlreadyExistsException if ignoreIfExists =>
        LOGGER.error(ex)
        return Seq.empty[Row]
      case ex: AnalysisException if ignoreIfExists =>
        if (ex.getCause != null && ex.getCause.getCause != null &&
            ex.getCause.getCause.isInstanceOf[AlreadyExistsException]) {
          LOGGER.error(ex)
          return Seq.empty[Row]
        } else {
          throw ex
        }
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
