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

import org.apache.spark.sql.{CarbonEnv, CarbonSource, Row, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, MetadataCommand}

/**
 * Command to create table in case of 'USING CARBONDATA' DDL
 *
 * @param catalogTable catalog table created by spark
 * @param ignoreIfExists ignore if table exists
 * @param sparkSession spark session
 */
case class CarbonCreateDataSourceTableCommand(
    catalogTable: CatalogTable,
    ignoreIfExists: Boolean,
    sparkSession: SparkSession)
  extends MetadataCommand {

  override def processMetadata(session: SparkSession): Seq[Row] = {
    // Run the spark command to create table in metastore before saving carbon schema
    // in table path.
    // This is required for spark 2.4, because spark 2.4 will fail to create table
    // if table path is created before hand
    val updatedCatalogTable =
      CarbonSource.updateCatalogTableWithCarbonSchema(catalogTable, session, None, false)
    val sparkCommand = CreateDataSourceTableCommand(updatedCatalogTable, ignoreIfExists)
    sparkCommand.run(session)

    // save the table info (carbondata's schema) in table path
    val tableName = catalogTable.identifier.table
    val dbName = CarbonEnv.getDatabaseName(catalogTable.identifier.database)(session)
    val tablePath = CarbonEnv.getTablePath(Some(dbName), tableName)(session)
    val tableInfo = CarbonSource.createTableInfo(
      dbName, tableName, tablePath, catalogTable.schema, catalogTable.properties, None, session)
    val metastore = CarbonEnv.getInstance(session).carbonMetaStore
    metastore.saveToDisk(tableInfo, tablePath)

    Seq.empty
  }

  override protected def opName: String = "CREATE TABLE USING CARBONDATA"

}
