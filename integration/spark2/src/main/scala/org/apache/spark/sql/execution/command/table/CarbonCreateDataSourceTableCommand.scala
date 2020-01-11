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

import org.apache.spark.sql.{AnalysisException, CarbonSource, Row, SparkSession}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.execution.command.{CreateDataSourceTableCommand, MetadataCommand}

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
    try {
      new CreateDataSourceTableCommand(
        CarbonSource.updateCatalogTableWithCarbonSchema(table, sparkSession, ignoreIfExists),
        ignoreIfExists
      ).run(sparkSession)
    } catch {
      case ex: TableAlreadyExistsException if ignoreIfExists =>
        LOGGER.error(ex)
        return Seq.empty[Row]
      case ex =>
        throw ex
    }
  }
}
