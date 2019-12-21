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

import java.util
import java.util.UUID

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.MetadataCommand

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.{SchemaEvolution, SchemaEvolutionEntry}
import org.apache.carbondata.core.metadata.schema.table.{TableInfo, TableSchema}

/**
* The syntax of using this command in SQL is:
* {{{
*   CREATE TABLE [IF NOT EXISTS] [db_name.]table_name
*   LIKE [other_db_name.]existing_table_name
* }}}
*/
case class CarbonCreateTableLikeCommand(
    sourceTable: TableIdentifier,
    targetTable: TableIdentifier,
    ifNotExists: Boolean = false) extends MetadataCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val srcTable = CarbonEnv.getCarbonTable(sourceTable.database, sourceTable.table)(sparkSession)
    if (!srcTable.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (srcTable.isChildTableForMV) {
      throw new MalformedCarbonCommandException("Unsupported operation on child table or datamap")
    }

    // copy schema of source table and update fields to target table
    val dstTableSchema = srcTable.getTableInfo.getFactTable.clone().asInstanceOf[TableSchema]
    dstTableSchema.setTableName(targetTable.table)
    dstTableSchema.setTableId(UUID.randomUUID().toString)

    val schemaEvol: SchemaEvolution = new SchemaEvolution
    val schEntryList: util.List[SchemaEvolutionEntry] = new util.ArrayList[SchemaEvolutionEntry]
    schemaEvol.setSchemaEvolutionEntryList(schEntryList)
    dstTableSchema.setSchemaEvolution(schemaEvol)

    // build table info for creating table
    val dstTableInfo = new TableInfo
    val dstDB = targetTable.database.getOrElse(sparkSession.catalog.currentDatabase)
    dstTableInfo.setDatabaseName(dstDB)
    dstTableInfo.setLastUpdatedTime(System.currentTimeMillis())
    dstTableInfo.setFactTable(dstTableSchema)

    CarbonCreateTableCommand(dstTableInfo, ifNotExists).run(sparkSession)
  }

  override protected def opName: String = "CREATE TABLE LIKE"
}
