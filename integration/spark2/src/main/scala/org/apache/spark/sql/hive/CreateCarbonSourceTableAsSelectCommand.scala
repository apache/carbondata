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

package org.apache.spark.sql.hive

import java.net.URI

import org.apache.spark.sql.{AnalysisException, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.{AlterTableRecoverPartitionsCommand, AtomicRunnableCommand}
import org.apache.spark.sql.execution.datasources.{DataSource, HadoopFsRelation}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.CarbonReflectionUtils

/**
 * Create table 'using carbondata' and insert the query result into it.
 *
 * @param table the Catalog Table
 * @param mode  SaveMode:Ignore,OverWrite,ErrorIfExists,Append
 * @param query the query whose result will be insert into the new relation
 *
 */

case class CreateCarbonSourceTableAsSelectCommand(
    table: CatalogTable,
    mode: SaveMode,
    query: LogicalPlan)
  extends AtomicRunnableCommand {

  override protected def innerChildren: Seq[LogicalPlan] = Seq(query)

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.provider.isDefined)

    val sessionState = sparkSession.sessionState
    val db = table.identifier.database.getOrElse(sessionState.catalog.getCurrentDatabase)
    val tableIdentWithDB = table.identifier.copy(database = Some(db))
    val tableName = tableIdentWithDB.unquotedString
    setAuditTable(db, table.identifier.table)

    if (sessionState.catalog.tableExists(tableIdentWithDB)) {
      assert(mode != SaveMode.Overwrite,
        s"Expect the table $tableName has been dropped when the save mode is Overwrite")

      if (mode == SaveMode.ErrorIfExists) {
        throw new AnalysisException(s"Table $tableName already exists. You need to drop it first.")
      }
      if (mode == SaveMode.Ignore) {
        // Since the table already exists and the save mode is Ignore, we will just return.
        return Seq.empty
      }

      saveDataIntoTable(
        sparkSession, table, table.storage.locationUri, query, SaveMode.Append, tableExists = true)
    } else {
      assert(table.schema.isEmpty)

      val tableLocation = if (table.tableType == CatalogTableType.MANAGED) {
        Some(sessionState.catalog.defaultTablePath(table.identifier))
      } else {
        table.storage.locationUri
      }
      val result = saveDataIntoTable(
        sparkSession, table, tableLocation, query, SaveMode.Overwrite, tableExists = false)

      result match {
        case fs: HadoopFsRelation if table.partitionColumnNames.nonEmpty &&
                                     sparkSession.sqlContext.conf.manageFilesourcePartitions =>
          // Need to recover partitions into the metastore so our saved data is visible.
          sessionState.executePlan(AlterTableRecoverPartitionsCommand(table.identifier)).toRdd
      }
    }

    Seq.empty[Row]
  }

  private def saveDataIntoTable(
      session: SparkSession,
      table: CatalogTable,
      tableLocation: Option[URI],
      data: LogicalPlan,
      mode: SaveMode,
      tableExists: Boolean): BaseRelation = {
    // Create the relation based on the input logical plan: `data`.
    val pathOption = tableLocation.map("path" -> CatalogUtils.URIToString(_))
    val dataSource = DataSource(
      session,
      className = table.provider.get,
      partitionColumns = table.partitionColumnNames,
      bucketSpec = table.bucketSpec,
      options = table.storage.properties ++ pathOption,
      catalogTable = if (tableExists) {
        Some(table)
      } else {
        None
      })

    try {
      val physicalPlan = session.sessionState.executePlan(data).executedPlan
      CarbonReflectionUtils.invokewriteAndReadMethod(dataSource,
        Dataset.ofRows(session, query),
        data,
        session,
        mode,
        query,
        physicalPlan)
    } catch {
      case ex: AnalysisException =>
        logError(s"Failed to write to table ${ table.identifier.unquotedString }", ex)
        throw ex
    }
  }

  override protected def opName: String = "CREATE TABLE AS SELECT"
}
