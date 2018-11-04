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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand

import org.apache.carbondata.api.CarbonStore.LOGGER
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.common.logging.impl.Audit
import org.apache.carbondata.core.metadata.schema.table.TableInfo

/**
 * Create table and insert the query result into it.
 *
 * @param tableInfo the Table Describe, which may contains serde, storage handler etc.
 * @param query the query whose result will be insert into the new relation
 * @param ifNotExistsSet allow continue working if it's already exists, otherwise
 *                      raise exception
 * @param tableLocation store location where the table need to be created
 */
case class CarbonCreateTableAsSelectCommand(
    tableInfo: TableInfo,
    query: LogicalPlan,
    ifNotExistsSet: Boolean = false,
    tableLocation: Option[String] = None) extends AtomicRunnableCommand {

  var loadCommand: CarbonInsertIntoCommand = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = tableInfo.getFactTable.getTableName
    var isTableCreated = false
    var databaseOpt: Option[String] = None
    if (tableInfo.getDatabaseName != null) {
      databaseOpt = Some(tableInfo.getDatabaseName)
    }
    val dbName = CarbonEnv.getDatabaseName(databaseOpt)(sparkSession)
    Audit.log(LOGGER, s"Request received for CTAS for $dbName.$tableName")
    // check if table already exists
    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      if (!ifNotExistsSet) {
        Audit.log(LOGGER,
          s"Table creation with Database name [$dbName] and Table name [$tableName] failed. " +
          s"Table [$tableName] already exists under database [$dbName]")
        throw new TableAlreadyExistsException(dbName, tableName)
      }
    } else {
      // execute command to create carbon table
      CarbonCreateTableCommand(tableInfo, ifNotExistsSet, tableLocation).run(sparkSession)
      isTableCreated = true
    }

    if (isTableCreated) {
      val tableName = tableInfo.getFactTable.getTableName
      var databaseOpt: Option[String] = None
      if (tableInfo.getDatabaseName != null) {
        databaseOpt = Some(tableInfo.getDatabaseName)
      }
      val dbName = CarbonEnv.getDatabaseName(databaseOpt)(sparkSession)
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
      val carbonDataSourceHadoopRelation = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .createCarbonDataSourceHadoopRelation(sparkSession,
          TableIdentifier(tableName, Option(dbName)))
      // execute command to load data into carbon table
      loadCommand = CarbonInsertIntoCommand(
        carbonDataSourceHadoopRelation,
        query,
        overwrite = false,
        partition = Map.empty)
      loadCommand.processMetadata(sparkSession)
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (null != loadCommand) {
      val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
      loadCommand.processData(sparkSession)
      val carbonTable = loadCommand.relation.carbonTable
      Audit.log(LOGGER, s"CTAS operation completed successfully for " +
                   s"${carbonTable.getDatabaseName}.${carbonTable.getTableName}")
    }
    Seq.empty
  }

  override def undoMetadata(sparkSession: SparkSession, exception: Exception): Seq[Row] = {
    val tableName = tableInfo.getFactTable.getTableName
    var databaseOpt: Option[String] = None
    if (tableInfo.getDatabaseName != null) {
      databaseOpt = Some(tableInfo.getDatabaseName)
    }
    val dbName = CarbonEnv.getDatabaseName(databaseOpt)(sparkSession)
    // drop the created table.
    CarbonDropTableCommand(
      ifExistsSet = false,
      Option(dbName), tableName).run(sparkSession)
    Seq.empty
  }
}
