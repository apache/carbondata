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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession, _}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.command.{Field, MetadataCommand, TableModel, TableNewProcessor}
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, OperationContext, OperationListenerBus}

case class CarbonCreateTableCommand(
    cm: TableModel,
    tableLocation: Option[String] = None,
    createDSTable: Boolean = true)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = cm.tableName
    val dbName = CarbonEnv.getDatabaseName(cm.databaseNameOp)(sparkSession)
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tableName]")

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tableName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tableName] failed. " +
          s"Table [$tableName] already exists under database [$dbName]")
        throw new TableAlreadyExistsException(dbName, tableName)
      }
    }

    val tablePath = tableLocation.getOrElse(
      CarbonEnv.getTablePath(cm.databaseNameOp, tableName)(sparkSession))
    val tableIdentifier = AbsoluteTableIdentifier.from(tablePath, dbName, tableName)
    val tableInfo: TableInfo = TableNewProcessor(cm, tableIdentifier)

    // Add validation for sort scope when create table
    val sortScopeOp = tableInfo.getFactTable.getTableProperties.asScala.get("sort_scope")
    if (sortScopeOp.isDefined && !CarbonUtil.isValidSortOption(sortScopeOp.get)) {
      throw new InvalidConfigurationException(
        s"Passing invalid SORT_SCOPE '${sortScopeOp.get}', valid SORT_SCOPE are 'NO_SORT', " +
        "'BATCH_SORT', 'LOCAL_SORT' and 'GLOBAL_SORT' ")
    }

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      CarbonException.analysisException("Table should have at least one column.")
    }

    val operationContext = new OperationContext
    val createTablePreExecutionEvent: CreateTablePreExecutionEvent =
      CreateTablePreExecutionEvent(sparkSession, tableIdentifier, Some(tableInfo))
    OperationListenerBus.getInstance.fireEvent(createTablePreExecutionEvent, operationContext)
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
    val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tableIdentifier)
    if (createDSTable) {
      try {
        val fields = new Array[Field](cm.dimCols.size + cm.msrCols.size)
        cm.dimCols.foreach(f => fields(f.schemaOrdinal) = f)
        cm.msrCols.foreach(f => fields(f.schemaOrdinal) = f)

        sparkSession.sparkContext.setLocalProperty(EXECUTION_ID_KEY, null)
        val tablePath = tableIdentifier.getTablePath
        sparkSession.sql(
          s"""CREATE TABLE $dbName.$tableName
             |(${ fields.map(f => f.rawSchema).mkString(",") })
             |USING org.apache.spark.sql.CarbonSource
             |OPTIONS (
             |  tableName "$tableName",
             |  dbName "$dbName",
             |  tablePath "$tablePath",
             |  path "$tablePath"
             |  $carbonSchemaString)
             """.stripMargin)
      } catch {
        case e: AnalysisException => throw e
        case e: Exception =>
          // call the drop table to delete the created table.
          CarbonEnv.getInstance(sparkSession).carbonMetastore
            .dropTable(tableIdentifier)(sparkSession)

          val msg = s"Create table'$tableName' in database '$dbName' failed."
          LOGGER.audit(msg)
          LOGGER.error(e, msg)
          CarbonException.analysisException(msg)
      }
    }
    val createTablePostExecutionEvent: CreateTablePostExecutionEvent =
      CreateTablePostExecutionEvent(sparkSession, tableIdentifier)
    OperationListenerBus.getInstance.fireEvent(createTablePostExecutionEvent, operationContext)
    LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tableName]")
    Seq.empty
  }
}
