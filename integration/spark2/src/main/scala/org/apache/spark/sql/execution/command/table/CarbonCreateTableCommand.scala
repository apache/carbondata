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

import org.apache.spark.sql.{CarbonEnv, GetDB, Row, SparkSession, _}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.SQLExecution.EXECUTION_ID_KEY
import org.apache.spark.sql.execution.command.{Field, MetadataCommand, TableModel, TableNewProcessor}
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.events.{CreateTablePostExecutionEvent, CreateTablePreExecutionEvent, OperationContext, OperationListenerBus}

case class CarbonCreateTableCommand(
    cm: TableModel,
    createDSTable: Boolean = true)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val storePath = CarbonProperties.getStorePath
    CarbonEnv.getInstance(sparkSession).carbonMetastore.
      checkSchemasModifiedTimeAndReloadTables()
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    cm.databaseName = GetDB.getDatabaseName(cm.databaseNameOp, sparkSession)
    val dbLocation = GetDB.getDatabaseLocation(cm.databaseName, sparkSession, storePath)
    val tablePath = dbLocation + CarbonCommonConstants.FILE_SEPARATOR + cm.tableName
    val tbName = cm.tableName
    val dbName = cm.databaseName
    LOGGER.audit(s"Creating Table with Database name [$dbName] and Table name [$tbName]")

    val tableInfo: TableInfo = TableNewProcessor(cm)

    // Add validation for sort scope when create table
    val sortScope = tableInfo.getFactTable.getTableProperties.asScala
      .getOrElse("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    if (!CarbonUtil.isValidSortOption(sortScope)) {
      throw new InvalidConfigurationException(
        s"Passing invalid SORT_SCOPE '$sortScope', valid SORT_SCOPE are 'NO_SORT', 'BATCH_SORT'," +
        s" 'LOCAL_SORT' and 'GLOBAL_SORT' ")
    }

    if (tableInfo.getFactTable.getListOfColumns.size <= 0) {
      CarbonException.analysisException("Table should have at least one column.")
    }

    if (sparkSession.sessionState.catalog.listTables(dbName)
      .exists(_.table.equalsIgnoreCase(tbName))) {
      if (!cm.ifNotExistsSet) {
        LOGGER.audit(
          s"Table creation with Database name [$dbName] and Table name [$tbName] failed. " +
          s"Table [$tbName] already exists under database [$dbName]")
        throw new TableAlreadyExistsException(dbName, dbName)
      }
    } else {
      val tableIdentifier = AbsoluteTableIdentifier.from(tablePath, dbName, tbName)
      val operationContext = new OperationContext
      val createTablePreExecutionEvent: CreateTablePreExecutionEvent =
        new CreateTablePreExecutionEvent(sparkSession,
          tableIdentifier.getCarbonTableIdentifier,
          tablePath)
      OperationListenerBus.getInstance.fireEvent(createTablePreExecutionEvent, operationContext)
      // Add Database to catalog and persist
      val catalog = CarbonEnv.getInstance(sparkSession).carbonMetastore
      val carbonSchemaString = catalog.generateTableSchemaString(tableInfo, tableIdentifier)
      if (createDSTable) {
        try {
          val fields = new Array[Field](cm.dimCols.size + cm.msrCols.size)
          cm.dimCols.foreach(f => fields(f.schemaOrdinal) = f)
          cm.msrCols.foreach(f => fields(f.schemaOrdinal) = f)

          sparkSession.sparkContext.setLocalProperty(EXECUTION_ID_KEY, null)
          sparkSession.sql(
            s"""CREATE TABLE $dbName.$tbName
               |(${ fields.map(f => f.rawSchema).mkString(",") })
               |USING org.apache.spark.sql.CarbonSource""".stripMargin +
            s""" OPTIONS (tableName "$tbName", dbName "$dbName", tablePath """.stripMargin +
            s""""$tablePath", path "$tablePath" $carbonSchemaString) """.stripMargin)
        } catch {
          case e: AnalysisException => throw e
          case e: Exception =>
            // call the drop table to delete the created table.
            CarbonEnv.getInstance(sparkSession).carbonMetastore
              .dropTable(tableIdentifier)(sparkSession)

            val msg = s"Create table'$tbName' in database '$dbName' failed."
            LOGGER.audit(msg)
            LOGGER.error(e, msg)
            CarbonException.analysisException(msg)
        }
      }
      val createTablePostExecutionEvent: CreateTablePostExecutionEvent =
        new CreateTablePostExecutionEvent(sparkSession, tableIdentifier.getCarbonTableIdentifier)
      OperationListenerBus.getInstance.fireEvent(createTablePostExecutionEvent, operationContext)
      LOGGER.audit(s"Table created with Database name [$dbName] and Table name [$tbName]")
    }
    Seq.empty
  }
}
