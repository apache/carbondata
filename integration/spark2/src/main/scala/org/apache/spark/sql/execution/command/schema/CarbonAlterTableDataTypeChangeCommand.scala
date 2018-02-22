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

package org.apache.spark.sql.execution.command.schema

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDataTypeChangeModel, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalog
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{AlterTableDataTypeChangePostEvent, AlterTableDataTypeChangePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.{ColumnSchema, SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, DataTypeConverterUtil}

private[sql] case class CarbonAlterTableDataTypeChangeCommand(
    alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableDataTypeChangeModel.tableName
    val dbName = alterTableDataTypeChangeModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    LOGGER.audit(s"Alter table change data type request has been received for $dbName.$tableName")
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    // get the latest carbon table and check for column existence
    var carbonTable: CarbonTable = null
    var timeStamp = 0L
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetastore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      val operationContext = new OperationContext
      val alterTableDataTypeChangeListener = AlterTableDataTypeChangePreEvent(sparkSession,
        carbonTable, alterTableDataTypeChangeModel)
      OperationListenerBus.getInstance()
        .fireEvent(alterTableDataTypeChangeListener, operationContext)
      val columnName = alterTableDataTypeChangeModel.columnName
      val carbonColumns = carbonTable.getCreateOrderColumn(tableName).asScala.filter(!_.isInvisible)
      if (!carbonColumns.exists(_.getColName.equalsIgnoreCase(columnName))) {
        LOGGER.audit(s"Alter table change data type request has failed. " +
                     s"Column $columnName does not exist")
        sys.error(s"Column does not exist: $columnName")
      }
      val carbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(columnName))
      if (carbonColumn.size == 1) {
        CarbonScalaUtil
          .validateColumnDataType(alterTableDataTypeChangeModel.dataTypeInfo, carbonColumn.head)
      } else {
        LOGGER.audit(s"Alter table change data type request has failed. " +
                     s"Column $columnName is invalid")
        sys.error(s"Invalid Column: $columnName")
      }
      // read the latest schema file
      val tableInfo: TableInfo = metastore.getThriftTableInfo(carbonTable)
      // maintain the added column for schema evolution history
      var addColumnSchema: ColumnSchema = null
      var deletedColumnSchema: ColumnSchema = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)
      columnSchemaList.foreach { columnSchema =>
        if (columnSchema.column_name.equalsIgnoreCase(columnName)) {
          deletedColumnSchema = columnSchema.deepCopy
          columnSchema.setData_type(DataTypeConverterUtil
            .convertToThriftDataType(alterTableDataTypeChangeModel.dataTypeInfo.dataType))
          columnSchema.setPrecision(alterTableDataTypeChangeModel.dataTypeInfo.precision)
          columnSchema.setScale(alterTableDataTypeChangeModel.dataTypeInfo.scale)
          addColumnSchema = columnSchema
        }
      }
      timeStamp = System.currentTimeMillis
      val schemaEvolutionEntry = new SchemaEvolutionEntry(timeStamp)
      schemaEvolutionEntry.setAdded(List(addColumnSchema).asJava)
      schemaEvolutionEntry.setRemoved(List(deletedColumnSchema).asJava)
      tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
        .setTime_stamp(System.currentTimeMillis)
      AlterTableUtil
        .updateSchemaInfo(carbonTable, schemaEvolutionEntry, tableInfo)(sparkSession,
          sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog])
      val alterTablePostExecutionEvent: AlterTableDataTypeChangePostEvent =
        new AlterTableDataTypeChangePostEvent(sparkSession, carbonTable,
          alterTableDataTypeChangeModel)
      OperationListenerBus.getInstance.fireEvent(alterTablePostExecutionEvent, operationContext)
      LOGGER.info(s"Alter table for data type change is successful for table $dbName.$tableName")
      LOGGER.audit(s"Alter table for data type change is successful for table $dbName.$tableName")
    } catch {
      case e: Exception => LOGGER
        .error("Alter table change datatype failed : " + e.getMessage)
        if (carbonTable != null) {
          AlterTableUtil.revertDataTypeChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        sys.error(s"Alter table data type change operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }
}
