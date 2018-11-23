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
import org.apache.spark.sql.execution.command.{AlterTableDataTypeChangeModel, DataTypeInfo, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalog
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.events.{AlterTableDataTypeChangePostEvent, AlterTableDataTypeChangePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.SchemaEvolutionEntry
import org.apache.carbondata.spark.util.DataTypeConverterUtil

private[sql] case class CarbonAlterTableDataTypeChangeCommand(
    alterTableDataTypeChangeModel: AlterTableDataTypeChangeModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableDataTypeChangeModel.tableName
    val dbName = alterTableDataTypeChangeModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    setAuditTable(dbName, tableName)
    setAuditInfo(Map(
      "column" -> alterTableDataTypeChangeModel.columnName,
      "newType" -> alterTableDataTypeChangeModel.dataTypeInfo.dataType))
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
      if (!carbonTable.canAllow(carbonTable, TableOperation.ALTER_CHANGE_DATATYPE,
        alterTableDataTypeChangeModel.columnName)) {
        throw new MalformedCarbonCommandException(
          "alter table change datatype is not supported for index datamap")
      }
      val operationContext = new OperationContext
      val alterTableDataTypeChangeListener = AlterTableDataTypeChangePreEvent(sparkSession,
        carbonTable, alterTableDataTypeChangeModel)
      OperationListenerBus.getInstance()
        .fireEvent(alterTableDataTypeChangeListener, operationContext)
      val columnName = alterTableDataTypeChangeModel.columnName
      val carbonColumns = carbonTable.getCreateOrderColumn(tableName).asScala.filter(!_.isInvisible)
      if (!carbonColumns.exists(_.getColName.equalsIgnoreCase(columnName))) {
        throwMetadataException(dbName, tableName, s"Column does not exist: $columnName")
      }
      val carbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(columnName))
      if (carbonColumn.size == 1) {
        validateColumnDataType(alterTableDataTypeChangeModel.dataTypeInfo, carbonColumn.head)
      } else {
        throwMetadataException(dbName, tableName, s"Invalid Column: $columnName")
      }
      // read the latest schema file
      val tableInfo: org.apache.carbondata.format.TableInfo =
        metastore.getThriftTableInfo(carbonTable)
      // maintain the added column for schema evolution history
      var addColumnSchema: org.apache.carbondata.format.ColumnSchema = null
      var deletedColumnSchema: org.apache.carbondata.format.ColumnSchema = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)
      columnSchemaList.foreach { columnSchema =>
        if (columnSchema.column_name.equalsIgnoreCase(columnName)) {
          deletedColumnSchema = columnSchema.deepCopy
          columnSchema.setData_type(
            DataTypeConverterUtil.convertToThriftDataType(
              alterTableDataTypeChangeModel.dataTypeInfo.dataType))
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
      val schemaConverter = new ThriftWrapperSchemaConverterImpl
      val a = List(schemaConverter.fromExternalToWrapperColumnSchema(addColumnSchema))
      val (tableIdentifier, schemaParts, cols) = AlterTableUtil.updateSchemaInfo(
        carbonTable, schemaEvolutionEntry, tableInfo, Some(a))(sparkSession)
      sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog]
        .alterColumnChangeDataType(tableIdentifier, schemaParts, cols)
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
      val alterTablePostExecutionEvent: AlterTableDataTypeChangePostEvent =
        new AlterTableDataTypeChangePostEvent(sparkSession, carbonTable,
          alterTableDataTypeChangeModel)
      OperationListenerBus.getInstance.fireEvent(alterTablePostExecutionEvent, operationContext)
      LOGGER.info(s"Alter table for data type change is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        if (carbonTable != null) {
          AlterTableUtil.revertDataTypeChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        throwMetadataException(dbName, tableName,
          s"Alter table data type change operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }

  /**
   * This method will validate a column for its data type and check whether the column data type
   * can be modified and update if conditions are met.
   */
  private def validateColumnDataType(
      dataTypeInfo: DataTypeInfo,
      carbonColumn: CarbonColumn): Unit = {
    carbonColumn.getDataType.getName match {
      case "INT" =>
        if (!dataTypeInfo.dataType.equals("bigint") && !dataTypeInfo.dataType.equals("long")) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                    s"${carbonColumn.getDataType.getName} cannot be modified. " +
                    s"Int can only be changed to bigInt or long")
        }
      case "DECIMAL" =>
        if (!dataTypeInfo.dataType.equals("decimal")) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type" +
                    s" ${ carbonColumn.getDataType.getName} cannot be modified." +
                    s" Decimal can be only be changed to Decimal of higher precision")
        }
        if (dataTypeInfo.precision <= carbonColumn.getColumnSchema.getPrecision) {
          sys.error(s"Given column ${carbonColumn.getColName} cannot be modified. " +
                    s"Specified precision value ${dataTypeInfo.precision} should be " +
                    s"greater than current precision value " +
                    s"${carbonColumn.getColumnSchema.getPrecision}")
        } else if (dataTypeInfo.scale < carbonColumn.getColumnSchema.getScale) {
          sys.error(s"Given column ${carbonColumn.getColName} cannot be modified. " +
                    s"Specified scale value ${dataTypeInfo.scale} should be greater or " +
                    s"equal to current scale value ${carbonColumn.getColumnSchema.getScale}")
        } else {
          // difference of precision and scale specified by user should not be less than the
          // difference of already existing precision and scale else it will result in data loss
          val carbonColumnPrecisionScaleDiff = carbonColumn.getColumnSchema.getPrecision -
                                               carbonColumn.getColumnSchema.getScale
          val dataInfoPrecisionScaleDiff = dataTypeInfo.precision - dataTypeInfo.scale
          if (dataInfoPrecisionScaleDiff < carbonColumnPrecisionScaleDiff) {
            sys.error(s"Given column ${carbonColumn.getColName} cannot be modified. " +
                      s"Specified precision and scale values will lead to data loss")
          }
        }
      case _ =>
        sys.error(s"Given column ${carbonColumn.getColName} with data type " +
                  s"${carbonColumn.getDataType.getName} cannot be modified. " +
                  s"Only Int and Decimal data types are allowed for modification")
    }
  }

  override protected def opName: String = "ALTER TABLE CHANGE DATA TYPE"
}
