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
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableColRenameAndDataTypeChangeModel, DataTypeInfo, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalog
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.events.{AlterTableColRenameAndDataTypeChangePostEvent, AlterTableColRenameAndDataTypeChangePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.{ColumnSchema, SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.util.DataTypeConverterUtil

private[sql] case class CarbonAlterTableColRenameDataTypeChangeCommand(
    alterTableColRenameAndDataTypeChangeModel: AlterTableColRenameAndDataTypeChangeModel)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableColRenameAndDataTypeChangeModel.tableName
    val dbName = alterTableColRenameAndDataTypeChangeModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    var isColumnRenameOnly = false
    var isDataTypeChangeOnly = false
    var isBothColRenameAndDataTypeChange = false
    setAuditTable(dbName, tableName)
    setAuditInfo(Map(
      "column" -> alterTableColRenameAndDataTypeChangeModel.columnName,
      "newColumn" -> alterTableColRenameAndDataTypeChangeModel.newColumnName,
      "newType" -> alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType))
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    var locks = List.empty[ICarbonLock]
    // get the latest carbon table and check for column existence
    var carbonTable: CarbonTable = null
    var timeStamp = 0L
    try {
      locks = AlterTableUtil
        .validateTableAndAcquireLock(dbName, tableName, locksToBeAcquired)(sparkSession)
      val metastore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      if (!carbonTable.canAllow(carbonTable, TableOperation.ALTER_COL_RENAME_AND_CHANGE_DATATYPE,
        alterTableColRenameAndDataTypeChangeModel.columnName)) {
        throw new MalformedCarbonCommandException(
          "alter table change datatype or column rename is not supported for index datamap")
      }
      val operationContext = new OperationContext
      val alterTableColRenameAndDataTypeChangePreEvent =
        AlterTableColRenameAndDataTypeChangePreEvent(sparkSession, carbonTable,
          alterTableColRenameAndDataTypeChangeModel)
      OperationListenerBus.getInstance()
        .fireEvent(alterTableColRenameAndDataTypeChangePreEvent, operationContext)
      val newColumnName = alterTableColRenameAndDataTypeChangeModel.newColumnName
      val oldColumnName = alterTableColRenameAndDataTypeChangeModel.columnName
      val carbonColumns = carbonTable.getCreateOrderColumn(tableName).asScala.filter(!_.isInvisible)
      if (!carbonColumns.exists(_.getColName.equalsIgnoreCase(oldColumnName))) {
        throwMetadataException(dbName, tableName, s"Column does not exist: $oldColumnName")
      }

      val carbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(oldColumnName))
      if (carbonColumn.size != 1) {
        throwMetadataException(dbName, tableName, s"Invalid Column: $oldColumnName")
      }
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename) {
        // check whether new column name is already an existing column name
        if (carbonColumns.exists(_.getColName.equalsIgnoreCase(newColumnName))) {
          throw new MalformedCarbonCommandException(s"Column Rename Operation failed. New " +
                                                    s"column name $newColumnName already exists" +
                                                    s" in table $tableName")
        }

        // if the datatype is source datatype, then it is just a column rename operation, else do
        // the datatype validation for not source datatype
        if (carbonColumn.head.getDataType.getName
          .equalsIgnoreCase(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType)) {
          isColumnRenameOnly = true
        } else {
          isBothColRenameAndDataTypeChange = true
        }

        // if column rename operation is on partition column, then fail the rename operation
        if (null != carbonTable.getPartitionInfo) {
          val partitionColumns = carbonTable.getPartitionInfo.getColumnSchemaList
          partitionColumns.asScala.foreach {
            col =>
              if (col.getColumnName.equalsIgnoreCase(oldColumnName)) {
                throw new MalformedCarbonCommandException(
                  s"Column Rename Operation failed. Renaming " +
                  s"the partition column $newColumnName is not " +
                  s"allowed")
              }
          }
        }
      } else {
        isDataTypeChangeOnly = true
      }
      if (isBothColRenameAndDataTypeChange || isDataTypeChangeOnly) {
        validateColumnDataType(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo,
          carbonColumn.head)
      }
      // read the latest schema file
      val tableInfo: TableInfo =
        metastore.getThriftTableInfo(carbonTable)
      // maintain the added column for schema evolution history
      var addColumnSchema: ColumnSchema = null
      var deletedColumnSchema: ColumnSchema = null
      val schemaEvolutionEntryList: scala.collection.mutable.ArrayBuffer[SchemaEvolutionEntry] =
        ArrayBuffer()
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)

      columnSchemaList.foreach { columnSchema =>
        if (isColumnRenameOnly || isDataTypeChangeOnly) {
          if (columnSchema.column_name.equalsIgnoreCase(oldColumnName)) {
            deletedColumnSchema = columnSchema.deepCopy()
            if (isColumnRenameOnly) {
              // if only column rename, just get the column schema and rename, make a
              // schemaEvolutionEntry
              columnSchema.setColumn_name(newColumnName)
            } else if (isDataTypeChangeOnly) {
              // if only datatype change,  just get the column schema and change datatype, make a
              // schemaEvolutionEntry
              columnSchema.setData_type(
                DataTypeConverterUtil.convertToThriftDataType(
                  alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType))
              columnSchema
                .setPrecision(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision)
              columnSchema.setScale(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale)
            }
            addColumnSchema = columnSchema
            timeStamp = System.currentTimeMillis()
            // make a new schema evolution entry after column rename or datatype change
            addNewSchemaEvolutionEntry(schemaEvolutionEntryList, timeStamp, addColumnSchema,
              deletedColumnSchema)
          }
        } else {
          // if both column rename and datatype change, then maintain added and delete column schema
          // for both the operations, change the column schema separately and then make different
          // schemaEvolutionEntry for both the operation
          if (columnSchema.column_name.equalsIgnoreCase(oldColumnName)) {
            // make a copy of column schema before rename
            deletedColumnSchema = columnSchema.deepCopy()
            columnSchema.setColumn_name(newColumnName)
            // added column schema after column rename
            addColumnSchema = columnSchema
            // get the timestamp and add this same timestamp in evolution entry for both column
            // rename and datatype change, which will help in easy reverting during failure
            timeStamp = System.currentTimeMillis()
            // make a new schema evolution entry after column rename or datatype change
            addNewSchemaEvolutionEntry(schemaEvolutionEntryList, timeStamp, addColumnSchema,
              deletedColumnSchema)

            // make a copy of column schema before datatype change
            deletedColumnSchema = columnSchema.deepCopy()
            columnSchema.setData_type(
              DataTypeConverterUtil.convertToThriftDataType(
                alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType))
            columnSchema
              .setPrecision(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision)
            columnSchema.setScale(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale)
            // added column schema after datatype change
            addColumnSchema = columnSchema
            // make a new schema evolution entry after datatype change
            addNewSchemaEvolutionEntry(schemaEvolutionEntryList, timeStamp, addColumnSchema,
              deletedColumnSchema)
          }
        }
      }

      // modify the table Properties with new column name if column rename happened
      if (isColumnRenameOnly || isBothColRenameAndDataTypeChange) {
        modifyTablePropertiesAfterColumnRename(tableInfo.fact_table.tableProperties.asScala,
          oldColumnName, newColumnName)
      }
      updateSchemaAndRefreshTable(sparkSession,
        carbonTable,
        tableInfo,
        addColumnSchema,
        schemaEvolutionEntryList.toList)
      val alterTableColRenameAndDataTypeChangePostEvent
      : AlterTableColRenameAndDataTypeChangePostEvent =
        AlterTableColRenameAndDataTypeChangePostEvent(sparkSession, carbonTable,
          alterTableColRenameAndDataTypeChangeModel)
      OperationListenerBus.getInstance
        .fireEvent(alterTableColRenameAndDataTypeChangePostEvent, operationContext)
      if (isDataTypeChangeOnly) {
        LOGGER.info(s"Alter table for data type change is successful for table $dbName.$tableName")
      } else if (isColumnRenameOnly) {
        LOGGER.info(s"Alter table for column rename is successful for table $dbName.$tableName")
      } else {
        LOGGER
          .info(s"Alter table for column rename and data type change is successful for table " +
                s"$dbName.$tableName")
      }
    } catch {
      case e: Exception =>
        if (carbonTable != null) {
          AlterTableUtil
            .revertColumnRenameAndDataTypeChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        throwMetadataException(dbName, tableName,
          s"Alter table data type change or column rename operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }

  /**
   * This method create a new SchemaEvolutionEntry and adds to SchemaEvolutionEntry List
   *
   * @param schemaEvolutionEntryList List to add new SchemaEvolutionEntry
   * @param addColumnSchema          added new column schema
   * @param deletedColumnSchema      old column schema which is deleted
   * @return
   */
  private def addNewSchemaEvolutionEntry(
      schemaEvolutionEntryList: mutable.ArrayBuffer[SchemaEvolutionEntry],
      timeStamp: Long,
      addColumnSchema: ColumnSchema,
      deletedColumnSchema: ColumnSchema): mutable.ArrayBuffer[SchemaEvolutionEntry] = {
    val schemaEvolutionEntry = new SchemaEvolutionEntry(timeStamp)
    schemaEvolutionEntry.setAdded(List(addColumnSchema).asJava)
    schemaEvolutionEntry.setRemoved(List(deletedColumnSchema).asJava)
    schemaEvolutionEntryList += schemaEvolutionEntry
  }

  /**
   * This method update the schema info and refresh the table
   *
   * @param sparkSession
   * @param carbonTable          carbonTable
   * @param tableInfo            tableInfo
   * @param addColumnSchema      added column schema
   * @param schemaEvolutionEntry new SchemaEvolutionEntry
   */
  private def updateSchemaAndRefreshTable(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      tableInfo: TableInfo,
      addColumnSchema: ColumnSchema,
      schemaEvolutionEntry: List[SchemaEvolutionEntry]): Unit = {
    tableInfo.getFact_table.getSchema_evolution.getSchema_evolution_history.get(0)
      .setTime_stamp(System.currentTimeMillis)
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    val a = List(schemaConverter.fromExternalToWrapperColumnSchema(addColumnSchema))
    val (tableIdentifier, schemaParts, cols) = AlterTableUtil.updateSchemaInfo(
      carbonTable, schemaEvolutionEntry, tableInfo)(sparkSession)
    sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog]
      .alterColumnChangeDataType(tableIdentifier, schemaParts, cols)
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
  }

  /**
   * This method modifies the table properties if column rename happened
   *
   * @param tableProperties
   */
  def modifyTablePropertiesAfterColumnRename(
      tableProperties: mutable.Map[String, String],
      oldColumnName: String,
      newColumnName: String): Unit = {
    tableProperties.foreach { tableProperty =>
      if (tableProperty._2.contains(oldColumnName)) {
        val tablePropertyKey = tableProperty._1
        val tablePropertyValue = tableProperty._2
        tableProperties
          .put(tablePropertyKey, tablePropertyValue.replace(oldColumnName, newColumnName))
      }
    }
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
