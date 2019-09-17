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

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDataTypeChangeModel, DataTypeInfo, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.apache.spark.util.{AlterTableUtil, SparkUtil}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.datatype.DecimalType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn
import org.apache.carbondata.events.{AlterTableColRenameAndDataTypeChangePostEvent, AlterTableColRenameAndDataTypeChangePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.{ColumnSchema, SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.util.DataTypeConverterUtil

abstract class CarbonAlterTableColumnRenameCommand(oldColumnName: String, newColumnName: String)
  extends MetadataCommand {

  protected def validColumnsForRenaming(carbonColumns: mutable.Buffer[CarbonColumn],
      oldCarbonColumn: CarbonColumn,
      carbonTable: CarbonTable): Unit = {
    // check whether new column name is already an existing column name
    if (carbonColumns.exists(_.getColName.equalsIgnoreCase(newColumnName))) {
      throw new MalformedCarbonCommandException(s"Column Rename Operation failed. New " +
                                                s"column name $newColumnName already exists" +
                                                s" in table ${ carbonTable.getTableName }")
    }

    // if the column rename is for complex column, block the operation
    if (oldCarbonColumn.isComplex) {
      throw new MalformedCarbonCommandException(s"Column Rename Operation failed. Rename " +
                                                s"column is unsupported for complex datatype " +
                                                s"column ${ oldCarbonColumn.getColName }")
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

  }
}

private[sql] case class CarbonAlterTableColRenameDataTypeChangeCommand(
    alterTableColRenameAndDataTypeChangeModel: AlterTableDataTypeChangeModel,
    childTableColumnRename: Boolean = false)
  extends CarbonAlterTableColumnRenameCommand(alterTableColRenameAndDataTypeChangeModel.columnName,
    alterTableColRenameAndDataTypeChangeModel.newColumnName) {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableColRenameAndDataTypeChangeModel.tableName
    val dbName = alterTableColRenameAndDataTypeChangeModel.databaseName
      .getOrElse(sparkSession.catalog.currentDatabase)
    var isDataTypeChange = false
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
      val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
      if (!alterTableColRenameAndDataTypeChangeModel.isColumnRename &&
          !carbonTable.canAllow(carbonTable, TableOperation.ALTER_CHANGE_DATATYPE,
            alterTableColRenameAndDataTypeChangeModel.columnName)) {
        throw new MalformedCarbonCommandException(
          "alter table change datatype is not supported for index datamap")
      }
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename &&
          !carbonTable.canAllow(carbonTable, TableOperation.ALTER_COLUMN_RENAME,
            alterTableColRenameAndDataTypeChangeModel.columnName)) {
        throw new MalformedCarbonCommandException(
          "alter table column rename is not supported for index datamap")
      }
      val operationContext = new OperationContext
      operationContext.setProperty("childTableColumnRename", childTableColumnRename)
      val alterTableColRenameAndDataTypeChangePreEvent =
        AlterTableColRenameAndDataTypeChangePreEvent(sparkSession, carbonTable,
          alterTableColRenameAndDataTypeChangeModel)
      OperationListenerBus.getInstance()
        .fireEvent(alterTableColRenameAndDataTypeChangePreEvent, operationContext)
      val newColumnName = alterTableColRenameAndDataTypeChangeModel.newColumnName.toLowerCase
      val oldColumnName = alterTableColRenameAndDataTypeChangeModel.columnName.toLowerCase
      val carbonColumns = carbonTable.getCreateOrderColumn(tableName).asScala.filter(!_.isInvisible)
      if (!carbonColumns.exists(_.getColName.equalsIgnoreCase(oldColumnName))) {
        throwMetadataException(dbName, tableName, s"Column does not exist: $oldColumnName")
      }

      val oldCarbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(oldColumnName))
      if (oldCarbonColumn.size != 1) {
        throwMetadataException(dbName, tableName, s"Invalid Column: $oldColumnName")
      }
      val newColumnPrecision = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision
      val newColumnScale = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename) {
        // validate the columns to be renamed
        validColumnsForRenaming(carbonColumns, oldCarbonColumn.head, carbonTable)
        // if the datatype is source datatype, then it is just a column rename operation, else set
        // the isDataTypeChange flag to true
        if (oldCarbonColumn.head.getDataType.getName
          .equalsIgnoreCase(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType)) {
          val newColumnPrecision =
            alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision
          val newColumnScale = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale
          // if the source datatype is decimal and there is change in precision and scale, then
          // along with rename, datatype change is also required for the command, so set the
          // isDataTypeChange flag to true in this case
          if (oldCarbonColumn.head.getDataType.getName.equalsIgnoreCase("decimal") &&
              (oldCarbonColumn.head.getDataType.asInstanceOf[DecimalType].getPrecision !=
               newColumnPrecision ||
               oldCarbonColumn.head.getDataType.asInstanceOf[DecimalType].getScale !=
               newColumnScale)) {
            isDataTypeChange = true
          }
        } else {
          isDataTypeChange = true
        }
      } else {
        isDataTypeChange = true
      }
      if (isDataTypeChange) {
        // if column datatype change operation is on partition column, then fail the datatype change
        // operation
        if (SparkUtil.isSparkVersionXandAbove("2.2") && null != carbonTable.getPartitionInfo) {
          val partitionColumns = carbonTable.getPartitionInfo.getColumnSchemaList
          partitionColumns.asScala.foreach {
            col =>
              if (col.getColumnName.equalsIgnoreCase(oldColumnName)) {
                throw new MalformedCarbonCommandException(
                  s"Alter datatype of the partition column $newColumnName is not allowed")
              }
          }
        }
        validateColumnDataType(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo,
          oldCarbonColumn.head)
      }
      // read the latest schema file
      val tableInfo: TableInfo =
        metaStore.getThriftTableInfo(carbonTable)
      // maintain the added column for schema evolution history
      var addColumnSchema: ColumnSchema = null
      var deletedColumnSchema: ColumnSchema = null
      val schemaEvolutionEntry: SchemaEvolutionEntry = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)

      columnSchemaList.foreach { columnSchema =>
        if (columnSchema.column_name.equalsIgnoreCase(oldColumnName)) {
          deletedColumnSchema = columnSchema.deepCopy()
          if (alterTableColRenameAndDataTypeChangeModel.isColumnRename) {
            // if only column rename, just get the column schema and rename, make a
            // schemaEvolutionEntry
            columnSchema.setColumn_name(newColumnName)
          }
          // if the column rename is false,it will be just datatype change only, then change the
          // datatype and make an evolution entry, If both the operations are happening, then rename
          // change datatype and make an evolution entry
          if (isDataTypeChange) {
            // if only datatype change,  just get the column schema and change datatype, make a
            // schemaEvolutionEntry
            columnSchema.setData_type(
              DataTypeConverterUtil.convertToThriftDataType(
                alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType))
            columnSchema
              .setPrecision(newColumnPrecision)
            columnSchema.setScale(newColumnScale)
          }
          addColumnSchema = columnSchema
          timeStamp = System.currentTimeMillis()
          // make a new schema evolution entry after column rename or datatype change
          AlterTableUtil
            .addNewSchemaEvolutionEntry(schemaEvolutionEntry, timeStamp, addColumnSchema,
              deletedColumnSchema)
        }
      }

      // modify the table Properties with new column name if column rename happened
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename) {
        AlterTableUtil
          .modifyTablePropertiesAfterColumnRename(tableInfo.fact_table.tableProperties.asScala,
            oldColumnName, newColumnName)
      }
      updateSchemaAndRefreshTable(sparkSession,
        carbonTable,
        tableInfo,
        addColumnSchema,
        schemaEvolutionEntry,
        oldCarbonColumn.head)
      val alterTableColRenameAndDataTypeChangePostEvent
      : AlterTableColRenameAndDataTypeChangePostEvent =
        AlterTableColRenameAndDataTypeChangePostEvent(sparkSession, carbonTable,
          alterTableColRenameAndDataTypeChangeModel)
      OperationListenerBus.getInstance
        .fireEvent(alterTableColRenameAndDataTypeChangePostEvent, operationContext)
      if (isDataTypeChange) {
        LOGGER
          .info(s"Alter table for column rename or data type change is successful for table " +
                s"$dbName.$tableName")
      }
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename) {
        LOGGER.info(s"Alter table for column rename is successful for table $dbName.$tableName")
      }
    } catch {
      case e: Exception =>
        if (carbonTable != null) {
          AlterTableUtil
            .revertColumnRenameAndDataTypeChanges(dbName, tableName, timeStamp)(sparkSession)
        }
        if (isDataTypeChange) {
          throwMetadataException(dbName, tableName,
            s"Alter table data type change operation failed: ${ e.getMessage }")
        } else {
          throwMetadataException(dbName, tableName,
            s"Alter table data type change or column rename operation failed: ${ e.getMessage }")
        }
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks)
    }
    Seq.empty
  }

  /**
   * This method update the schema info and refresh the table
   *
   * @param sparkSession
   * @param carbonTable              carbonTable
   * @param tableInfo                tableInfo
   * @param addColumnSchema          added column schema
   * @param schemaEvolutionEntry     new SchemaEvolutionEntry
   */
  private def updateSchemaAndRefreshTable(sparkSession: SparkSession,
      carbonTable: CarbonTable,
      tableInfo: TableInfo,
      addColumnSchema: ColumnSchema,
      schemaEvolutionEntry: SchemaEvolutionEntry,
      oldCarbonColumn: CarbonColumn): Unit = {
    val schemaConverter = new ThriftWrapperSchemaConverterImpl
    // get the carbon column in schema order
    val carbonColumns = carbonTable.getCreateOrderColumn(carbonTable.getTableName).asScala
      .collect { case carbonColumn if !carbonColumn.isInvisible => carbonColumn.getColumnSchema }
    // get the schema ordinal of the column for which the dataType changed or column is renamed
    val schemaOrdinal = carbonColumns.indexOf(carbonColumns
      .filter { column => column.getColumnName.equalsIgnoreCase(oldCarbonColumn.getColName) }.head)
    // update the schema changed column at the specific index in carbonColumns based on schema order
    carbonColumns
      .update(schemaOrdinal, schemaConverter.fromExternalToWrapperColumnSchema(addColumnSchema))
    // In case of spark2.2 and above and , when we call
    // alterExternalCatalogForTableWithUpdatedSchema to update the new schema to external catalog
    // in case of rename column or change datatype, spark gets the catalog table and then it itself
    // adds the partition columns if the table is partition table for all the new data schema sent
    // by carbon, so there will be duplicate partition columns, so send the columns without
    // partition columns
    val columns = if (SparkUtil.isSparkVersionXandAbove("2.2") &&
                      carbonTable.isHivePartitionTable) {
      val partitionColumns = carbonTable.getPartitionInfo.getColumnSchemaList.asScala
      val carbonColumnsWithoutPartition = carbonColumns.filterNot(col => partitionColumns.contains(
        col))
      Some(carbonColumnsWithoutPartition)
    } else {
      Some(carbonColumns)
    }
    val (tableIdentifier, schemaParts) = AlterTableUtil.updateSchemaInfo(
      carbonTable,
      schemaEvolutionEntry,
      tableInfo)(sparkSession)
    CarbonSessionCatalogUtil
      .alterColumnChangeDataTypeOrRename(tableIdentifier, schemaParts, columns, sparkSession)
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
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
        if (!dataTypeInfo.dataType.equalsIgnoreCase("bigint") &&
            !dataTypeInfo.dataType.equalsIgnoreCase("long")) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                    s"${ carbonColumn.getDataType.getName } cannot be modified. " +
                    s"Int can only be changed to bigInt or long")
        }
      case "DECIMAL" =>
        if (!dataTypeInfo.dataType.equalsIgnoreCase("decimal")) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type" +
                    s" ${ carbonColumn.getDataType.getName } cannot be modified." +
                    s" Decimal can be only be changed to Decimal of higher precision")
        }
        if (dataTypeInfo.precision <= carbonColumn.getColumnSchema.getPrecision) {
          sys.error(s"Given column ${ carbonColumn.getColName } cannot be modified. " +
                    s"Specified precision value ${ dataTypeInfo.precision } should be " +
                    s"greater than current precision value " +
                    s"${ carbonColumn.getColumnSchema.getPrecision }")
        } else if (dataTypeInfo.scale < carbonColumn.getColumnSchema.getScale) {
          sys.error(s"Given column ${ carbonColumn.getColName } cannot be modified. " +
                    s"Specified scale value ${ dataTypeInfo.scale } should be greater or " +
                    s"equal to current scale value ${ carbonColumn.getColumnSchema.getScale }")
        } else {
          // difference of precision and scale specified by user should not be less than the
          // difference of already existing precision and scale else it will result in data loss
          val carbonColumnPrecisionScaleDiff = carbonColumn.getColumnSchema.getPrecision -
                                               carbonColumn.getColumnSchema.getScale
          val dataInfoPrecisionScaleDiff = dataTypeInfo.precision - dataTypeInfo.scale
          if (dataInfoPrecisionScaleDiff < carbonColumnPrecisionScaleDiff) {
            sys.error(s"Given column ${ carbonColumn.getColName } cannot be modified. " +
                      s"Specified precision and scale values will lead to data loss")
          }
        }
      case _ =>
        sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                  s"${ carbonColumn.getDataType.getName } cannot be modified. " +
                  s"Only Int and Decimal data types are allowed for modification")
    }
  }

  override protected def opName: String = "ALTER TABLE CHANGE DATA TYPE OR RENAME COLUMN"
}
