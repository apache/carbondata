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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.hive.metastore.api.InvalidOperationException
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.{AlterTableDataTypeChangeModel, DataTypeInfo, MetadataCommand}
import org.apache.spark.sql.hive.CarbonSessionCatalogUtil
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.features.TableOperation
import org.apache.carbondata.core.locks.{ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl
import org.apache.carbondata.core.metadata.datatype.{DataTypes, DecimalType}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, CarbonDimension}
import org.apache.carbondata.events.{AlterTableColRenameAndDataTypeChangePostEvent, AlterTableColRenameAndDataTypeChangePreEvent, OperationContext, OperationListenerBus}
import org.apache.carbondata.format.{ColumnSchema, DataType, SchemaEvolutionEntry, TableInfo}
import org.apache.carbondata.spark.util.{CommonUtil, DataTypeConverterUtil}

abstract class CarbonAlterTableColumnRenameCommand(oldColumnName: String, newColumnName: String)
  extends MetadataCommand {
  import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema

  protected def validColumnsForRenaming(columnSchemaList: mutable.Buffer[ColumnSchema],
      alteredColumnNamesMap: mutable.LinkedHashMap[String, String],
      carbonTable: CarbonTable): Unit = {
    // check whether new column name is already an existing column name
    alteredColumnNamesMap.foreach(keyVal => if (columnSchemaList.exists(_.getColumnName
      .equalsIgnoreCase(keyVal._2))) {
      throw new MalformedCarbonCommandException(s"Column Rename Operation failed. New " +
                                                s"column name ${ keyVal._2 } already exists" +
                                                s" in table ${ carbonTable.getTableName }")
    })

    // if column rename operation is on bucket column, then fail the rename operation
    if (null != carbonTable.getBucketingInfo) {
      val bucketColumns = carbonTable.getBucketingInfo.getListOfColumns
      bucketColumns.asScala.foreach {
        col =>
          if (col.getColumnName.equalsIgnoreCase(oldColumnName)) {
            throw new MalformedCarbonCommandException(
              s"Column Rename Operation failed. Renaming " +
              s"the bucket column $oldColumnName is not " +
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
  // stores mapping of altered column names: old-column-name -> new-column-name.
  // Including both parent/table and children columns
  val alteredColumnNamesMap = collection.mutable.LinkedHashMap.empty[String, String]
  // stores mapping of altered column data types: old-column-name -> new-column-datatype.
  // Including both parent/table and children columns
  val alteredDatatypesMap = collection.mutable.LinkedHashMap.empty[String, String]

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
      if (!carbonTable.isTransactionalTable) {
        throw new MalformedCarbonCommandException(
          "Unsupported operation on non transactional table")
      }
      if (!alterTableColRenameAndDataTypeChangeModel.isColumnRename &&
          !carbonTable.canAllow(carbonTable, TableOperation.ALTER_CHANGE_DATATYPE,
            alterTableColRenameAndDataTypeChangeModel.columnName)) {
        throw new MalformedCarbonCommandException(
          "alter table change datatype is not supported for index indexSchema")
      }
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename &&
          !carbonTable.canAllow(carbonTable, TableOperation.ALTER_COLUMN_RENAME,
            alterTableColRenameAndDataTypeChangeModel.columnName)) {
        throw new MalformedCarbonCommandException(
          "alter table column rename is not supported for index indexSchema")
      }
      // Do not allow spatial index column and its source columns to be changed.
      AlterTableUtil.validateColumnsWithSpatialIndexProperties(carbonTable,
        List(alterTableColRenameAndDataTypeChangeModel.columnName))
      val operationContext = new OperationContext
      operationContext.setProperty("childTableColumnRename", childTableColumnRename)
      val alterTableColRenameAndDataTypeChangePreEvent =
        AlterTableColRenameAndDataTypeChangePreEvent(sparkSession, carbonTable,
          alterTableColRenameAndDataTypeChangeModel)
      OperationListenerBus.getInstance()
        .fireEvent(alterTableColRenameAndDataTypeChangePreEvent, operationContext)
      val newColumnName = alterTableColRenameAndDataTypeChangeModel.newColumnName.toLowerCase
      val oldColumnName = alterTableColRenameAndDataTypeChangeModel.columnName.toLowerCase
      val isColumnRename = alterTableColRenameAndDataTypeChangeModel.isColumnRename
      if (isColumnRename) {
        alteredColumnNamesMap += (oldColumnName -> newColumnName)
      }
      val newColumnComment = alterTableColRenameAndDataTypeChangeModel.newColumnComment
      val carbonColumns = carbonTable.getCreateOrderColumn().asScala.filter(!_.isInvisible)
      if (!carbonColumns.exists(_.getColName.equalsIgnoreCase(oldColumnName))) {
        throwMetadataException(dbName, tableName, s"Column does not exist: $oldColumnName")
      }

      val oldCarbonColumn = carbonColumns.filter(_.getColName.equalsIgnoreCase(oldColumnName))
      if (oldCarbonColumn.size != 1) {
        throwMetadataException(dbName, tableName, s"Invalid Column: $oldColumnName")
      }
      val newColumnPrecision = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision
      val newColumnScale = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale
      // set isDataTypeChange flag
      val oldDatatype = oldCarbonColumn.head.getDataType
      val newDatatype = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType
      if (oldDatatype.getName.equalsIgnoreCase(newDatatype)) {
        val newColumnPrecision =
          alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision
        val newColumnScale = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale
        // if the source datatype is decimal and there is change in precision and scale, then
        // along with rename, datatype change is also required for the command, so set the
        // isDataTypeChange flag to true in this case
        if (DataTypes.isDecimal(oldDatatype) &&
            (oldDatatype.asInstanceOf[DecimalType].getPrecision !=
             newColumnPrecision ||
             oldDatatype.asInstanceOf[DecimalType].getScale !=
             newColumnScale)) {
          isDataTypeChange = true
        }
        if (oldDatatype.isComplexType) {
          val oldParent = oldCarbonColumn.head
          val oldChildren = oldParent.asInstanceOf[CarbonDimension].getListOfChildDimensions.asScala
            .toList
          AlterTableUtil.validateComplexStructure(oldChildren,
            alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.getChildren(),
            alteredColumnNamesMap, alteredDatatypesMap)
        }
      } else {
        if (oldDatatype.isComplexType) {
          throw new UnsupportedOperationException(
            "Old and new complex columns are not compatible in structure")
        }
        isDataTypeChange = true
      }

      // If there is no columnrename and datatype change and comment change
      // return directly without execution
      if (!isColumnRename && !isDataTypeChange && !newColumnComment.isDefined &&
          alteredColumnNamesMap.isEmpty && alteredDatatypesMap.isEmpty) {
        return Seq.empty
      }
      // if column datatype change operation is on partition column, then fail the
      // chang column operation
      if (null != carbonTable.getPartitionInfo) {
        val partitionColumns = carbonTable.getPartitionInfo.getColumnSchemaList
        partitionColumns.asScala.foreach {
          col =>
            if (col.getColumnName.equalsIgnoreCase(oldColumnName)) {
              throw new InvalidOperationException(
                s"Alter on partition column $newColumnName is not supported")
            }
        }
      }

      if (!alteredColumnNamesMap.isEmpty) {
        // validate the columns to be renamed
        validColumnsForRenaming(carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala,
          alteredColumnNamesMap, carbonTable)
      }

      if (isDataTypeChange) {
        // validate the columns to change datatype
        AlterTableUtil.validateColumnDataType(alterTableColRenameAndDataTypeChangeModel
          .dataTypeInfo,
          oldCarbonColumn.head)
      }
      // read the latest schema file
      val tableInfo: TableInfo =
        metaStore.getThriftTableInfo(carbonTable)
      // maintain the added column for schema evolution history
      var addedTableColumnSchema: ColumnSchema = null
      var deletedColumnSchema: ColumnSchema = null
      var schemaEvolutionEntry: SchemaEvolutionEntry = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)

      var addedColumnsList: List[ColumnSchema] = List.empty[ColumnSchema]
      var deletedColumnsList: List[ColumnSchema] = List.empty[ColumnSchema]

      /*
      * columnSchemaList is a flat structure containing all column schemas including both parent
      * and child.
      * It is iterated and rename/change-datatype update are made in this list itself.
      * Entry is made to the schemaEvolutionEntry for each of the update.
      */
      columnSchemaList.foreach { columnSchema =>
        val columnName = columnSchema.column_name
        val isTableColumnAltered = columnName.equalsIgnoreCase(oldColumnName)
        var isSchemaEntryRequired = false
        deletedColumnSchema = columnSchema.deepCopy()

        if (isTableColumnAltered) {
          // isColumnRename will be true if the table-column/parent-column name has been altered,
          // just get the columnSchema and rename, and make a schemaEvolutionEntry
          if (isColumnRename) {
            columnSchema.setColumn_name(newColumnName)
            isSchemaEntryRequired = true
          }

          // if the table column rename is false, it will be just table column datatype change
          // only, then change the datatype and make an evolution entry, If both the operations
          // are happening, then rename, change datatype and make an evolution entry
          if (isDataTypeChange) {
            // if only datatype change,  just get the column schema and change datatype, make a
            // schemaEvolutionEntry
            columnSchema.setData_type(
              DataTypeConverterUtil.convertToThriftDataType(
                alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.dataType))
            columnSchema
              .setPrecision(newColumnPrecision)
            columnSchema.setScale(newColumnScale)
            isSchemaEntryRequired = true
          }

          if (newColumnComment.isDefined && columnSchema.getColumnProperties != null) {
            columnSchema.getColumnProperties.put(
              CarbonCommonConstants.COLUMN_COMMENT, newColumnComment.get)
          } else if (newColumnComment.isDefined) {
            val newColumnProperties = new util.HashMap[String, String]
            newColumnProperties.put(CarbonCommonConstants.COLUMN_COMMENT, newColumnComment.get)
            columnSchema.setColumnProperties(newColumnProperties)
          }
          addedTableColumnSchema = columnSchema
        } else if (isComplexChild(columnSchema)) {
          // check if name is altered
          if (!alteredColumnNamesMap.isEmpty) {
            if (alteredColumnNamesMap.contains(columnName)) {
              // matches exactly
              val newComplexChildName = alteredColumnNamesMap(columnName)
              columnSchema.setColumn_name(newComplexChildName)
              isSchemaEntryRequired = true
            } else {
              val alteredParent = checkIfParentIsAltered(columnName)
              /*
               * Lets say, if complex schema is: str struct<a: int>
               * and if parent column is changed from str -> str2
               * then its child name should also be changed from str.a -> str2.a
               */
              if (alteredParent != null) {
                val newParent = alteredColumnNamesMap(alteredParent)
                val newComplexChildName = newParent + columnName
                  .split(alteredParent)(1)
                columnSchema.setColumn_name(newComplexChildName)
                isSchemaEntryRequired = true
              }
            }
          }
          // check if datatype is altered
          if (!alteredDatatypesMap.isEmpty && alteredDatatypesMap.get(columnName) != None) {
            val newDatatype = alteredDatatypesMap.get(columnName).get
            if (newDatatype.equals(CarbonCommonConstants.LONG)) {
              columnSchema.setData_type(DataType.LONG)
            } else if (newDatatype.contains(CarbonCommonConstants.DECIMAL)) {
              val (newPrecision, newScale) = CommonUtil.getScaleAndPrecision(newDatatype)
              columnSchema.setPrecision(newPrecision)
              columnSchema.setScale(newScale)
            }
            isSchemaEntryRequired = true
          }
        }

        // make a new schema evolution entry after column rename or datatype change
        if (isSchemaEntryRequired) {
          addedColumnsList ++= List(columnSchema)
          deletedColumnsList ++= List(deletedColumnSchema)
          schemaEvolutionEntry = AlterTableUtil.addNewSchemaEvolutionEntry(schemaEvolutionEntry,
            addedColumnsList,
            deletedColumnsList)
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
        addedTableColumnSchema,
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
          if (carbonTable.isTransactionalTable) {
            AlterTableUtil
              .revertColumnRenameAndDataTypeChanges(dbName, tableName, timeStamp)(sparkSession)
          }
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

  private def isComplexChild(columnSchema: ColumnSchema): Boolean = {
    columnSchema.column_name.contains(CarbonCommonConstants.POINT)
  }

  private def isChildOfTheGivenColumn(columnSchemaName: String, oldColumnName: String): Boolean = {
    columnSchemaName.startsWith(oldColumnName + CarbonCommonConstants.POINT)
  }

  private def checkIfParentIsAltered(columnSchemaName: String): String = {
    var parent: String = null
    alteredColumnNamesMap.foreach(keyVal => {
      if (isChildOfTheGivenColumn(columnSchemaName, keyVal._1)) {
        parent = keyVal._1
      }
    })
    parent
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
    val carbonColumns = carbonTable.getCreateOrderColumn().asScala
      .collect { case carbonColumn if !carbonColumn.isInvisible => carbonColumn.getColumnSchema }
    // get the schema ordinal of the column for which the dataType changed or column is renamed
    val schemaOrdinal = carbonColumns.indexOf(carbonColumns
      .filter { column => column.getColumnName.equalsIgnoreCase(oldCarbonColumn.getColName) }.head)
    // update the schema changed column at the specific index in carbonColumns based on schema order
    carbonColumns
      .update(schemaOrdinal, schemaConverter.fromExternalToWrapperColumnSchema(addColumnSchema))
    // When we call
    // alterExternalCatalogForTableWithUpdatedSchema to update the new schema to external catalog
    // in case of rename column or change datatype, spark gets the catalog table and then it itself
    // adds the partition columns if the table is partition table for all the new data schema sent
    // by carbon, so there will be duplicate partition columns, so send the columns without
    // partition columns
    val columns = if (carbonTable.isHivePartitionTable) {
      val partitionColumns = carbonTable.getPartitionInfo.getColumnSchemaList.asScala
      Some(carbonColumns.filterNot(col => partitionColumns.contains(col)))
    } else {
      Some(carbonColumns)
    }
    val tableIdentifier = AlterTableUtil.updateSchemaInfo(
      carbonTable, schemaEvolutionEntry, tableInfo)(sparkSession)
    CarbonSessionCatalogUtil.alterColumnChangeDataTypeOrRename(
      tableIdentifier, columns, sparkSession)
    sparkSession.catalog.refreshTable(tableIdentifier.quotedString)
  }

  override protected def opName: String = "ALTER TABLE CHANGE DATA TYPE OR RENAME COLUMN"
}
