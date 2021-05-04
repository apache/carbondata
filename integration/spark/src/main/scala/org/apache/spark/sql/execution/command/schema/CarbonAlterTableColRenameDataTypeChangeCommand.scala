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
  // stores mapping of altered children column names:
  // new_column_name -> column_datatype
  val alteredColumnNamesMap = collection.mutable.HashMap.empty[String, String]

  // stores mapping of: column_name -> new_column_datatype
  val alteredColumnDatatypesMap = collection.mutable.HashMap.empty[String, String]

  /**
   * This method checks the structure of the old and new complex columns, and-
   * 1. throws exception if the number of complex-levels in both columns does not match
   * 2. throws exception if the number of children of both columns does not match
   * 3. creates alteredColumnNamesMap: new_column_name -> datatype. Here new_column_name are those
   *    names of the columns that are altered.
   * 4. creates alteredColumnDatatypesMap: column_name -> new_datatype.
   * These maps will later be used while altering the table schema
   */
  def validateComplexStructure(dimension: List[CarbonDimension],
      newDataTypeInfo: List[DataTypeInfo]): Unit = {
    if (dimension == null && newDataTypeInfo == null) {
      throw new UnsupportedOperationException(
        "both old and new dimensions are null")
    } else if (dimension == null || newDataTypeInfo == null) {
      throw new UnsupportedOperationException(
        "because either the old or the new dimension is null")
    } else if (dimension.size != newDataTypeInfo.size) {
      throw new UnsupportedOperationException(
        "because number of children of old and new complex columns are not the same")
    } else {
      for (i <- 0 to newDataTypeInfo.size - 1) {
        val old_column_name = dimension(i).getColName.split('.').last
        val old_column_datatype = dimension(i).getDataType.getName
        val new_column_name = newDataTypeInfo(i).name.split('.').last
        val new_column_datatype = newDataTypeInfo(i).dataType
        if (!old_column_datatype.equalsIgnoreCase(new_column_datatype)) {
          /**
           * datatypes of complex children cannot be altered. So throwing exception for now.
           * TODO: use alteredColumnDatatypesMap to update the carbon schema
           */
          alteredColumnDatatypesMap += (dimension(i).getColName -> new_column_datatype)
          throw new UnsupportedOperationException(
            "because datatypes of complex children cannot be altered")
        }
        if (!old_column_name.equalsIgnoreCase(new_column_name)) {
          alteredColumnNamesMap += (dimension(i).getColName -> newDataTypeInfo(i).name)
        }
        if (old_column_datatype.equalsIgnoreCase(CarbonCommonConstants.MAP) ||
            new_column_datatype.equalsIgnoreCase(CarbonCommonConstants.MAP)) {
          throw new UnsupportedOperationException(
            "cannot alter complex structure that includes map type column")
        } else if (new_column_datatype.equalsIgnoreCase(CarbonCommonConstants.ARRAY) ||
                   old_column_datatype.equalsIgnoreCase(CarbonCommonConstants.ARRAY) ||
                   new_column_datatype.equalsIgnoreCase(CarbonCommonConstants.STRUCT) ||
            old_column_datatype.equalsIgnoreCase(CarbonCommonConstants.STRUCT)) {
          if (!new_column_datatype.equalsIgnoreCase(old_column_datatype)) {
            throw new UnsupportedOperationException(
              "because old and new complex columns are not compatible in structure")
          }
          validateComplexStructure(dimension(i).getListOfChildDimensions.asScala.toList,
            newDataTypeInfo(i).getChildren())
        }
      }
    }
  }

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
      if (isColumnRename && (DataTypes.isMapType(oldDatatype) ||
                             newDatatype.equalsIgnoreCase(CarbonCommonConstants.MAP))) {
        throw new UnsupportedOperationException("cannot alter map type column")
      }
      if (oldDatatype.getName.equalsIgnoreCase(newDatatype)) {
        val newColumnPrecision =
          alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.precision
        val newColumnScale = alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.scale
        // if the source datatype is decimal and there is change in precision and scale, then
        // along with rename, datatype change is also required for the command, so set the
        // isDataTypeChange flag to true in this case
        if (oldDatatype.getName.equalsIgnoreCase(CarbonCommonConstants.DECIMAL) &&
            (oldDatatype.asInstanceOf[DecimalType].getPrecision !=
             newColumnPrecision ||
             oldDatatype.asInstanceOf[DecimalType].getScale !=
             newColumnScale)) {
          isDataTypeChange = true
        }
        if (DataTypes.isArrayType(oldDatatype) ||
            DataTypes.isStructType(oldDatatype)) {
          val oldParent = oldCarbonColumn.head
          val oldChildren = oldParent
            .asInstanceOf[CarbonDimension]
            .getListOfChildDimensions
            .asScala
            .toList
          validateComplexStructure(oldChildren,
            alterTableColRenameAndDataTypeChangeModel.dataTypeInfo.getChildren())
        }
      } else {
        if (oldDatatype.isComplexType ||
            newDatatype.equalsIgnoreCase(CarbonCommonConstants.ARRAY) ||
            newDatatype.equalsIgnoreCase(CarbonCommonConstants.STRUCT)) {
          throw new UnsupportedOperationException(
            "because old and new complex columns are not compatible in structure")
        }
        isDataTypeChange = true
      }

      // If there is no columnrename and datatype change and comment change
      // return directly without execution
      if (!isColumnRename && !isDataTypeChange && !newColumnComment.isDefined &&
          alteredColumnNamesMap.size == 0) {
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
      if (alterTableColRenameAndDataTypeChangeModel.isColumnRename) {
        // validate the columns to be renamed
        validColumnsForRenaming(carbonColumns, oldCarbonColumn.head, carbonTable)
      }
      if (isDataTypeChange) {
        // validate the columns to change datatype
        validateColumnDataType(alterTableColRenameAndDataTypeChangeModel.dataTypeInfo,
          oldCarbonColumn.head)
      }
      // read the latest schema file
      val tableInfo: TableInfo =
        metaStore.getThriftTableInfo(carbonTable)
      // maintain the added column for schema evolution history
      var addColumnSchema: ColumnSchema = null
      var deletedColumnSchema: ColumnSchema = null
      var schemaEvolutionEntry: SchemaEvolutionEntry = null
      val columnSchemaList = tableInfo.fact_table.table_columns.asScala.filter(!_.isInvisible)
      // to validate duplicate children columns
      var UniqueColumnSet: mutable.Set[String] = mutable.Set()


      columnSchemaList.foreach { columnSchema =>
        val columnSchemaName = columnSchema.column_name
        // column to be renamed is a parent/table column or complex child column
        if (columnSchemaName.equalsIgnoreCase(oldColumnName) ||
            isChildOfOldColumn(columnSchemaName, oldColumnName)) {
          deletedColumnSchema = columnSchema.deepCopy()
          // if the table column name has been altered
          if (isColumnRename) {
            // if only table column rename, just get the column schema and rename, make a
            // schemaEvolutionEntry
            if (isChildOfOldColumn(columnSchemaName, oldColumnName)) {
              val newComplexChildName = newColumnName +
                                        columnSchemaName.substring(oldColumnName.length)
              columnSchema.setColumn_name(newComplexChildName)
            } else {
              columnSchema.setColumn_name(newColumnName)
            }
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
          }
          // only table columns are eligible to have comment
          if (!isComplexChild(columnSchema)) {
            if (newColumnComment.isDefined && columnSchema.getColumnProperties != null) {
              columnSchema.getColumnProperties.put(
                CarbonCommonConstants.COLUMN_COMMENT, newColumnComment.get)
            } else if (newColumnComment.isDefined) {
              val newColumnProperties = new util.HashMap[String, String]
              newColumnProperties.put(CarbonCommonConstants.COLUMN_COMMENT, newColumnComment.get)
              columnSchema.setColumnProperties(newColumnProperties)
            }
            addColumnSchema = columnSchema
            timeStamp = System.currentTimeMillis()
            // make a new schema evolution entry after column rename or datatype change
            schemaEvolutionEntry = AlterTableUtil
              .addNewSchemaEvolutionEntry(timeStamp, addColumnSchema, deletedColumnSchema)
          }
        }

        if (alteredColumnNamesMap.nonEmpty) {
          // if complex-child or its children has been renamed
          if (alteredColumnNamesMap.contains(columnSchemaName)) {
            // matches exactly
            val newComplexChildName = alteredColumnNamesMap(columnSchemaName)
            columnSchema.setColumn_name(newComplexChildName)
          } else {
            val oldParent = checkIfParentIsAltered(columnSchemaName)
            if(oldParent!= null) {
              val newParent = alteredColumnNamesMap(oldParent)
              val newComplexChildName = newParent + columnSchemaName
                .split(oldParent)(1)
              columnSchema.setColumn_name(newComplexChildName)
            }
          }
        }
        if (alteredColumnDatatypesMap.contains(columnSchemaName)) {
          deletedColumnSchema = columnSchema.deepCopy()
          columnSchema.setData_type(
            DataTypeConverterUtil.convertToThriftDataType(
              alteredColumnDatatypesMap(columnSchemaName)))
        }
        // validate duplicate child columns
        if (UniqueColumnSet.contains(columnSchema.getColumn_name)) {
          throw new UnsupportedOperationException(
            "because duplicate columns are present")
        }
        UniqueColumnSet += columnSchema.getColumn_name
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

  private def isComplexChild(columnSchema: ColumnSchema): Boolean = {
    columnSchema.column_name.contains(CarbonCommonConstants.POINT)
  }

  private def isChildOfOldColumn(columnSchemaName: String, oldColumnName: String): Boolean = {
    columnSchemaName.startsWith(oldColumnName + CarbonCommonConstants.POINT)
  }

  private def checkIfParentIsAltered(columnSchemaName: String): String = {
    for ((oldComplexChildName, newComplexChildName) <- alteredColumnNamesMap) {
      if (isChildOfOldColumn(columnSchemaName, oldComplexChildName)) {
        return oldComplexChildName
      }
    }
    null
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

  /**
   * This method will validate a column for its data type and check whether the column data type
   * can be modified and update if conditions are met.
   */
  private def validateColumnDataType(
      dataTypeInfo: DataTypeInfo,
      carbonColumn: CarbonColumn): Unit = {
    carbonColumn.getDataType.getName.toLowerCase() match {
      case CarbonCommonConstants.INT =>
        if (!dataTypeInfo.dataType.equalsIgnoreCase(CarbonCommonConstants.BIGINT) &&
            !dataTypeInfo.dataType.equalsIgnoreCase(CarbonCommonConstants.LONG)) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                    s"${ carbonColumn.getDataType.getName } cannot be modified. " +
                    s"Int can only be changed to bigInt or long")
        }
      case CarbonCommonConstants.DECIMAL =>
        if (!dataTypeInfo.dataType.equalsIgnoreCase(CarbonCommonConstants.DECIMAL)) {
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
      case CarbonCommonConstants.ARRAY =>
        validateColumnDataType(dataTypeInfo.getChildren()(0),
          carbonColumn.asInstanceOf[CarbonDimension].getListOfChildDimensions.asScala.toList(0))
      case CarbonCommonConstants.STRUCT =>
        for (i <- 0 to dataTypeInfo.getChildren().size - 1) {
          validateColumnDataType(dataTypeInfo.getChildren()(i),
            carbonColumn.asInstanceOf[CarbonDimension].getListOfChildDimensions.asScala.toList(i))
        }
        // if incoming type is dimension or string type
      case _ =>
        if (!carbonColumn.getDataType.getName.equalsIgnoreCase(dataTypeInfo.dataType)) {
          sys.error(s"Given column ${ carbonColumn.getColName } with data type " +
                    s"${ carbonColumn.getDataType.getName } cannot be modified. " +
                    s"Only Int and Decimal data types are allowed for modification")
        }
    }
  }

  override protected def opName: String = "ALTER TABLE CHANGE DATA TYPE OR RENAME COLUMN"
}
