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

package org.apache.spark.sql.execution.command

import java.util
import java.util.UUID

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.CarbonTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.{BucketingInfo, SchemaEvolution, SchemaEvolutionEntry}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.service.CarbonCommonFactory
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.load.FailureCauses
import org.apache.carbondata.spark.merger.CompactionType
import org.apache.carbondata.spark.util.{DataTypeConverterUtil, GlobalDictionaryUtil}

case class TableModel(
    ifNotExistsSet: Boolean,
    var databaseName: String,
    databaseNameOp: Option[String],
    tableName: String,
    tableProperties: Map[String, String],
    dimCols: Seq[Field],
    msrCols: Seq[Field],
    highcardinalitydims: Option[Seq[String]],
    noInvertedIdxCols: Option[Seq[String]],
    columnGroups: Seq[String],
    colProps: Option[util.Map[String,
    util.List[ColumnProperty]]] = None,
    bucketFields: Option[BucketFields])

case class Field(column: String, var dataType: Option[String], name: Option[String],
    children: Option[List[Field]], parent: String = null,
    storeType: Option[String] = Some("columnar"),
    var schemaOrdinal: Int = -1,
    var precision: Int = 0, var scale: Int = 0, var rawSchema: String = "")

case class ColumnProperty(key: String, value: String)

case class ComplexField(complexType: String, primitiveField: Option[Field],
    complexField: Option[ComplexField])

case class Partitioner(partitionClass: String, partitionColumn: Array[String], partitionCount: Int,
    nodeList: Array[String])

case class PartitionerField(partitionColumn: String, dataType: Option[String],
    columnComment: String)

case class BucketFields(bucketColumns: Seq[String], numberOfBuckets: Int)

case class DataLoadTableFileMapping(table: String, loadPath: String)

case class ExecutionErrors(var failureCauses: FailureCauses, var errorMsg: String )

case class CarbonMergerMapping(storeLocation: String,
    hdfsStoreLocation: String,
    metadataFilePath: String,
    var mergedLoadName: String,
    tableCreationTime: Long,
    databaseName: String,
    factTableName: String,
    validSegments: Array[String],
    tableId: String,
    campactionType: CompactionType,
    // maxSegmentColCardinality is Cardinality of last segment of compaction
    var maxSegmentColCardinality: Array[Int],
    // maxSegmentColumnSchemaList is list of column schema of last segment of compaction
    var maxSegmentColumnSchemaList: List[ColumnSchema])

case class NodeInfo(TaskId: String, noOfBlocks: Int)

case class AlterTableModel(dbName: Option[String],
                           tableName: String,
                           segmentUpdateStatusManager: Option[SegmentUpdateStatusManager],
                           compactionType: String,
                           factTimeStamp: Option[Long],
                           alterSql: String)

case class UpdateTableModel(isUpdate: Boolean,
                            updatedTimeStamp: Long,
                            var executorErrors: ExecutionErrors)

case class CompactionModel(compactionSize: Long,
    compactionType: CompactionType,
    carbonTable: CarbonTable,
    tableCreationTime: Long,
    isDDLTrigger: Boolean)

case class CompactionCallableModel(storePath: String,
    carbonLoadModel: CarbonLoadModel,
    storeLocation: String,
    carbonTable: CarbonTable,
    cubeCreationTime: Long,
    loadsToMerge: util.List[LoadMetadataDetails],
    sqlContext: SQLContext,
    compactionType: CompactionType)

case class DataTypeInfo(dataType: String, precision: Int = 0, scale: Int = 0)

case class AlterTableDataTypeChangeModel(dataTypeInfo: DataTypeInfo,
    databaseName: Option[String],
    tableName: String,
    columnName: String,
    newColumnName: String)

case class AlterTableRenameModel(
    oldTableIdentifier: TableIdentifier,
    newTableIdentifier: TableIdentifier
)

case class AlterTableAddColumnsModel(
    databaseName: Option[String],
    tableName: String,
    tableProperties: Map[String, String],
    dimCols: Seq[Field],
    msrCols: Seq[Field],
    highCardinalityDims: Seq[String])

case class AlterTableDropColumnModel(databaseName: Option[String],
    tableName: String,
    columns: List[String])

class AlterTableProcessor(
    alterTableModel: AlterTableAddColumnsModel,
    dbName: String,
    tableInfo: TableInfo,
    carbonTablePath: CarbonTablePath,
    tableIdentifier: CarbonTableIdentifier,
    storePath: String) {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def process: Seq[ColumnSchema] = {
    val tableSchema = tableInfo.getFactTable
    val tableCols = tableSchema.getListOfColumns.asScala
    val existingColsSize = tableCols.size
    var allColumns = tableCols.filter(x => x.isDimensionColumn)
    var newCols = Seq[ColumnSchema]()

    alterTableModel.dimCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      encoders.add(Encoding.DICTIONARY)
      val columnSchema: ColumnSchema = getColumnSchema(
        DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        isCol = true,
        encoders,
        isDimensionCol = true,
        -1,
        field.precision,
        field.scale,
        field.schemaOrdinal + existingColsSize)
      allColumns ++= Seq(columnSchema)
      newCols ++= Seq(columnSchema)
    })

    allColumns ++= tableCols.filter(x => !x.isDimensionColumn)
    alterTableModel.msrCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      val columnSchema: ColumnSchema = getColumnSchema(
        DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        isCol = true,
        encoders,
        isDimensionCol = false,
        -1,
        field.precision,
        field.scale,
        field.schemaOrdinal + existingColsSize)
      allColumns ++= Seq(columnSchema)
      newCols ++= Seq(columnSchema)
    })

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    allColumns.filter(x => !x.isInvisible).groupBy(_.getColumnName)
      .foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate column found with name: $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${ dbName }.${ alterTableModel.tableName }. " +
        s"Duplicate column found with name: $name")
      sys.error(s"Duplicate column found with name: $name")
    })

    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator()
    columnValidator.validateColumns(allColumns)

    // populate table properties map
    val tablePropertiesMap = tableSchema.getTableProperties
    alterTableModel.tableProperties.foreach {
      x => val value = tablePropertiesMap.get(x._1)
        if (null != value) {
          tablePropertiesMap.put(x._1, value + "," + x._2)
        } else {
          tablePropertiesMap.put(x._1, x._2)
        }
    }

    // This part will create dictionary file for all newly added dictionary columns
    // if valid default value is provided,
    // then that value will be included while creating dictionary file
    val defaultValueString = "default.value."
    newCols.foreach { col =>
      var rawData: String = null
      for (elem <- alterTableModel.tableProperties) {
        if (elem._1.toLowerCase.startsWith(defaultValueString)) {
          if (col.getColumnName.equalsIgnoreCase(elem._1.substring(defaultValueString.length))) {
            rawData = elem._2
            val data = DataTypeUtil.convertDataToBytesBasedOnDataType(elem._2, col.getDataType)
            if (null != data) {
              col.setDefaultValue(data)
            } else {
              LOGGER
                .error(
                  "Invalid default value for new column " + dbName + "." +
                  alterTableModel.tableName +
                  "." + col.getColumnName + " : " + elem._2)
            }
          }
        }
      }
      if (col.getEncodingList.contains(Encoding.DICTIONARY) &&
          !col.getEncodingList.contains(Encoding.DIRECT_DICTIONARY)) {
        GlobalDictionaryUtil
          .loadDefaultDictionaryValueForNewColumn(carbonTablePath,
            col,
            tableIdentifier,
            storePath,
            rawData)
      }
    }

    tableSchema.setListOfColumns(allColumns.asJava)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    newCols
  }

  private def getColumnSchema(dataType: DataType, colName: String, isCol: Boolean,
      encoders: java.util.List[Encoding], isDimensionCol: Boolean,
      colGroup: Integer, precision: Integer, scale: Integer, schemaOrdinal: Int): ColumnSchema = {
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(dataType)
    columnSchema.setColumnName(colName)
    if (alterTableModel.highCardinalityDims.contains(colName)) {
      encoders.remove(encoders.remove(Encoding.DICTIONARY))
    }
    if (dataType == DataType.TIMESTAMP || dataType == DataType.DATE) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    val colPropMap = new java.util.HashMap[String, String]()
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = CarbonCommonFactory.getColumnUniqueIdGenerator
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(
      alterTableModel.databaseName.getOrElse(dbName),
      columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setColumnar(isCol)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setColumnGroup(colGroup)
    columnSchema.setPrecision(precision)
    columnSchema.setScale(scale)
    columnSchema.setSchemaOrdinal(schemaOrdinal)
    columnSchema.setUseInvertedIndex(isDimensionCol)
    columnSchema
  }
}
object TableNewProcessor {
  def apply(cm: TableModel): TableInfo = {
    new TableNewProcessor(cm).process
  }
}

class TableNewProcessor(cm: TableModel) {

  var index = 0
  var rowGroup = 0

  def getAllChildren(fieldChildren: Option[List[Field]]): Seq[ColumnSchema] = {
    var allColumns: Seq[ColumnSchema] = Seq[ColumnSchema]()
    fieldChildren.foreach(fields => {
      fields.foreach(field => {
        val encoders = new java.util.ArrayList[Encoding]()
        encoders.add(Encoding.DICTIONARY)
        val columnSchema: ColumnSchema = getColumnSchema(
          DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
          field.name.getOrElse(field.column), index,
          isCol = true, encoders, isDimensionCol = true, rowGroup, field.precision, field.scale,
          field.schemaOrdinal)
        allColumns ++= Seq(columnSchema)
        index = index + 1
        rowGroup = rowGroup + 1
        if (field.children.get != null) {
          columnSchema.setNumberOfChild(field.children.get.size)
          allColumns ++= getAllChildren(field.children)
        }
      })
    })
    allColumns
  }

  def getColumnSchema(dataType: DataType, colName: String, index: Integer, isCol: Boolean,
      encoders: java.util.List[Encoding], isDimensionCol: Boolean,
      colGroup: Integer, precision: Integer, scale: Integer, schemaOrdinal: Int): ColumnSchema = {
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(dataType)
    columnSchema.setColumnName(colName)
    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())
    if (highCardinalityDims.contains(colName)) {
      encoders.remove(encoders.remove(Encoding.DICTIONARY))
    }
    if (dataType == DataType.TIMESTAMP || dataType == DataType.DATE) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = CarbonCommonFactory.getColumnUniqueIdGenerator
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(cm.databaseName,
      columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setColumnar(isCol)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setColumnGroup(colGroup)
    columnSchema.setPrecision(precision)
    columnSchema.setScale(scale)
    columnSchema.setSchemaOrdinal(schemaOrdinal)
    // TODO: Need to fill RowGroupID, converted type
    // & Number of Children after DDL finalization
    columnSchema
  }

  // process create dml fields and create wrapper TableInfo object
  def process: TableInfo = {
    val LOGGER = LogServiceFactory.getLogService(TableNewProcessor.getClass.getName)
    var allColumns = Seq[ColumnSchema]()
    var index = 0
    cm.dimCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      encoders.add(Encoding.DICTIONARY)
      val columnSchema: ColumnSchema = getColumnSchema(
        DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        index,
        isCol = true,
        encoders,
        isDimensionCol = true,
        -1,
        field.precision,
        field.scale,
        field.schemaOrdinal)
      allColumns ++= Seq(columnSchema)
      index = index + 1
      if (field.children.isDefined && field.children.get != null) {
        columnSchema.setNumberOfChild(field.children.get.size)
        allColumns ++= getAllChildren(field.children)
      }
    })

    cm.msrCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      val columnSchema: ColumnSchema = getColumnSchema(
        DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        index,
        isCol = true,
        encoders,
        isDimensionCol = false,
        -1,
        field.precision,
        field.scale,
        field.schemaOrdinal)
      val measureCol = columnSchema

      allColumns ++= Seq(measureCol)
      index = index + 1
    })

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    allColumns.groupBy(_.getColumnName).foreach(f => if (f._2.size > 1) {
      val name = f._1
      LOGGER.error(s"Duplicate column found with name: $name")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
            s"for ${ cm.databaseName }.${ cm.tableName }" +
            s"Duplicate column found with name: $name")
      sys.error(s"Duplicate dimensions found with name: $name")
    })

    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())

    checkColGroupsValidity(cm.columnGroups, allColumns, highCardinalityDims)

    updateColumnGroupsInFields(cm.columnGroups, allColumns)

    var newOrderedDims = scala.collection.mutable.ListBuffer[ColumnSchema]()
    val complexDims = scala.collection.mutable.ListBuffer[ColumnSchema]()
    val measures = scala.collection.mutable.ListBuffer[ColumnSchema]()
    for (column <- allColumns) {
      if (highCardinalityDims.contains(column.getColumnName)) {
        newOrderedDims += column
      } else if (column.isComplex) {
        complexDims += column
      } else if (column.isDimensionColumn) {
        newOrderedDims += column
      } else {
        measures += column
      }

    }

    // Setting the boolean value of useInvertedIndex in column schema
    val noInvertedIndexCols = cm.noInvertedIdxCols.getOrElse(Seq())
    for (column <- allColumns) {
      // When the column is measure or the specified no inverted index column in DDL,
      // set useInvertedIndex to false, otherwise true.
      if (noInvertedIndexCols.contains(column.getColumnName) ||
          cm.msrCols.exists(_.column.equalsIgnoreCase(column.getColumnName))) {
        column.setUseInvertedIndex(false)
      } else {
        column.setUseInvertedIndex(true)
      }
    }

    // Adding dummy measure if no measure is provided
    if (measures.size < 1) {
      val encoders = new java.util.ArrayList[Encoding]()
      val columnSchema: ColumnSchema = getColumnSchema(DataType.DOUBLE,
        CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
        index,
        true,
        encoders,
        false,
        -1, 0, 0, schemaOrdinal = -1)
      columnSchema.setInvisible(true)
      val measureColumn = columnSchema
      measures += measureColumn
      allColumns = allColumns ++ measures
    }
    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator()
    columnValidator.validateColumns(allColumns)
    newOrderedDims = newOrderedDims ++ complexDims ++ measures

    val tableInfo = new TableInfo()
    val tableSchema = new TableSchema()
    val schemaEvol = new SchemaEvolution()
    schemaEvol.setSchemaEvolutionEntryList(new util.ArrayList[SchemaEvolutionEntry]())
    tableSchema.setTableId(UUID.randomUUID().toString)
    // populate table properties map
    val tablePropertiesMap = new java.util.HashMap[String, String]()
    cm.tableProperties.foreach {
      x => tablePropertiesMap.put(x._1, x._2)
    }
    tableSchema.setTableProperties(tablePropertiesMap)
    if (cm.bucketFields.isDefined) {
      val bucketCols = cm.bucketFields.get.bucketColumns.map { b =>
        val col = allColumns.find(_.getColumnName.equalsIgnoreCase(b))
        col match {
          case Some(colSchema: ColumnSchema) =>
            if (colSchema.isDimensionColumn && !colSchema.isComplex) {
              colSchema
            } else {
              LOGGER.error(s"Bucket field must be dimension column and " +
                           s"should not be measure or complex column: ${colSchema.getColumnName}")
              sys.error(s"Bucket field must be dimension column and " +
                        s"should not be measure or complex column: ${colSchema.getColumnName}")
            }
          case _ =>
            LOGGER.error(s"Bucket field is not present in table columns")
            sys.error(s"Bucket field is not present in table columns")
        }
      }
      tableSchema.setBucketingInfo(
        new BucketingInfo(bucketCols.asJava, cm.bucketFields.get.numberOfBuckets))
    }
    tableSchema.setTableName(cm.tableName)
    tableSchema.setListOfColumns(allColumns.asJava)
    tableSchema.setSchemaEvalution(schemaEvol)
    tableInfo.setDatabaseName(cm.databaseName)
    tableInfo.setTableUniqueName(cm.databaseName + "_" + cm.tableName)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo.setAggregateTableList(new util.ArrayList[TableSchema]())
    tableInfo
  }

  //  For checking if the specified col group columns are specified in fields list.
  protected def checkColGroupsValidity(colGrps: Seq[String],
      allCols: Seq[ColumnSchema],
      highCardCols: Seq[String]): Unit = {
    if (null != colGrps) {
      colGrps.foreach(columngroup => {
        val rowCols = columngroup.split(",")
        rowCols.foreach(colForGrouping => {
          var found: Boolean = false
          // check for dimensions + measures
          allCols.foreach(eachCol => {
            if (eachCol.getColumnName.equalsIgnoreCase(colForGrouping.trim())) {
              found = true
            }
          })
          // check for No Dicitonary dimensions
          highCardCols.foreach(noDicCol => {
            if (colForGrouping.trim.equalsIgnoreCase(noDicCol)) {
              found = true
            }
          })

          if (!found) {
            sys.error(s"column $colForGrouping is not present in Field list")
          }
        })
      })
    }
  }

  // For updating the col group details for fields.
  private def updateColumnGroupsInFields(colGrps: Seq[String], allCols: Seq[ColumnSchema]): Unit = {
    if (null != colGrps) {
      var colGroupId = -1
      colGrps.foreach(columngroup => {
        colGroupId += 1
        val rowCols = columngroup.split(",")
        rowCols.foreach(row => {

          allCols.foreach(eachCol => {

            if (eachCol.getColumnName.equalsIgnoreCase(row.trim)) {
              eachCol.setColumnGroup(colGroupId)
              eachCol.setColumnar(false)
            }
          })
        })
      })
    }
  }
}
