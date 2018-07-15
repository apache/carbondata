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
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes, DecimalType}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema._
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, RelationIdentifier,
  TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.{ColumnSchema,
  ParentColumnTableRelation}
import org.apache.carbondata.core.service.impl.ColumnUniqueIdGenerator
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentDetailVO, SegmentUpdateStatusManager}
import org.apache.carbondata.core.util.{CarbonUtil, DataTypeUtil}
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CompactionType
import org.apache.carbondata.spark.CarbonSparkFactory
import org.apache.carbondata.spark.util.DataTypeConverterUtil

case class TableModel(
    ifNotExistsSet: Boolean,
    databaseNameOp: Option[String],
    tableName: String,
    tableProperties: Map[String, String],
    dimCols: Seq[Field],
    msrCols: Seq[Field],
    sortKeyDims: Option[Seq[String]],
    varcharCols: Option[Seq[String]],
    highcardinalitydims: Option[Seq[String]],
    noInvertedIdxCols: Option[Seq[String]],
    colProps: Option[util.Map[String, util.List[ColumnProperty]]] = None,
    bucketFields: Option[BucketFields],
    partitionInfo: Option[PartitionInfo],
    tableComment: Option[String] = None,
    var parentTable: Option[CarbonTable] = None,
    var dataMapRelation: Option[scala.collection.mutable.LinkedHashMap[Field, DataMapField]] = None)

case class Field(column: String, var dataType: Option[String], name: Option[String],
    children: Option[List[Field]], parent: String = null,
    storeType: Option[String] = Some("columnar"),
    var schemaOrdinal: Int = -1,
    var precision: Int = 0, var scale: Int = 0, var rawSchema: String = "",
    var columnComment: String = "") {
  override def equals(o: Any) : Boolean = o match {
    case that: Field =>
      that.column.equalsIgnoreCase(this.column)
    case _ => false
  }
  override def hashCode : Int = column.hashCode
}

case class DataMapField(var aggregateFunction: String = "",
    columnTableRelationList: Option[Seq[ColumnTableRelation]] = None) {
}

case class ColumnTableRelation(parentColumnName: String, parentColumnId: String,
    parentTableName: String, parentDatabaseName: String, parentTableId: String) {
}

case class ColumnProperty(key: String, value: String)

case class ComplexField(complexType: String, primitiveField: Option[Field],
    complexField: Option[ComplexField])

case class PartitionerField(partitionColumn: String, dataType: Option[String],
    columnComment: String)

case class BucketFields(bucketColumns: Seq[String], numberOfBuckets: Int)

case class DataLoadTableFileMapping(table: String, loadPath: String)

case class ExecutionErrors(var failureCauses: FailureCauses, var errorMsg: String )

case class CarbonMergerMapping(
    hdfsStoreLocation: String,
    metadataFilePath: String,
    var mergedLoadName: String,
    databaseName: String,
    factTableName: String,
    validSegments: Array[Segment],
    tableId: String,
    campactionType: CompactionType,
    // maxSegmentColCardinality is Cardinality of last segment of compaction
    var maxSegmentColCardinality: Array[Int],
    // maxSegmentColumnSchemaList is list of column schema of last segment of compaction
    var maxSegmentColumnSchemaList: List[ColumnSchema],
    currentPartitions: Option[Seq[PartitionSpec]])

case class NodeInfo(TaskId: String, noOfBlocks: Int)

case class AlterTableModel(
    dbName: Option[String],
    tableName: String,
    segmentUpdateStatusManager: Option[SegmentUpdateStatusManager],
    compactionType: String,
    factTimeStamp: Option[Long],
    var alterSql: String,
    customSegmentIds: Option[List[String]] = None)

case class UpdateTableModel(
    isUpdate: Boolean,
    updatedTimeStamp: Long,
    var executorErrors: ExecutionErrors,
    deletedSegments: Seq[Segment])

case class CompactionModel(compactionSize: Long,
    compactionType: CompactionType,
    carbonTable: CarbonTable,
    isDDLTrigger: Boolean,
    currentPartitions: Option[Seq[PartitionSpec]],
    customSegmentIds: Option[List[String]])

case class CompactionCallableModel(carbonLoadModel: CarbonLoadModel,
    carbonTable: CarbonTable,
    loadsToMerge: util.List[SegmentDetailVO],
    sqlContext: SQLContext,
    compactionType: CompactionType,
    currentPartitions: Option[Seq[PartitionSpec]],
    compactedSegments: java.util.List[String])

case class AlterPartitionModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    oldPartitionIds: List[Int],
    sqlContext: SQLContext
)

case class SplitPartitionCallableModel(carbonLoadModel: CarbonLoadModel,
    segmentId: String,
    partitionId: String,
    oldPartitionIds: List[Int],
    sqlContext: SQLContext)

case class DropPartitionCallableModel(carbonLoadModel: CarbonLoadModel,
    segmentId: Segment,
    partitionId: String,
    oldPartitionIds: List[Int],
    dropWithData: Boolean,
    carbonTable: CarbonTable,
    sqlContext: SQLContext)

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

case class AlterTableDropPartitionModel(databaseName: Option[String],
    tableName: String,
    partitionId: String,
    dropWithData: Boolean)

case class AlterTableSplitPartitionModel(databaseName: Option[String],
    tableName: String,
    partitionId: String,
    splitInfo: List[String])

class AlterTableColumnSchemaGenerator(
    alterTableModel: AlterTableAddColumnsModel,
    dbName: String,
    tableInfo: TableInfo,
    tableIdentifier: AbsoluteTableIdentifier,
    sc: SparkContext) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  private def isSortColumn(columnName: String): Boolean = {
    val sortColumns = alterTableModel.tableProperties.get("sort_columns")
    if(sortColumns.isDefined) {
      sortColumns.get.contains(columnName)
    } else {
      true
    }
  }

  private def isVarcharColumn(columnName: String): Boolean = {
    val varcharColumns = alterTableModel.tableProperties.get("long_string_columns")
    if (varcharColumns.isDefined) {
      varcharColumns.get.contains(columnName)
    } else {
      false
    }
  }

  def process: Seq[ColumnSchema] = {
    val tableSchema = tableInfo.getFactTable
    val tableCols = tableSchema.getListOfColumns.asScala
    val existingColsSize = tableCols.size
    var allColumns = tableCols.filter(x => x.isDimensionColumn)
    var newCols = Seq[ColumnSchema]()

    alterTableModel.dimCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      encoders.add(Encoding.DICTIONARY)
      val columnSchema: ColumnSchema = TableNewProcessor.createColumnSchema(
        field,
        encoders,
        isDimensionCol = true,
        field.precision,
        field.scale,
        field.schemaOrdinal + existingColsSize,
        alterTableModel.highCardinalityDims,
        alterTableModel.databaseName.getOrElse(dbName),
        isSortColumn(field.name.getOrElse(field.column)),
        isVarcharColumn(field.name.getOrElse(field.column)))
      allColumns ++= Seq(columnSchema)
      newCols ++= Seq(columnSchema)
    })
    allColumns ++= tableCols.filter(x => !x.isDimensionColumn)
    alterTableModel.msrCols.foreach(field => {
      val encoders = new java.util.ArrayList[Encoding]()
      val columnSchema: ColumnSchema = TableNewProcessor.createColumnSchema(
        field,
        encoders,
        isDimensionCol = false,
        field.precision,
        field.scale,
        field.schemaOrdinal + existingColsSize,
        alterTableModel.highCardinalityDims,
        alterTableModel.databaseName.getOrElse(dbName)
      )
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

    if (newCols.exists(_.getDataType.isComplexType)) {
      LOGGER.error(s"Complex column cannot be added")
      LOGGER.audit(
        s"Validation failed for Create/Alter Table Operation " +
        s"for ${ dbName }.${ alterTableModel.tableName }. " +
        s"Complex column cannot be added")
      sys.error(s"Complex column cannot be added")
    }

    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator
    columnValidator.validateColumns(allColumns)


    def getLocalDictColumnList(tableProperties: scala.collection.mutable.Map[String, String],
        columns: scala.collection.mutable.ListBuffer[ColumnSchema]): (scala.collection.mutable
    .ListBuffer[ColumnSchema], scala.collection.mutable.ListBuffer[ColumnSchema]) = {
      val includeColumns = new scala.collection.mutable.ListBuffer[ColumnSchema]
      val excludeColumns = new scala.collection.mutable.ListBuffer[ColumnSchema]
      val localDictIncludeColumns = if (tableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE).isDefined) {
        tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE)
      } else {
        null
      }
      val localDictExcludeColumns = if (tableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE).isDefined) {
        tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE)
      } else {
        null
      }
      if (null != localDictIncludeColumns) {
        if (null == localDictExcludeColumns) {
          columns.foreach { column =>
            if (localDictIncludeColumns.contains(column.getColumnName)) {
              includeColumns.append(column)
            } else {
              if (column.getDataType.equals(DataTypes.STRING) ||
                  column.getDataType.equals(DataTypes.VARCHAR) ||
                  column.getDataType.toString.equals("ARRAY") ||
                  column.getDataType.toString.equals("STRUCT")) {
                excludeColumns.append(column)
              }
            }
          }
        } else {
          columns.foreach { column =>
            if (localDictIncludeColumns.contains(column.getColumnName) &&
                !localDictExcludeColumns.contains(column.getColumnName)) {
              includeColumns.append(column)
            } else if (localDictExcludeColumns.contains(column.getColumnName)) {
              excludeColumns.append(column)
            }
          }
        }
      } else {
        if (null == localDictExcludeColumns) {
          columns.foreach { column =>
            if (column.getDataType.equals(DataTypes.STRING) ||
                column.getDataType.equals(DataTypes.VARCHAR) ||
                column.getDataType.toString.equals("ARRAY") ||
                column.getDataType.toString.equals("STRUCT")) {
              includeColumns.append(column)
            }
          }
        } else {
          columns.foreach { column =>
            if (!localDictExcludeColumns.contains(column.getColumnName) &&
                (column.getDataType.equals(DataTypes.STRING) ||
                 column.getDataType.equals(DataTypes.VARCHAR) ||
                 column.getDataType.toString.equals("ARRAY") ||
                 column.getDataType.toString.equals("STRUCT"))) {
              includeColumns.append(column)
            } else if (localDictExcludeColumns.contains(column.getColumnName)) {
              excludeColumns.append(column)
            }
          }
        }
      }

      (includeColumns, excludeColumns)
    }

    val columnsWithoutNewCols = new scala.collection.mutable.ListBuffer[ColumnSchema]
    allColumns.foreach { column =>
      if (!newCols.exists(x => x.getColumnName.equalsIgnoreCase(column.getColumnName))) {
        columnsWithoutNewCols += column
      }
    }


    if (alterTableModel.tableProperties != null) {
      CarbonUtil
        .setLocalDictColumnsToWrapperSchema(newCols.asJava,
          alterTableModel.tableProperties.asJava,
          tableSchema.getTableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE))
    }

    val includeExcludeColOfMainTable = getLocalDictColumnList(tableSchema.getTableProperties
      .asScala,
      columnsWithoutNewCols)
    val alterMutableTblProperties: scala.collection.mutable.Map[String, String] = mutable
      .Map(alterTableModel.tableProperties.toSeq: _*)
    val includeExcludeColOfAlterTable = getLocalDictColumnList(alterMutableTblProperties,
      newCols.to[mutable.ListBuffer])

    // Append all Local Dictionary Include and Exclude columns of Alter Table to that of Main Table
    includeExcludeColOfMainTable._1.appendAll(includeExcludeColOfAlterTable._1)
    includeExcludeColOfMainTable._2.appendAll(includeExcludeColOfAlterTable._2)

    val localDictionaryIncludeColumn = new StringBuilder
    val localDictionaryExcludeColumn = new StringBuilder
    includeExcludeColOfMainTable._1.foreach { column =>
      localDictionaryIncludeColumn.append(column.getColumnName).append(",")
    }
    includeExcludeColOfMainTable._2.foreach { column =>
      localDictionaryExcludeColumn.append(column.getColumnName).append(",")
    }

    // populate table properties map
    val tablePropertiesMap = tableSchema.getTableProperties
    alterTableModel.tableProperties.foreach {
      case (key, mapValue) =>
        val value = tablePropertiesMap.get(key)
        if (null != value) {
          if (value != mapValue) {
            tablePropertiesMap.put(key, value + "," + mapValue)
          }
        } else {
          tablePropertiesMap.put(key, mapValue)
        }
    }

    // The Final Map should contain the combined Local Dictionary Include and
    // Local Dictionary Exclude Columns from both Main table and Alter table
    tablePropertiesMap
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE, localDictionaryIncludeColumn.toString())
    tablePropertiesMap
      .put(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE, localDictionaryExcludeColumn.toString())
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
            val data = DataTypeUtil.convertDataToBytesBasedOnDataType(elem._2, col)
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
        else if (elem._1.equalsIgnoreCase("no_inverted_index") &&
                 (elem._2.split(",").contains(col.getColumnName))) {
          col.getEncodingList.remove(Encoding.INVERTED_INDEX)
        }
      }
    }
    tableSchema.setListOfColumns(allColumns.asJava)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    newCols
  }

}

// TODO: move this to carbon store API
object TableNewProcessor {
  def apply(
      cm: TableModel): TableInfo = {
    new TableNewProcessor(cm).process
  }

  def createColumnSchema(
      field: Field,
      encoders: java.util.List[Encoding],
      isDimensionCol: Boolean,
      precision: Int,
      scale: Int,
      schemaOrdinal: Int,
      highCardinalityDims: Seq[String],
      databaseName: String,
      isSortColumn: Boolean = false,
      isVarcharColumn: Boolean = false): ColumnSchema = {
    val dataType = DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse(""))
    if (DataTypes.isDecimal(dataType)) {
      dataType.asInstanceOf[DecimalType].setPrecision(field.precision)
      dataType.asInstanceOf[DecimalType].setScale(field.scale)
    }
    val columnSchema = new ColumnSchema()
    if (isVarcharColumn) {
      columnSchema.setDataType(DataTypes.VARCHAR)
    } else {
      columnSchema.setDataType(dataType)
    }
    val colName = field.name.getOrElse(field.column)
    columnSchema.setColumnName(colName)
    if (highCardinalityDims.contains(colName)) {
      encoders.remove(Encoding.DICTIONARY)
    }
    if (dataType == DataTypes.DATE) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    if (dataType == DataTypes.TIMESTAMP && !highCardinalityDims.contains(colName)) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = ColumnUniqueIdGenerator.getInstance
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setPrecision(precision)
    columnSchema.setScale(scale)
    columnSchema.setSchemaOrdinal(schemaOrdinal)
    columnSchema.setUseInvertedIndex(isDimensionCol)
    columnSchema.setSortColumn(isSortColumn)
    columnSchema
  }
}

class TableNewProcessor(cm: TableModel) {

  def getAllChildren(fieldChildren: Option[List[Field]],
      useDictionaryEncoding: Boolean): Seq[ColumnSchema] = {
    var allColumns: Seq[ColumnSchema] = Seq[ColumnSchema]()
    fieldChildren.foreach(fields => {
      fields.foreach(field => {
        val encoders = new java.util.ArrayList[Encoding]()
        if (useDictionaryEncoding) {
          encoders.add(Encoding.DICTIONARY)
        }
        val columnSchema: ColumnSchema = getColumnSchema(
          DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
          field.name.getOrElse(field.column),
          encoders,
          true,
          field,
          cm.dataMapRelation,
          useDictionaryEncoding = useDictionaryEncoding)
        allColumns ++= Seq(columnSchema)
        if (field.children.get != null) {
          columnSchema.setNumberOfChild(field.children.get.size)
          allColumns ++= getAllChildren(field.children, useDictionaryEncoding)
        }
      })
    })
    allColumns
  }

  // varchar column is a string column that in long_string_columns
  private def isVarcharColumn(colName : String): Boolean = {
    cm.varcharCols.get.contains(colName)
  }

  def getColumnSchema(
      dataType: DataType,
      colName: String,
      encoders: java.util.List[Encoding],
      isDimensionCol: Boolean,
      field: Field,
      map: Option[scala.collection.mutable.LinkedHashMap[Field, DataMapField]],
      useDictionaryEncoding: Boolean = true) : ColumnSchema = {
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(dataType)
    columnSchema.setColumnName(colName)
    val isParentColumnRelation = map.isDefined && map.get.get(field).isDefined
    if (!isParentColumnRelation) {
      val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())
      if (highCardinalityDims.contains(colName)) {
        encoders.remove(Encoding.DICTIONARY)
      }
    if (dataType == DataTypes.DATE && useDictionaryEncoding) {
        encoders.add(Encoding.DIRECT_DICTIONARY)
      }
      if (dataType == DataTypes.TIMESTAMP &&
          !highCardinalityDims.contains(colName) && useDictionaryEncoding) {
        encoders.add(Encoding.DIRECT_DICTIONARY)
      }
    }
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = ColumnUniqueIdGenerator.getInstance
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setPrecision(field.precision)
    columnSchema.setScale(field.scale)
    columnSchema.setSchemaOrdinal(field.schemaOrdinal)
    columnSchema.setSortColumn(false)
    if (isVarcharColumn(colName)) {
      columnSchema.setDataType(DataTypes.VARCHAR)
    }
    if (isParentColumnRelation) {
      val dataMapField = map.get.get(field).get
      columnSchema.setFunction(dataMapField.aggregateFunction)
      val columnRelationList = dataMapField.columnTableRelationList.get
      val parentColumnTableRelationList = new util.ArrayList[ParentColumnTableRelation]
      columnRelationList.foreach {
        columnRelation =>
          val relationIdentifier = new RelationIdentifier(
            columnRelation.parentDatabaseName,
            columnRelation.parentTableName,
            columnRelation.parentTableId)
          val parentColumnTableRelation = new ParentColumnTableRelation(
            relationIdentifier,
            columnRelation.parentColumnId,
            columnRelation.parentColumnName)
          parentColumnTableRelationList.add(parentColumnTableRelation)
      }
      columnSchema.setParentColumnTableRelations(parentColumnTableRelationList)
    }
    // TODO: Need to fill RowGroupID, converted type
    // & Number of Children after DDL finalization
    if (field.columnComment.nonEmpty) {
      var columnProperties = columnSchema.getColumnProperties
      if (columnProperties == null) {
        columnProperties = new util.HashMap[String, String]()
        columnSchema.setColumnProperties(columnProperties)
      }
      columnProperties.put(CarbonCommonConstants.COLUMN_COMMENT, field.columnComment)
    }
    columnSchema
  }

  // process create dml fields and create wrapper TableInfo object
  def process: TableInfo = {
    val LOGGER = LogServiceFactory.getLogService(TableNewProcessor.getClass.getName)
    var allColumns = Seq[ColumnSchema]()
    var index = 0
    var measureCount = 0

    // Sort columns should be at the begin of all columns
    cm.sortKeyDims.get.foreach { keyDim =>
      val field = cm.dimCols.find(keyDim equals _.column).get
      val encoders = if (getEncoderFromParent(field)) {
        cm.parentTable.get.getColumnByName(
          cm.parentTable.get.getTableName,
          cm.dataMapRelation.get.get(field).get.columnTableRelationList.
            get(0).parentColumnName).getEncoder
      } else {
        val encoders = new java.util.ArrayList[Encoding]()
        encoders.add(Encoding.DICTIONARY)
        encoders
      }
      val columnSchema = getColumnSchema(
        DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        encoders,
        true,
        field,
        cm.dataMapRelation)
      columnSchema.setSortColumn(true)
      allColumns :+= columnSchema
      index = index + 1
    }

    val dictionaryIncludeCols = cm.tableProperties
      .getOrElse(CarbonCommonConstants.DICTIONARY_INCLUDE, "")

    def addDimensionCol(field: Field): Unit = {
      val sortField = cm.sortKeyDims.get.find(field.column equals _)
      if (sortField.isEmpty) {
        val encoders = if (getEncoderFromParent(field)) {
          cm.parentTable.get.getColumnByName(
            cm.parentTable.get.getTableName,
            cm.dataMapRelation.get.get(field).get.
              columnTableRelationList.get(0).parentColumnName).getEncoder
        } else {
          val encoders = new java.util.ArrayList[Encoding]()
          encoders.add(Encoding.DICTIONARY)
          encoders
        }
        val columnSchema = getColumnSchema(
          DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
          field.name.getOrElse(field.column),
          encoders,
          true,
          field,
          cm.dataMapRelation)
        allColumns :+= columnSchema
        index = index + 1
        if (field.children.isDefined && field.children.get != null) {
          val includeDictionaryEncoding = dictionaryIncludeCols.contains(field.column)
          if (!includeDictionaryEncoding) {
            columnSchema.getEncodingList.remove(Encoding.DICTIONARY)
          }
          columnSchema.setNumberOfChild(field.children.get.size)
          allColumns ++= getAllChildren(field.children, includeDictionaryEncoding)
        }
      }
    }
    // dimensions that are not varchar
    cm.dimCols.filter(field => !cm.varcharCols.get.contains(field.column))
      .foreach(addDimensionCol(_))
    // dimensions that are varchar
    cm.dimCols.filter(field => cm.varcharCols.get.contains(field.column))
      .foreach(addDimensionCol(_))

    // check whether the column is a local dictionary column and set in column schema
    if (null != cm.tableProperties) {
      CarbonUtil
        .setLocalDictColumnsToWrapperSchema(allColumns.asJava,
          cm.tableProperties.asJava,
          cm.tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE))
    }
    cm.msrCols.foreach { field =>
      // if aggregate function is defined in case of preaggregate and agg function is sum or avg
      // then it can be stored as measure
      var isAggFunPresent = false
      // getting the encoder from maintable so whatever encoding is applied in maintable
      // same encoder can be applied on aggregate table
      val encoders = if (getEncoderFromParent(field)) {
        isAggFunPresent =
          cm.dataMapRelation.get.get(field).get.aggregateFunction.equalsIgnoreCase("sum") ||
          cm.dataMapRelation.get.get(field).get.aggregateFunction.equals("avg") ||
          cm.dataMapRelation.get.get(field).get.aggregateFunction.equals("count")
        if (!isAggFunPresent) {
          cm.parentTable.get.getColumnByName(
            cm.parentTable.get.getTableName,
            cm.dataMapRelation.get.get(field).get.columnTableRelationList.get(0).parentColumnName)
            .getEncoder
        } else {
          new java.util.ArrayList[Encoding]()
        }
      } else {
        new java.util.ArrayList[Encoding]()
      }
      // check if it can be dimension column
      val isDimColumn = !encoders.isEmpty && !isAggFunPresent
      val columnSchema = getColumnSchema(
        DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse("")),
        field.name.getOrElse(field.column),
        encoders,
        isDimColumn,
        field,
        cm.dataMapRelation)
      allColumns :+= columnSchema
      index = index + 1
      if (!isDimColumn) {
        measureCount += 1
      }
    }

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    allColumns.groupBy(_.getColumnName).foreach { f =>
      if (f._2.size > 1) {
        val name = f._1
        LOGGER.error(s"Duplicate column found with name: $name")
        LOGGER.audit(
          s"Validation failed for Create/Alter Table Operation " +
          s"Duplicate column found with name: $name")
        CarbonException.analysisException(s"Duplicate dimensions found with name: $name")
      }
    }

    val highCardinalityDims = cm.highcardinalitydims.getOrElse(Seq())

    // Setting the boolean value of useInvertedIndex in column schema, if Paranet table is defined
    // Encoding is already decided above
    if (!cm.parentTable.isDefined) {
      val noInvertedIndexCols = cm.noInvertedIdxCols.getOrElse(Seq())
      LOGGER.info("NoINVERTEDINDEX columns are : " + noInvertedIndexCols.mkString(","))
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
    }
    // Adding dummy measure if no measure is provided
    if (measureCount == 0) {
      val encoders = new java.util.ArrayList[Encoding]()
      val field = Field(
        CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
        Some(DataTypes.DOUBLE.getName),
        Some(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE),
        None
      )
      val columnSchema: ColumnSchema = getColumnSchema(
        DataTypes.DOUBLE,
        CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE,
        encoders,
        false,
        field,
        cm.dataMapRelation)
      columnSchema.setInvisible(true)
      allColumns :+= columnSchema
    }
    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator
    columnValidator.validateColumns(allColumns)

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
    // Add table comment to table properties
    tablePropertiesMap.put("comment", cm.tableComment.getOrElse(""))
    val badRecordsPath = getBadRecordsPath(tablePropertiesMap,
      cm.tableName,
      tableSchema.getTableId,
      cm.databaseNameOp.getOrElse("default"))
    tablePropertiesMap.put("bad_records_path", badRecordsPath)
    tableSchema.setTableProperties(tablePropertiesMap)
    if (cm.bucketFields.isDefined) {
      val bucketCols = cm.bucketFields.get.bucketColumns.map { b =>
        val col = allColumns.find(_.getColumnName.equalsIgnoreCase(b))
        col match {
          case Some(colSchema: ColumnSchema) =>
            if (colSchema.isDimensionColumn && !colSchema.getDataType.isComplexType) {
              colSchema
            } else {
              LOGGER.error(s"Bucket field must be dimension column and " +
                           s"should not be measure or complex column: ${ colSchema.getColumnName }")
              CarbonException.analysisException(s"Bucket field must be dimension column and " +
                                                s"should not be measure or complex column: ${
                                                  colSchema
                                                    .getColumnName
                                                }")
            }
          case _ =>
            LOGGER.error(s"Bucket field is not present in table columns")
            CarbonException.analysisException(s"Bucket field is not present in table columns")
        }
      }
      tableSchema.setBucketingInfo(
        new BucketingInfo(bucketCols.asJava, cm.bucketFields.get.numberOfBuckets))
    }
    if (cm.partitionInfo.isDefined) {
      val partitionInfo = cm.partitionInfo.get
      val partitionColumnSchema = partitionInfo.getColumnSchemaList.asScala
      val partitionCols = allColumns.filter { column =>
        partitionColumnSchema.exists(_.getColumnName.equalsIgnoreCase(column.getColumnName))
      }
      val orderCols =
        partitionColumnSchema.map(
          f => partitionCols.find(_.getColumnName.equalsIgnoreCase(f.getColumnName)).get).asJava
      partitionInfo.setColumnSchemaList(orderCols)
      tableSchema.setPartitionInfo(partitionInfo)
    }
    tableSchema.setTableName(cm.tableName)
    tableSchema.setListOfColumns(allColumns.asJava)
    tableSchema.setSchemaEvolution(schemaEvol)
    tableInfo.setDatabaseName(cm.databaseNameOp.getOrElse(null))
    tableInfo.setTableUniqueName(CarbonTable.buildUniqueName(cm.databaseNameOp.getOrElse(null),
      cm.tableName))
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo
  }

  private def getBadRecordsPath(tablePropertiesMap: util.HashMap[String, String],
      tableName: String,
      tableId: String,
      databaseName: String): String = {
    val badRecordsPath = tablePropertiesMap.asScala
      .getOrElse("bad_records_path", CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL)
    if (badRecordsPath == null || badRecordsPath.isEmpty) {
      CarbonCommonConstants.CARBON_BADRECORDS_LOC_DEFAULT_VAL
    } else {
      badRecordsPath + CarbonCommonConstants.FILE_SEPARATOR + databaseName +
      CarbonCommonConstants.FILE_SEPARATOR + s"${tableName}_$tableId"
    }
  }

  /**
   * Method to check to get the encoder from parent or not
   *
   * @param field column field
   * @return get encoder from parent
   */
  private def getEncoderFromParent(field: Field): Boolean = {
    cm.parentTable.isDefined &&
    cm.dataMapRelation.get.get(field).isDefined &&
    cm.dataMapRelation.get.get(field).get.columnTableRelationList.size == 1
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
            CarbonException.analysisException(
              s"column $colForGrouping is not present in Field list")
          }
        })
      })
    }
  }
}
