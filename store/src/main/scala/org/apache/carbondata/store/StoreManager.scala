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

package org.apache.carbondata.store

import java.text.SimpleDateFormat
import java.util.UUID
import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet, Map}
import scala.language.implicitConversions

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.{BucketingInfo, PartitionInfo, SchemaEvolution, SchemaEvolutionEntry}
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.{TableInfo, TableSchema}
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.service.CarbonCommonFactory
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.store.util.{CommonUtil, DataTypeConverterUtil, PartitionUtils}

object StoreManager {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def createTable(
      tableName: String,
      dbName: String,
      fields: Seq[Field],
      partitionFields: Seq[PartitionerField],
      bucketFields: Option[BucketFields],
      tableProperties: Map[String, String]
  ): TableInfo = {
    val tableModel = StoreManager.prepareTableModel(
      ifNotExistPresent = false,
      Option(dbName),
      tableName,
      fields,
      partitionFields,
      tableProperties,
      bucketFields)
    val tableInfo = createTable(tableModel)
    // val tablePath = CarbonEnv.getInstance(sparkSession).storePath +
    // "/" + dbName + "/" + tableName
    // Table(tableInfo, tablePath)
    tableInfo
  }

  // process create dml fields and create wrapper TableInfo object
  def createTable(tableModel: TableModel): TableInfo = {
    val LOGGER = LogServiceFactory.getLogService(StoreManager.getClass.getName)
    var allColumns = Seq[ColumnSchema]()
    var index = 0
    var measureCount = 0

    // Sort columns should be at the begin of all columns
    tableModel.sortKeyDims.get.foreach { keyDim =>
      val field = tableModel.dimCols.find(keyDim equals _.column).get
      val encoders = new java.util.ArrayList[Encoding]()
      encoders.add(Encoding.DICTIONARY)
      val columnSchema: ColumnSchema = StoreManager.createColumnSchema(
        field,
        encoders,
        isDimensionCol = true,
        field.schemaOrdinal,
        tableModel.highcardinalitydims.getOrElse(Seq()),
        tableModel.databaseName)
      columnSchema.setSortColumn(true)
      allColumns :+= columnSchema
      index = index + 1
    }

    tableModel.dimCols.foreach { field =>
      val sortField = tableModel.sortKeyDims.get.find(field.column equals _)
      if (sortField.isEmpty) {
        val encoders = new java.util.ArrayList[Encoding]()
        encoders.add(Encoding.DICTIONARY)
        val columnSchema: ColumnSchema = StoreManager.createColumnSchema(
          field,
          encoders,
          isDimensionCol = true,
          field.schemaOrdinal,
          tableModel.highcardinalitydims.getOrElse(Seq()),
          tableModel.databaseName)
        allColumns :+= columnSchema
        index = index + 1
        if (field.children.isDefined && field.children.get != null) {
          columnSchema.setNumberOfChild(field.children.get.size)
          allColumns ++= getAllChildren(tableModel, field.children)
        }
      }
    }

    tableModel.msrCols.foreach { field =>
      val encoders = new java.util.ArrayList[Encoding]()
      val columnSchema: ColumnSchema = StoreManager.createColumnSchema(
        field,
        encoders,
        isDimensionCol = false,
        field.schemaOrdinal,
        tableModel.highcardinalitydims.getOrElse(Seq()),
        tableModel.databaseName)
      allColumns :+= columnSchema
      index = index + 1
      measureCount += 1
    }

    // Check if there is any duplicate measures or dimensions.
    // Its based on the dimension name and measure name
    allColumns.groupBy(_.getColumnName).foreach { f =>
      if (f._2.size > 1) {
        val name = f._1
        LOGGER.error(s"Duplicate column found with name: $name")
        LOGGER.audit(
          s"Validation failed for Create/Alter Table Operation " +
          s"for ${ tableModel.databaseName }.${ tableModel.tableName }" +
          s"Duplicate column found with name: $name")
        sys.error(s"Duplicate dimensions found with name: $name")
      }
    }

    // Setting the boolean value of useInvertedIndex in column schema
    val noInvertedIndexCols = tableModel.noInvertedIdxCols.getOrElse(Seq())
    LOGGER.info("NoINVERTEDINDEX columns are : " + noInvertedIndexCols.mkString(","))
    for (column <- allColumns) {
      // When the column is measure or the specified no inverted index column in DDL,
      // set useInvertedIndex to false, otherwise true.
      if (noInvertedIndexCols.contains(column.getColumnName) ||
          tableModel.msrCols.exists(_.column.equalsIgnoreCase(column.getColumnName))) {
        column.setUseInvertedIndex(false)
      } else {
        column.setUseInvertedIndex(true)
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
      val columnSchema: ColumnSchema = StoreManager.createColumnSchema(
        field,
        encoders,
        isDimensionCol = false,
        -1,
        tableModel.highcardinalitydims.getOrElse(Seq()),
        tableModel.databaseName)
      columnSchema.setInvisible(true)
      allColumns :+= columnSchema
    }
    val columnValidator = CarbonSparkFactory.getCarbonColumnValidator
    columnValidator.validateColumns(allColumns)

    val tableInfo = new TableInfo()
    val tableSchema = new TableSchema()
    val schemaEvol = new SchemaEvolution()
    schemaEvol.setSchemaEvolutionEntryList(new java.util.ArrayList[SchemaEvolutionEntry]())
    tableSchema.setTableId(UUID.randomUUID().toString)
    // populate table properties map
    val tablePropertiesMap = new java.util.HashMap[String, String]()
    tableModel.tableProperties.foreach {
      x => tablePropertiesMap.put(x._1, x._2)
    }
    tableSchema.setTableProperties(tablePropertiesMap)
    if (tableModel.bucketFields.isDefined) {
      val bucketCols = tableModel.bucketFields.get.bucketColumns.map { b =>
        val col = allColumns.find(_.getColumnName.equalsIgnoreCase(b))
        col match {
          case Some(colSchema: ColumnSchema) =>
            if (colSchema.isDimensionColumn && !colSchema.getDataType.isComplexType) {
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
        new BucketingInfo(bucketCols.asJava, tableModel.bucketFields.get.numberOfBuckets))
    }
    if (tableModel.partitionInfo.isDefined) {
      val partitionInfo = tableModel.partitionInfo.get
      val PartitionColumnSchema = partitionInfo.getColumnSchemaList.asScala
      val partitionCols = allColumns.filter { column =>
        PartitionColumnSchema.exists(_.getColumnName.equalsIgnoreCase(column.getColumnName))
      }.asJava
      partitionInfo.setColumnSchemaList(partitionCols)
      tableSchema.setPartitionInfo(partitionInfo)
    }
    tableSchema.setTableName(tableModel.tableName)
    tableSchema.setListOfColumns(allColumns.asJava)
    tableSchema.setSchemaEvalution(schemaEvol)
    tableInfo.setDatabaseName(tableModel.databaseName)
    tableInfo.setTableUniqueName(tableModel.databaseName + "_" + tableModel.tableName)
    tableInfo.setLastUpdatedTime(System.currentTimeMillis())
    tableInfo.setFactTable(tableSchema)
    tableInfo
  }

  def createColumnSchema(
      field: Field,
      encoders: java.util.List[Encoding],
      isDimensionCol: Boolean,
      schemaOrdinal: Int,
      highCardinalityDims: Seq[String],
      databaseName: String,
      isSortColumn: Boolean = false): ColumnSchema = {
    val dataType = DataTypeConverterUtil.convertToCarbonType(field.dataType.getOrElse(""))
    val columnSchema = new ColumnSchema()
    columnSchema.setDataType(dataType)
    val colName = field.name.getOrElse(field.column)
    columnSchema.setColumnName(colName)
    if (highCardinalityDims.contains(colName)) {
      encoders.remove(Encoding.DICTIONARY)
    }
    if (dataType == DataTypes.DATE) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    if (dataType == DataTypes.TIMESTAMP && ! highCardinalityDims.contains(colName)) {
      encoders.add(Encoding.DIRECT_DICTIONARY)
    }
    columnSchema.setEncodingList(encoders)
    val colUniqueIdGenerator = CarbonCommonFactory.getColumnUniqueIdGenerator
    val columnUniqueId = colUniqueIdGenerator.generateUniqueId(databaseName, columnSchema)
    columnSchema.setColumnUniqueId(columnUniqueId)
    columnSchema.setColumnReferenceId(columnUniqueId)
    columnSchema.setColumnar(true)
    columnSchema.setPrecision(field.precision)
    columnSchema.setScale(field.scale)
    columnSchema.setDimensionColumn(isDimensionCol)
    columnSchema.setSchemaOrdinal(schemaOrdinal)
    columnSchema.setUseInvertedIndex(isDimensionCol)
    columnSchema.setSortColumn(isSortColumn)
    columnSchema
  }

  /**
   * This will prepate the Model
   */
  def prepareTableModel(
      ifNotExistPresent: Boolean,
      dbName: Option[String],
      tableName: String,
      fields: Seq[Field],
      partitionCols: Seq[PartitionerField],
      tableProperties: Map[String, String],
      bucketFields: Option[BucketFields],
      isAlterFlow: Boolean = false
  ): TableModel = {

    if (tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS).isDefined) {
      throw new MalformedCarbonCommandException(
        s"${CarbonCommonConstants.COLUMN_GROUPS} is deprecated")
    }

    fields.zipWithIndex.foreach { case (field, index) =>
      field.schemaOrdinal = index
    }
    val (dims, msrs, noDictionaryDims, sortKeyDims) = extractDimAndMsrFields(
      fields, tableProperties)

    // column properties
    val colProps = extractColumnProperties(fields, tableProperties)
    // get no inverted index columns from table properties.
    val noInvertedIdxCols = extractNoInvertedIndexColumns(fields, tableProperties)
    // get partitionInfo
    val partitionInfo = getPartitionInfo(partitionCols, tableProperties)

    // validate the tableBlockSize from table properties
    CommonUtil.validateTableBlockSize(tableProperties)

    TableModel(
      ifNotExistPresent,
      dbName.getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
      dbName,
      tableName,
      tableProperties,
      reorderDimensions(dims.map(f => normalizeType(f)).map(f => addParent(f))),
      msrs.map(f => normalizeType(f)),
      Option(sortKeyDims),
      Option(noDictionaryDims),
      Option(noInvertedIdxCols),
      null,
      Some(colProps),
      bucketFields: Option[BucketFields],
      partitionInfo)
  }

  private def addParent(field: Field): Field = {
    field.dataType.getOrElse("NIL") match {
      case "Array" => Field(field.column, Some("Array"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType, field.schemaOrdinal, rawSchema = field.rawSchema)
      case "Struct" => Field(field.column, Some("Struct"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType, field.schemaOrdinal, rawSchema = field.rawSchema)
      case _ => field
    }
  }

  private def appendParentForEachChild(field: Field, parentName: String): Field = {
    field.dataType.getOrElse("NIL") match {
      case "String" => Field(parentName + "." + field.column, Some("String"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "SmallInt" => Field(parentName + "." + field.column, Some("SmallInt"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Integer" => Field(parentName + "." + field.column, Some("Integer"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Long" => Field(parentName + "." + field.column, Some("Long"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Double" => Field(parentName + "." + field.column, Some("Double"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Float" => Field(parentName + "." + field.column, Some("Double"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Timestamp" => Field(parentName + "." + field.column, Some("Timestamp"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Numeric" => Field(parentName + "." + field.column, Some("Numeric"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Array" => Field(parentName + "." + field.column, Some("Array"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
          .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "Struct" => Field(parentName + "." + field.column, Some("Struct"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
          .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "BigInt" => Field(parentName + "." + field.column, Some("BigInt"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Decimal" => Field(parentName + "." + field.column, Some("Decimal"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName,
        field.storeType, field.schemaOrdinal, field.precision, field.scale)
      case _ => field
    }
  }

  private def normalizeType(field: Field): Field = {
    val dataType = field.dataType.getOrElse("NIL")
    dataType match {
      case "string" =>
        Field(field.column, Some("String"), field.name, Some(null), field.parent,
          field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
        )
      case "smallint" =>
        Field(field.column, Some("SmallInt"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema)
      case "integer" | "int" =>
        Field(field.column, Some("Integer"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema)
      case "long" => Field(field.column, Some("Long"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
      )
      case "double" => Field(field.column, Some("Double"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
      )
      case "float" => Field(field.column, Some("Double"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
      )
      case "timestamp" =>
        Field(field.column, Some("Timestamp"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema)
      case "numeric" => Field(field.column, Some("Numeric"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
      )
      case "array" =>
        Field(field.column, Some("Array"), field.name,
          field.children.map(f => f.map(normalizeType(_))),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema)
      case "struct" =>
        Field(field.column, Some("Struct"), field.name,
          field.children.map(f => f.map(normalizeType(_))),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema)
      case "bigint" => Field(field.column, Some("BigInt"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
      )
      case "decimal" => Field(field.column, Some("Decimal"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema
      )
      // checking if the nested data type contains the child type as decimal(10,0),
      // if it is present then extracting the precision and scale. resetting the data type
      // with Decimal.
      case _ if dataType.startsWith("decimal") =>
        val (precision, scale) = CommonUtil.getScaleAndPrecision(dataType)
        Field(field.column,
          Some("Decimal"),
          field.name,
          Some(null),
          field.parent,
          field.storeType, field.schemaOrdinal, precision,
          scale,
          field.rawSchema
        )
      case _ =>
        field
    }
  }

  private def reorderDimensions(dims: Seq[Field]): Seq[Field] = {
    var complexDimensions: Seq[Field] = Seq()
    var dimensions: Seq[Field] = Seq()
    dims.foreach { dimension =>
      dimension.dataType.getOrElse("NIL") match {
        case "Array" => complexDimensions = complexDimensions :+ dimension
        case "Struct" => complexDimensions = complexDimensions :+ dimension
        case _ => dimensions = dimensions :+ dimension
      }
    }
    dimensions ++ complexDimensions
  }

  protected def getPartitionInfo(partitionCols: Seq[PartitionerField],
      tableProperties: Map[String, String]): Option[PartitionInfo] = {
    val timestampFormatter = new SimpleDateFormat(CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
    val dateFormatter = new SimpleDateFormat(CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
    if (partitionCols.isEmpty) {
      None
    } else {
      var partitionType: String = ""
      var numPartitions = 0
      var rangeInfo = List[String]()
      var listInfo = List[List[String]]()

      val columnDataType = DataTypeConverterUtil.
        convertToCarbonType(partitionCols.head.dataType.get)
      if (tableProperties.get(CarbonCommonConstants.PARTITION_TYPE).isDefined) {
        partitionType = tableProperties(CarbonCommonConstants.PARTITION_TYPE)
      }
      if (tableProperties.get(CarbonCommonConstants.NUM_PARTITIONS).isDefined) {
        numPartitions = tableProperties(CarbonCommonConstants.NUM_PARTITIONS)
          .toInt
      }
      if (tableProperties.get(CarbonCommonConstants.RANGE_INFO).isDefined) {
        rangeInfo = tableProperties(CarbonCommonConstants.RANGE_INFO).split(",")
          .map(_.trim()).toList
        CommonUtil.validateRangeInfo(rangeInfo, columnDataType, timestampFormatter, dateFormatter)
      }
      if (tableProperties.get(CarbonCommonConstants.LIST_INFO).isDefined) {
        val originListInfo = tableProperties(CarbonCommonConstants.LIST_INFO)
        listInfo = PartitionUtils.getListInfo(originListInfo)
        CommonUtil.validateListInfo(listInfo)
      }
      val cols : ArrayBuffer[ColumnSchema] = new ArrayBuffer[ColumnSchema]()
      partitionCols.foreach(partition_col => {
        val columnSchema = new ColumnSchema
        columnSchema.setDataType(DataTypeConverterUtil.
          convertToCarbonType(partition_col.dataType.get))
        columnSchema.setColumnName(partition_col.partitionColumn)
        cols += columnSchema
      })

      var partitionInfo : PartitionInfo = null
      partitionType.toUpperCase() match {
        case "HASH" => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.HASH)
          partitionInfo.initialize(numPartitions)
        case "RANGE" => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.RANGE)
          partitionInfo.setRangeInfo(rangeInfo.asJava)
          partitionInfo.initialize(rangeInfo.size + 1)
        case "LIST" => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.LIST)
          partitionInfo.setListInfo(listInfo.map(_.asJava).asJava)
          partitionInfo.initialize(listInfo.size + 1)
      }
      Some(partitionInfo)
    }
  }

  private def getKey(parentColumnName: Option[String],
      columnName: String): (String, String) = {
    if (parentColumnName.isDefined) {
      if (columnName == "val") {
        (parentColumnName.get, parentColumnName.get + "." + columnName)
      } else {
        (parentColumnName.get + "." + columnName, parentColumnName.get + "." + columnName)
      }
    } else {
      (columnName, columnName)
    }
  }

  /**
   * This will extract the no inverted columns fields.
   * By default all dimensions use inverted index.
   *
   * @param fields
   * @param tableProperties
   * @return
   */
  protected def extractNoInvertedIndexColumns(fields: Seq[Field],
      tableProperties: Map[String, String]): Seq[String] = {
    // check whether the column name is in fields
    var noInvertedIdxColsProps: Array[String] = Array[String]()
    var noInvertedIdxCols: Seq[String] = Seq[String]()

    if (tableProperties.get(CarbonCommonConstants.NO_INVERTED_INDEX).isDefined) {
      noInvertedIdxColsProps =
        tableProperties(CarbonCommonConstants.NO_INVERTED_INDEX).split(',').map(_.trim)
      noInvertedIdxColsProps.foreach { noInvertedIdxColProp =>
        if (!fields.exists(x => x.column.equalsIgnoreCase(noInvertedIdxColProp))) {
          val errormsg = "NO_INVERTED_INDEX column: " + noInvertedIdxColProp +
                         " does not exist in table. Please check create table statement."
          throw new MalformedCarbonCommandException(errormsg)
        }
      }
    }
    // check duplicate columns and only 1 col left
    val distinctCols = noInvertedIdxColsProps.toSet
    // extract the no inverted index columns
    fields.foreach(field => {
      if (distinctCols.exists(x => x.equalsIgnoreCase(field.column))) {
        noInvertedIdxCols :+= field.column
      }
    }
    )
    noInvertedIdxCols
  }

  /**
   * This will extract the Dimensions and NoDictionary Dimensions fields.
   * By default all string cols are dimensions.
   *
   * @param fields
   * @param tableProperties
   * @return
   */
  protected def extractDimAndMsrFields(fields: Seq[Field],
      tableProperties: Map[String, String]): (Seq[Field], Seq[Field], Seq[String], Seq[String]) = {
    var dimFields: LinkedHashSet[Field] = LinkedHashSet[Field]()
    var msrFields: Seq[Field] = Seq[Field]()
    var dictExcludeCols: Array[String] = Array[String]()
    var noDictionaryDims: Seq[String] = Seq[String]()
    var dictIncludeCols: Seq[String] = Seq[String]()

    // All columns in sortkey should be there in create table cols
    val sortKeyOption = tableProperties.get(CarbonCommonConstants.SORT_COLUMNS)
    var sortKeyDimsTmp: Seq[String] = Seq[String]()
    val sortKeyString: String = if (sortKeyOption.isDefined) {
      CarbonUtil.unquoteChar(sortKeyOption.get) trim
    } else {
      ""
    }
    if (!sortKeyString.isEmpty) {
      val sortKey = sortKeyString.split(',').map(_.trim)
      if (sortKey.diff(sortKey.distinct).length > 0 ||
          (sortKey.length > 1 && sortKey.contains(""))) {
        throw new MalformedCarbonCommandException(
          "SORT_COLUMNS Either having duplicate columns : " +
          sortKey.diff(sortKey.distinct).mkString(",") + " or it contains illegal argumnet.")
      }

      sortKey.foreach { column =>
        if (!fields.exists(x => x.column.equalsIgnoreCase(column))) {
          val errormsg = "sort_columns: " + column +
                         " does not exist in table. Please check create table statement."
          throw new MalformedCarbonCommandException(errormsg)
        } else {
          val dataType = fields.find(x =>
            x.column.equalsIgnoreCase(column)).get.dataType.get
          if (isDataTypeSupportedForSortColumn(dataType)) {
            val errormsg = s"sort_columns is unsupported for $dataType datatype column: " + column
            throw new MalformedCarbonCommandException(errormsg)
          }
        }
      }

      sortKey.foreach { dimension =>
        if (!sortKeyDimsTmp.exists(dimension.equalsIgnoreCase)) {
          fields.foreach { field =>
            if (field.column.equalsIgnoreCase(dimension)) {
              sortKeyDimsTmp :+= field.column
            }
          }
        }
      }
    }

    // All excluded cols should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).isDefined) {
      LOGGER.warn("dictionary_exclude option was deprecated, " +
                  "by default string column does not use global dictionary.")
      dictExcludeCols =
        tableProperties(CarbonCommonConstants.DICTIONARY_EXCLUDE).split(',').map(_.trim)
      dictExcludeCols
        .foreach { dictExcludeCol =>
          if (!fields.exists(x => x.column.equalsIgnoreCase(dictExcludeCol))) {
            val errormsg = "DICTIONARY_EXCLUDE column: " + dictExcludeCol +
                           " does not exist in table. Please check create table statement."
            throw new MalformedCarbonCommandException(errormsg)
          } else {
            val dataType = fields.find(x =>
              x.column.equalsIgnoreCase(dictExcludeCol)).get.dataType.get
            if (isComplexDimDictionaryExclude(dataType)) {
              val errormsg = "DICTIONARY_EXCLUDE is unsupported for complex datatype column: " +
                             dictExcludeCol
              throw new MalformedCarbonCommandException(errormsg)
            } else if (!isDataTypeSupportedForDictionary_Exclude(dataType)) {
              val errorMsg = "DICTIONARY_EXCLUDE is unsupported for " + dataType.toLowerCase() +
                             " data type column: " + dictExcludeCol
              throw new MalformedCarbonCommandException(errorMsg)
            }
          }
        }
    }
    // All included cols should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).isDefined) {
      dictIncludeCols =
        tableProperties(CarbonCommonConstants.DICTIONARY_INCLUDE).split(",").map(_.trim)
      dictIncludeCols.foreach { distIncludeCol =>
        if (!fields.exists(x => x.column.equalsIgnoreCase(distIncludeCol.trim))) {
          val errormsg = "DICTIONARY_INCLUDE column: " + distIncludeCol.trim +
                         " does not exist in table. Please check create table statement."
          throw new MalformedCarbonCommandException(errormsg)
        }
      }
    }

    // include cols should not contain exclude cols
    dictExcludeCols.foreach { dicExcludeCol =>
      if (dictIncludeCols.exists(x => x.equalsIgnoreCase(dicExcludeCol))) {
        val errormsg = "DICTIONARY_EXCLUDE can not contain the same column: " + dicExcludeCol +
                       " with DICTIONARY_INCLUDE. Please check create table statement."
        throw new MalformedCarbonCommandException(errormsg)
      }
    }

    // by default consider all String cols as dims and if any dictionary include isn't present then
    // add it to noDictionaryDims list. consider all dictionary excludes/include cols as dims
    fields.foreach { field =>
      if (dictExcludeCols.exists(x => x.equalsIgnoreCase(field.column))) {
        noDictionaryDims :+= field.column
        dimFields += field
      } else if (dictIncludeCols.exists(x => x.equalsIgnoreCase(field.column))) {
        dimFields += field
      } else if (DataTypeUtil.getDataType(field.dataType.get.toUpperCase) == DataTypes.TIMESTAMP &&
                 !dictIncludeCols.exists(x => x.equalsIgnoreCase(field.column))) {
        noDictionaryDims :+= field.column
        dimFields += field
      } else if (isDetectAsDimentionDatatype(field.dataType.get)) {
        dimFields += field
        // consider all String cols as noDicitonaryDims by default
        if (DataTypes.STRING.getName.equalsIgnoreCase(field.dataType.get)) {
          noDictionaryDims :+= field.column
        }
      } else if (sortKeyDimsTmp.exists(x => x.equalsIgnoreCase(field.column)) &&
                 isDefaultMeasure(field.dataType) &&
                 (!field.dataType.get.equalsIgnoreCase("STRING"))) {
        throw new MalformedCarbonCommandException(s"Illegal argument in sort_column.Check if you " +
                                                  s"have included UNSUPPORTED DataType column{${
                                                    field.column}}in sort_columns.")
      } else if (sortKeyDimsTmp.exists(x => x.equalsIgnoreCase(field.column))) {
        noDictionaryDims :+= field.column
        dimFields += field
      } else {
        msrFields :+= field
      }
    }

    var sortKeyDims = sortKeyDimsTmp
    if (sortKeyOption.isEmpty) {
      // if SORT_COLUMNS was not defined, add all dimension to SORT_COLUMNS.
      dimFields.foreach { field =>
        if (!isComplexDimDictionaryExclude(field.dataType.get)) {
          sortKeyDims :+= field.column
        }
      }
    }
    if (sortKeyDims.isEmpty) {
      // no SORT_COLUMNS
      tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, "")
    } else {
      tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, sortKeyDims.mkString(","))
    }
    (dimFields.toSeq, msrFields, noDictionaryDims, sortKeyDims)
  }

  /**
   * detect dimention data type
   *
   * @param dimensionDatatype
   */
  private def isDetectAsDimentionDatatype(dimensionDatatype: String): Boolean = {
    val dimensionType = Array("string", "array", "struct", "timestamp", "date", "char")
    dimensionType.exists(x => dimensionDatatype.toLowerCase.contains(x))
  }

  /**
   * detects whether complex dimension is part of dictionary_exclude
   */
  def isComplexDimDictionaryExclude(dimensionDataType: String): Boolean = {
    val dimensionType = Array("array", "struct")
    dimensionType.exists(x => x.equalsIgnoreCase(dimensionDataType))
  }

  private def isDefaultMeasure(dataType: Option[String]): Boolean = {
    val measureList = Array("DOUBLE", "DECIMAL", "FLOAT")
    measureList.exists(dataType.get.equalsIgnoreCase(_))
  }

  /**
   * detects whether datatype is part of sort_column
   */
  private def isDataTypeSupportedForSortColumn(columnDataType: String): Boolean = {
    val dataTypes = Array("array", "struct", "double", "float", "decimal")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }

  /**
   * detects whether datatype is part of dictionary_exclude
   */
  private def isDataTypeSupportedForDictionary_Exclude(columnDataType: String): Boolean = {
    val dataTypes = Array("string", "timestamp", "int", "long", "bigint")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }


  private def extractColumnProperties(fields: Seq[Field], tableProperties: Map[String, String]):
  java.util.Map[String, java.util.List[ColumnProperty]] = {
    val colPropMap = new java.util.HashMap[String, java.util.List[ColumnProperty]]()
    fields.foreach { field =>
      if (field.children.isDefined && field.children.get != null) {
        fillAllChildrenColumnProperty(field.column, field.children, tableProperties, colPropMap)
      } else {
        fillColumnProperty(None, field.column, tableProperties, colPropMap)
      }
    }
    colPropMap
  }

  protected def fillAllChildrenColumnProperty(parent: String, fieldChildren: Option[List[Field]],
      tableProperties: Map[String, String],
      colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    fieldChildren.foreach { fields =>
      fields.foreach { field =>
        fillColumnProperty(Some(parent), field.column, tableProperties, colPropMap)
      }
    }
  }

  private def fillColumnProperty(parentColumnName: Option[String],
      columnName: String,
      tableProperties: Map[String, String],
      colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    val (tblPropKey, colProKey) = getKey(parentColumnName, columnName)
    val colProps = CommonUtil.getColumnProperties(tblPropKey, tableProperties)
    if (colProps.isDefined) {
      colPropMap.put(colProKey, colProps.get)
    }
  }

  private def getAllChildren(tableModel: TableModel,
      fieldChildren: Option[List[Field]]): Seq[ColumnSchema] = {
    var allColumns: Seq[ColumnSchema] = Seq[ColumnSchema]()
    fieldChildren.foreach { fields =>
      fields.foreach { field =>
        val encoders = new java.util.ArrayList[Encoding]()
        encoders.add(Encoding.DICTIONARY)
        val columnSchema: ColumnSchema = StoreManager.createColumnSchema(
          field,
          encoders,
          isDimensionCol = true,
          field.schemaOrdinal,
          tableModel.highcardinalitydims.getOrElse(Seq()),
          tableModel.databaseName)
        allColumns ++= Seq(columnSchema)
        if (field.children.get != null) {
          columnSchema.setNumberOfChild(field.children.get.size)
          allColumns ++= getAllChildren(tableModel, field.children)
        }
      }
    }
    allColumns
  }

}
