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

package org.apache.spark.sql.catalyst

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet, ListBuffer, Map}
import scala.language.implicitConversions

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.DeprecatedFeatureException
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.exception.InvalidConfigurationException
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, CustomIndex}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil, DataTypeConverterUtil}


object CarbonParserUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  protected val doubleQuotedString = "\"([^\"]+)\"".r
  protected val singleQuotedString = "'([^']+)'".r
  protected val escapedIdentifier = "`([^`]+)`".r

  private def reorderDimensions(dims: Seq[Field], varcharCols: Seq[String]): Seq[Field] = {
    var dimensions: Seq[Field] = Seq()
    var varcharDimensions: Seq[Field] = Seq()
    var complexDimensions: Seq[Field] = Seq()
    dims.foreach { dimension =>
      dimension.dataType.getOrElse("NIL") match {
        case "Array" => complexDimensions = complexDimensions :+ dimension
        case "Struct" => complexDimensions = complexDimensions :+ dimension
        case "Map" => complexDimensions = complexDimensions :+ dimension
        case "String" =>
          if (varcharCols.exists(dimension.column.equalsIgnoreCase)) {
            varcharDimensions = varcharDimensions :+ dimension
          } else {
            dimensions = dimensions :+ dimension
          }
        case _ => dimensions = dimensions :+ dimension
      }
    }
    dimensions ++ varcharDimensions ++ complexDimensions
  }

  /**
   * this function validates for the column names as tupleId, PositionReference and positionId
   * @param fields
   */
  private def validateColumnNames(fields: Seq[Field]): Unit = {
    fields.foreach { col =>
      if (col.column.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID) ||
          col.column.equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_POSITIONID) ||
          col.column.equalsIgnoreCase(CarbonCommonConstants.POSITION_REFERENCE)) {
        throw new MalformedCarbonCommandException(
          s"Carbon Implicit column ${col.column} is not allowed in" +
          s" column name while creating table")
      }
    }
  }

  /**
   * The method parses, validates and processes the index_handler property.
   * @param tableProperties Table properties
   * @param tableFields Sequence of table fields
   * @return <Seq[Field]> Sequence of index fields to add to table fields
   */
  private def processIndexProperty(tableProperties: mutable.Map[String, String],
      tableFields: Seq[Field]): Seq[Field] = {
    val option = tableProperties.get(CarbonCommonConstants.INDEX_HANDLER)
    val fields = ListBuffer[Field]()
    if (option.isDefined) {
      if (option.get.trim.isEmpty) {
        throw new MalformedCarbonCommandException(
          s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
          s"Option value is empty.")
      }
      option.get.split(",").map(_.trim).foreach { handler =>
        // Validate target column name
        if (tableFields.exists(_.column.equalsIgnoreCase(handler))) {
          throw new MalformedCarbonCommandException(
            s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
            s"handler: $handler must not match with any other column name in the table")
        }
        val TYPE = s"${ CarbonCommonConstants.INDEX_HANDLER }.$handler.type"
        val SOURCE_COLUMNS = s"${ CarbonCommonConstants.INDEX_HANDLER }.$handler.sourcecolumns"
        val SOURCE_COLUMN_TYPES
        = s"${ CarbonCommonConstants.INDEX_HANDLER }.$handler.sourcecolumntypes"
        val HANDLER_CLASS = s"${ CarbonCommonConstants.INDEX_HANDLER }.$handler.class"
        val HANDLER_INSTANCE = s"${ CarbonCommonConstants.INDEX_HANDLER }.$handler.instance"

        val handlerType = tableProperties.get(TYPE)
        if (handlerType.isEmpty || handlerType.get.trim.isEmpty) {
          throw new MalformedCarbonCommandException(
            s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
            s"$TYPE property must be specified.")
        }
        val sourceColumnsOption = tableProperties.get(SOURCE_COLUMNS)
        if (sourceColumnsOption.isEmpty || sourceColumnsOption.get.trim.isEmpty) {
          throw new MalformedCarbonCommandException(
            s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
            s"$SOURCE_COLUMNS property must be specified.")
        }
        val sourcesWithoutSpaces = sourceColumnsOption.get.replaceAll("\\s", "")
        // Validate source columns
        val sources = sourcesWithoutSpaces.split(",")
        if (sources.distinct.length != sources.size) {
          throw new MalformedCarbonCommandException(
            s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
            s"$SOURCE_COLUMNS property cannot have duplicate columns.")
        }
        val sourceTypes = StringBuilder.newBuilder
        sources.foreach { column =>
          tableFields.find(_.column.equalsIgnoreCase(column)) match {
            case Some(field) => sourceTypes.append(field.dataType.get).append(",")
            case None =>
              throw new MalformedCarbonCommandException(
                s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
                s"Source column: $column in property " +
                s"$SOURCE_COLUMNS must be a column in the table.")
          }
        }
        tableProperties.put(SOURCE_COLUMNS, sourcesWithoutSpaces)
        tableProperties.put(SOURCE_COLUMN_TYPES, sourceTypes.dropRight(1).toString())
        val handlerClass = tableProperties.get(HANDLER_CLASS)
        val handlerClassName: String = handlerClass match {
          case Some(className) => className.trim
          case None =>
            // use handler type to find the default implementation
            if (handlerType.get.trim.equalsIgnoreCase(CarbonCommonConstants.GEOHASH)) {
              // Use GeoHash default implementation
              val className = CustomIndex.CUSTOM_INDEX_DEFAULT_IMPL
              tableProperties.put(HANDLER_CLASS, className)
              className
            } else {
              throw new MalformedCarbonCommandException(
                s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property is invalid. " +
                s"Unsupported value: ${ handlerType.get } specified for property $TYPE.")
            }
        }
        try {
          val handlerClass: Class[_] = java.lang.Class.forName(handlerClassName)
          val instance = handlerClass.newInstance().asInstanceOf[CustomIndex[_]]
          instance.init(handler, tableProperties.asJava)
          tableProperties.put(HANDLER_INSTANCE, CustomIndex.getCustomInstance(instance))
        } catch {
          case ex@(_: ClassNotFoundException | _: InstantiationError | _: IllegalAccessException |
                   _: ClassCastException) =>
            val err = s"Carbon ${ CarbonCommonConstants.INDEX_HANDLER } property process failed. "
            LOGGER.error(err, ex)
            throw new MalformedCarbonCommandException(err, ex)
        }
        // Add index handler as a sort column if it is not already present in it.
        CarbonScalaUtil.addIndexHandlerToSortColumns(handler, sources, tableProperties)
        fields += Field(handler, Some("BigInt"), Some(handler), Some(null), index = true)
      }
    }
    fields
  }

  /**
   * This will prepare the Model from the Tree details.
   *
   * @param ifNotExistPresent
   * @param dbName
   * @param tableName
   * @param fields
   * @param partitionCols
   * @param tableProperties
   * @return
   */
  def prepareTableModel(
      ifNotExistPresent: Boolean,
      dbName: Option[String],
      tableName: String,
      fields: Seq[Field],
      partitionCols: Seq[PartitionerField],
      tableProperties: Map[String, String],
      bucketFields: Option[BucketFields],
      isAlterFlow: Boolean = false,
      tableComment: Option[String] = None): TableModel = {

    // Process index handler property
    val indexFields = processIndexProperty(tableProperties, fields)
    val allFields = fields ++ indexFields

    // do not allow below key words as column name
    validateColumnNames(allFields)

    fields.zipWithIndex.foreach { case (field, index) =>
      field.schemaOrdinal = index
    }

    // If sort_scope is not no_sort && sort_columns specified by user is empty, then throw exception
    if (tableProperties.get(CarbonCommonConstants.SORT_COLUMNS).isDefined
        && tableProperties(CarbonCommonConstants.SORT_COLUMNS).equalsIgnoreCase("") &&
        tableProperties.get(CarbonCommonConstants.SORT_SCOPE).isDefined &&
        !tableProperties(CarbonCommonConstants.SORT_SCOPE)
          .equalsIgnoreCase(SortScope.NO_SORT.name())) {
      throw new MalformedCarbonCommandException(
        s"Cannot set SORT_COLUMNS as empty when SORT_SCOPE is ${
          tableProperties(CarbonCommonConstants.SORT_SCOPE)
        } ")
    }
    val (dims, msrs, noDictionaryDims, sortKeyDims, varcharColumns) = extractDimAndMsrFields(
      fields, indexFields, tableProperties)

    // column properties
    val colProps = extractColumnProperties(fields, tableProperties)

    // validate the local dictionary property if defined
    if (tableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE).isDefined) {
      if (!CarbonScalaUtil
        .validateLocalDictionaryEnable(tableProperties(CarbonCommonConstants
          .LOCAL_DICTIONARY_ENABLE))) {
        tableProperties.put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
          CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT)
      }
    } else if (!isAlterFlow) {
      // if LOCAL_DICTIONARY_ENABLE is not defined, try to get from system level property
      tableProperties
        .put(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE,
          CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.LOCAL_DICTIONARY_SYSTEM_ENABLE,
              CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT))
    }

    // validate the local dictionary threshold property if defined
    if (tableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD).isDefined) {
      if (!CarbonScalaUtil
        .validateLocalDictionaryThreshold(tableProperties(CarbonCommonConstants
          .LOCAL_DICTIONARY_THRESHOLD))) {
        LOGGER.debug(
          "invalid value is configured for local_dictionary_threshold, considering the " +
          "default value")
        tableProperties.put(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
          CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT)
      }
    }

    // validate the local dictionary columns defined, this we will validated if the local dictionary
    // is enabled, else it is not validated
    if (tableProperties.get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE).isDefined &&
        tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE).trim
          .equalsIgnoreCase("true")) {
      var localDictIncludeColumns: Seq[String] = Seq[String]()
      var localDictExcludeColumns: Seq[String] = Seq[String]()
      val isLocalDictIncludeDefined = tableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE)
        .isDefined
      val isLocalDictExcludeDefined = tableProperties
        .get(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE)
        .isDefined
      if (isLocalDictIncludeDefined) {
        localDictIncludeColumns =
          tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE).split(",").map(_.trim)
        // validate all the local dictionary include columns
        CarbonScalaUtil
          .validateLocalConfiguredDictionaryColumns(fields,
            tableProperties,
            localDictIncludeColumns)
      }
      if (isLocalDictExcludeDefined) {
        localDictExcludeColumns =
          tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE).split(",").map(_.trim)
        // validate all the local dictionary exclude columns
        CarbonScalaUtil
          .validateLocalConfiguredDictionaryColumns(fields,
            tableProperties,
            localDictExcludeColumns)
      }

      // validate if both local dictionary include and exclude contains same column
      CarbonScalaUtil.validateDuplicateLocalDictIncludeExcludeColmns(tableProperties)
    }

    // get no inverted index columns from table properties.
    val noInvertedIdxCols = extractNoInvertedIndexColumns(fields, tableProperties)
    // get inverted index columns from table properties
    val invertedIdxCols = extractInvertedIndexColumns(fields, tableProperties)

    // Validate if columns present in inverted index are part of sort columns.
    if (invertedIdxCols.nonEmpty) {
      invertedIdxCols.foreach { column =>
        if (!sortKeyDims.contains(column)) {
          val errMsg = "INVERTED_INDEX column: " + column + " should be present in SORT_COLUMNS"
          throw new MalformedCarbonCommandException(errMsg)
        }
      }
    }

    // check for any duplicate columns in inverted and noinverted columns defined in tblproperties
    if (invertedIdxCols.nonEmpty && noInvertedIdxCols.nonEmpty) {
      invertedIdxCols.foreach { distCol =>
        if (noInvertedIdxCols.exists(x => x.equalsIgnoreCase(distCol.trim))) {
          val duplicateColumns = (invertedIdxCols ++ noInvertedIdxCols)
            .diff((invertedIdxCols ++ noInvertedIdxCols).distinct).distinct
          val errMsg = "Column ambiguity as duplicate column(s):" +
                       duplicateColumns.mkString(",") +
                       " is present in INVERTED_INDEX " +
                       "and NO_INVERTED_INDEX. Duplicate columns are not allowed."
          throw new MalformedCarbonCommandException(errMsg)
        }
      }
    }

    if (tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).isDefined) {
      // validate the column_meta_cache option
      val tableColumns = dims.view.filterNot(_.index).map(x => x.name.get) ++
                         msrs.map(x => x.name.get)
      CommonUtil.validateColumnMetaCacheFields(
        dbName.getOrElse(CarbonCommonConstants.DATABASE_DEFAULT_NAME),
        tableName,
        tableColumns,
        tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).get,
        tableProperties)
      val columnsToBeCached = tableProperties.get(CarbonCommonConstants.COLUMN_META_CACHE).get
      if (columnsToBeCached.nonEmpty) {
        columnsToBeCached.split(",").foreach { column =>
          val dimFieldToBeCached = dims.filter(x => x.name.get.equals(column))
          // first element is taken as each column with have a unique name
          // check for complex type column
          if (dimFieldToBeCached.nonEmpty &&
              isComplexType(dimFieldToBeCached(0).dataType.get)) {
            val errorMessage =
              s"$column is a complex type column and complex type is not allowed for " +
              s"the option(s): ${ CarbonCommonConstants.COLUMN_META_CACHE }"
            throw new MalformedCarbonCommandException(errorMessage)
          } else if (dimFieldToBeCached.nonEmpty && DataTypes.BINARY.getName
            .equalsIgnoreCase(dimFieldToBeCached(0).dataType.get)) {
            val errorMessage =
              s"$column is a binary data type column and binary data type is not allowed for " +
              s"the option(s): ${CarbonCommonConstants.COLUMN_META_CACHE}"
            throw new MalformedCarbonCommandException(errorMessage)
          }
        }
      }
    }
    // validate the cache level
    if (tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).isDefined) {
      CommonUtil.validateCacheLevel(
        tableProperties.get(CarbonCommonConstants.CACHE_LEVEL).get,
        tableProperties)
    }
    // long_string_columns columns cannot be in no_inverted_index columns
    var longStringColumns = varcharColumns.map(_.toUpperCase)
    var noInvColIntersecLongStrCols = longStringColumns
      .intersect(noInvertedIdxCols.map(_.toUpperCase))
    if (!noInvColIntersecLongStrCols.isEmpty) {
      throw new MalformedCarbonCommandException(
        s"Column(s): ${
          noInvColIntersecLongStrCols.mkString(",")
        } both in no_inverted_index and long_string_columns which is not allowed.")
    }
    // long_string_columns columns cannot be in partition columns
    var partitionColIntersecLongStrCols = longStringColumns
      .intersect(partitionCols.map(col => col.partitionColumn.toUpperCase))
    if (!partitionColIntersecLongStrCols.isEmpty) {
      throw new MalformedCarbonCommandException(
        s"Column(s): ${
          partitionColIntersecLongStrCols.mkString(",")
        } both in partition and long_string_columns which is not allowed.")
    }
    // validate the block size and blocklet size, page size in table properties
    CommonUtil.validateSize(tableProperties, CarbonCommonConstants.TABLE_BLOCKSIZE)
    CommonUtil.validateSize(tableProperties, CarbonCommonConstants.TABLE_BLOCKLET_SIZE)
    CommonUtil.validatePageSizeInmb(tableProperties, CarbonCommonConstants.TABLE_PAGE_SIZE_INMB)
    // validate table level properties for compaction
    CommonUtil.validateTableLevelCompactionProperties(tableProperties)
    // validate flat folder property.
    CommonUtil.validateFlatFolder(tableProperties)
    // validate load_min_size_inmb property
    CommonUtil.validateLoadMinSize(tableProperties,
      CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB)

    TableModel(
      ifNotExistPresent,
      dbName,
      tableName,
      tableProperties.toMap,
      reorderDimensions(dims.map(f => normalizeType(f)).map(f => addParent(f)), varcharColumns),
      msrs.map(f => normalizeType(f)),
      Option(sortKeyDims),
      Option(varcharColumns),
      Option(noDictionaryDims),
      Option(noInvertedIdxCols),
      Option(invertedIdxCols),
      Some(colProps),
      bucketFields: Option[BucketFields],
      getPartitionInfo(partitionCols),
      tableComment)
  }

  /**
   * This method validates the long string columns, will check:
   * 1.the column in tblproperty long_string_columns must be in table fields.
   * 2.the column datatype in tblproperty long_string_columns should be string.
   * 3.the columns in tblproperty long_string_columns cannot be duplicate
   *
   * @param fields table fields
   * @param varcharCols the columns in tblproperty long_string_columns
   * @return
   */
  private def validateLongStringColumns(fields: Seq[Field],
      varcharCols: Seq[String]): Unit = {
    var longStringColumnsMap: Map[String, Field] = Map[String, Field]()
    fields.foreach(field =>
      longStringColumnsMap.put(field.column.toUpperCase, field)
    )
    var dataTypeErr: Set[String] = Set[String]()
    var duplicateColumnErr: Map[String, Int] = Map[String, Int]()
    var nullColumnErr: Set[String] = Set[String]()
    var tmpStr: String = ""
    varcharCols.foreach {
      column =>
        tmpStr = column.toUpperCase
        duplicateColumnErr.get(tmpStr) match {
          case None => duplicateColumnErr.put(tmpStr, 1)
          case Some(count) => duplicateColumnErr.put(tmpStr, count + 1)
        }
        longStringColumnsMap.get(tmpStr) match {
          case None => nullColumnErr += column
          case Some(field) => if (!DataTypes.STRING.getName.equalsIgnoreCase(field.dataType.get)) {
            dataTypeErr += column
          }
        }
    }
    if (!nullColumnErr.isEmpty) {
      val errMsg = s"long_string_columns: ${
        nullColumnErr.mkString(",")
      } does not exist in table. Please check the create table statement."
      throw new MalformedCarbonCommandException(errMsg)
    }

    var duplicateColumns = duplicateColumnErr.filter(kv => kv._2 != 1).keySet
    if (!duplicateColumns.isEmpty) {
      val errMsg = s"Column ambiguity as duplicate column(s):${
        duplicateColumns.mkString(",")
      } is present in long_string_columns. Duplicate columns are not allowed."
      throw new MalformedCarbonCommandException(errMsg)
    }

    if (dataTypeErr.nonEmpty) {
      val errMsg = s"long_string_columns: ${
        dataTypeErr.mkString(",")
      } ,its data type is not string. Please check the create table statement."
      throw new MalformedCarbonCommandException(errMsg)
    }
  }

  /**
   * @param partitionCols
   */
  protected def getPartitionInfo(partitionCols: Seq[PartitionerField]): Option[PartitionInfo] = {
    if (partitionCols.isEmpty) {
      None
    } else {
      val cols : ArrayBuffer[ColumnSchema] = new ArrayBuffer[ColumnSchema]()
      partitionCols.foreach(partition_col => {
        val columnSchema = new ColumnSchema
        columnSchema.setDataType(DataTypeConverterUtil.
          convertToCarbonType(partition_col.dataType.get))
        columnSchema.setColumnName(partition_col.partitionColumn)
        cols += columnSchema
      })
      Some(new PartitionInfo(cols.asJava, PartitionType.NATIVE_HIVE))
    }
  }

  protected def extractColumnProperties(fields: Seq[Field], tableProperties: Map[String, String]):
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
    fieldChildren.foreach(fields => {
      fields.foreach(field => {
        fillColumnProperty(Some(parent), field.column, tableProperties, colPropMap)
      }
      )
    }
    )
  }

  protected def fillColumnProperty(parentColumnName: Option[String],
      columnName: String,
      tableProperties: Map[String, String],
      colPropMap: java.util.HashMap[String, java.util.List[ColumnProperty]]) {
    val (tblPropKey, colProKey) = getKey(parentColumnName, columnName)
    val colProps = CommonUtil.getColumnProperties(tblPropKey, tableProperties)
    if (colProps.isDefined) {
      colPropMap.put(colProKey, colProps.get)
    }
  }

  def getKey(parentColumnName: Option[String],
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
   * This is still kept for backward compatibility, from carbondata-1.6 onwards,  by default all the
   * dimensions will be no inverted index only
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
          val errorMsg = "NO_INVERTED_INDEX column: " + noInvertedIdxColProp +
                         " does not exist in table. Please check the create table statement."
          throw new MalformedCarbonCommandException(errorMsg)
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

  protected def extractInvertedIndexColumns(fields: Seq[Field],
      tableProperties: Map[String, String]): Seq[String] = {
    // check whether the column name is in fields
    var invertedIdxColsProps: Array[String] = Array[String]()
    var invertedIdxCols: Seq[String] = Seq[String]()

    if (tableProperties.get(CarbonCommonConstants.INVERTED_INDEX).isDefined) {
      invertedIdxColsProps =
        tableProperties(CarbonCommonConstants.INVERTED_INDEX).split(',').map(_.trim)
      invertedIdxColsProps.foreach { invertedIdxColProp =>
        if (!fields.exists(x => x.column.equalsIgnoreCase(invertedIdxColProp))) {
          val errorMsg = "INVERTED_INDEX column: " + invertedIdxColProp +
                         " does not exist in table. Please check the create table statement."
          throw new MalformedCarbonCommandException(errorMsg)
        }
      }
    }
    // check duplicate columns and only 1 col left
    val distinctCols = invertedIdxColsProps.toSet
    // extract the inverted index columns
    fields.foreach(field => {
      if (distinctCols.exists(x => x.equalsIgnoreCase(field.column))) {
        invertedIdxCols :+= field.column
      }
    }
    )
    invertedIdxCols
  }

  /**
   * This will extract the Dimensions and NoDictionary Dimensions fields.
   * By default all string cols are dimensions.
   *
   * @param fields
   * @param indexFields
   * @param tableProperties
   * @return
   */
  protected def extractDimAndMsrFields(fields: Seq[Field], indexFields: Seq[Field],
      tableProperties: Map[String, String]):
  (Seq[Field], Seq[Field], Seq[String], Seq[String], Seq[String]) = {
    var dimFields: LinkedHashSet[Field] = LinkedHashSet[Field]()
    var msrFields: Seq[Field] = Seq[Field]()
    var noDictionaryDims: Seq[String] = Seq[String]()
    var varcharCols: Seq[String] = Seq[String]()

    val allFields = fields ++ indexFields

    // All long_string cols should be there in create table cols and should be of string data type
    if (tableProperties.get(CarbonCommonConstants.LONG_STRING_COLUMNS).isDefined) {
      varcharCols = tableProperties(CarbonCommonConstants.LONG_STRING_COLUMNS)
        .split(",")
        .map(_.trim.toLowerCase)
      validateLongStringColumns(fields, varcharCols)
    }

    // All columns in sortkey should be there in create table cols
    var sortKeyOption = tableProperties.get(CarbonCommonConstants.SORT_COLUMNS)
    if (!sortKeyOption.isDefined) {
      // default no columns are selected for sorting in no_sort scope
      sortKeyOption = Some("")
    }
    val sortKeyString: String = CarbonUtil.unquoteChar(sortKeyOption.get) trim
    var sortKeyDimsTmp: Seq[String] = Seq[String]()
    if (!sortKeyString.isEmpty) {
      val sortKey = sortKeyString.split(',').map(_.trim)
      CommonUtil.validateSortColumns(
        sortKey,
        allFields.map { field => (field.column, field.dataType.get) },
        varcharCols
      )
      sortKey.foreach { dimension =>
        if (!sortKeyDimsTmp.exists(dimension.equalsIgnoreCase)) {
          allFields.foreach { field =>
            if (field.column.equalsIgnoreCase(dimension)) {
              sortKeyDimsTmp :+= field.column
            }
          }
        }
      }
    }

    // range_column should be there in create table cols
    if (tableProperties.get(CarbonCommonConstants.RANGE_COLUMN).isDefined) {
      val rangeColumn = tableProperties.get(CarbonCommonConstants.RANGE_COLUMN).get.trim
      if (rangeColumn.contains(",")) {
        val errorMsg = "range_column not support multiple columns"
        throw new MalformedCarbonCommandException(errorMsg)
      }
      val rangeField = fields.find(_.column.equalsIgnoreCase(rangeColumn))
      val dataType = rangeField.get.dataType.get
      if (rangeField.isEmpty) {
        val errorMsg = "range_column: " + rangeColumn +
                       " does not exist in table. Please check the create table statement."
        throw new MalformedCarbonCommandException(errorMsg)
      } else if (DataTypes.BINARY.getName.equalsIgnoreCase(dataType) ||
                 DataTypes.BOOLEAN.getName.equalsIgnoreCase(dataType) ||
                 CarbonCommonConstants.ARRAY.equalsIgnoreCase(dataType) ||
                 CarbonCommonConstants.STRUCT.equalsIgnoreCase(dataType) ||
                 CarbonCommonConstants.MAP.equalsIgnoreCase(dataType) ||
                 CarbonCommonConstants.DECIMAL.equalsIgnoreCase(dataType)) {
        throw new MalformedCarbonCommandException(
          s"RANGE_COLUMN doesn't support $dataType data type: " + rangeColumn)
      } else {
        tableProperties.put(CarbonCommonConstants.RANGE_COLUMN, rangeField.get.column)
      }
    }

    // Global dictionary is deprecated since 2.0
    if (tableProperties.contains(CarbonCommonConstants.DICTIONARY_INCLUDE) ||
        tableProperties.contains(CarbonCommonConstants.DICTIONARY_EXCLUDE)) {
      DeprecatedFeatureException.globalDictNotSupported()
    }

    // by default consider all String cols as dims and if any dictionary include isn't present then
    // add it to noDictionaryDims list. consider all dictionary excludes/include cols as dims
    allFields.foreach { field =>
      if (field.dataType.get.toUpperCase.equals("TIMESTAMP")) {
        noDictionaryDims :+= field.column
        dimFields += field
      } else if (isDetectAsDimensionDataType(field.dataType.get)) {
        dimFields += field
        // consider all String and binary cols as noDicitonaryDims by default
        if ((DataTypes.STRING.getName.equalsIgnoreCase(field.dataType.get)) ||
            (DataTypes.BINARY.getName.equalsIgnoreCase(field.dataType.get))) {
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

    if (sortKeyDimsTmp.isEmpty) {
      // no SORT_COLUMNS
      tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, "")
    } else {
      tableProperties.put(CarbonCommonConstants.SORT_COLUMNS, sortKeyDimsTmp.mkString(","))
    }
    (dimFields.toSeq, msrFields, noDictionaryDims, sortKeyDimsTmp, varcharCols)
  }

  def isDefaultMeasure(dataType: Option[String]): Boolean = {
    val measureList = Array("DOUBLE", "DECIMAL", "FLOAT")
    measureList.exists(dataType.get.equalsIgnoreCase(_))
  }

  /**
   * detect dimension data type
   *
   * @param dimensionDatatype
   */
  def isDetectAsDimensionDataType(dimensionDatatype: String): Boolean = {
    val dimensionType = Array(
      "string",
      "array",
      "struct",
      "map",
      "timestamp",
      "date",
      "char",
      "binary")
    dimensionType.exists(x => dimensionDatatype.toLowerCase.contains(x))
  }

  /**
   * detects whether it is complex type
   */
  def isComplexType(dimensionDataType: String): Boolean = {
    val dimensionType = Array("array", "struct", "map")
    dimensionType.exists(x => x.equalsIgnoreCase(dimensionDataType))
  }

  /**
   * detects whether datatype is part of sort_column
   */
  private def isDataTypeSupportedForSortColumn(columnDataType: String): Boolean = {
    val dataTypes = Array("array", "struct", "map", "double", "float", "decimal", "binary")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }

  /**
   * detects whether datatype is part of dictionary_exclude
   */
  def isDataTypeSupportedForDictionary_Exclude(columnDataType: String): Boolean = {
    val dataTypes =
      Array("string", "timestamp", "int", "integer", "long", "bigint", "struct", "array",
        "map", "binary")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }

  /**
   * Extract the DbName and table name.
   *
   * @param tableNameParts
   * @return
   */
  protected def extractDbNameTableName(tableNameParts: Node): (Option[String], String) = {
    val (db, tableName) =
      tableNameParts.getChildren.asScala.map {
        case Token(part, Nil) => cleanIdentifier(part)
      } match {
        case Seq(tableOnly) => (None, tableOnly)
        case Seq(databaseName, table) => (Some(convertDbNameToLowerCase(databaseName)), table)
      }

    (db, tableName)
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName
   * @return String
   */
  protected def convertDbNameToLowerCase(dbName: String) = {
    dbName.toLowerCase
  }

  /**
   * This method will convert the database name to lower case
   *
   * @param dbName
   * @return Option of String
   */
  def convertDbNameToLowerCase(dbName: Option[String]): Option[String] = {
    dbName match {
      case Some(databaseName) => Some(convertDbNameToLowerCase(databaseName))
      case None => dbName
    }
  }

  protected def cleanIdentifier(ident: String): String = {
    ident match {
      case escapedIdentifier(i) => i
      case plainIdent => plainIdent
    }
  }

  protected def getClauses(clauseNames: Seq[String], nodeList: Seq[ASTNode]): Seq[Option[Node]] = {
    var remainingNodes = nodeList
    val clauses = clauseNames.map { clauseName =>
      val (matches, nonMatches) = remainingNodes.partition(_.getText.toUpperCase == clauseName)
      remainingNodes = nonMatches ++ (if (matches.nonEmpty) {
        matches.tail
      } else {
        Nil
      })
      matches.headOption
    }

    if (remainingNodes.nonEmpty) {
      CarbonException.analysisException(
        s"""Unhandled clauses:
           |You are likely trying to use an unsupported carbon feature."""".stripMargin)
    }
    clauses
  }

  object Token {
    /** @return matches of the form (tokenName, children). */
    def unapply(t: Any): Option[(String, Seq[ASTNode])] = {
      t match {
        case t: ASTNode =>
          CurrentOrigin.setPosition(t.getLine, t.getCharPositionInLine)
          Some((t.getText,
            Option(t.getChildren).map(_.asScala.toList).getOrElse(Nil).asInstanceOf[Seq[ASTNode]]))
        case _ => None
      }
    }
  }

  /**
   * Extract the table properties token
   *
   * @param node
   * @return
   */
  protected def getProperties(node: Node): Seq[(String, String)] = {
    node match {
      case Token("TOK_TABLEPROPLIST", list) =>
        list.map {
          case Token("TOK_TABLEPROPERTY", Token(key, Nil) :: Token(value, Nil) :: Nil) =>
            (unquoteString(key), unquoteStringWithoutLowerConversion(value))
        }
    }
  }

  protected def unquoteString(str: String) = {
    str match {
      case singleQuotedString(s) => s.toLowerCase()
      case doubleQuotedString(s) => s.toLowerCase()
      case other => other
    }
  }

  protected def unquoteStringWithoutLowerConversion(str: String) = {
    str match {
      case singleQuotedString(s) => s
      case doubleQuotedString(s) => s
      case other => other
    }
  }

  def validateOptions(optionList: Option[List[(String, String)]]): Unit = {

    // validate with all supported options
    val options = optionList.get.groupBy(x => x._1)
    val supportedOptions = Seq("DELIMITER",
      "QUOTECHAR",
      "FILEHEADER",
      "ESCAPECHAR",
      "MULTILINE",
      "COMPLEX_DELIMITER_LEVEL_1",
      "COMPLEX_DELIMITER_LEVEL_2",
      "COMPLEX_DELIMITER_LEVEL_3",
      "SERIALIZATION_NULL_FORMAT",
      "BAD_RECORDS_LOGGER_ENABLE",
      "BAD_RECORDS_ACTION",
      "MAXCOLUMNS",
      "COMMENTCHAR",
      "DATEFORMAT",
      "BAD_RECORD_PATH",
      "GLOBAL_SORT_PARTITIONS",
      "IS_EMPTY_DATA_BAD_RECORD",
      "HEADER",
      "TIMESTAMPFORMAT",
      "SKIP_EMPTY_LINE",
      "SORT_COLUMN_BOUNDS",
      "LOAD_MIN_SIZE_INMB",
      "SCALE_FACTOR",
      "BINARY_DECODER",
      "SORT_SCOPE"
    )
    var isSupported = true
    val invalidOptions = StringBuilder.newBuilder
    options.foreach(value => {
      if (!supportedOptions.exists(x => x.equalsIgnoreCase(value._1))) {
        isSupported = false
        invalidOptions.append(value._1)
      }

    }
    )
    if (!isSupported) {
      val errorMessage = "Error: Invalid option(s): " + invalidOptions.toString()
      throw new MalformedCarbonCommandException(errorMessage)
    }

    // Validate QUOTECHAR length
    if (options.exists(_._1.equalsIgnoreCase("QUOTECHAR"))) {
      val quoteChar: String = options.get("quotechar").get.head._2
      if (quoteChar.length > 1 ) {
        throw new MalformedCarbonCommandException("QUOTECHAR cannot be more than one character.")
      }
    }

    // Validate COMMENTCHAR length
    if (options.exists(_._1.equalsIgnoreCase("COMMENTCHAR"))) {
      val commentChar: String = options.get("commentchar").get.head._2
      if (commentChar.length > 1) {
        throw new MalformedCarbonCommandException("COMMENTCHAR cannot be more than one character.")
      }
    }

    // Validate ESCAPECHAR length
    if (options.exists(_._1.equalsIgnoreCase("ESCAPECHAR"))) {
      val escapeChar: String = options.get("escapechar").get.head._2
      if (escapeChar.length > 1 && !CarbonLoaderUtil.isValidEscapeSequence(escapeChar)) {
        throw new MalformedCarbonCommandException("ESCAPECHAR cannot be more than one character.")
      }
    }

    if (options.exists(_._1.equalsIgnoreCase("MAXCOLUMNS"))) {
      val maxColumns: String = options("maxcolumns").head._2
      try {
        maxColumns.toInt
      } catch {
        case _: NumberFormatException =>
          throw new MalformedCarbonCommandException(
            "option MAXCOLUMNS can only contain integer values")
      }
    }

    if (options.exists(_._1.equalsIgnoreCase("BAD_RECORDS_LOGGER_ENABLE"))) {
      val optionValue: String = options("bad_records_logger_enable").head._2
      val isValid = CarbonUtil.validateBoolean(optionValue)
      if (!isValid) throw new MalformedCarbonCommandException(
        "option BAD_RECORDS_LOGGER_ENABLE can have only either TRUE or FALSE, " +
        "It shouldn't be " + optionValue)
    }

    if (options.exists(_._1.equalsIgnoreCase("BAD_RECORDS_ACTION"))) {
      val optionValue: String = options("bad_records_action").head._2
      try {
        LoggerAction.valueOf(optionValue.toUpperCase)
      }
      catch {
        case _: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(
            "option BAD_RECORDS_ACTION can have only either FORCE or IGNORE or REDIRECT or FAIL")
      }
    }
    if (options.exists(_._1.equalsIgnoreCase("IS_EMPTY_DATA_BAD_RECORD"))) {
      val optionValue: String = options("is_empty_data_bad_record").head._2
      if (!("true".equalsIgnoreCase(optionValue) || "false".equalsIgnoreCase(optionValue))) {
        throw new MalformedCarbonCommandException(
          "option IS_EMPTY_DATA_BAD_RECORD can have option either true or false")
      }
    }

    if (options.exists(_._1.equalsIgnoreCase("SKIP_EMPTY_LINE"))) {
      val optionValue: String = options.get("skip_empty_line").get.head._2
      if (!("true".equalsIgnoreCase(optionValue) || "false".equalsIgnoreCase(optionValue))) {
        throw new MalformedCarbonCommandException(
          "option SKIP_EMPTY_LINE can have option either true or false")
      }
    }

    // Validate SORT_SCOPE
    if (options.exists(_._1.equalsIgnoreCase("SORT_SCOPE"))) {
      val optionValue: String = options.get("sort_scope").get.head._2
      if (!CarbonUtil.isValidSortOption(optionValue)) {
        throw new InvalidConfigurationException(
          s"Passing invalid SORT_SCOPE '$optionValue', valid SORT_SCOPE are 'NO_SORT'," +
          s" 'LOCAL_SORT' and 'GLOBAL_SORT' ")
      }
    }

    // check for duplicate options
    val duplicateOptions = options filter {
      case (_, optionList) => optionList.size > 1
    }
    val duplicates = StringBuilder.newBuilder
    if (duplicateOptions.nonEmpty) {
      duplicateOptions.foreach(x => {
        duplicates.append(x._1)
      }
      )
      val errorMessage = "Error: Duplicate option(s): " + duplicates.toString()
      throw new MalformedCarbonCommandException(errorMessage)
    }
  }

  private def normalizeType(field: Field): Field = {
    val dataType = field.dataType.getOrElse("NIL")
    dataType match {
      case "string" =>
        Field(field.column, Some("String"), field.name, Some(null), field.parent,
          field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
          field.columnComment)
      case "smallint" =>
        Field(field.column, Some("SmallInt"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "integer" | "int" =>
        Field(field.column, Some("Integer"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "long" => Field(field.column, Some("Long"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
      case "double" => Field(field.column, Some("Double"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
      case "float" => Field(field.column, Some("Double"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
      case "timestamp" =>
        Field(field.column, Some("Timestamp"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "date" =>
        Field(field.column, Some("Date"), field.name, Some(null),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "numeric" => Field(field.column, Some("Numeric"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
      case "array" =>
        Field(field.column, Some("Array"), field.name,
          field.children.map(f => f.map(normalizeType(_))),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "struct" =>
        Field(field.column, Some("Struct"), field.name,
          field.children.map(f => f.map(normalizeType(_))),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "map" =>
        Field(field.column, Some("Map"), field.name,
          field.children.map(f => f.map(normalizeType(_))),
          field.parent, field.storeType, field.schemaOrdinal,
          field.precision, field.scale, field.rawSchema, field.columnComment)
      case "bigint" => Field(field.column, Some("BigInt"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment, field.index)
      case "decimal" => Field(field.column, Some("Decimal"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
      case "boolean" => Field(field.column, Some("Boolean"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
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
          field.rawSchema,
          field.columnComment
        )
      case _ =>
        field
    }
  }

  private def addParent(field: Field): Field = {
    field.dataType.getOrElse("NIL") match {
      case "Array" => Field(field.column, Some("Array"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType, field.schemaOrdinal, rawSchema = field.rawSchema,
        columnComment = field.columnComment)
      case "Struct" => Field(field.column, Some("Struct"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType, field.schemaOrdinal, rawSchema = field.rawSchema,
        columnComment = field.columnComment)
      case "Map" => Field(field.column, Some("Map"), field.name,
        field.children.map(f => f.map(appendParentForEachChild(_, field.column))), field.parent,
        field.storeType, field.schemaOrdinal, rawSchema = field.rawSchema,
        columnComment = field.columnComment)
      case _ => field
    }
  }

  private def appendParentForEachChild(field: Field, parentName: String): Field = {
    field.dataType.getOrElse("NIL") match {
      case "String" => Field(parentName + "." + field.column, Some("String"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "binary" => Field(parentName + "." + field.column, Some("Binary"),
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
      case "Date" => Field(parentName + "." + field.column, Some("Date"),
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
      case "Map" => Field(parentName + "." + field.column, Some("Map"),
        Some(parentName + "." + field.name.getOrElse(None)),
        field.children
          .map(f => f.map(appendParentForEachChild(_, parentName + "." + field.column))),
        parentName)
      case "BigInt" => Field(parentName + "." + field.column, Some("BigInt"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case "Decimal" => Field(parentName + "." + field.column, Some("Decimal"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName,
        field.storeType, field.schemaOrdinal, field.precision, field.scale)
      case "Boolean" => Field(parentName + "." + field.column, Some("Boolean"),
        Some(parentName + "." + field.name.getOrElse(None)), Some(null), parentName)
      case _ => field
    }
  }

  /**
   * This method will parse the given data type and validate against the allowed data types
   *
   * @param dataType datatype string given by the user in DDL
   * @param values values defined when the decimal datatype is given in DDL
   * @return DataTypeInfo object with datatype, precision and scale
   */
  def parseDataType(
      dataType: String,
      values: Option[List[(Int, Int)]],
      isColumnRename: Boolean): DataTypeInfo = {
    var precision: Int = 0
    var scale: Int = 0
    dataType match {
      case "bigint" | "long" =>
        if (values.isDefined) {
          throw new MalformedCarbonCommandException("Invalid data type")
        }
        DataTypeInfo(DataTypeConverterUtil.convertToCarbonType(dataType).getName.toLowerCase)
      case "decimal" =>
        if (values.isDefined) {
          precision = values.get(0)._1
          scale = values.get(0)._2
        } else {
          throw new MalformedCarbonCommandException("Decimal format provided is invalid")
        }
        // precision should be > 0 and <= 38 and scale should be >= 0 and <= 38
        if (precision < 1 || precision > 38) {
          throw new MalformedCarbonCommandException("Invalid value for precision")
        } else if (scale < 0 || scale > 38) {
          throw new MalformedCarbonCommandException("Invalid value for scale")
        }
        DataTypeInfo("decimal", precision, scale)
      case _ =>
        if (isColumnRename) {
          DataTypeInfo(DataTypeConverterUtil.convertToCarbonType(dataType).getName.toLowerCase)
        } else {
          throw new MalformedCarbonCommandException("Data type provided is invalid.")
        }
    }
  }

  def checkFieldDefaultValue(fieldName: String, defaultValueColumnName: String): Boolean = {
    defaultValueColumnName.equalsIgnoreCase("default.value." + fieldName)
  }

  def convertFieldNamesToLowercase(field: Field): Field = {
    val name = field.column.toLowerCase
    field.copy(column = name, name = Some(name))
  }
}
