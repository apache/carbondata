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

import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet, Map}
import scala.language.implicitConversions
import scala.util.matching.Regex

import org.apache.hadoop.hive.ql.lib.Node
import org.apache.hadoop.hive.ql.parse._
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.constants.LoggerAction
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.util.{CommonUtil, DataTypeConverterUtil}

/**
 * TODO remove the duplicate code and add the common methods to common class.
 * Parser for All Carbon DDL cases
 */
abstract class CarbonDDLSqlParser extends AbstractCarbonSparkSQLParser {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  protected val AGGREGATE = carbonKeyWord("AGGREGATE")
  protected val AS = carbonKeyWord("AS")
  protected val AGGREGATION = carbonKeyWord("AGGREGATION")
  protected val ALL = carbonKeyWord("ALL")
  protected val HIGH_CARDINALITY_DIMS = carbonKeyWord("NO_DICTIONARY")
  protected val BEFORE = carbonKeyWord("BEFORE")
  protected val BY = carbonKeyWord("BY")
  protected val CARDINALITY = carbonKeyWord("CARDINALITY")
  protected val CASCADE = carbonKeyWord("CASCADE")
  protected val CLASS = carbonKeyWord("CLASS")
  protected val CLEAN = carbonKeyWord("CLEAN")
  protected val COLS = carbonKeyWord("COLS")
  protected val COLUMNS = carbonKeyWord("COLUMNS")
  protected val COMPACT = carbonKeyWord("COMPACT")
  protected val FINISH = carbonKeyWord("FINISH")
  protected val STREAMING = carbonKeyWord("STREAMING")
  protected val CREATE = carbonKeyWord("CREATE")
  protected val CUBE = carbonKeyWord("CUBE")
  protected val CUBES = carbonKeyWord("CUBES")
  protected val DATA = carbonKeyWord("DATA")
  protected val DATABASE = carbonKeyWord("DATABASE")
  protected val DATABASES = carbonKeyWord("DATABASES")
  protected val DELETE = carbonKeyWord("DELETE")
  protected val DELIMITER = carbonKeyWord("DELIMITER")
  protected val DESCRIBE = carbonKeyWord("DESCRIBE")
  protected val DESC = carbonKeyWord("DESC")
  protected val DETAIL = carbonKeyWord("DETAIL")
  protected val DIMENSIONS = carbonKeyWord("DIMENSIONS")
  protected val DIMFOLDERPATH = carbonKeyWord("DIMFOLDERPATH")
  protected val DROP = carbonKeyWord("DROP")
  protected val ESCAPECHAR = carbonKeyWord("ESCAPECHAR")
  protected val EXCLUDE = carbonKeyWord("EXCLUDE")
  protected val EXPLAIN = carbonKeyWord("EXPLAIN")
  protected val EXTENDED = carbonKeyWord("EXTENDED")
  protected val FORMATTED = carbonKeyWord("FORMATTED")
  protected val FACT = carbonKeyWord("FACT")
  protected val FIELDS = carbonKeyWord("FIELDS")
  protected val FILEHEADER = carbonKeyWord("FILEHEADER")
  protected val SERIALIZATION_NULL_FORMAT = carbonKeyWord("SERIALIZATION_NULL_FORMAT")
  protected val BAD_RECORDS_LOGGER_ENABLE = carbonKeyWord("BAD_RECORDS_LOGGER_ENABLE")
  protected val BAD_RECORDS_ACTION = carbonKeyWord("BAD_RECORDS_ACTION")
  protected val IS_EMPTY_DATA_BAD_RECORD = carbonKeyWord("IS_EMPTY_DATA_BAD_RECORD")
  protected val IS_EMPTY_COMMA_DATA_BAD_RECORD = carbonKeyWord("IS_NULL_DATA_BAD_RECORD")
  protected val SKIP_EMPTY_LINE = carbonKeyWord("SKIP_EMPTY_LINE")
  protected val FILES = carbonKeyWord("FILES")
  protected val FROM = carbonKeyWord("FROM")
  protected val HIERARCHIES = carbonKeyWord("HIERARCHIES")
  protected val IN = carbonKeyWord("IN")
  protected val INCLUDE = carbonKeyWord("INCLUDE")
  protected val INPATH = carbonKeyWord("INPATH")
  protected val INTO = carbonKeyWord("INTO")
  protected val LEVELS = carbonKeyWord("LEVELS")
  protected val LIKE = carbonKeyWord("LIKE")
  protected val LOAD = carbonKeyWord("LOAD")
  protected val LOCAL = carbonKeyWord("LOCAL")
  protected val MAPPED = carbonKeyWord("MAPPED")
  protected val MEASURES = carbonKeyWord("MEASURES")
  protected val MERGE = carbonKeyWord("MERGE")
  protected val MULTILINE = carbonKeyWord("MULTILINE")
  protected val COMPLEX_DELIMITER_LEVEL_1 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_1")
  protected val COMPLEX_DELIMITER_LEVEL_2 = carbonKeyWord("COMPLEX_DELIMITER_LEVEL_2")
  protected val OPTIONS = carbonKeyWord("OPTIONS")
  protected val OUTPATH = carbonKeyWord("OUTPATH")
  protected val OVERWRITE = carbonKeyWord("OVERWRITE")
  protected val PARTITION = carbonKeyWord("PARTITION")
  protected val PARTITION_COUNT = carbonKeyWord("PARTITION_COUNT")
  protected val PARTITIONDATA = carbonKeyWord("PARTITIONDATA")
  protected val PARTITIONER = carbonKeyWord("PARTITIONER")
  protected val PARTITIONS = carbonKeyWord("PARTITIONS")
  protected val QUOTECHAR = carbonKeyWord("QUOTECHAR")
  protected val RELATION = carbonKeyWord("RELATION")
  protected val SCHEMA = carbonKeyWord("SCHEMA")
  protected val SCHEMAS = carbonKeyWord("SCHEMAS")
  protected val SET = Keyword("SET")
  protected val SHOW = carbonKeyWord("SHOW")
  protected val SPLIT = carbonKeyWord("SPLIT")
  protected val TABLES = carbonKeyWord("TABLES")
  protected val TABLE = carbonKeyWord("TABLE")
  protected val TERMINATED = carbonKeyWord("TERMINATED")
  protected val TYPE = carbonKeyWord("TYPE")
  protected val UPDATE = carbonKeyWord("UPDATE")
  protected val USE = carbonKeyWord("USE")
  protected val WHERE = Keyword("WHERE")
  protected val WITH = carbonKeyWord("WITH")
  protected val AGGREGATETABLE = carbonKeyWord("AGGREGATETABLE")
  protected val ABS = carbonKeyWord("abs")

  protected val FOR = carbonKeyWord("FOR")
  protected val SCRIPTS = carbonKeyWord("SCRIPTS")
  protected val USING = carbonKeyWord("USING")
  protected val LIMIT = carbonKeyWord("LIMIT")
  protected val DEFAULTS = carbonKeyWord("DEFAULTS")
  protected val ALTER = carbonKeyWord("ALTER")
  protected val ADD = carbonKeyWord("ADD")

  protected val IF = carbonKeyWord("IF")
  protected val NOT = carbonKeyWord("NOT")
  protected val EXISTS = carbonKeyWord("EXISTS")
  protected val DIMENSION = carbonKeyWord("DIMENSION")
  protected val STARTTIME = carbonKeyWord("STARTTIME")
  protected val SEGMENTS = carbonKeyWord("SEGMENTS")
  protected val SEGMENT = carbonKeyWord("SEGMENT")

  protected val STRING = carbonKeyWord("STRING")
  protected val INTEGER = carbonKeyWord("INTEGER")
  protected val TIMESTAMP = carbonKeyWord("TIMESTAMP")
  protected val DATE = carbonKeyWord("DATE")
  protected val CHAR = carbonKeyWord("CHAR")
  protected val VARCHAR = carbonKeyWord("VARCHAR")
  protected val NUMERIC = carbonKeyWord("NUMERIC")
  protected val DECIMAL = carbonKeyWord("DECIMAL")
  protected val DOUBLE = carbonKeyWord("DOUBLE")
  protected val FLOAT = carbonKeyWord("FLOAT")
  protected val SHORT = carbonKeyWord("SHORT")
  protected val INT = carbonKeyWord("INT")
  protected val BOOLEAN = carbonKeyWord("BOOLEAN")
  protected val LONG = carbonKeyWord("LONG")
  protected val BIGINT = carbonKeyWord("BIGINT")
  protected val ARRAY = carbonKeyWord("ARRAY")
  protected val STRUCT = carbonKeyWord("STRUCT")
  protected val SMALLINT = carbonKeyWord("SMALLINT")
  protected val CHANGE = carbonKeyWord("CHANGE")
  protected val TBLPROPERTIES = carbonKeyWord("TBLPROPERTIES")
  protected val ID = carbonKeyWord("ID")
  protected val DATAMAP = carbonKeyWord("DATAMAP")
  protected val ON = carbonKeyWord("ON")
  protected val DMPROPERTIES = carbonKeyWord("DMPROPERTIES")
  protected val SELECT = carbonKeyWord("SELECT")

  protected val doubleQuotedString = "\"([^\"]+)\"".r
  protected val singleQuotedString = "'([^']+)'".r

  protected val newReservedWords =
    this.getClass
      .getMethods
      .filter(_.getReturnType == classOf[Keyword])
      .map(_.invoke(this).asInstanceOf[Keyword].str)

  override val lexical = {
    val sqllex = new SqlLexical()
    sqllex.initialize(newReservedWords)
    sqllex

  }

  import lexical.Identifier

  implicit def regexToParser(regex: Regex): Parser[String] = {
    acceptMatch(
    s"identifier matching regex ${ regex }",
    { case Identifier(str) if regex.unapplySeq(str).isDefined => str }
    )
  }

  /**
   * This will convert key word to regular expression.
   *
   * @param keys
   * @return
   */
  private def carbonKeyWord(keys: String) = {
    ("(?i)" + keys).r
  }

  protected val escapedIdentifier = "`([^`]+)`".r

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
   * This will prepate the Model from the Tree details.
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

    // do not allow below key words as column name
    validateColumnNames(fields)

    fields.zipWithIndex.foreach { case (field, index) =>
      field.schemaOrdinal = index
    }
    val (dims, msrs, noDictionaryDims, sortKeyDims) = extractDimAndMsrFields(
      fields, tableProperties)

    // column properties
    val colProps = extractColumnProperties(fields, tableProperties)
    // get column groups configuration from table properties.
    val groupCols: Seq[String] = updateColumnGroupsInField(tableProperties,
      noDictionaryDims, msrs, dims)
    if (groupCols != null) {
      throw new MalformedCarbonCommandException(
        s"${CarbonCommonConstants.COLUMN_GROUPS} is deprecated")
    }

    // get no inverted index columns from table properties.
    val noInvertedIdxCols = extractNoInvertedIndexColumns(fields, tableProperties)
    // get partitionInfo
    val partitionInfo = getPartitionInfo(partitionCols, tableProperties)

    // validate the tableBlockSize from table properties
    CommonUtil.validateTableBlockSize(tableProperties)
    // validate table level properties for compaction
    CommonUtil.validateTableLevelCompactionProperties(tableProperties)

    TableModel(
      ifNotExistPresent,
      dbName,
      tableName,
      tableProperties.toMap,
      reorderDimensions(dims.map(f => normalizeType(f)).map(f => addParent(f))),
      msrs.map(f => normalizeType(f)),
      Option(sortKeyDims),
      Option(noDictionaryDims),
      Option(noInvertedIdxCols),
      groupCols,
      Some(colProps),
      bucketFields: Option[BucketFields],
      partitionInfo,
      tableComment)
  }

  /**
   * Extract the column groups configuration from table properties.
   * Based on this Row groups of fields will be determined.
   *
   * @param tableProperties
   * @return
   */
  protected def updateColumnGroupsInField(tableProperties: mutable.Map[String, String],
      noDictionaryDims: Seq[String],
      msrs: Seq[Field],
      dims: Seq[Field]): Seq[String] = {
    if (tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS).isDefined) {

      var splittedColGrps: Seq[String] = Seq[String]()
      val nonSplitCols: String = tableProperties.get(CarbonCommonConstants.COLUMN_GROUPS).get

      // row groups will be specified in table properties like -> "(col1,col2),(col3,col4)"
      // here first splitting the value by () . so that the above will be splitted into 2 strings.
      // [col1,col2] [col3,col4]
      val m: Matcher = Pattern.compile("\\(([^)]+)\\)").matcher(nonSplitCols)
      while (m.find()) {
        val oneGroup: String = m.group(1)
        CommonUtil.validateColumnGroup(oneGroup, noDictionaryDims, msrs, splittedColGrps, dims)
        val arrangedColGrp = rearrangedColumnGroup(oneGroup, dims)
        splittedColGrps :+= arrangedColGrp
      }
      // This will  be furthur handled.
      CommonUtil.arrangeColGrpsInSchemaOrder(splittedColGrps, dims)
    } else {
      null
    }
  }

  def rearrangedColumnGroup(colGroup: String, dims: Seq[Field]): String = {
    // if columns in column group is not in schema order than arrange it in schema order
    var colGrpFieldIndx: Seq[Int] = Seq[Int]()
    colGroup.split(',').map(_.trim).foreach { x =>
      dims.zipWithIndex.foreach { dim =>
        if (dim._1.column.equalsIgnoreCase(x)) {
          colGrpFieldIndx :+= dim._2
        }
      }
    }
    // sort it
    colGrpFieldIndx = colGrpFieldIndx.sorted
    // check if columns in column group is in schema order
    if (!checkIfInSequence(colGrpFieldIndx)) {
      throw new MalformedCarbonCommandException("Invalid column group:" + colGroup)
    }
    def checkIfInSequence(colGrpFieldIndx: Seq[Int]): Boolean = {
      for (i <- 0 until (colGrpFieldIndx.length - 1)) {
        if ((colGrpFieldIndx(i + 1) - colGrpFieldIndx(i)) != 1) {
          throw new MalformedCarbonCommandException(
            "Invalid column group,column in group should be contiguous as per schema.")
        }
      }
      true
    }
    val colGrpNames: StringBuilder = StringBuilder.newBuilder
    for (i <- colGrpFieldIndx.indices) {
      colGrpNames.append(dims(colGrpFieldIndx(i)).column)
      if (i < (colGrpFieldIndx.length - 1)) {
        colGrpNames.append(",")
      }
    }
    colGrpNames.toString()
  }

  /**
   * @param partitionCols
   * @param tableProperties
   */
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
        case _ => partitionInfo = new PartitionInfo(cols.asJava, PartitionType.NATIVE_HIVE)
          partitionInfo.setListInfo(listInfo.map(_.asJava).asJava)
      }
      Some(partitionInfo)
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
        tableProperties.get(CarbonCommonConstants.NO_INVERTED_INDEX).get.split(',').map(_.trim)
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
        tableProperties.get(CarbonCommonConstants.DICTIONARY_EXCLUDE).get.split(',').map(_.trim)
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
      } else if (field.dataType.get.toUpperCase.equals("TIMESTAMP") &&
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

  def isDefaultMeasure(dataType: Option[String]): Boolean = {
    val measureList = Array("DOUBLE", "DECIMAL", "FLOAT")
    measureList.exists(dataType.get.equalsIgnoreCase(_))
  }

  /**
   * It fills non string dimensions in dimFields
   */
  def fillNonStringDimension(dictIncludeCols: Seq[String],
      field: Field, dimFields: LinkedHashSet[Field]) {
    var dictInclude = false
    if (dictIncludeCols.nonEmpty) {
      dictIncludeCols.foreach(dictIncludeCol =>
        if (field.column.equalsIgnoreCase(dictIncludeCol)) {
          dictInclude = true
        })
    }
    if (dictInclude) {
      dimFields += field
    }
  }

  /**
   * detect dimention data type
   *
   * @param dimensionDatatype
   */
  def isDetectAsDimentionDatatype(dimensionDatatype: String): Boolean = {
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
  def isDataTypeSupportedForDictionary_Exclude(columnDataType: String): Boolean = {
    val dataTypes = Array("string", "timestamp", "int", "long", "bigint")
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
            val reslovedKey = unquoteString(key)
            if (needToConvertToLowerCase(reslovedKey)) {
              (reslovedKey, unquoteString(value))
            } else {
              (reslovedKey, unquoteStringWithoutLowerConversion(value))
            }
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

  private def needToConvertToLowerCase(key: String): Boolean = {
    val noConvertList = Array("LIST_INFO", "RANGE_INFO")
    !noConvertList.exists(x => x.equalsIgnoreCase(key))
  }

  protected def validateOptions(optionList: Option[List[(String, String)]]): Unit = {

    // validate with all supported options
    val options = optionList.get.groupBy(x => x._1)
    val supportedOptions = Seq("DELIMITER", "QUOTECHAR", "FILEHEADER", "ESCAPECHAR", "MULTILINE",
      "COMPLEX_DELIMITER_LEVEL_1", "COMPLEX_DELIMITER_LEVEL_2", "COLUMNDICT",
      "SERIALIZATION_NULL_FORMAT", "BAD_RECORDS_LOGGER_ENABLE", "BAD_RECORDS_ACTION",
      "ALL_DICTIONARY_PATH", "MAXCOLUMNS", "COMMENTCHAR", "DATEFORMAT", "BAD_RECORD_PATH",
      "BATCH_SORT_SIZE_INMB", "GLOBAL_SORT_PARTITIONS", "SINGLE_PASS",
      "IS_EMPTY_DATA_BAD_RECORD", "HEADER", "TIMESTAMPFORMAT", "SKIP_EMPTY_LINE",
      "SORT_COLUMN_BOUNDS"
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
      val escapechar: String = options.get("escapechar").get.head._2
      if (escapechar.length > 1 && !CarbonLoaderUtil.isValidEscapeSequence(escapechar)) {
        throw new MalformedCarbonCommandException("ESCAPECHAR cannot be more than one character.")
      }
    }

    //  COLUMNDICT and ALL_DICTIONARY_PATH can not be used together.
    if (options.exists(_._1.equalsIgnoreCase("COLUMNDICT")) &&
        options.exists(_._1.equalsIgnoreCase("ALL_DICTIONARY_PATH"))) {
      val errorMessage = "Error: COLUMNDICT and ALL_DICTIONARY_PATH can not be used together" +
                         " in options"
      throw new MalformedCarbonCommandException(errorMessage)
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

    if (options.exists(_._1.equalsIgnoreCase("BAD_RECORDS_ACTION"))) {
      val optionValue: String = options("bad_records_action").head._2
      try {
        LoggerAction.valueOf(optionValue.toUpperCase)
      }
      catch {
        case _: IllegalArgumentException =>
          throw new MalformedCarbonCommandException(
            "option BAD_RECORDS_ACTION can have only either FORCE or IGNORE or REDIRECT")
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

    // check for duplicate options
    val duplicateOptions = options filter {
      case (_, optionlist) => optionlist.size > 1
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

  protected lazy val dbTableIdentifier: Parser[Seq[String]] =
    (ident <~ ".").? ~ ident ^^ {
      case databaseName ~ tableName =>
        if (databaseName.isDefined) {
          Seq(databaseName.get, tableName)
        } else {
          Seq(tableName)
        }
    }

  protected lazy val loadOptions: Parser[(String, String)] =
    (stringLit <~ "=") ~ stringLit ^^ {
      case opt ~ optvalue => (opt.trim.toLowerCase(), optvalue)
      case _ => ("", "")
    }

  protected lazy val partitions: Parser[(String, Option[String])] =
    (ident <~ "=".?) ~ stringLit.? ^^ {
      case opt ~ optvalue => (opt.trim, optvalue)
      case _ => ("", None)
    }

  protected lazy val valueOptions: Parser[(Int, Int)] =
    (numericLit <~ ",") ~ numericLit ^^ {
      case opt ~ optvalue => (opt.toInt, optvalue.toInt)
      case _ => (0, 0)
    }

  protected lazy val columnOptions: Parser[(String, String)] =
    (stringLit <~ ",") ~ stringLit ^^ {
      case opt ~ optvalue => (opt, optvalue)
      case _ =>
        throw new MalformedCarbonCommandException(s"value cannot be empty")
    }

  protected lazy val dimCol: Parser[Field] = anyFieldDef

  protected lazy val primitiveTypes =
    STRING ^^^ "string" |BOOLEAN ^^^ "boolean" | INTEGER ^^^ "integer" |
    TIMESTAMP ^^^ "timestamp" | NUMERIC ^^^ "numeric" |
    (LONG | BIGINT) ^^^ "bigint" | (SHORT | SMALLINT) ^^^ "smallint" |
    INT ^^^ "int" | DOUBLE ^^^ "double" | FLOAT ^^^ "double" | decimalType |
    DATE ^^^ "date" | charType

  /**
   * Matching the char data type and returning the same.
   */
  private lazy val charType =
    (CHAR | VARCHAR ) ~ opt("(" ~>numericLit <~ ")") ^^ {
      case (char ~ _) =>
        s"$char"
    }

  /**
   * Matching the decimal(10,0) data type and returning the same.
   */
  private lazy val decimalType =
  DECIMAL ~ (("(" ~> numericLit <~ ",") ~ (numericLit <~ ")")).? ^^ {
    case decimal ~ precisionAndScale => if (precisionAndScale.isDefined) {
      s"decimal(${ precisionAndScale.get._1 }, ${ precisionAndScale.get._2 })"
    } else {
      s"decimal(10,0)"
    }
  }

  protected lazy val nestedType: Parser[Field] = structFieldType | arrayFieldType |
                                                 primitiveFieldType

  lazy val anyFieldDef: Parser[Field] =
    (ident | stringLit) ~ (":".? ~> nestedType) ~ (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 =>
        Field(e1, e2.dataType, Some(e1), e2.children, null, e3)
    }

  lazy val addPreAgg: Parser[String] =
    SELECT ~> restInput <~ opt(";") ^^ {
      case query =>
        "select preAGG() as preAgg, " + query
    }

  lazy val addPreAggLoad: Parser[String] =
    SELECT ~> restInput <~ opt(";") ^^ {
      case query =>
        "select preAggLoad() as preAggLoad, " + query
    }

  protected lazy val primitiveFieldType: Parser[Field] =
    primitiveTypes ^^ {
      case e1 =>
        Field("unknown", Some(e1), Some("unknown"), Some(null))
    }

  protected lazy val arrayFieldType: Parser[Field] =
    ((ARRAY ^^^ "array") ~> "<" ~> nestedType <~ ">") ^^ {
      case e1 =>
        Field("unknown", Some("array"), Some("unknown"),
          Some(List(Field("val", e1.dataType, Some("val"),
            e1.children))))
    }

  protected lazy val structFieldType: Parser[Field] =
    ((STRUCT ^^^ "struct") ~> "<" ~> repsep(anyFieldDef, ",") <~ ">") ^^ {
      case e1 =>
        Field("unknown", Some("struct"), Some("unknown"), Some(e1))
    }

  protected lazy val measureCol: Parser[Field] =
    (ident | stringLit) ~ (INTEGER ^^^ "integer" | NUMERIC ^^^ "numeric" | SHORT ^^^ "smallint" |
                           BIGINT ^^^ "bigint" | DECIMAL ^^^ "decimal").? ~
    (AS ~> (ident | stringLit)).? ~ (IN ~> (ident | stringLit)).? ^^ {
      case e1 ~ e2 ~ e3 ~ e4 => Field(e1, e2, e3, Some(null))
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
      case "bigint" => Field(field.column, Some("BigInt"), field.name, Some(null), field.parent,
        field.storeType, field.schemaOrdinal, field.precision, field.scale, field.rawSchema,
        field.columnComment)
      case "decimal" => Field(field.column, Some("Decimal"), field.name, Some(null), field.parent,
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

  protected lazy val segmentId: Parser[String] =
    numericLit ^^ { u => u } |
    elem("decimal", p => {
      p.getClass.getSimpleName.equals("FloatLit") ||
      p.getClass.getSimpleName.equals("DecimalLit")
    }) ^^ (_.chars)

  /**
   * This method will parse the given data type and validate against the allowed data types
   *
   * @param dataType
   * @param values
   * @return
   */
  def parseDataType(dataType: String, values: Option[List[(Int, Int)]]): DataTypeInfo = {
    dataType match {
      case "bigint" | "long" =>
        if (values.isDefined) {
          throw new MalformedCarbonCommandException("Invalid data type")
        }
        DataTypeInfo(dataType)
      case "decimal" =>
        var precision: Int = 0
        var scale: Int = 0
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
        throw new MalformedCarbonCommandException("Data type provided is invalid.")
    }
  }
}
