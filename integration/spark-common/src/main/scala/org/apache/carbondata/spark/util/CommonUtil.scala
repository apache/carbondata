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

package org.apache.carbondata.spark.util

import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.Map

import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.sql.execution.command.{ColumnProperty, Field, PartitionerField}
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.processing.csvload.CSVInputFormat
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.newflow.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.spark.exception.MalformedCarbonCommandException

object CommonUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_DECIMALTYPE = """decimaltype\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  def validateColumnGroup(colGroup: String, noDictionaryDims: Seq[String],
      msrs: Seq[Field], retrievedColGrps: Seq[String], dims: Seq[Field]) {
    val colGrpCols = colGroup.split(',').map(_.trim)
    colGrpCols.foreach { x =>
      // if column is no dictionary
      if (noDictionaryDims.contains(x)) {
        throw new MalformedCarbonCommandException(
          "Column group is not supported for no dictionary columns:" + x)
      } else if (msrs.exists(msr => msr.column.equals(x))) {
        // if column is measure
        throw new MalformedCarbonCommandException("Column group is not supported for measures:" + x)
      } else if (foundIndExistingColGrp(x)) {
        throw new MalformedCarbonCommandException("Column is available in other column group:" + x)
      } else if (isComplex(x, dims)) {
        throw new MalformedCarbonCommandException(
          "Column group doesn't support Complex column:" + x)
      } else if (isTimeStampColumn(x, dims)) {
        throw new MalformedCarbonCommandException(
          "Column group doesn't support Timestamp datatype:" + x)
      }// if invalid column is
      else if (!dims.exists(dim => dim.column.equalsIgnoreCase(x))) {
        // present
        throw new MalformedCarbonCommandException(
          "column in column group is not a valid column: " + x
        )
      }
    }
    // check if given column is present in other groups
    def foundIndExistingColGrp(colName: String): Boolean = {
      retrievedColGrps.foreach { colGrp =>
        if (colGrp.split(",").contains(colName)) {
          return true
        }
      }
      false
    }

  }


  def isTimeStampColumn(colName: String, dims: Seq[Field]): Boolean = {
    dims.foreach { dim =>
      if (dim.column.equalsIgnoreCase(colName)) {
        if (dim.dataType.isDefined && null != dim.dataType.get &&
            "timestamp".equalsIgnoreCase(dim.dataType.get)) {
          return true
        }
      }
    }
    false
  }

  def isComplex(colName: String, dims: Seq[Field]): Boolean = {
    dims.foreach { x =>
      if (x.children.isDefined && null != x.children.get && x.children.get.nonEmpty) {
        val children = x.children.get
        if (x.column.equals(colName)) {
          return true
        } else {
          children.foreach { child =>
            val fieldName = x.column + "." + child.column
            if (fieldName.equalsIgnoreCase(colName)) {
              return true
            }
          }
        }
      }
    }
    false
  }

  def getColumnProperties(column: String,
      tableProperties: Map[String, String]): Option[util.List[ColumnProperty]] = {
    val fieldProps = new util.ArrayList[ColumnProperty]()
    val columnPropertiesStartKey = CarbonCommonConstants.COLUMN_PROPERTIES + "." + column + "."
    tableProperties.foreach {
      case (key, value) =>
        if (key.startsWith(columnPropertiesStartKey)) {
          fieldProps.add(ColumnProperty(key.substring(columnPropertiesStartKey.length(),
            key.length()), value))
        }
    }
    if (fieldProps.isEmpty) {
      None
    } else {
      Some(fieldProps)
    }
  }

  def validateTblProperties(tableProperties: Map[String, String], fields: Seq[Field]): Boolean = {
    val itr = tableProperties.keys
    var isValid: Boolean = true
    tableProperties.foreach {
      case (key, value) =>
        if (!validateFields(key, fields)) {
          isValid = false
          throw new MalformedCarbonCommandException(s"Invalid table properties ${ key }")
        }
    }
    isValid
  }

  /**
   * 1. If partitioned by clause exists, then partition_type should be defined
   * 2. If partition_type is Hash, then number_of_partitions should be defined
   * 3. If partition_type is List, then list_info should be defined
   * 4. If partition_type is Range, then range_info should be defined
   * 5. Only support single level partition for now
   * @param tableProperties
   * @param partitionerFields
   * @return partition clause and definition in tblproperties are valid or not
   */
  def validatePartitionColumns(tableProperties: Map[String, String],
      partitionerFields: Seq[PartitionerField]): Boolean = {
    var isValid: Boolean = true
    val partitionType = tableProperties.get(CarbonCommonConstants.PARTITION_TYPE)
    val numPartitions = tableProperties.get(CarbonCommonConstants.NUM_PARTITIONS)
    val rangeInfo = tableProperties.get(CarbonCommonConstants.RANGE_INFO)
    val listInfo = tableProperties.get(CarbonCommonConstants.LIST_INFO)

    if (partitionType.isEmpty) {
      isValid = false
    } else {
      partitionType.get.toUpperCase() match {
        case "HASH" => if (!numPartitions.isDefined
        || scala.util.Try(numPartitions.get.toInt).isFailure
        || numPartitions.get.toInt <= 0) {
          isValid = false
        }
        case "LIST" => if (!listInfo.isDefined) {
          isValid = false
        } else {
          listInfo.get.replace("(", "").replace(")", "").split(",").map(_.trim).foreach(
            isValid &= validateTypeConvert(partitionerFields(0), _))
        }
        case "RANGE" => if (!rangeInfo.isDefined) {
          isValid = false
        } else {
          rangeInfo.get.split(",").map(_.trim).foreach(
            isValid &= validateTypeConvert(partitionerFields(0), _))
        }
        case "RANGE_INTERVAL" => isValid = false
        case _ => isValid = false
      }
      // only support one partition column for now
      if (partitionerFields.length > 1) isValid = false
    }
    isValid
  }

  def validateTypeConvertForSpark2(partitionerField: PartitionerField, value: String): Boolean = {
    val result = partitionerField.dataType.get.toLowerCase match {
      case "integertype" =>
        scala.util.Try(value.toInt).isSuccess
      case "stringtype" =>
        scala.util.Try(value.toString).isSuccess
      case "longtype" =>
        scala.util.Try(value.toLong).isSuccess
      case "floattype" =>
        scala.util.Try(value.toFloat).isSuccess
      case "doubletype" =>
        scala.util.Try(value.toDouble).isSuccess
      case "numerictype" =>
        scala.util.Try(value.toDouble).isSuccess
      case "smallinttype" =>
        scala.util.Try(value.toShort).isSuccess
      case "tinyinttype" =>
        scala.util.Try(value.toShort).isSuccess
      case "shorttype" =>
        scala.util.Try(value.toShort).isSuccess
      case FIXED_DECIMALTYPE(_, _) =>
        val parField = partitionerField.dataType.get.split(",")
        val precision = parField(0).substring(12).toInt
        val scale = parField(1).substring(0, parField(1).length - 1).toInt
        val pattern = "^([-]?[0-9]{0," + (precision - scale) +
                      "})([.][0-9]{1," + scale + "})?$"
        value.matches(pattern)
      case "timestamptype" =>
        val timeStampFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT))
        if (scala.util.Try(timeStampFormat.parse(value)).isSuccess) {
          true
        } else {
          false
        }
      case "datetype" =>
        val dateFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT))
        if (scala.util.Try(dateFormat.parse(value)).isSuccess) {
          true
        } else {
          false
        }
      case others =>
       if (others != null && others.startsWith("char")) {
         scala.util.Try(value.toString).isSuccess
        } else if (others != null && others.startsWith("varchar")) {
         scala.util.Try(value.toString).isSuccess
        } else {
          throw new MalformedCarbonCommandException(
            "UnSupported partition type: " + partitionerField.dataType)
        }
    }
    result
  }

  def validateTypeConvert(partitionerField: PartitionerField, value: String): Boolean = {
    val result = partitionerField.dataType.get.toLowerCase() match {
      case "int" =>
        scala.util.Try(value.toInt).isSuccess
      case "string" =>
        scala.util.Try(value.toString).isSuccess
      case "bigint" =>
        scala.util.Try(value.toLong).isSuccess
      case "long" =>
        scala.util.Try(value.toLong).isSuccess
      case "float" =>
        scala.util.Try(value.toFloat).isSuccess
      case "double" =>
        scala.util.Try(value.toDouble).isSuccess
      case "numeric" =>
        scala.util.Try(value.toDouble).isSuccess
      case "smallint" =>
        scala.util.Try(value.toShort).isSuccess
      case "tinyint" =>
        scala.util.Try(value.toShort).isSuccess
      case "boolean" =>
        scala.util.Try(value.toBoolean).isSuccess
      case FIXED_DECIMAL(_, _) =>
        val parField = partitionerField.dataType.get.split(",")
        val precision = parField(0).substring(8).toInt
        val scale = parField(1).substring(0, parField(1).length - 1).toInt
        val pattern = "^([-]?[0-9]{0," + (precision - scale) +
                      "})([.][0-9]{1," + scale + "})?$"
        value.matches(pattern)
      case "timestamp" =>
        val timeStampFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT))
        scala.util.Try(timeStampFormat.parse(value)).isSuccess
      case "date" =>
        val dateFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT))
        scala.util.Try(dateFormat.parse(value)).isSuccess
      case _ =>
        validateTypeConvertForSpark2(partitionerField, value)
    }
    result
  }

  def validateFields(key: String, fields: Seq[Field]): Boolean = {
    var isValid: Boolean = false
    fields.foreach { field =>
      if (field.children.isDefined && field.children.get != null) {
        field.children.foreach(fields => {
          fields.foreach(complexfield => {
            val column = if ("val" == complexfield.column) {
              field.column
            } else {
              field.column + "." + complexfield.column
            }
            if (validateColumnProperty(key, column)) {
              isValid = true
            }
          }
          )
        }
        )
      } else {
        if (validateColumnProperty(key, field.column)) {
          isValid = true
        }
      }

    }
    isValid
  }

  def validateColumnProperty(key: String, column: String): Boolean = {
    if (!key.startsWith(CarbonCommonConstants.COLUMN_PROPERTIES)) {
      return true
    }
    val columnPropertyKey = CarbonCommonConstants.COLUMN_PROPERTIES + "." + column + "."
    if (key.startsWith(columnPropertyKey)) {
      true
    } else {
      false
    }
  }

  /**
   * @param colGrps
   * @param dims
   * @return columns of column groups in schema order
   */
  def arrangeColGrpsInSchemaOrder(colGrps: Seq[String], dims: Seq[Field]): Seq[String] = {
    def sortByIndex(colGrp1: String, colGrp2: String) = {
      val firstCol1 = colGrp1.split(",")(0)
      val firstCol2 = colGrp2.split(",")(0)
      val dimIndex1: Int = getDimIndex(firstCol1, dims)
      val dimIndex2: Int = getDimIndex(firstCol2, dims)
      dimIndex1 < dimIndex2
    }
    val sortedColGroups: Seq[String] = colGrps.sortWith(sortByIndex)
    sortedColGroups
  }

  /**
   * @param colName
   * @param dims
   * @return return index for given column in dims
   */
  def getDimIndex(colName: String, dims: Seq[Field]): Int = {
    var index: Int = -1
    dims.zipWithIndex.foreach { h =>
      if (h._1.column.equalsIgnoreCase(colName)) {
        index = h._2.toInt
      }
    }
    index
  }

  /**
   * This method will validate the table block size specified by the user
   *
   * @param tableProperties
   */
  def validateTableBlockSize(tableProperties: Map[String, String]): Unit = {
    var tableBlockSize: Integer = 0
    if (tableProperties.get(CarbonCommonConstants.TABLE_BLOCKSIZE).isDefined) {
      val blockSizeStr: String =
        parsePropertyValueStringInMB(tableProperties(CarbonCommonConstants.TABLE_BLOCKSIZE))
      try {
        tableBlockSize = Integer.parseInt(blockSizeStr)
      } catch {
        case e: NumberFormatException =>
          throw new MalformedCarbonCommandException("Invalid table_blocksize value found: " +
                                                    s"$blockSizeStr, only int value from 1 MB to " +
                                                    s"2048 MB is supported.")
      }
      if (tableBlockSize < CarbonCommonConstants.BLOCK_SIZE_MIN_VAL ||
          tableBlockSize > CarbonCommonConstants.BLOCK_SIZE_MAX_VAL) {
        throw new MalformedCarbonCommandException("Invalid table_blocksize value found: " +
                                                  s"$blockSizeStr, only int value from 1 MB to " +
                                                  s"2048 MB is supported.")
      }
      tableProperties.put(CarbonCommonConstants.TABLE_BLOCKSIZE, blockSizeStr)
    }
  }

  /**
   * This method will parse the configure string from 'XX MB/M' to 'XX'
   *
   * @param propertyValueString
   */
  def parsePropertyValueStringInMB(propertyValueString: String): String = {
    var parsedPropertyValueString: String = propertyValueString
    if (propertyValueString.trim.toLowerCase.endsWith("mb")) {
      parsedPropertyValueString = propertyValueString.trim.toLowerCase
        .substring(0, propertyValueString.trim.toLowerCase.lastIndexOf("mb")).trim
    }
    if (propertyValueString.trim.toLowerCase.endsWith("m")) {
      parsedPropertyValueString = propertyValueString.trim.toLowerCase
        .substring(0, propertyValueString.trim.toLowerCase.lastIndexOf("m")).trim
    }
    parsedPropertyValueString
  }

  def readLoadMetadataDetails(model: CarbonLoadModel, storePath: String): Unit = {
    val metadataPath = model.getCarbonDataLoadSchema.getCarbonTable.getMetaDataFilepath
    val details = SegmentStatusManager.readLoadMetadata(metadataPath)
    model.setLoadMetadataDetails(details.toList.asJava)
  }

  def configureCSVInputFormat(configuration: Configuration,
      carbonLoadModel: CarbonLoadModel): Unit = {
    CSVInputFormat.setCommentCharacter(configuration, carbonLoadModel.getCommentChar)
    CSVInputFormat.setCSVDelimiter(configuration, carbonLoadModel.getCsvDelimiter)
    CSVInputFormat.setEscapeCharacter(configuration, carbonLoadModel.getEscapeChar)
    CSVInputFormat.setMaxColumns(configuration, carbonLoadModel.getMaxColumns)
    CSVInputFormat.setNumberOfColumns(configuration, carbonLoadModel.getCsvHeaderColumns.length
      .toString)
    CSVInputFormat.setHeaderExtractionEnabled(configuration,
      carbonLoadModel.getCsvHeader == null || carbonLoadModel.getCsvHeader.isEmpty)
    CSVInputFormat.setQuoteCharacter(configuration, carbonLoadModel.getQuoteChar)
    CSVInputFormat.setReadBufferSize(configuration, CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.CSV_READ_BUFFER_SIZE,
        CarbonCommonConstants.CSV_READ_BUFFER_SIZE_DEFAULT))
  }

  def configSplitMaxSize(context: SparkContext, filePaths: String,
      hadoopConfiguration: Configuration): Unit = {
    val defaultParallelism = if (context.defaultParallelism < 1) {
      1
    } else {
      context.defaultParallelism
    }
    val spaceConsumed = FileUtils.getSpaceOccupied(filePaths)
    val blockSize =
      hadoopConfiguration.getLongBytes("dfs.blocksize", CarbonCommonConstants.CARBON_256MB)
    LOGGER.info("[Block Distribution]")
    // calculate new block size to allow use all the parallelism
    if (spaceConsumed < defaultParallelism * blockSize) {
      var newSplitSize: Long = spaceConsumed / defaultParallelism
      if (newSplitSize < CarbonCommonConstants.CARBON_16MB) {
        newSplitSize = CarbonCommonConstants.CARBON_16MB
      }
      hadoopConfiguration.set(FileInputFormat.SPLIT_MAXSIZE, newSplitSize.toString)
      LOGGER.info(s"totalInputSpaceConsumed: $spaceConsumed , " +
                  s"defaultParallelism: $defaultParallelism")
      LOGGER.info(s"mapreduce.input.fileinputformat.split.maxsize: ${ newSplitSize.toString }")
    }
  }

  def getCsvHeaderColumns(carbonLoadModel: CarbonLoadModel): Array[String] = {
    val delimiter = if (StringUtils.isEmpty(carbonLoadModel.getCsvDelimiter)) {
      CarbonCommonConstants.COMMA
    } else {
      CarbonUtil.delimiterConverter(carbonLoadModel.getCsvDelimiter)
    }
    var csvFile: String = null
    var csvHeader: String = carbonLoadModel.getCsvHeader
    val csvColumns = if (StringUtils.isBlank(csvHeader)) {
      // read header from csv file
      csvFile = carbonLoadModel.getFactFilePath.split(",")(0)
      csvHeader = CarbonUtil.readHeader(csvFile)
      if (StringUtils.isBlank(csvHeader)) {
        throw new CarbonDataLoadingException("First line of the csv is not valid.")
      }
      csvHeader.toLowerCase().split(delimiter).map(_.replaceAll("\"", "").trim)
    } else {
      csvHeader.toLowerCase.split(CarbonCommonConstants.COMMA).map(_.trim)
    }

    if (!CarbonDataProcessorUtil.isHeaderValid(carbonLoadModel.getTableName, csvColumns,
        carbonLoadModel.getCarbonDataLoadSchema)) {
      if (csvFile == null) {
        LOGGER.error("CSV header in DDL is not proper."
                     + " Column names in schema and CSV header are not the same.")
        throw new CarbonDataLoadingException(
          "CSV header in DDL is not proper. Column names in schema and CSV header are "
          + "not the same.")
      } else {
        LOGGER.error(
          "CSV header in input file is not proper. Column names in schema and csv header are not "
          + "the same. Input file : " + csvFile)
        throw new CarbonDataLoadingException(
          "CSV header in input file is not proper. Column names in schema and csv header are not "
          + "the same. Input file : " + csvFile)
      }
    }
    csvColumns
  }

  def validateMaxColumns(csvHeaders: Array[String], maxColumns: String): Int = {
    /*
    User configures both csvheadercolumns, maxcolumns,
      if csvheadercolumns >= maxcolumns, give error
      if maxcolumns > threashold, give error
    User configures csvheadercolumns
      if csvheadercolumns >= maxcolumns(default) then maxcolumns = csvheadercolumns+1
      if csvheadercolumns >= threashold, give error
    User configures nothing
      if csvheadercolumns >= maxcolumns(default) then maxcolumns = csvheadercolumns+1
      if csvheadercolumns >= threashold, give error
     */
    val columnCountInSchema = csvHeaders.length
    var maxNumberOfColumnsForParsing = 0
    val maxColumnsInt = getMaxColumnValue(maxColumns)
    if (maxColumnsInt != null) {
      if (columnCountInSchema >= maxColumnsInt) {
        sys.error(s"csv headers should be less than the max columns: $maxColumnsInt")
      } else if (maxColumnsInt > CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
        sys.error(s"max columns cannot be greater than the threshold value: ${
          CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING
        }")
      } else {
        maxNumberOfColumnsForParsing = maxColumnsInt
      }
    } else if (columnCountInSchema >= CSVInputFormat.THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
      sys.error(s"csv header columns should be less than max threashold: ${
        CSVInputFormat
          .THRESHOLD_MAX_NUMBER_OF_COLUMNS_FOR_PARSING
      }")
    } else if (columnCountInSchema >= CSVInputFormat.DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING) {
      maxNumberOfColumnsForParsing = columnCountInSchema + 1
    } else {
      maxNumberOfColumnsForParsing = CSVInputFormat.DEFAULT_MAX_NUMBER_OF_COLUMNS_FOR_PARSING
    }
    maxNumberOfColumnsForParsing
  }

  private def getMaxColumnValue(maxColumn: String): Integer = {
    if (maxColumn != null) {
      try {
        maxColumn.toInt
      } catch {
        case e: Exception =>
          LOGGER.error(s"Invalid value for max column in load options ${ e.getMessage }")
          null
      }
    } else {
      null
    }
  }

}
