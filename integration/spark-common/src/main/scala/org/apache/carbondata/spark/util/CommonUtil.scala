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


import java.io.File
import java.text.SimpleDateFormat
import java.util
import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.{ColumnProperty, Field, PartitionerField}
import org.apache.spark.sql.types.{MetadataBuilder, StringType}
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.memory.{UnsafeMemoryManager, UnsafeSortMemoryManager}
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.datatype.{DataType, DataTypes}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.scan.partition.PartitionUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{ByteUtil, CarbonProperties}
import org.apache.carbondata.core.util.comparator.Comparator
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil


object CommonUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_DECIMALTYPE = """decimaltype\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

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
   * 2. If partition_type is Hash, then num_partitions should be defined
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
      isValid = true
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
        case _ => isValid = true
      }
      // only support one partition column for now
      if (partitionerFields.length > 1 && !partitionType.get.toUpperCase.equals("NATIVE_HIVE")) {
        isValid = false
      }
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
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
        scala.util.Try(timeStampFormat.parse(value)).isSuccess
      case "datetype" =>
        val dateFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
        scala.util.Try(dateFormat.parse(value)).isSuccess
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
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
          CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
        scala.util.Try(timeStampFormat.parse(value)).isSuccess
      case "date" =>
        val dateFormat = new SimpleDateFormat(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
        scala.util.Try(dateFormat.parse(value)).isSuccess
      case _ =>
        validateTypeConvertForSpark2(partitionerField, value)
    }

    if(!result) {
      throw new MalformedCarbonCommandException(s"Invalid Partition Values for partition " +
        s"column: ${partitionerField.partitionColumn}")
    } else {
      result
    }
  }

  /**
   * To verify the range info is in correct order
   * @param rangeInfo
   * @param columnDataType
   * @param timestampFormatter
   * @param dateFormatter
   */
  def validateRangeInfo(rangeInfo: List[String], columnDataType: DataType,
      timestampFormatter: SimpleDateFormat, dateFormatter: SimpleDateFormat): Unit = {
    if (rangeInfo.size <= 1) {
      throw new
         MalformedCarbonCommandException("Range info must define a valid range.Please check again!")
    }
    val comparator = Comparator.getComparator(columnDataType)
    var head = columnDataType match {
      case DataTypes.STRING => ByteUtil.toBytes(rangeInfo.head)
      case _ => PartitionUtil.getDataBasedOnDataType(rangeInfo.head, columnDataType,
        timestampFormatter, dateFormatter)
    }
    val iterator = rangeInfo.tail.toIterator
    while (iterator.hasNext) {
      val next = columnDataType match {
        case DataTypes.STRING => ByteUtil.toBytes(iterator.next())
        case _ => PartitionUtil.getDataBasedOnDataType(iterator.next(), columnDataType,
          timestampFormatter, dateFormatter)
      }
      if (next == null) {
        sys.error(
          "Data in range info must be the same type with the partition field's type "
            + columnDataType)
      }
      if (comparator.compare(head, next) < 0) {
        head = next
      } else {
        sys.error("Range info must be in ascending order, please check again!")
      }
    }
  }

  def validateSplitListInfo(originListInfo: List[String], newListInfo: List[String],
      originList: List[List[String]]): Unit = {
    if (originListInfo.size == 1) {
      sys.error("The target list partition cannot be split, please check again!")
    }
    if (newListInfo.size == 1) {
      sys.error("Can't split list to one partition, please check again!")
    }
    if (!(newListInfo.size < originListInfo.size)) {
      sys.error("The size of new list must be smaller than original list, please check again!")
    }
    val tempList = newListInfo.mkString(",").split(",")
      .map(_.replace("(", "").replace(")", "").trim)
    if (tempList.length != originListInfo.size) {
      sys.error("The total number of elements in new list must equal to original list!")
    }
    if (!(tempList diff originListInfo).isEmpty) {
      sys.error("The elements in new list must exist in original list")
    }
  }

  def validateAddListInfo(newListInfo: List[String], originList: List[List[String]]): Unit = {
    if (newListInfo.size < 1) {
      sys.error("Please add at least one new partition")
    }
    for (originElementGroup <- originList) {
      for (newElement <- newListInfo ) {
        if (originElementGroup.contains(newElement)) {
          sys.error(s"The partition $newElement is already exist! Please check again!")
        }
      }
    }
  }

  def validateListInfo(listInfo: List[List[String]]): Unit = {
    val list = listInfo.flatten
    if (list.distinct.size != list.size) {
      sys.error("Duplicate elements defined in LIST_INFO!")
    }
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
   * validate table level properties for compaction
   *
   * @param tableProperties
   */
  def validateTableLevelCompactionProperties(tableProperties: Map[String, String]): Unit = {
    validateMajorCompactionSize(tableProperties)
    validateAutoLoadMerge(tableProperties)
    validateCompactionLevelThreshold(tableProperties)
    validateCompactionPreserveSegmentsOrAllowedDays(tableProperties,
      CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS)
    validateCompactionPreserveSegmentsOrAllowedDays(tableProperties,
      CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS)
  }

  /**
   * This method will validate the major compaction size specified by the user
   * the property is used while doing major compaction
   *
   * @param tableProperties
   */
  def validateMajorCompactionSize(tableProperties: Map[String, String]): Unit = {
    var majorCompactionSize: Integer = 0
    val tblPropName = CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE
    if (tableProperties.get(tblPropName).isDefined) {
      val majorCompactionSizeStr: String =
        parsePropertyValueStringInMB(tableProperties(tblPropName))
      try {
        majorCompactionSize = Integer.parseInt(majorCompactionSizeStr)
      } catch {
        case e: NumberFormatException =>
          throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
            s"$majorCompactionSizeStr, only int value greater than 0 is supported.")
      }
      if (majorCompactionSize < 0) {
        throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
          s"$majorCompactionSizeStr, only int value greater than 0 is supported.")
      }
      tableProperties.put(tblPropName, majorCompactionSizeStr)
    }
  }

  /**
   * This method will validate the auto merge load property specified by the user
   * the property is used while doing minor compaction
   *
   * @param tableProperties
   */
  def validateAutoLoadMerge(tableProperties: Map[String, String]): Unit = {
    val tblPropName = CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE
    if (tableProperties.get(tblPropName).isDefined) {
      val trimStr = tableProperties(tblPropName).trim
      if (!trimStr.equalsIgnoreCase("true") && !trimStr.equalsIgnoreCase("false")) {
        throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
          s"$trimStr, only true|false is supported.")
      }
      tableProperties.put(tblPropName, trimStr)
    }
  }

  /**
   * This method will validate the flat folder property specified by the user
   *
   * @param tableProperties
   */
  def validateFlatFolder(tableProperties: Map[String, String]): Unit = {
    val tblPropName = CarbonCommonConstants.FLAT_FOLDER
    if (tableProperties.get(tblPropName).isDefined) {
      val trimStr = tableProperties(tblPropName).trim
      if (!trimStr.equalsIgnoreCase("true") && !trimStr.equalsIgnoreCase("false")) {
        throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
                                                  s"$trimStr, only true|false is supported.")
      }
      tableProperties.put(tblPropName, trimStr)
    }
  }

  /**
   * This method will validate the compaction level threshold property specified by the user
   * the property is used while doing minor compaction
   *
   * @param tableProperties
   */
  def validateCompactionLevelThreshold(tableProperties: Map[String, String]): Unit = {
    val tblPropName = CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD
    if (tableProperties.get(tblPropName).isDefined) {
      val regularedStr = tableProperties(tblPropName).replace(" ", "")
      try {
        val levels: Array[String] = regularedStr.split(",")
        val thresholds = regularedStr.split(",").map(levelThresholdStr => levelThresholdStr.toInt)
        if (!thresholds.forall(t => t < 100 && t >= 0)) {
          throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
            s"$regularedStr, only int values separated by comma and between 0 " +
            s"and 100 are supported.")
        }
      }
      catch {
        case e: NumberFormatException =>
          throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
            s"$regularedStr, only int values separated by comma and between 0 " +
            s"and 100 are supported.")
      }
      tableProperties.put(tblPropName, regularedStr)
    }
  }

  /**
   * This method will validate the compaction preserve segments property
   * or compaction allowed days property
   *
   * @param tableProperties
   * @param tblPropName
   */
  def validateCompactionPreserveSegmentsOrAllowedDays(tableProperties: Map[String, String],
                                                      tblPropName: String): Unit = {
    if (tblPropName.equals(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS) ||
      tblPropName.equals(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS)) {
      var propValue: Integer = 0
      if (tableProperties.get(tblPropName).isDefined) {
        val propValueStr = tableProperties(tblPropName).trim
        try {
          propValue = Integer.parseInt(propValueStr)
        } catch {
          case e: NumberFormatException =>
            throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
              s"$propValueStr, only int value between 0 and 100 is supported.")
        }
        if (propValue < 0 || propValue > 100) {
          throw new MalformedCarbonCommandException(s"Invalid $tblPropName value found: " +
            s"$propValueStr, only int value between 0 and 100 is supported.")
        }
        tableProperties.put(tblPropName, propValueStr)
      }
    }
  }

  /**
   * This method will validate the table block size and blocklet size specified by the user
   *
   * @param tableProperties table property specified by user
   * @param propertyName property name
   */
  def validateSize(tableProperties: Map[String, String], propertyName: String): Unit = {
    var size: Integer = 0
    if (tableProperties.get(propertyName).isDefined) {
      val blockSizeStr: String =
        parsePropertyValueStringInMB(tableProperties(propertyName))
      try {
        size = Integer.parseInt(blockSizeStr)
      } catch {
        case e: NumberFormatException =>
          throw new MalformedCarbonCommandException(s"Invalid $propertyName value found: " +
                                                    s"$blockSizeStr, only int value from 1 MB to " +
                                                    s"2048 MB is supported.")
      }
      if (size < CarbonCommonConstants.BLOCK_SIZE_MIN_VAL ||
          size > CarbonCommonConstants.BLOCK_SIZE_MAX_VAL) {
        throw new MalformedCarbonCommandException(s"Invalid $propertyName value found: " +
                                                  s"$blockSizeStr, only int value from 1 MB to " +
                                                  s"2048 MB is supported.")
      }
      tableProperties.put(propertyName, blockSizeStr)
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

  def configureCSVInputFormat(configuration: Configuration,
      carbonLoadModel: CarbonLoadModel): Unit = {
    CSVInputFormat.setCommentCharacter(configuration, carbonLoadModel.getCommentChar)
    CSVInputFormat.setCSVDelimiter(configuration, carbonLoadModel.getCsvDelimiter)
    CSVInputFormat.setSkipEmptyLine(configuration, carbonLoadModel.getSkipEmptyLine)
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
    val spaceConsumed = FileUtils.getSpaceOccupied(filePaths, hadoopConfiguration)
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

  def getPartitionInfo(columnName: String, partitionType: PartitionType,
      partitionInfo: PartitionInfo): Seq[Row] = {
    var result = Seq.newBuilder[Row]
    partitionType match {
      case PartitionType.RANGE =>
        result.+=(RowFactory.create("0" + ", " + columnName + " = DEFAULT"))
        val rangeInfo = partitionInfo.getRangeInfo
        val size = rangeInfo.size() - 1
        for (index <- 0 to size) {
          if (index == 0) {
            val id = partitionInfo.getPartitionId(index + 1).toString
            val desc = columnName + " < " + rangeInfo.get(index)
            result.+=(RowFactory.create(id + ", " + desc))
          } else {
            val id = partitionInfo.getPartitionId(index + 1).toString
            val desc = rangeInfo.get(index - 1) + " <= " + columnName + " < " + rangeInfo.get(index)
            result.+=(RowFactory.create(id + ", " + desc))
          }
        }
      case PartitionType.RANGE_INTERVAL =>
        result.+=(RowFactory.create(columnName + " = "))
      case PartitionType.LIST =>
        result.+=(RowFactory.create("0" + ", " + columnName + " = DEFAULT"))
        val listInfo = partitionInfo.getListInfo
        listInfo.asScala.foreach {
          f =>
            val id = partitionInfo.getPartitionId(listInfo.indexOf(f) + 1).toString
            val desc = columnName + " = " + f.toArray().mkString(", ")
            result.+=(RowFactory.create(id + ", " + desc))
        }
      case PartitionType.HASH =>
        val hashNumber = partitionInfo.getNumPartitions
        result.+=(RowFactory.create(columnName + " = HASH_NUMBER(" + hashNumber.toString() + ")"))
        result.+=(RowFactory.create(s"partitionIds = ${partitionInfo.getPartitionIds}"))
      case others =>
        result.+=(RowFactory.create(columnName + " = "))
    }
    val rows = result.result()
    rows
  }

  def partitionInfoOutput: Seq[Attribute] = Seq(
    AttributeReference("Partition(Id, DESC)", StringType, false,
      new MetadataBuilder().putString("comment", "partition").build())()
  )

  /**
   * Method to clear the memory for a task
   * if present
   */
  def clearUnsafeMemory(taskId: String) {
    UnsafeMemoryManager.
      INSTANCE.freeMemoryAll(taskId)
    UnsafeSortMemoryManager.
      INSTANCE.freeMemoryAll(taskId)
  }

  /**
   * The in-progress segments which are in stale state will be marked as deleted
   * when driver is initializing.
   * @param databaseLocation
   * @param dbName
   */
  def cleanInProgressSegments(databaseLocation: String, dbName: String): Unit = {
    val loaderDriver = CarbonProperties.getInstance().
      getProperty(CarbonCommonConstants.DATA_MANAGEMENT_DRIVER,
        CarbonCommonConstants.DATA_MANAGEMENT_DRIVER_DEFAULT).toBoolean
    if (!loaderDriver) {
      return
    }
    try {
      val fileType = FileFactory.getFileType(databaseLocation)
      if (FileFactory.isFileExist(databaseLocation, fileType)) {
        val file = FileFactory.getCarbonFile(databaseLocation, fileType)
        if (file.isDirectory) {
          val tableFolders = file.listFiles()
          tableFolders.foreach { tableFolder =>
            if (tableFolder.isDirectory) {
              val tablePath = databaseLocation +
                              CarbonCommonConstants.FILE_SEPARATOR + tableFolder.getName
              val tableUniqueName = dbName + "_" + tableFolder.getName
              val tableStatusFile =
                CarbonTablePath.getTableStatusFilePath(tablePath)
              if (FileFactory.isFileExist(tableStatusFile, fileType)) {
                try {
                  val carbonTable = CarbonMetadata.getInstance
                    .getCarbonTable(tableUniqueName)
                  SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, true, null)
                } catch {
                  case _: Exception =>
                    LOGGER.warn(s"Error while cleaning table " +
                                s"${ tableUniqueName }")
                }
              }
            }
          }
        }
      }
    } catch {
      case s: java.io.FileNotFoundException =>
        // Create folders and files.
        LOGGER.error(s)
    }
  }

  def getScaleAndPrecision(dataType: String): (Int, Int) = {
    val m: Matcher = Pattern.compile("^decimal\\(([^)]+)\\)").matcher(dataType)
    m.find()
    val matchedString: String = m.group(1)
    val scaleAndPrecision = matchedString.split(",")
    (Integer.parseInt(scaleAndPrecision(0).trim), Integer.parseInt(scaleAndPrecision(1).trim))
  }


  def setTempStoreLocation(
      index: Int,
      carbonLoadModel: CarbonLoadModel,
      isCompactionFlow: Boolean,
      isAltPartitionFlow: Boolean) : Unit = {
    val storeLocation = getTempStoreLocations(index.toString).mkString(File.pathSeparator)

    val tempLocationKey = CarbonDataProcessorUtil.getTempStoreLocationKey(
      carbonLoadModel.getDatabaseName,
      carbonLoadModel.getTableName,
      carbonLoadModel.getSegmentId,
      carbonLoadModel.getTaskNo,
      isCompactionFlow,
      isAltPartitionFlow)
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation)
  }

  /**
   * get the temp locations for each process thread
   *
   * @param index the id for each process thread
   * @return an array of temp locations
   */
  def getTempStoreLocations(index: String) : Array[String] = {
    var storeLocation: Array[String] = Array[String]()
    val isCarbonUseYarnLocalDir = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR,
      CarbonCommonConstants.CARBON_LOADING_USE_YARN_LOCAL_DIR_DEFAULT).equalsIgnoreCase("true")
    val tmpLocationSuffix =
      s"${File.separator}carbon${System.nanoTime()}${CarbonCommonConstants.UNDERSCORE}$index"
    if (isCarbonUseYarnLocalDir) {
      val yarnStoreLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)

      if (null != yarnStoreLocations && yarnStoreLocations.nonEmpty) {
        storeLocation = yarnStoreLocations.map(_ + tmpLocationSuffix)
      } else {
        LOGGER.warn("It seems that the we didn't configure local dirs for yarn," +
                    " so we are unable to use them for data loading." +
                    " Here we will fall back using the java tmp dir.")
        storeLocation = storeLocation :+ (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
      }
    } else {
      storeLocation = storeLocation :+ (System.getProperty("java.io.tmpdir") + tmpLocationSuffix)
    }
    storeLocation
  }
  /**
   * This method will validate the cache level
   *
   * @param cacheLevel
   * @param tableProperties
   */
  def validateCacheLevel(cacheLevel: String, tableProperties: Map[String, String]): Unit = {
    val supportedCacheLevel = Seq("BLOCK", "BLOCKLET")
    if (cacheLevel.trim.isEmpty) {
      val errorMessage = "Invalid value: Empty column names for the option(s): " +
                         CarbonCommonConstants.CACHE_LEVEL
      throw new MalformedCarbonCommandException(errorMessage)
    } else {
      val trimmedCacheLevel = cacheLevel.trim.toUpperCase
      if (!supportedCacheLevel.contains(trimmedCacheLevel)) {
        val errorMessage = s"Invalid value: Allowed vaLues for ${
          CarbonCommonConstants.CACHE_LEVEL} are BLOCK AND BLOCKLET"
        throw new MalformedCarbonCommandException(errorMessage)
      }
      tableProperties.put(CarbonCommonConstants.CACHE_LEVEL, trimmedCacheLevel)
    }
  }

  /**
   * This will validate the column meta cache i.e the columns to be cached.
   * By default all dimensions will be cached.
   * If the property is already defined in create table DDL then validate it,
   * else add all the dimension columns as columns to be cached to table properties.
   * valid values for COLUMN_META_CACHE can either be empty or can have one or more comma
   * separated values
   */
  def validateColumnMetaCacheFields(dbName: String,
      tableName: String,
      tableColumns: Seq[String],
      cachedColumns: String,
      tableProperties: Map[String, String]): Unit = {
    val tableIdentifier = TableIdentifier(tableName, Some(dbName))
    // below check is added because empty value for column_meta_cache is allowed and in that
    // case there should not be any validation
    if (!cachedColumns.equals("")) {
      validateColumnMetaCacheOption(tableIdentifier, dbName, cachedColumns, tableColumns)
      val columnsToBeCached = cachedColumns.split(",").map(x => x.trim.toLowerCase).toSeq
      // make the columns in create table order and then add it to table properties
      val createOrder = tableColumns.filter(col => columnsToBeCached.contains(col))
      tableProperties.put(CarbonCommonConstants.COLUMN_META_CACHE, createOrder.mkString(","))
    }
  }

  /**
   * Validate the column_meta_cache option in tableproperties
   *
   * @param tableIdentifier
   * @param databaseName
   * @param columnsToBeCached
   * @param tableColumns
   */
  private def validateColumnMetaCacheOption(tableIdentifier: TableIdentifier,
      databaseName: String,
      columnsToBeCached: String,
      tableColumns: Seq[String]): Unit = {
    // check if only empty spaces are given in the property value
    if (columnsToBeCached.trim.isEmpty) {
      val errorMessage = "Invalid value: Empty column names for the option(s): " +
                         CarbonCommonConstants.COLUMN_META_CACHE
      throw new MalformedCarbonCommandException(errorMessage)
    } else {
      val columns: Array[String] = columnsToBeCached.split(',').map(x => x.toLowerCase.trim)
      // Check for duplicate column names
      columns.groupBy(col => col.toLowerCase).foreach(f => if (f._2.size > 1) {
        throw new MalformedCarbonCommandException(s"Duplicate column name found : ${ f._1 }")
      })
      columns.foreach(col => {
        // check if any intermediate column is empty
        if (null == col || col.trim.isEmpty) {
          val errorMessage = "Invalid value: Empty column names for the option(s): " +
                             CarbonCommonConstants.COLUMN_META_CACHE
          throw new MalformedCarbonCommandException(errorMessage)
        }
        // check if the column exists in the table
        if (!tableColumns.contains(col.toLowerCase)) {
          val errorMessage = s"Column $col does not exists in the table ${ tableIdentifier.table }"
          throw new MalformedCarbonCommandException(errorMessage)
        }
      })
    }
  }

  /**
   * This method will validate single node minimum load data volume of table specified by the user
   *
   * @param tableProperties table property specified by user
   * @param propertyName property name
   */
  def validateLoadMinSize(tableProperties: Map[String, String], propertyName: String): Unit = {
    var size: Integer = 0
    if (tableProperties.get(propertyName).isDefined) {
      val loadSizeStr: String =
        parsePropertyValueStringInMB(tableProperties(propertyName))
      try {
        size = Integer.parseInt(loadSizeStr)
      } catch {
        case e: NumberFormatException =>
          throw new MalformedCarbonCommandException(s"Invalid $propertyName value found: " +
                                                    s"$loadSizeStr, only int value greater " +
                                                    s"than 0 is supported.")
      }
      // if the value is negative, set the value is 0
      if(size > 0) {
        tableProperties.put(propertyName, loadSizeStr)
      } else {
        tableProperties.put(propertyName, CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT)
      }
    }
  }
}
