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

import java.io.{ByteArrayOutputStream, DataOutputStream, File}
import java.math.BigDecimal
import java.text.SimpleDateFormat
import java.util
import java.util.UUID
import java.util.regex.{Matcher, Pattern}

import scala.collection.JavaConverters._
import scala.collection.mutable.Map
import scala.math.BigDecimal.RoundingMode

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.execution.command.{ColumnProperty, Field, PartitionerField}
import org.apache.spark.sql.types.{ArrayType, DataType, DateType, DecimalType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.DateDirectDictionaryGenerator
import org.apache.carbondata.core.memory.{UnsafeMemoryManager, UnsafeSortMemoryManager}
import org.apache.carbondata.core.metadata.CarbonMetadata
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataTypeUtil, ThreadLocalTaskInfo}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.datatypes.{ArrayDataType, GenericDataType, StructDataType}
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration
import org.apache.carbondata.processing.loading.complexobjects.{ArrayObject, StructObject}
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder
import org.apache.carbondata.processing.loading.converter.impl.FieldEncoderFactory
import org.apache.carbondata.processing.loading.csvinput.CSVInputFormat
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil

object CommonUtil {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  val FIXED_DECIMALTYPE = """decimaltype\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

  val ONE_KB: Long = 1024L
  val ONE_KB_BI: BigDecimal = BigDecimal.valueOf(ONE_KB)
  val ONE_MB: Long = ONE_KB * ONE_KB
  val ONE_MB_BI: BigDecimal = BigDecimal.valueOf(ONE_MB)
  val ONE_GB: Long = ONE_KB * ONE_MB
  val ONE_GB_BI: BigDecimal = BigDecimal.valueOf(ONE_GB)
  val ONE_TB: Long = ONE_KB * ONE_GB
  val ONE_TB_BI: BigDecimal = BigDecimal.valueOf(ONE_TB)
  val ONE_PB: Long = ONE_KB * ONE_TB
  val ONE_PB_BI: BigDecimal = BigDecimal.valueOf(ONE_PB)
  val ONE_EB: Long = ONE_KB * ONE_PB
  val ONE_EB_BI: BigDecimal = BigDecimal.valueOf(ONE_EB)

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
    if (fields.nonEmpty) {
      tableProperties.foreach {
        case (key, value) =>
          if (!validateFields(key, fields)) {
            isValid = false
            throw new MalformedCarbonCommandException(s"Invalid table properties $key")
          }
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
   * This method will validate the table page size
   *
   * @param tableProperties table property specified by user
   * @param propertyName property name
   */
  def validatePageSizeInmb(tableProperties: Map[String, String], propertyName: String): Unit = {
    var size: Integer = 0
    if (tableProperties.get(propertyName).isDefined) {
      val pageSize: String =
        parsePropertyValueStringInMB(tableProperties(propertyName))
      val minPageSize = CarbonCommonConstants.TABLE_PAGE_SIZE_MIN_INMB
      val maxPageSize = CarbonCommonConstants.TABLE_PAGE_SIZE_MAX_INMB
      try {
        size = Integer.parseInt(pageSize)
      } catch {
        case e: NumberFormatException =>
          throw new MalformedCarbonCommandException(s"Invalid $propertyName value found: " +
                                                    s"$pageSize, only int value from $minPageSize" +
                                                    s" to " +
                                                    s"$maxPageSize is supported.")
      }
      if (size < minPageSize || size > maxPageSize) {
        throw new MalformedCarbonCommandException(s"Invalid $propertyName value found: " +
                                                  s"$pageSize, only int value from $minPageSize " +
                                                  s"to " +
                                                  s"$maxPageSize is supported.")
      }
      tableProperties.put(propertyName, pageSize)
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

  /**
   * Method to clear the memory for a task
   * if present
   */
  def clearUnsafeMemory(taskId: String) {
    UnsafeMemoryManager.
      INSTANCE.freeMemoryAll(taskId)
    UnsafeSortMemoryManager.
      INSTANCE.freeMemoryAll(taskId)
    ThreadLocalTaskInfo.clearCarbonTaskInfo()
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
      if (FileFactory.isFileExist(databaseLocation)) {
        val file = FileFactory.getCarbonFile(databaseLocation)
        if (file.isDirectory) {
          val tableFolders = file.listFiles()
          tableFolders.foreach { tableFolder =>
            if (tableFolder.isDirectory) {
              val tablePath = databaseLocation +
                              CarbonCommonConstants.FILE_SEPARATOR + tableFolder.getName
              val tableUniqueName = CarbonTable.buildUniqueName(dbName, tableFolder.getName)
              val tableStatusFile =
                CarbonTablePath.getTableStatusFilePath(tablePath)
              if (FileFactory.isFileExist(tableStatusFile)) {
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
    val tmpLocationSuffix = s"${ File.separator }carbon${
      UUID.randomUUID().toString
        .replace("-", "")
    }${ CarbonCommonConstants.UNDERSCORE }$index"
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

  def isDataTypeSupportedForSortColumn(columnDataType: String): Boolean = {
    val dataTypes = Array("array", "struct", "map", "double", "float", "decimal", "binary")
    dataTypes.exists(x => x.equalsIgnoreCase(columnDataType))
  }

  def validateSortScope(newProperties: Map[String, String]): Unit = {
    val sortScopeOption = newProperties.get(CarbonCommonConstants.SORT_SCOPE)
    if (sortScopeOption.isDefined) {
      if (!CarbonUtil.isValidSortOption(sortScopeOption.get)) {
        throw new MalformedCarbonCommandException(
          s"Invalid SORT_SCOPE ${ sortScopeOption.get }, " +
          s"valid SORT_SCOPE are 'NO_SORT', 'LOCAL_SORT' and 'GLOBAL_SORT'")
      }
    }
  }

  def validateSortColumns(
      sortKey: Array[String],
      fields: Seq[(String, String)],
      varcharCols: Seq[String]
  ): Unit = {
    if (sortKey.diff(sortKey.distinct).length > 0 ||
        (sortKey.length > 1 && sortKey.contains(""))) {
      throw new MalformedCarbonCommandException(
        "SORT_COLUMNS Either having duplicate columns : " +
        sortKey.diff(sortKey.distinct).mkString(",") + " or it contains illegal argumnet.")
    }

    sortKey.foreach { column =>
      if (!fields.exists(x => x._1.equalsIgnoreCase(column))) {
        val errorMsg = "sort_columns: " + column +
                       " does not exist in table. Please check the create table statement."
        throw new MalformedCarbonCommandException(errorMsg)
      } else {
        val dataType = fields.find(x =>
          x._1.equalsIgnoreCase(column)).get._2
        if (isDataTypeSupportedForSortColumn(dataType)) {
          val errorMsg = s"sort_columns is unsupported for $dataType datatype column: " + column
          throw new MalformedCarbonCommandException(errorMsg)
        }
        if (varcharCols.exists(x => x.equalsIgnoreCase(column))) {
          throw new MalformedCarbonCommandException(
            s"sort_columns is unsupported for long string datatype column: $column")
        }
      }
    }
  }

  def validateSortColumns(carbonTable: CarbonTable, newProperties: Map[String, String]): Unit = {
    val fields = carbonTable.getVisibleDimensions.asScala ++ carbonTable.getVisibleMeasures.asScala
    val tableProperties = carbonTable.getTableInfo.getFactTable.getTableProperties
    var sortKeyOption = newProperties.get(CarbonCommonConstants.SORT_COLUMNS)
    val varcharColsString = tableProperties.get(CarbonCommonConstants.LONG_STRING_COLUMNS)
    val varcharCols: Seq[String] = if (varcharColsString == null) {
      Seq.empty[String]
    } else {
      varcharColsString.split(",").map(_.trim)
    }

    if (!sortKeyOption.isDefined) {
      // default no columns are selected for sorting in no_sort scope
      sortKeyOption = Some("")
    }
    val sortKeyString = CarbonUtil.unquoteChar(sortKeyOption.get).trim
    if (!sortKeyString.isEmpty) {
      val sortKey = sortKeyString.split(',').map(_.trim)
      validateSortColumns(
        sortKey,
        fields.map { field => (field.getColName, field.getDataType.getName) },
        varcharCols
      )
    }
  }

  def bytesToDisplaySize(size: Long): String = {
    try {
      bytesToDisplaySize(BigDecimal.valueOf(size))
    } catch {
      case _: Exception =>
        size.toString
    }
  }

  // This method converts the bytes count to display size upto 2 decimal places
  def bytesToDisplaySize(size: BigDecimal): String = {
    var displaySize: String = null
    if (size.divideToIntegralValue(ONE_EB_BI).compareTo(BigDecimal.ZERO) > 0) {
      displaySize = size.divide(ONE_EB_BI).setScale(2, RoundingMode.HALF_DOWN).doubleValue() + " EB"
    } else if (size.divideToIntegralValue(ONE_PB_BI).compareTo(BigDecimal.ZERO) > 0) {
      displaySize = size.divide(ONE_PB_BI).setScale(2, RoundingMode.HALF_DOWN).doubleValue() + " PB"
    } else if (size.divideToIntegralValue(ONE_TB_BI).compareTo(BigDecimal.ZERO) > 0) {
      displaySize = size.divide(ONE_TB_BI).setScale(2, RoundingMode.HALF_DOWN).doubleValue() + " TB"
    } else if (size.divideToIntegralValue(ONE_GB_BI).compareTo(BigDecimal.ZERO) > 0) {
      displaySize = size.divide(ONE_GB_BI).setScale(2, RoundingMode.HALF_DOWN).doubleValue() + " GB"
    } else if (size.divideToIntegralValue(ONE_MB_BI).compareTo(BigDecimal.ZERO) > 0) {
      displaySize = size.divide(ONE_MB_BI).setScale(2, RoundingMode.HALF_DOWN).doubleValue() + " MB"
    } else if (size.divideToIntegralValue(ONE_KB_BI).compareTo(BigDecimal.ZERO) > 0) {
      displaySize = size.divide(ONE_KB_BI).setScale(2, RoundingMode.HALF_DOWN).doubleValue() + " KB"
    } else {
      displaySize = size + " B"
    }
    displaySize
  }

  def getObjectArrayFromInternalRowAndConvertComplexType(row: InternalRow,
      fieldTypes: Seq[DataType],
      outputArrayLength: Int): Array[AnyRef] = {
    val data = new Array[AnyRef](outputArrayLength)
    var i = 0
    val fieldTypesLen = fieldTypes.length
    while (i < fieldTypesLen) {
      if (!row.isNullAt(i)) {
        fieldTypes(i) match {
          case StringType =>
            // get normal (non-UTF8) string
            data(i) = row.getString(i)
          case d: DecimalType =>
            data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
          case arrayType : ArrayType =>
            data(i) = convertSparkComplexTypeToCarbonObject(row.getArray(i), arrayType)
          case structType : StructType =>
            data(i) = convertSparkComplexTypeToCarbonObject(row.getStruct(i,
              structType.fields.length), structType)
          case mapType : MapType =>
            data(i) = convertSparkComplexTypeToCarbonObject(row.getMap(i), mapType)
          case other =>
            data(i) = row.get(i, other)
        }
      }
      i += 1
    }
    data
  }

  /**
   * After converting complex objects to carbon objects, need to convert to byte array
   *
   * @param row
   * @param fields
   * @param dataFieldsWithComplexDataType
   * @return
   */
  def getObjectArrayFromInternalRowAndConvertComplexTypeForGlobalSort(
      row: InternalRow,
      fields: Seq[StructField],
      dataFieldsWithComplexDataType: Map[String, GenericDataType[_]]): Array[AnyRef] = {
    val data = new Array[AnyRef](fields.size)
    val badRecordLogHolder = new BadRecordLogHolder();
    var i = 0
    val fieldTypesLen = fields.length
    while (i < fieldTypesLen) {
      if (!row.isNullAt(i)) {
        fields(i).dataType match {
          case StringType =>
            data(i) = DataTypeUtil.getBytesDataDataTypeForNoDictionaryColumn(row.getString(i),
              DataTypes.STRING)
          case d: DecimalType =>
            data(i) = row.getDecimal(i, d.precision, d.scale).toJavaBigDecimal
          case arrayType : ArrayType =>
            val result = convertSparkComplexTypeToCarbonObject(row.get(i, arrayType), arrayType)
            // convert carbon complex object to byte array
            val byteArray: ByteArrayOutputStream = new ByteArrayOutputStream()
            val dataOutputStream: DataOutputStream = new DataOutputStream(byteArray)
            dataFieldsWithComplexDataType(fields(i).name).asInstanceOf[ArrayDataType]
              .writeByteArray(result.asInstanceOf[ArrayObject],
                dataOutputStream,
                badRecordLogHolder)
            dataOutputStream.close()
            data(i) = byteArray.toByteArray.asInstanceOf[AnyRef]
          case structType : StructType =>
            val result = convertSparkComplexTypeToCarbonObject(row.get(i, structType), structType)
            // convert carbon complex object to byte array
            val byteArray: ByteArrayOutputStream = new ByteArrayOutputStream()
            val dataOutputStream: DataOutputStream = new DataOutputStream(byteArray)
            dataFieldsWithComplexDataType(fields(i).name).asInstanceOf[StructDataType]
              .writeByteArray(result.asInstanceOf[StructObject],
                dataOutputStream,
                badRecordLogHolder)
            dataOutputStream.close()
            data(i) = byteArray.toByteArray.asInstanceOf[AnyRef]
          case mapType : MapType =>
            val result = convertSparkComplexTypeToCarbonObject(row.get(i, mapType), mapType)
            // convert carbon complex object to byte array
            val byteArray: ByteArrayOutputStream = new ByteArrayOutputStream()
            val dataOutputStream: DataOutputStream = new DataOutputStream(byteArray)
            dataFieldsWithComplexDataType(fields(i).name).asInstanceOf[ArrayDataType]
              .writeByteArray(result.asInstanceOf[ArrayObject],
                dataOutputStream,
                badRecordLogHolder)
            dataOutputStream.close()
            data(i) = byteArray.toByteArray.asInstanceOf[AnyRef]
          case other =>
            data(i) = row.get(i, other)
        }
      }
      i += 1
    }
    data
  }

  private def convertSparkComplexTypeToCarbonObject(data: AnyRef,
      objectDataType: DataType): AnyRef = {
    objectDataType match {
      case _: ArrayType =>
        val arrayDataType = objectDataType.asInstanceOf[ArrayType]
        val arrayData = data.asInstanceOf[UnsafeArrayData]
        val size = arrayData.numElements()
        val childDataType = arrayDataType.elementType
        val arrayChildObjects = new Array[AnyRef](size)
        var i = 0
        while (i < size) {
          arrayChildObjects(i) = convertSparkComplexTypeToCarbonObject(arrayData.get(i,
            childDataType), childDataType)
          i = i + 1
        }
        new ArrayObject(arrayChildObjects)
      case _: MapType =>
        val mapDataType = objectDataType.asInstanceOf[MapType]
        val keyDataType = mapDataType.keyType
        val valueDataType = mapDataType.valueType
        val mapData = data.asInstanceOf[UnsafeMapData]
        val size = mapData.numElements()
        val keys = mapData.keyArray()
        val values = mapData.valueArray()
        val arrayMapChildObjects = new Array[AnyRef](size)
        var i = 0
        while (i < size) {
          val structChildObjects = new Array[AnyRef](2)
          structChildObjects(0) = convertSparkComplexTypeToCarbonObject(keys.get(i, keyDataType),
            keyDataType)
          structChildObjects(1) = convertSparkComplexTypeToCarbonObject(values.get(i,
            valueDataType), valueDataType)
          arrayMapChildObjects(i) = new StructObject(structChildObjects)
          i = i + 1
        }
        new ArrayObject(arrayMapChildObjects)
      case _: StructType =>
        val structDataType = objectDataType.asInstanceOf[StructType]
        val structData = data.asInstanceOf[UnsafeRow]
        val size = structData.numFields()
        val structChildObjects = new Array[AnyRef](size)
        var i = 0
        val childrenSchema = structDataType.fields
        while (i < size) {
          structChildObjects(i) = convertSparkComplexTypeToCarbonObject(structData.get(i,
            childrenSchema(i).dataType), childrenSchema(i).dataType)
          i = i + 1
        }
        new StructObject(structChildObjects)
      case _: DateType =>
        if (data == null) {
          CarbonCommonConstants.DIRECT_DICT_VALUE_NULL.asInstanceOf[AnyRef]
        } else {
          (data.asInstanceOf[Int] + DateDirectDictionaryGenerator.cutOffDate).asInstanceOf[AnyRef]
        }
      case _: TimestampType =>
        if (data == null) {
          null
        } else {
          (data.asInstanceOf[Long] / 1000).asInstanceOf[AnyRef]
        }
      case _ => data
    }
  }

  def convertComplexDataType(dataFieldsWithComplexDataType: Map[String,
    GenericDataType[_]], configuration: CarbonDataLoadConfiguration): Unit = {
    val fields = configuration.getDataFields
    val nullFormat = configuration
      .getDataLoadProperty(DataLoadProcessorConstants.SERIALIZATION_NULL_FORMAT)
      .toString
    for (field <- fields) {
      if (field.getColumn.isComplex) {
        dataFieldsWithComplexDataType +=
        (field.getColumn.getColName ->
         FieldEncoderFactory.createComplexDataType(field, nullFormat, null))
      }
    }
  }

}
