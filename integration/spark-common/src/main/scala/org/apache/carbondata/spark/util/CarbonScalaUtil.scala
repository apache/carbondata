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

import java.{lang, util}
import java.io.IOException
import java.lang.ref.Reference
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.util.Try

import com.univocity.parsers.common.TextParsingException
import org.apache.log4j.Logger
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.{Field, UpdateTableModel}
import org.apache.spark.sql.types._
import org.apache.spark.util.CarbonReflectionUtils

import org.apache.carbondata.common.exceptions.MetadataProcessException
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory
import org.apache.carbondata.core.metadata.ColumnIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, DataMapSchema}
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.streaming.parser.FieldConverter

object CarbonScalaUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def getString(value: Any,
      serializationNullFormat: String,
      delimiterLevel1: String,
      delimiterLevel2: String,
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat,
      isVarcharType: Boolean = false,
      level: Int = 1): String = {
    FieldConverter.objectToString(value, serializationNullFormat, delimiterLevel1,
      delimiterLevel2, timeStampFormat, dateFormat, isVarcharType = isVarcharType, level)
  }

  /**
   * Converts incoming value to String after converting data as per the data type.
   * @param value Input value to convert
   * @param dataType Datatype to convert and then convert to String
   * @param timeStampFormat Timestamp format to convert in case of timestamp datatypes
   * @param dateFormat DataFormat to convert in case of DateType datatype
   * @return converted String
   */
  def convertToDateAndTimeFormats(
      value: String,
      dataType: DataType,
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat): String = {
    val defaultValue = value != null && value.equalsIgnoreCase(hivedefaultpartition)
    try {
      dataType match {
        case TimestampType if timeStampFormat != null =>
          if (defaultValue) {
            timeStampFormat.format(new Date())
          } else {
            timeStampFormat.format(DateTimeUtils.stringToTime(value))
          }
        case DateType if dateFormat != null =>
          if (defaultValue) {
            dateFormat.format(new Date())
          } else {
            dateFormat.format(DateTimeUtils.stringToTime(value))
          }
        case _ =>
          val convertedValue =
            DataTypeUtil
              .getDataBasedOnDataType(value,
                CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
          if (convertedValue == null) {
            if (defaultValue) {
              return dataType match {
                case BooleanType => "false"
                case _ => "0"
              }
            }
            throw new MalformedCarbonCommandException(
              s"Value $value with datatype $dataType on static partition is not correct")
          }
          value
      }
    } catch {
      case e: Exception =>
        throw new MalformedCarbonCommandException(
          s"Value $value with datatype $dataType on static partition is not correct")
    }
  }

  /**
   * Converts incoming value to String after converting data as per the data type.
   * @param value Input value to convert
   * @param column column which it value belongs to
   * @return converted String
   */
  def convertToCarbonFormat(
      value: String,
      column: CarbonColumn,
      forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary],
      table: CarbonTable): String = {
    if (column.hasEncoding(Encoding.DICTIONARY)) {
      if (column.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        if (column.getDataType.equals(CarbonDataTypes.TIMESTAMP)) {
          val time = DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
            column.getDataType,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
          ).getValueFromSurrogate(value.toInt)
          if (time == null) {
            return null
          }
          return DateTimeUtils.timestampToString(time.toString.toLong * 1000)
        } else if (column.getDataType.equals(CarbonDataTypes.DATE)) {
          val date = DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
            column.getDataType,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT
          ).getValueFromSurrogate(value.toInt)
          if (date == null) {
            return null
          }
          return DateTimeUtils.dateToString(date.toString.toInt)
        }
      }
      val dictionaryPath =
        table.getTableInfo.getFactTable.getTableProperties.get(
          CarbonCommonConstants.DICTIONARY_PATH)
      val dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
        table.getAbsoluteTableIdentifier,
        column.getColumnIdentifier, column.getDataType,
        dictionaryPath)
      return forwardDictionaryCache.get(
        dictionaryColumnUniqueIdentifier).getDictionaryValueForKey(value.toInt)
    }
    try {
      column.getDataType match {
        case CarbonDataTypes.TIMESTAMP =>
          DateTimeUtils.timestampToString(value.toLong * 1000)
        case CarbonDataTypes.DATE =>
          DateTimeUtils.dateToString(DateTimeUtils.millisToDays(value.toLong))
        case _ => value
      }
    } catch {
      case e: Exception =>
        value
    }
  }

  /**
   * Converts incoming value to String after converting data as per the data type.
   * @param value Input value to convert
   * @param column column which it value belongs to
   * @return converted String
   */
  def convertStaticPartitions(
      value: String,
      column: ColumnSchema,
      table: CarbonTable): String = {
    try {
      if (column.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        if (column.getDataType.equals(CarbonDataTypes.TIMESTAMP)) {
          return DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
            column.getDataType,
            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT
          ).generateDirectSurrogateKey(value).toString
        } else if (column.getDataType.equals(CarbonDataTypes.DATE)) {
          return DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
            column.getDataType,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT
          ).generateDirectSurrogateKey(value).toString
        }
      } else if (column.hasEncoding(Encoding.DICTIONARY)) {
        val cacheProvider: CacheProvider = CacheProvider.getInstance
        val reverseCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
          cacheProvider.createCache(CacheType.REVERSE_DICTIONARY)
        val dictionaryPath =
          table.getTableInfo.getFactTable.getTableProperties.get(
            CarbonCommonConstants.DICTIONARY_PATH)
        val dictionaryColumnUniqueIdentifier = new DictionaryColumnUniqueIdentifier(
          table.getAbsoluteTableIdentifier,
          new ColumnIdentifier(
            column.getColumnUniqueId,
            column.getColumnProperties,
            column.getDataType),
          column.getDataType,
          dictionaryPath)
        return reverseCache.get(dictionaryColumnUniqueIdentifier).getSurrogateKey(value).toString
      }
      column.getDataType match {
        case CarbonDataTypes.TIMESTAMP =>
          DateTimeUtils.stringToTime(value).getTime.toString
        case CarbonDataTypes.DATE =>
          DateTimeUtils.stringToTime(value).getTime.toString
        case _ => value
      }
    } catch {
      case e: Exception =>
        value
    }
  }

  private val hivedefaultpartition = "__HIVE_DEFAULT_PARTITION__"

  /**
   * Update partition values as per the right date and time format
   * @return updated partition spec
   */
  def updatePartitions(partitionSpec: mutable.LinkedHashMap[String, String],
      table: CarbonTable): mutable.LinkedHashMap[String, String] = {
    val cacheProvider: CacheProvider = CacheProvider.getInstance
    val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
      cacheProvider.createCache(CacheType.FORWARD_DICTIONARY)
    partitionSpec.map { case (col, pvalue) =>
      // replace special string with empty value.
      val value = if (pvalue == null) {
        hivedefaultpartition
      } else if (pvalue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        ""
      } else {
        pvalue
      }
      val carbonColumn = table.getColumnByName(table.getTableName, col.toLowerCase)
      val dataType =
        CarbonSparkDataSourceUtil.convertCarbonToSparkDataType(carbonColumn.getDataType)
      try {
        if (value.equals(hivedefaultpartition)) {
          (col, value)
        } else {
          val convertedString =
            CarbonScalaUtil.convertToCarbonFormat(
              value,
              carbonColumn,
              forwardDictionaryCache,
              table)
          if (convertedString == null) {
            (col, hivedefaultpartition)
          } else {
            (col, convertedString)
          }
        }
      } catch {
        case e: Exception =>
          (col, value)
      }
    }
  }

  /**
   * Update partition values as per the right date and time format
   */
  def updatePartitions(
      parts: Seq[CatalogTablePartition],
      table: CarbonTable): Seq[CatalogTablePartition] = {
    parts.map { f =>
      val specLinkedMap: mutable.LinkedHashMap[String, String] = mutable.LinkedHashMap
        .empty[String, String]
      f.spec.foreach(fSpec => specLinkedMap.put(fSpec._1, fSpec._2))
      val changedSpec =
        updatePartitions(
          specLinkedMap,
          table).toMap
      f.copy(spec = changedSpec)
    }.groupBy(p => p.spec).map(f => f._2.head).toSeq // Avoid duplicates by do groupby
  }

  /**
   * returns  all fields except tupleId field as it is not required in the value
   *
   * @param fields
   * @return
   */
  def getAllFieldsWithoutTupleIdField(fields: Array[StructField]): Seq[Column] = {
    // getting all fields except tupleId field as it is not required in the value
    val otherFields = fields.toSeq
      .filter(field => !field.name
        .equalsIgnoreCase(CarbonCommonConstants.CARBON_IMPLICIT_COLUMN_TUPLEID))
      .map(field => {
        new Column(field.name)
      })
    otherFields
  }

  /**
   * If the table is from an old store then the table parameters are in lowercase. In the current
   * code we are reading the parameters as camel case.
   * This method will convert all the schema parts to camel case
   *
   * @param parameters
   * @return
   */
  def getDeserializedParameters(parameters: Map[String, String]): Map[String, String] = {
    val keyParts = parameters.getOrElse("spark.sql.sources.options.keys.numparts", "0").toInt
    if (keyParts == 0) {
      parameters
    } else {
      val keyStr = 0 until keyParts map {
        i => parameters(s"spark.sql.sources.options.keys.part.$i")
      }
      val finalProperties = scala.collection.mutable.Map.empty[String, String]
      keyStr foreach {
        key =>
          var value = ""
          for (numValues <- 0 until parameters(key.toLowerCase() + ".numparts").toInt) {
            value += parameters(key.toLowerCase() + ".part" + numValues)
          }
          finalProperties.put(key, value)
      }
      // Database name would be extracted from the parameter first. There can be a scenario where
      // the dbName is not written to the old schema therefore to be on a safer side we are
      // extracting dbName from tableName if it exists.
      val dbAndTableName = finalProperties("tableName").split(".")
      if (dbAndTableName.length > 1) {
        finalProperties.put("dbName", dbAndTableName(0))
        finalProperties.put("tableName", dbAndTableName(1))
      } else {
        finalProperties.put("tableName", dbAndTableName(0))
      }
      // Overriding the tablePath in case tablepath already exists. This will happen when old
      // table schema is updated by the new code then both `path` and `tablepath` will exist. In
      // this case use tablepath
      parameters.get("tablepath") match {
        case Some(tablePath) => finalProperties.put("tablePath", tablePath)
        case None =>
      }
      finalProperties.toMap
    }
  }

  /**
   * Retrieve error message from exception
   */
  def retrieveAndLogErrorMsg(ex: Throwable, logger: Logger): (String, String) = {
    var errorMessage = "DataLoad failure"
    var executorMessage = ""
    if (ex != null) {
      ex match {
        case sparkException: SparkException =>
          if (sparkException.getCause.isInstanceOf[IOException]) {
            if (sparkException.getCause.getCause.isInstanceOf[MetadataProcessException]) {
              executorMessage = sparkException.getCause.getCause.getMessage
              errorMessage = errorMessage + ": " + executorMessage
            } else {
              executorMessage = sparkException.getCause.getMessage
              errorMessage = errorMessage + ": " + executorMessage
            }
          } else if (sparkException.getCause.isInstanceOf[DataLoadingException] ||
                     sparkException.getCause.isInstanceOf[CarbonDataLoadingException]) {
            executorMessage = sparkException.getCause.getMessage
            errorMessage = errorMessage + ": " + executorMessage
          } else if (sparkException.getCause.isInstanceOf[TextParsingException]) {
            executorMessage = CarbonDataProcessorUtil
              .trimErrorMessage(sparkException.getCause.getMessage)
            errorMessage = errorMessage + " : " + executorMessage
          } else if (sparkException.getCause.isInstanceOf[SparkException]) {
            val (executorMsgLocal, errorMsgLocal) =
              retrieveAndLogErrorMsg(sparkException.getCause, logger)
            executorMessage = executorMsgLocal
            errorMessage = errorMsgLocal
          }
        case aex: AnalysisException =>
          logger.error(aex.getMessage())
          throw aex
        case _ =>
          if (ex.getCause != null) {
            executorMessage = ex.getCause.getMessage
            errorMessage = errorMessage + ": " + executorMessage
          }
      }
    }
    (executorMessage, errorMessage)
  }

  /**
   * Update error inside update model
   */
  def updateErrorInUpdateModel(updateModel: UpdateTableModel, executorMessage: String): Unit = {
    if (updateModel.executorErrors.failureCauses == FailureCauses.NONE) {
      updateModel.executorErrors.failureCauses = FailureCauses.EXECUTOR_FAILURE
      if (null != executorMessage && !executorMessage.isEmpty) {
        updateModel.executorErrors.errorMsg = executorMessage
      } else {
        updateModel.executorErrors.errorMsg = "Update failed as the data load has failed."
      }
    }
  }

  /**
   * Generate unique number to be used as partition number of file name
   */
  def generateUniqueNumber(taskId: Int,
      segmentId: String,
      partitionNumber: lang.Long): String = {
    String.valueOf(Math.pow(10, 2).toInt + segmentId.toInt) +
    String.valueOf(Math.pow(10, 5).toInt + taskId) +
    String.valueOf(partitionNumber + Math.pow(10, 5).toInt)
  }

  /**
   * Use reflection to clean the parser objects which are set in thread local to avoid memory issue
   */
  def cleanParserThreadLocals(): Unit = {
    try {
      // Get a reference to the thread locals table of the current thread
      val thread = Thread.currentThread
      val threadLocalsField = classOf[Thread].getDeclaredField("inheritableThreadLocals")
      threadLocalsField.setAccessible(true)
      val threadLocalTable = threadLocalsField.get(thread)
      // Get a reference to the array holding the thread local variables inside the
      // ThreadLocalMap of the current thread
      val threadLocalMapClass = Class.forName("java.lang.ThreadLocal$ThreadLocalMap")
      val tableField = threadLocalMapClass.getDeclaredField("table")
      tableField.setAccessible(true)
      val table = tableField.get(threadLocalTable)
      // The key to the ThreadLocalMap is a WeakReference object. The referent field of this object
      // is a reference to the actual ThreadLocal variable
      val referentField = classOf[Reference[Thread]].getDeclaredField("referent")
      referentField.setAccessible(true)
      var i = 0
      while (i < lang.reflect.Array.getLength(table)) {
        // Each entry in the table array of ThreadLocalMap is an Entry object
        val entry = lang.reflect.Array.get(table, i)
        if (entry != null) {
          // Get a reference to the thread local object and remove it from the table
          val threadLocal = referentField.get(entry).asInstanceOf[ThreadLocal[_]]
          if (threadLocal != null &&
              threadLocal.getClass.getName.startsWith("scala.util.DynamicVariable")) {
            threadLocal.remove()
          }
        }
        i += 1
      }
    } catch {
      case e: Exception =>
        // ignore it
    }
  }

  /**
   * Create datamap provider using class name
   */
  def createDataMapProvider(
      className: String,
      sparkSession: SparkSession,
      table: CarbonTable,
      schema: DataMapSchema): Object = {
    CarbonReflectionUtils.createObject(
      className,
      table,
      sparkSession,
      schema)._1.asInstanceOf[Object]
  }

  /**
   * this method validates the local dictionary columns configurations
   *
   * @param tableProperties
   * @param localDictColumns
   */
  def validateLocalDictionaryColumns(tableProperties: mutable.Map[String, String],
      localDictColumns: Seq[String]): Unit = {
    var dictIncludeColumns: Seq[String] = Seq[String]()

    // check if the duplicate columns are specified in table schema
    if (localDictColumns.distinct.lengthCompare(localDictColumns.size) != 0) {
      val duplicateColumns = localDictColumns
        .diff(localDictColumns.distinct).distinct
      val errMsg =
        "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE contains Duplicate Columns: " +
        duplicateColumns.mkString(",") +
        ". Please check the DDL."
      throw new MalformedCarbonCommandException(errMsg)
    }

    // check if the same column is present in both dictionary include and local dictionary columns
    // configuration
    if (tableProperties.get(CarbonCommonConstants.DICTIONARY_INCLUDE).isDefined) {
      dictIncludeColumns =
        tableProperties(CarbonCommonConstants.DICTIONARY_INCLUDE).split(",").map(_.trim)
      localDictColumns.foreach { distCol =>
        if (dictIncludeColumns.exists(x => x.equalsIgnoreCase(distCol.trim))) {
          val commonColumn = (dictIncludeColumns ++ localDictColumns)
            .diff((dictIncludeColumns ++ localDictColumns).distinct).distinct
          val errormsg = "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: " +
                         commonColumn.mkString(",") +
                         " specified in Dictionary include. Local Dictionary will not be " +
                         "generated for Dictionary include columns. Please check the DDL."
          throw new MalformedCarbonCommandException(errormsg)
        }
      }
    }
  }

  /**
   * this method validates the local dictionary enable property
   *
   * @param localDictionaryEnable
   * @return
   */
  def validateLocalDictionaryEnable(localDictionaryEnable: String): Boolean = {
    Try(localDictionaryEnable.toBoolean) match {
      case scala.util.Success(value) =>
        true
      case scala.util.Failure(ex) =>
        false
    }
  }

  /**
   * this method validates the local dictionary threshold property
   *
   * @param localDictionaryThreshold
   * @return
   */
  def validateLocalDictionaryThreshold(localDictionaryThreshold: String): Boolean = {
    // if any invalid value is configured for LOCAL_DICTIONARY_THRESHOLD, then default value
    // will be
    // considered which is 1000
    Try(localDictionaryThreshold.toInt) match {
      case scala.util.Success(value) =>
        if (value < CarbonCommonConstants.LOCAL_DICTIONARY_MIN ||
            value > CarbonCommonConstants.LOCAL_DICTIONARY_MAX) {
          false
        } else {
          true
        }
      case scala.util.Failure(ex) =>
        false
    }
  }

  /**
   * This method validate if both local dictionary include and exclude contains same column
   *
   * @param tableProperties
   */
  def validateDuplicateLocalDictIncludeExcludeColmns(tableProperties: mutable.Map[String,
    String]): Unit = {
    val isLocalDictIncludeDefined = tableProperties
      .get(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE)
      .isDefined
    val isLocalDictExcludeDefined = tableProperties
      .get(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE)
      .isDefined
    if (isLocalDictIncludeDefined && isLocalDictExcludeDefined) {
      val localDictIncludeCols = tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_INCLUDE)
        .split(",").map(_.trim)
      val localDictExcludeCols = tableProperties(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE)
        .split(",").map(_.trim)
      localDictIncludeCols.foreach { distCol =>
        if (localDictExcludeCols.exists(x => x.equalsIgnoreCase(distCol.trim))) {
          val duplicateColumns = (localDictIncludeCols ++ localDictExcludeCols)
            .diff((localDictIncludeCols ++ localDictExcludeCols).distinct).distinct
          val errMsg = "Column ambiguity as duplicate column(s):" +
                       duplicateColumns.mkString(",") +
                       " is present in LOCAL_DICTIONARY_INCLUDE " +
                       "and LOCAL_DICTIONARY_EXCLUDE. Duplicate columns are not allowed."
          throw new MalformedCarbonCommandException(errMsg)
        }
      }
    }
  }

  /**
   * This method validates all the child columns of complex column recursively to check whether
   * any of the child column is of string dataType or not
   *
   * @param field
   */
  def validateChildColumnsRecursively(field: Field): Boolean = {
    if (field.children.isDefined && null != field.children.get) {
      field.children.get.exists { childColumn =>
        if (childColumn.children.isDefined && null != childColumn.children.get) {
          validateChildColumnsRecursively(childColumn)
        } else {
          childColumn.dataType.get.equalsIgnoreCase("string")
        }
      }
    } else {
      false
    }
  }

  /**
   * This method validates the local dictionary configured columns
   *
   * @param fields
   * @param tableProperties
   */
  def validateLocalConfiguredDictionaryColumns(fields: Seq[Field],
      tableProperties: mutable.Map[String, String], localDictColumns: Seq[String]): Unit = {
    var dictIncludeColumns: Seq[String] = Seq[String]()

    // validate the local dict columns
    CarbonScalaUtil.validateLocalDictionaryColumns(tableProperties, localDictColumns)
    // check if the column specified exists in table schema
    localDictColumns.foreach { distCol =>
      if (!fields.exists(x => x.column.equalsIgnoreCase(distCol.trim))) {
        val errormsg = "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: " + distCol.trim +
                       " does not exist in table. Please check the DDL."
        throw new MalformedCarbonCommandException(errormsg)
      }
    }

    // check if column is other than STRING or VARCHAR datatype
    localDictColumns.foreach { dictColm =>
      if (fields
        .exists(x => x.column.equalsIgnoreCase(dictColm) &&
                     !x.dataType.get.equalsIgnoreCase("STRING") &&
                     !x.dataType.get.equalsIgnoreCase("VARCHAR") &&
                     !x.dataType.get.equalsIgnoreCase("STRUCT") &&
                     !x.dataType.get.equalsIgnoreCase("ARRAY"))) {
        val errormsg = "LOCAL_DICTIONARY_INCLUDE/LOCAL_DICTIONARY_EXCLUDE column: " +
                       dictColm.trim +
                       " is not a string/complex/varchar datatype column. LOCAL_DICTIONARY_COLUMN" +
                       " should be no dictionary string/complex/varchar datatype column." +
                       "Please check the DDL."
        throw new MalformedCarbonCommandException(errormsg)
      }
    }

    // Validate whether any of the child columns of complex dataType column is a string column
    localDictColumns.foreach { dictColm =>
      if (fields
        .exists(x => x.column.equalsIgnoreCase(dictColm) && x.children.isDefined &&
                     null != x.children.get &&
                     !validateChildColumnsRecursively(x))) {
        val errMsg =
          s"None of the child columns of complex dataType column $dictColm specified in " +
          "local_dictionary_include are not of string dataType."
        throw new MalformedCarbonCommandException(errMsg)
      }
    }
  }

  def isStringDataType(dataType: DataType): Boolean = {
    dataType == StringType
  }

  /**
   * Rearrange the column schema with all the sort columns at first. In case of ALTER ADD COLUMNS,
   * if the newly added column is a sort column it will be at the last. But we expects all the
   * SORT_COLUMNS always at first
   *
   * @param columnSchemas
   * @return
   */
  def reArrangeColumnSchema(columnSchemas: mutable.Buffer[ColumnSchema]): mutable
  .Buffer[ColumnSchema] = {
    val newColumnSchemas = mutable.Buffer[ColumnSchema]()
    newColumnSchemas ++= columnSchemas.filter(columnSchema => columnSchema.isSortColumn)
    newColumnSchemas ++= columnSchemas.filterNot(columnSchema => columnSchema.isSortColumn)
    newColumnSchemas
  }

}
