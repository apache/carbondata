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
import java.nio.charset.Charset
import java.text.SimpleDateFormat
import java.util.Date

import com.univocity.parsers.common.TextParsingException
import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.CatalogTablePartition
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.command.{DataTypeInfo, UpdateTableModel}
import org.apache.spark.sql.types._

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogService
import org.apache.carbondata.core.cache.{Cache, CacheProvider, CacheType}
import org.apache.carbondata.core.cache.dictionary.{Dictionary, DictionaryColumnUniqueIdentifier}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory
import org.apache.carbondata.core.metadata.ColumnIdentifier
import org.apache.carbondata.core.metadata.datatype.{DataType => CarbonDataType, DataTypes => CarbonDataTypes, StructField => CarbonStructField}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.core.util.DataTypeUtil
import org.apache.carbondata.processing.exception.DataLoadingException
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil

object CarbonScalaUtil {
  def convertSparkToCarbonDataType(dataType: DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataTypes.STRING
      case ShortType => CarbonDataTypes.SHORT
      case IntegerType => CarbonDataTypes.INT
      case LongType => CarbonDataTypes.LONG
      case DoubleType => CarbonDataTypes.DOUBLE
      case FloatType => CarbonDataTypes.FLOAT
      case DateType => CarbonDataTypes.DATE
      case BooleanType => CarbonDataTypes.BOOLEAN
      case TimestampType => CarbonDataTypes.TIMESTAMP
      case ArrayType(elementType, _) =>
        CarbonDataTypes.createArrayType(CarbonScalaUtil.convertSparkToCarbonDataType(elementType))
      case StructType(fields) =>
        val carbonFields = new util.ArrayList[CarbonStructField]
        fields.map { field =>
          carbonFields.add(
            new CarbonStructField(
              field.name,
              CarbonScalaUtil.convertSparkToCarbonDataType(field.dataType)))
        }
        CarbonDataTypes.createStructType(carbonFields)
      case NullType => CarbonDataTypes.NULL
      case decimal: DecimalType =>
        CarbonDataTypes.createDecimalType(decimal.precision, decimal.scale)
      case _ => throw new UnsupportedOperationException("getting " + dataType + " from spark")
    }
  }

  def convertSparkToCarbonSchemaDataType(dataType: String): String = {
    dataType match {
      case CarbonCommonConstants.STRING_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.INTEGER_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.BYTE_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.SHORT_TYPE => CarbonCommonConstants.SHORT
      case CarbonCommonConstants.LONG_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DOUBLE_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.FLOAT_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DECIMAL_TYPE => CarbonCommonConstants.NUMERIC
      case CarbonCommonConstants.DATE_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.BOOLEAN_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.TIMESTAMP_TYPE => CarbonCommonConstants.TIMESTAMP
      case anyType => anyType
    }
  }

  def convertCarbonToSparkDataType(dataType: CarbonDataType): types.DataType = {
    if (CarbonDataTypes.isDecimal(dataType)) {
      DecimalType.SYSTEM_DEFAULT
    } else {
      dataType match {
        case CarbonDataTypes.STRING => StringType
        case CarbonDataTypes.SHORT => ShortType
        case CarbonDataTypes.INT => IntegerType
        case CarbonDataTypes.LONG => LongType
        case CarbonDataTypes.DOUBLE => DoubleType
        case CarbonDataTypes.BOOLEAN => BooleanType
        case CarbonDataTypes.TIMESTAMP => TimestampType
        case CarbonDataTypes.DATE => DateType
      }
    }
  }

  def getString(value: Any,
      serializationNullFormat: String,
      delimiterLevel1: String,
      delimiterLevel2: String,
      timeStampFormat: SimpleDateFormat,
      dateFormat: SimpleDateFormat,
      level: Int = 1): String = {
    if (value == null) {
      serializationNullFormat
    } else {
      value match {
        case s: String => if (s.length > CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT) {
          throw new Exception("Dataload failed, String length cannot exceed " +
                              CarbonCommonConstants.MAX_CHARS_PER_COLUMN_DEFAULT + " characters")
        } else {
          s
        }
        case d: java.math.BigDecimal => d.toPlainString
        case i: java.lang.Integer => i.toString
        case d: java.lang.Double => d.toString
        case t: java.sql.Timestamp => timeStampFormat format t
        case d: java.sql.Date => dateFormat format d
        case b: java.lang.Boolean => b.toString
        case s: java.lang.Short => s.toString
        case f: java.lang.Float => f.toString
        case bs: Array[Byte] => new String(bs,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET))
        case s: scala.collection.Seq[Any] =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          s.foreach { x =>
            builder.append(getString(x, serializationNullFormat, delimiterLevel1,
                delimiterLevel2, timeStampFormat, dateFormat, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - delimiter.length())
        case m: scala.collection.Map[Any, Any] =>
          throw new Exception("Unsupported data type: Map")
        case r: org.apache.spark.sql.Row =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          for (i <- 0 until r.length) {
            builder.append(getString(r(i), serializationNullFormat, delimiterLevel1,
                delimiterLevel2, timeStampFormat, dateFormat, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - delimiter.length())
        case other => other.toString
      }
    }
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
              .getDataBasedOnDataType(value, convertSparkToCarbonDataType(dataType))
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
  def updatePartitions(
      partitionSpec: Map[String, String],
  table: CarbonTable): Map[String, String] = {
    val cacheProvider: CacheProvider = CacheProvider.getInstance
    val forwardDictionaryCache: Cache[DictionaryColumnUniqueIdentifier, Dictionary] =
      cacheProvider.createCache(CacheType.FORWARD_DICTIONARY)
    partitionSpec.map{ case (col, pvalue) =>
      // replace special string with empty value.
      val value = if (pvalue == null) {
        hivedefaultpartition
      } else if (pvalue.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        ""
      } else {
        pvalue
      }
      val carbonColumn = table.getColumnByName(table.getTableName, col.toLowerCase)
      val dataType = CarbonScalaUtil.convertCarbonToSparkDataType(carbonColumn.getDataType)
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
    parts.map{ f =>
      val changedSpec =
        updatePartitions(
          f.spec,
          table)
      f.copy(spec = changedSpec)
    }.groupBy(p => p.spec).map(f => f._2.head).toSeq // Avoid duplicates by do groupby
  }

  /**
   * This method will validate a column for its data type and check whether the column data type
   * can be modified and update if conditions are met
   *
   * @param dataTypeInfo
   * @param carbonColumn
   */
  def validateColumnDataType(dataTypeInfo: DataTypeInfo, carbonColumn: CarbonColumn): Unit = {
    carbonColumn.getDataType.getName match {
      case "INT" =>
        if (!dataTypeInfo.dataType.equals("bigint") && !dataTypeInfo.dataType.equals("long")) {
          sys
            .error(s"Given column ${ carbonColumn.getColName } with data type ${
              carbonColumn
                .getDataType.getName
            } cannot be modified. Int can only be changed to bigInt or long")
        }
      case "DECIMAL" =>
        if (!dataTypeInfo.dataType.equals("decimal")) {
          sys
            .error(s"Given column ${ carbonColumn.getColName } with data type ${
              carbonColumn.getDataType.getName
            } cannot be modified. Decimal can be only be changed to Decimal of higher precision")
        }
        if (dataTypeInfo.precision <= carbonColumn.getColumnSchema.getPrecision) {
          sys
            .error(s"Given column ${
              carbonColumn
                .getColName
            } cannot be modified. Specified precision value ${
              dataTypeInfo
                .precision
            } should be greater than current precision value ${
              carbonColumn.getColumnSchema
                .getPrecision
            }")
        } else if (dataTypeInfo.scale < carbonColumn.getColumnSchema.getScale) {
          sys
            .error(s"Given column ${
              carbonColumn
                .getColName
            } cannot be modified. Specified scale value ${
              dataTypeInfo
                .scale
            } should be greater or equal to current scale value ${
              carbonColumn.getColumnSchema
                .getScale
            }")
        } else {
          // difference of precision and scale specified by user should not be less than the
          // difference of already existing precision and scale else it will result in data loss
          val carbonColumnPrecisionScaleDiff = carbonColumn.getColumnSchema.getPrecision -
                                               carbonColumn.getColumnSchema.getScale
          val dataInfoPrecisionScaleDiff = dataTypeInfo.precision - dataTypeInfo.scale
          if (dataInfoPrecisionScaleDiff < carbonColumnPrecisionScaleDiff) {
            sys
              .error(s"Given column ${
                carbonColumn
                  .getColName
              } cannot be modified. Specified precision and scale values will lead to data loss")
          }
        }
      case _ =>
        sys
          .error(s"Given column ${ carbonColumn.getColName } with data type ${
            carbonColumn
              .getDataType.getName
          } cannot be modified. Only Int and Decimal data types are allowed for modification")
    }
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
  def retrieveAndLogErrorMsg(ex: Throwable, logger: LogService): (String, String) = {
    var errorMessage = "DataLoad failure"
    var executorMessage = ""
    if (ex != null) {
      ex match {
        case sparkException: SparkException =>
          if (sparkException.getCause.isInstanceOf[DataLoadingException] ||
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
}
