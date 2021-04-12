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
import java.time.ZoneId
import java.util.Date

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil
import org.apache.spark.sql.catalyst.parser.ParserUtils.string
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory
import org.apache.carbondata.core.metadata.datatype.{DataTypes => CarbonDataTypes}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.{ByteUtil, DataTypeUtil}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.CarbonScalaUtilHelper.LOGGER
import org.apache.carbondata.streaming.CarbonStreamException
import org.apache.carbondata.streaming.index.StreamFileIndex
import org.apache.carbondata.streaming.segment.StreamSegment
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.TaskContext
import org.apache.spark.internal.io.FileCommitProtocol
import org.apache.spark.internal.io.FileCommitProtocol.TaskCommitMessage
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.expressions.{Alias, GetArrayItem, GetMapValue, GetStructField}
import org.apache.spark.sql.catalyst.parser.SqlBaseParser.CreateHiveTableContext
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.{CodegenSupport, QueryExecution, SQLExecution, SparkPlan}
import org.apache.spark.sql.execution.streaming.CarbonAppendableStreamSink.{WriteDataFileJobDescription, writeDataFileTask}
import org.apache.spark.util.SerializableConfiguration

object CarbonScalaUtilHelper {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Converts incoming value to String after converting data as per the data type.
   *
   * @param value           Input value to convert
   * @param dataType        Datatype to convert and then convert to String
   * @param timeStampFormat Timestamp format to convert in case of timestamp data types
   * @param dateFormat      DataFormat to convert in case of DateType datatype
   * @return converted String
   */
  def convertToDateAndTimeFormats(
                                   value: String,
                                   dataType: DataType,
                                   timeStampFormat: SimpleDateFormat,
                                   dateFormat: SimpleDateFormat): String = {
    val defaultValue = value != null && value.equalsIgnoreCase(hiveDefaultPartition)
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
   *
   * @param value           Input value to convert
   * @param dataType        Datatype to convert and then convert to String
   * @param timeStampFormat Timestamp format to convert in case of timestamp data types
   * @param dateFormat      DataFormat to convert in case of DateType datatype
   * @return converted String
   */
  def convertStaticPartitionToValues(
                                      value: String,
                                      dataType: DataType,
                                      timeStampFormat: SimpleDateFormat,
                                      dateFormat: SimpleDateFormat): AnyRef = {
    val defaultValue = value != null && value.equalsIgnoreCase(hiveDefaultPartition)
    try {
      dataType match {
        case TimestampType if timeStampFormat != null =>
          val formattedString =
            if (defaultValue) {
              timeStampFormat.format(new Date())
            } else {
              timeStampFormat.format(DateTimeUtils.stringToTime(value))
            }
          val convertedValue =
            DataTypeUtil
              .getDataBasedOnDataType(formattedString,
                CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(TimestampType))
          convertedValue
        case DateType if dateFormat != null =>
          val formattedString =
            if (defaultValue) {
              dateFormat.format(new Date())
            } else {
              dateFormat.format(DateTimeUtils.stringToTime(value))
            }
          val convertedValue =
            DataTypeUtil
              .getDataBasedOnDataType(formattedString,
                CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(TimestampType))
          val date = CarbonScalaUtil.generateDictionaryKey(convertedValue.asInstanceOf[Long])
          date.asInstanceOf[AnyRef]
        case BinaryType =>
          // TODO: decode required ? currently it is working
          ByteUtil.toBytes(value)
        case _ =>
          val convertedValue =
            DataTypeUtil
              .getDataBasedOnDataType(value,
                CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(dataType))
          if (convertedValue == null) {
            if (defaultValue) {
              dataType match {
                case BooleanType =>
                  return false.asInstanceOf[AnyRef]
                case _ =>
                  return 0.asInstanceOf[AnyRef]
              }
            }
            throw new MalformedCarbonCommandException(
              s"Value $value with datatype $dataType on static partition is not correct")
          }
          convertedValue
      }
    } catch {
      case e: Exception =>
        throw new MalformedCarbonCommandException(
          s"Value $value with datatype $dataType on static partition is not correct")
    }
  }

  /**
   * Converts incoming value to String after converting data as per the data type.
   *
   * @param value  Input value to convert
   * @param column column which it value belongs to
   * @return converted String
   */
  def convertToCarbonFormat(
                             value: String,
                             column: CarbonColumn): String = {
    try {
      column.getDataType match {
        case CarbonDataTypes.TIMESTAMP =>
          DateTimeUtils.timestampToString(value.toLong * 1000)
        case CarbonDataTypes.DATE =>
          val date = DirectDictionaryKeyGeneratorFactory.getDirectDictionaryGenerator(
            column.getDataType,
            CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT
          ).getValueFromSurrogate(value.toInt)
          if (date == null) {
            null
          } else {
            DateTimeUtils.dateToString(date.toString.toInt)
          }
        case _ => value
      }
    } catch {
      case e: Exception =>
        value
    }
  }

  /**
   * Converts incoming value to String after converting data as per the data type.
   *
   * @param value  Input value to convert
   * @param column column which it value belongs to
   * @return converted String
   */
  def convertStaticPartitions(
                               value: String,
                               column: ColumnSchema): String = {
    try {
      if (column.getDataType.equals(CarbonDataTypes.DATE)) {
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

  private val hiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__"

  /**
   * create Hadoop Job by using the specified Configuration
   */
  def createHadoopJob(conf: Configuration = FileFactory.getConfiguration): Job = {
    val jobConf = new JobConf(conf)
    SparkHadoopUtil.get.addCredentials(jobConf)
    Job.getInstance(jobConf)
  }

  def checkComplexDataType(alias: Alias): Unit = {
    if (alias.child.isInstanceOf[GetMapValue] ||
      alias.child.isInstanceOf[GetStructField] ||
      alias.child.isInstanceOf[GetArrayItem]) {
      throw new UnsupportedOperationException(
        s"MV is not supported for complex datatype child columns and complex datatype " +
          s"return types of function :" + alias.child.simpleString(1000))
    }
  }

  def getTableIden(u: UnresolvedRelation) : Some[TableIdentifier] = {
    Some(u.tableIdentifier)
  }

  def convertToTimestamp(v: Any) : Option[DateTimeUtils.SQLTimestamp] = {
    DateTimeUtils.stringToTimestamp(UTF8String.fromString(v.toString))
  }

  def getTableNameFromUnresolvedRelation(unresolvedRelation: UnresolvedRelation) : String = {
    unresolvedRelation.tableIdentifier.table
  }

  def joinOp(needFullRefresh: Boolean, logicalPlan: LogicalPlan) : Boolean = {
    var needRefresh = needFullRefresh
    logicalPlan.transformDown {
      case join@Join(_, _, _, _) =>
        needRefresh = true
        join
    }
    needRefresh
  }

  def getInternalRow(withProjections: CodegenSupport) : RDD[InternalRow] = {
    withProjections.inputRDDs().head
  }

  def getComments(ctx: CreateHiveTableContext) : Option[String] = {
    Option(ctx.STRING(0)).map(string)
  }
}
