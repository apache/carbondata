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

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.metadata.datatype.{DataType => CarbonDataType}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastorage.store.impl.FileFactory
import org.apache.carbondata.core.util.CarbonProperties

object CarbonScalaUtil {
  def convertSparkToCarbonDataType(
      dataType: org.apache.spark.sql.types.DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.STRING
      case ShortType => CarbonDataType.SHORT
      case IntegerType => CarbonDataType.INT
      case LongType => CarbonDataType.LONG
      case DoubleType => CarbonDataType.DOUBLE
      case FloatType => CarbonDataType.FLOAT
      case DateType => CarbonDataType.DATE
      case BooleanType => CarbonDataType.BOOLEAN
      case TimestampType => CarbonDataType.TIMESTAMP
      case ArrayType(_, _) => CarbonDataType.ARRAY
      case StructType(_) => CarbonDataType.STRUCT
      case NullType => CarbonDataType.NULL
      case _ => CarbonDataType.DECIMAL
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
    dataType match {
      case CarbonDataType.STRING => StringType
      case CarbonDataType.SHORT => ShortType
      case CarbonDataType.INT => IntegerType
      case CarbonDataType.LONG => LongType
      case CarbonDataType.DOUBLE => DoubleType
      case CarbonDataType.BOOLEAN => BooleanType
      case CarbonDataType.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case CarbonDataType.TIMESTAMP => TimestampType
    }
  }

  def updateDataType(
      currentDataType: org.apache.spark.sql.types.DataType): org.apache.spark.sql.types.DataType = {
    currentDataType match {
      case decimal: DecimalType =>
        val scale = currentDataType.asInstanceOf[DecimalType].scale
        DecimalType(DecimalType.MAX_PRECISION, scale)
      case _ =>
        currentDataType
    }
  }

  def getKettleHome(sqlContext: SQLContext): String = {
    var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
    if (null == kettleHomePath) {
      kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home")
    }
    if (null == kettleHomePath) {
      val carbonHome = System.getenv("CARBON_HOME")
      if (null != carbonHome) {
        kettleHomePath = carbonHome + "/processing/carbonplugins"
      }
    }
    if (kettleHomePath != null) {
      val sparkMaster = sqlContext.sparkContext.getConf.get("spark.master").toLowerCase()
      // get spark master, if local, need to correct the kettle home
      // e.g: --master local, the executor running in local machine
      if (sparkMaster.startsWith("local")) {
        val kettleHomeFileType = FileFactory.getFileType(kettleHomePath)
        val kettleHomeFile = FileFactory.getCarbonFile(kettleHomePath, kettleHomeFileType)
        // check if carbon.kettle.home path is exists
        if (!kettleHomeFile.exists()) {
          // get the path of this class
          // e.g: file:/srv/bigdata/install/spark/sparkJdbc/carbonlib/carbon-
          // xxx.jar!/org/carbondata/spark/rdd/
          var jarFilePath = this.getClass.getResource("").getPath
          val endIndex = jarFilePath.indexOf(".jar!") + 4
          // get the jar file path
          // e.g: file:/srv/bigdata/install/spark/sparkJdbc/carbonlib/carbon-*.jar
          jarFilePath = jarFilePath.substring(0, endIndex)
          val jarFileType = FileFactory.getFileType(jarFilePath)
          val jarFile = FileFactory.getCarbonFile(jarFilePath, jarFileType)
          // get the parent folder of the jar file
          // e.g:file:/srv/bigdata/install/spark/sparkJdbc/carbonlib
          val carbonLibPath = jarFile.getParentFile.getPath
          // find the kettle home under the previous folder
          // e.g:file:/srv/bigdata/install/spark/sparkJdbc/carbonlib/cabonplugins
          kettleHomePath = carbonLibPath + File.separator + CarbonCommonConstants.KETTLE_HOME_NAME
          val logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
          logger.error(s"carbon.kettle.home path is not exists, reset it as $kettleHomePath")
          val newKettleHomeFileType = FileFactory.getFileType(kettleHomePath)
          val newKettleHomeFile = FileFactory.getCarbonFile(kettleHomePath, newKettleHomeFileType)
          // check if the found kettle home exists
          if (!newKettleHomeFile.exists()) {
            sys.error("Kettle home not found. Failed to reset carbon.kettle.home")
          }
        }
      }
    } else {
      sys.error("carbon.kettle.home is not set")
    }
    kettleHomePath
  }

  def getString(value: Any,
      serializationNullFormat: String,
      delimiterLevel1: String,
      delimiterLevel2: String,
      format: SimpleDateFormat,
      level: Int = 1): String = {
    if (value == null) {
      serializationNullFormat
    } else {
      value match {
        case s: String => s
        case d: java.math.BigDecimal => d.toPlainString
        case i: java.lang.Integer => i.toString
        case d: java.lang.Double => d.toString
        case t: java.sql.Timestamp => format format t
        case d: java.sql.Date => format format d
        case b: java.lang.Boolean => b.toString
        case s: java.lang.Short => s.toString
        case f: java.lang.Float => f.toString
        case bs: Array[Byte] => new String(bs)
        case s: scala.collection.Seq[Any] =>
          val delimiter = if (level == 1) {
            delimiterLevel1
          } else {
            delimiterLevel2
          }
          val builder = new StringBuilder()
          s.foreach { x =>
            builder.append(getString(x, serializationNullFormat, delimiterLevel1,
                delimiterLevel2, format, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - 1)
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
                delimiterLevel2, format, level + 1)).append(delimiter)
          }
          builder.substring(0, builder.length - 1)
        case other => other.toString
      }
    }
  }
}
