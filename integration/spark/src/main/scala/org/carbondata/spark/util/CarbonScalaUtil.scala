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

package org.carbondata.spark.util

import java.io.File

import scala.collection.JavaConverters._

import org.apache.spark.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.Level
import org.apache.spark.sql.hive.{CarbonMetaData, DictionaryMap}
import org.apache.spark.sql.types._

import org.carbondata.core.carbon.metadata.datatype.DataType
import org.carbondata.core.carbon.metadata.encoder.Encoding
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.util.CarbonProperties
import org.carbondata.query.expression.{DataType => CarbonDataType}

object CarbonScalaUtil extends Logging {
  def convertSparkToCarbonDataType(
      dataType: org.apache.spark.sql.types.DataType): CarbonDataType = {
    dataType match {
      case StringType => CarbonDataType.StringType
      case IntegerType => CarbonDataType.IntegerType
      case LongType => CarbonDataType.LongType
      case DoubleType => CarbonDataType.DoubleType
      case FloatType => CarbonDataType.FloatType
      case DateType => CarbonDataType.DateType
      case BooleanType => CarbonDataType.BooleanType
      case TimestampType => CarbonDataType.TimestampType
      case ArrayType(_, _) => CarbonDataType.ArrayType
      case StructType(_) => CarbonDataType.StructType
      case NullType => CarbonDataType.NullType
      case _ => CarbonDataType.DecimalType
    }
  }

  def convertSparkToCarbonSchemaDataType(dataType: String): String = {
    dataType match {
      case CarbonCommonConstants.STRING_TYPE => CarbonCommonConstants.STRING
      case CarbonCommonConstants.INTEGER_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.BYTE_TYPE => CarbonCommonConstants.INTEGER
      case CarbonCommonConstants.SHORT_TYPE => CarbonCommonConstants.INTEGER
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

  def convertSparkColumnToCarbonLevel(field: (String, String)): Seq[Level] = {
    field._2 match {
      case CarbonCommonConstants.STRING_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.STRING))
      case CarbonCommonConstants.INTEGER_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.INTEGER))
      case CarbonCommonConstants.BYTE_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.INTEGER))
      case CarbonCommonConstants.SHORT_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.INTEGER))
      case CarbonCommonConstants.LONG_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.DOUBLE_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.FLOAT_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.DECIMAL_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.NUMERIC))
      case CarbonCommonConstants.DATE_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.STRING))
      case CarbonCommonConstants.BOOLEAN_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.STRING))
      case CarbonCommonConstants.TIMESTAMP_TYPE => Seq(
        Level(field._1, field._1, Int.MaxValue, CarbonCommonConstants.TIMESTAMP))
    }
  }

  def convertCarbonToSparkDataType(dataType: DataType): types.DataType = {
    dataType match {
      case DataType.STRING => StringType
      case DataType.INT => IntegerType
      case DataType.LONG => LongType
      case DataType.DOUBLE => DoubleType
      case DataType.BOOLEAN => BooleanType
      case DataType.DECIMAL => DecimalType.SYSTEM_DEFAULT
      case DataType.TIMESTAMP => TimestampType
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

  case class TransformHolder(rdd: Any, mataData: CarbonMetaData)

  object CarbonSparkUtil {
    def createBaseRDD(carbonContext: CarbonContext, carbonTable: CarbonTable): TransformHolder = {
      val relation = CarbonEnv.getInstance(carbonContext).carbonCatalog
        .lookupRelation1(Option(carbonTable.getDatabaseName),
          carbonTable.getFactTableName, None)(carbonContext).asInstanceOf[CarbonRelation]
      val rdd = new SchemaRDD(carbonContext, relation)
      rdd.registerTempTable(carbonTable.getFactTableName)
      TransformHolder(rdd, createSparkMeta(carbonTable))
    }

    def createSparkMeta(carbonTable: CarbonTable): CarbonMetaData = {
      val dimensionsAttr = carbonTable.getDimensionByTableName(carbonTable.getFactTableName)
                           .asScala.map(x => x.getColName) // wf : may be problem
      val measureAttr = carbonTable.getMeasureByTableName(carbonTable.getFactTableName)
                        .asScala.map(x => x.getColName)
      val dictionary =
        carbonTable.getDimensionByTableName(carbonTable.getFactTableName).asScala.map { f =>
        (f.getColName.toLowerCase,
          f.hasEncoding(Encoding.DICTIONARY) && !f.hasEncoding(Encoding.DIRECT_DICTIONARY))
      }
      CarbonMetaData(dimensionsAttr, measureAttr, carbonTable, DictionaryMap(dictionary.toMap))
    }
  }

  def getKettleHome(sqlContext: SQLContext): String = {
    var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
    if (null == kettleHomePath) {
      kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home")
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
          logInfo(s"carbon.kettle.home path is not exists, reset it as $kettleHomePath")
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
}
