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

package org.apache.carbondata.spark

import org.apache.hadoop.fs.Path
import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.LoadTable
import org.apache.spark.sql.types._

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.metadata.datatype.{DataType => CarbonType}
import org.apache.carbondata.core.constants.CarbonCommonConstants

class CarbonDataFrameWriter(val dataFrame: DataFrame) {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def saveAsCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    checkContext()
    val cc = CarbonContext.getInstance(dataFrame.sqlContext.sparkContext)

    // create a new table using dataframe's schema and write its content into the table
    cc.sql(makeCreateTableString(dataFrame.schema, new CarbonOption(parameters)))
    writeToCarbonFile(parameters)
  }

  def appendToCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    // append the data as a new load
    checkContext()
    writeToCarbonFile(parameters)
  }

  private def writeToCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    val options = new CarbonOption(parameters)
    val cc = CarbonContext.getInstance(dataFrame.sqlContext.sparkContext)
    if (options.tempCSV) {
      loadTempCSV(options, cc)
    } else {
      loadDataFrame(options, cc)
    }
  }

  /**
   * Firstly, saving DataFrame to CSV files
   * Secondly, load CSV files
   * @param options
   * @param cc
   */
  private def loadTempCSV(options: CarbonOption, cc: CarbonContext): Unit = {
    // temporary solution: write to csv file, then load the csv into carbon
    val storePath = CarbonEnv.get.carbonMetastore.storePath
    val tempCSVFolder = new StringBuilder(storePath).append(CarbonCommonConstants.FILE_SEPARATOR)
      .append("tempCSV")
      .append(CarbonCommonConstants.UNDERSCORE).append(options.dbName)
      .append(CarbonCommonConstants.UNDERSCORE).append(options.tableName)
      .append(CarbonCommonConstants.UNDERSCORE).append(System.nanoTime()).toString
    writeToTempCSVFile(tempCSVFolder, options)

    val tempCSVPath = new Path(tempCSVFolder)
    val fs = tempCSVPath.getFileSystem(dataFrame.sqlContext.sparkContext.hadoopConfiguration)

    def countSize(): Double = {
      var size: Double = 0
      val itor = fs.listFiles(tempCSVPath, true)
      while (itor.hasNext) {
        val f = itor.next()
        if (f.getPath.getName.startsWith("part-")) {
          size += f.getLen
        }
      }
      size
    }

    LOGGER.info(s"temporary CSV file size: ${countSize / 1024 / 1024} MB")

    try {
      cc.sql(makeLoadString(tempCSVFolder, options))
    } finally {
      fs.delete(tempCSVPath, true)
    }
  }

  private def checkContext(): Unit = {
    // To avoid derby problem, dataframe need to be writen and read using CarbonContext
    require(dataFrame.sqlContext.isInstanceOf[CarbonContext],
      "Error in saving dataframe to carbon file, must use CarbonContext to save dataframe"
    )
  }

  private def writeToTempCSVFile(tempCSVFolder: String, options: CarbonOption): Unit = {
    var writer: DataFrameWriter =
      dataFrame.write
        .format(csvPackage)
        .option("header", "false")
        .mode(SaveMode.Overwrite)

    if (options.compress) {
      writer = writer.option("codec", "gzip")
    }

    writer.save(tempCSVFolder)
  }

  /**
   * Loading DataFrame directly without saving DataFrame to CSV files.
   * @param options
   * @param cc
   */
  private def loadDataFrame(options: CarbonOption, cc: CarbonContext): Unit = {
    val header = dataFrame.columns.mkString(",")
    LoadTable(
      Some(options.dbName),
      options.tableName,
      null,
      Seq(),
      Map("fileheader" -> header),
      isOverwriteExist = false,
      null,
      Some(dataFrame)).run(cc)
  }

  private def csvPackage: String = "com.databricks.spark.csv.newapi"

  private def convertToCarbonType(sparkType: DataType): String = {
    sparkType match {
      case StringType => CarbonType.STRING.getName
      case IntegerType => CarbonType.INT.getName
      case ByteType => CarbonType.INT.getName
      case ShortType => CarbonType.SHORT.getName
      case LongType => CarbonType.LONG.getName
      case FloatType => CarbonType.DOUBLE.getName
      case DoubleType => CarbonType.DOUBLE.getName
      case BooleanType => CarbonType.DOUBLE.getName
      case TimestampType => CarbonType.TIMESTAMP.getName
      case other => sys.error(s"unsupported type: $other")
    }
  }

  private def makeCreateTableString(schema: StructType, options: CarbonOption): String = {
    val carbonSchema = schema.map { field =>
      s"${ field.name } ${ convertToCarbonType(field.dataType) }"
    }
    s"""
          CREATE TABLE IF NOT EXISTS ${options.dbName}.${options.tableName}
          (${ carbonSchema.mkString(", ") })
          STORED BY '${ CarbonContext.datasourceName }'
      """
  }

  private def makeLoadString(csvFolder: String, options: CarbonOption): String = {
    if (options.useKettle) {
      s"""
          LOAD DATA INPATH '$csvFolder'
          INTO TABLE ${options.dbName}.${options.tableName}
          OPTIONS ('FILEHEADER' = '${dataFrame.columns.mkString(",")}')
      """
    } else {
      s"""
          LOAD DATA INPATH '$csvFolder'
          INTO TABLE ${options.dbName}.${options.tableName}
          OPTIONS ('FILEHEADER' = '${dataFrame.columns.mkString(",")}', 'USE_KETTLE' = 'false')
      """
    }
  }

}
