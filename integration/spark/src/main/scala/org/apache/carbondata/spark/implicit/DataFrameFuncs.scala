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
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, CarbonContext, DataFrame}

import org.apache.carbondata.core.carbon.metadata.datatype.{DataType => CarbonType}

class DataFrameFuncs(dataFrame: DataFrame) {

  /**
    * Saves DataFrame as CarbonData files.
    */
  def saveAsCarbonFile(parameters: Map[String, String] = Map()): Unit = {
//    // To avoid derby problem, dataframe need to be writen and read using CarbonContext
//    require(dataFrame.sqlContext.isInstanceOf[CarbonContext],
//      "Error in saving dataframe to carbon file, must use CarbonContext to save dataframe"
//    )

    val options = new CarbonOption(parameters)
    val tableName = options.tableName

    // temporary solution: write to csv file, then load the csv into carbon
    val tempCSVFolder = s"./tempCSV"
    dataFrame.write
        .format(csvPackage)
        .option("header", "true")
        .mode(SaveMode.Overwrite)
        .save(tempCSVFolder)

    //val cc = CarbonContext.getInstance(dataFrame.sqlContext.sparkContext)
    val sqlContext = dataFrame.sqlContext
    val tempCSVPath = new Path(tempCSVFolder)
    val fs = tempCSVPath.getFileSystem(sqlContext.sparkContext.hadoopConfiguration)

    try {
      sqlContext.sql(makeCreateTableString(dataFrame.schema, options))

      // add 'csv' as file extension to all generated part file
      val itor = fs.listFiles(tempCSVPath, true)
      while (itor.hasNext) {
        val f = itor.next()
        if (f.getPath.getName.startsWith("part-")) {
          val newPath = s"${ f.getPath.getParent }/${ f.getPath.getName }.csv"
          if (!fs.rename(f.getPath, new Path(newPath))) {
            sqlContext.sql(s"DROP TABLE ${ options.tableName }")
            throw new RuntimeException("File system rename failed when loading data into carbon")
          }
        }
      }
      sqlContext.sql(makeLoadString(tableName, tempCSVFolder))
    } finally {
      fs.delete(tempCSVPath, true)
    }
  }

  private def csvPackage: String = "com.databricks.spark.csv"

  private def convertToCarbonType(sparkType: DataType): String = {
    sparkType match {
      case StringType => CarbonType.STRING.name
      case IntegerType => CarbonType.INT.name
      case ByteType => CarbonType.INT.name
      case ShortType => CarbonType.SHORT.name
      case LongType => CarbonType.LONG.name
      case FloatType => CarbonType.DOUBLE.name
      case DoubleType => CarbonType.DOUBLE.name
      case BooleanType => CarbonType.DOUBLE.name
      case TimestampType => CarbonType.TIMESTAMP.name
      case other => sys.error(s"unsupported type: $other")
    }
  }

  private def makeCreateTableString(schema: StructType, option: CarbonOption): String = {
    val tableName = option.tableName
    val carbonSchema = schema.map { field =>
      s"${ field.name } ${ convertToCarbonType(field.dataType) }"
    }
    s"""
          CREATE TABLE IF NOT EXISTS $tableName
          (${ carbonSchema.mkString(", ") })
          STORED BY '${ CarbonContext.datasourceName }'
      """
  }

  private def makeLoadString(tableName: String, csvFolder: String): String = {
    s"""
          LOAD DATA INPATH '$csvFolder'
          INTO TABLE $tableName
      """
  }

  def appendToCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    // find out table
    // find out streaming segment
    // for each rdd partition, find out the appendable carbon file
    // check whether it is full
    // if full, create new file
    // append to it: create blocklet header and data, call thrift to convert, write hdfs
  }

}
