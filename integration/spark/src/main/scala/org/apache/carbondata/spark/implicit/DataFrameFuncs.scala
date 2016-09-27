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

import org.apache.spark.Logging
import org.apache.spark.sql.{CarbonContext, DataFrame}
import org.apache.spark.sql.execution.command.LoadTable
import org.apache.spark.sql.types._

import org.apache.carbondata.core.carbon.metadata.datatype.{DataType => CarbonType}

class DataFrameFuncs(dataFrame: DataFrame) extends Logging {

  /**
   * Saves DataFrame as CarbonData files.
   */
  def saveAsCarbonFile(parameters: Map[String, String] = Map()): Unit = {
    // To avoid derby problem, dataframe need to be writen and read using CarbonContext
    require(dataFrame.sqlContext.isInstanceOf[CarbonContext],
      "Error in saving dataframe to carbon file, must use CarbonContext to save dataframe"
    )

    val cc = CarbonContext.getInstance(dataFrame.sqlContext.sparkContext)
    val options = new CarbonOption(parameters, cc)
    cc.sql(makeCreateTableString(dataFrame.schema, options))
    val header = dataFrame.columns.mkString(",")
    LoadTable(
      Some(options.dbName),
      options.tableName,
      null,
      Seq(),
      Map(("fileheader" -> header)),
      false,
      null,
      Some(dataFrame)).run(cc)
  }

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
    val dbName = option.dbName
    val carbonSchema = schema.map { field =>
      s"${ field.name } ${ convertToCarbonType(field.dataType) }"
    }
    s"""
          CREATE TABLE IF NOT EXISTS $dbName.$tableName
          (${ carbonSchema.mkString(", ") })
          STORED BY '${ CarbonContext.datasourceName }'
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
