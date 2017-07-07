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

package org.apache.carbondata.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}

case class CsvHeaderSchema(columnName: String, dataType: DataType, isNullable: Boolean)

trait DataFrameUtil {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)


  /**
   * This method get the column names from dataframe
   *
   * @param dataFrame
   * @return
   */
  def getColumnNames(dataFrame: DataFrame): List[String] = {
    dataFrame.columns.toList
  }

  /**
   * This method returns column Name From File Header
   *
   * @param properties
   * @param header_count
   * @return
   */
  def getColumnNameFromFileHeader(properties: LoadProperties, header_count: Int): List[String] = {
    properties.fileHeaders match {
      case Some(columnList) => columnList
      case _ => LOGGER.info("Headers not present !!")
        (0 until header_count).toList map { count => "_c" + count }
    }
  }

  /**
   * This method returns List of CsvHeaderSchema containing datatypes of each column
   *
   * @param dataFrame
   * @return
   */
  def getColumnDataTypes(dataFrame: DataFrame): List[CsvHeaderSchema] = {
    dataFrame.schema.toList map { colStructure => CsvHeaderSchema(colStructure.name,
      colStructure.dataType,
      colStructure.nullable)
    }
  }


  /**
   * This methos returns header count from dataframe
   *
   * @param dataFrame
   * @return
   */
  def getHeaderCount(dataFrame: DataFrame): Int = {
    dataFrame.columns.length
  }

}

object DataFrameUtil extends DataFrameUtil
