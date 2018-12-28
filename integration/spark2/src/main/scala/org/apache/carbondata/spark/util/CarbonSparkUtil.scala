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

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.fs.s3a.Constants.{ACCESS_KEY, ENDPOINT, SECRET_KEY}
import org.apache.spark.sql.hive.{CarbonMetaData, CarbonRelation, DictionaryMap}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}
import org.apache.carbondata.core.util.CarbonUtil

case class TransformHolder(rdd: Any, mataData: CarbonMetaData)

/**
 * carbon spark common methods
 */
object CarbonSparkUtil {

  def createSparkMeta(carbonTable: CarbonTable): CarbonMetaData = {
    val dimensionsAttr = carbonTable.getDimensionByTableName(carbonTable.getTableName)
      .asScala.map(x => x.getColName) // wf : may be problem
    val measureAttr = carbonTable.getMeasureByTableName(carbonTable.getTableName)
      .asScala.map(x => x.getColName)
    val dictionary =
      carbonTable.getDimensionByTableName(carbonTable.getTableName).asScala.map { f =>
        (f.getColName.toLowerCase,
          f.hasEncoding(Encoding.DICTIONARY) && !f.hasEncoding(Encoding.DIRECT_DICTIONARY) &&
            !f.getDataType.isComplexType)
      }
    CarbonMetaData(dimensionsAttr,
      measureAttr,
      carbonTable,
      DictionaryMap(dictionary.toMap),
      CarbonUtil.hasAggregationDataMap(carbonTable))
  }

  def createCarbonRelation(tableInfo: TableInfo, tablePath: String): CarbonRelation = {
    val table = CarbonTable.buildFromTableInfo(tableInfo)
    CarbonRelation(
      tableInfo.getDatabaseName,
      tableInfo.getFactTable.getTableName,
      CarbonSparkUtil.createSparkMeta(table),
      table)
  }

  /**
   * return's the formatted column comment if column comment is present else empty("")
   *
   * @param carbonColumn the column of carbonTable
   * @return string
   */
  def getColumnComment(carbonColumn: CarbonColumn): String = {
    {
      val columnProperties = carbonColumn.getColumnProperties
      if (columnProperties != null) {
        val comment: String = columnProperties.get(CarbonCommonConstants.COLUMN_COMMENT)
        if (comment != null && comment != null) {
          return " comment \"" + comment + "\""
        }
      }
      ""
    }
  }

  /**
   * the method return's raw schema
   *
   * @param carbonRelation logical plan for one carbon table
   * @return schema
   */
  def getRawSchema(carbonRelation: CarbonRelation): String = {
    val fields = new Array[String](
      carbonRelation.dimensionsAttr.size + carbonRelation.measureAttr.size)
    val carbonTable = carbonRelation.carbonTable
    val columnSchemas: mutable.Buffer[ColumnSchema] = carbonTable.getTableInfo.getFactTable.
      getListOfColumns.asScala
      .filter(cSchema => !cSchema.isInvisible && cSchema.getSchemaOrdinal != -1).
      sortWith(_.getSchemaOrdinal < _.getSchemaOrdinal)
    val columnList = columnSchemas.toList.asJava
    carbonRelation.dimensionsAttr.foreach(attr => {
      val carbonColumn = carbonTable.getColumnByName(carbonRelation.tableName, attr.name)
      val columnComment = getColumnComment(carbonColumn)
      fields(columnList.indexOf(carbonColumn.getColumnSchema)) =
        '`' + attr.name + '`' + ' ' + attr.dataType.catalogString + columnComment
    })
    carbonRelation.measureAttr.foreach(msrAtrr => {
      val carbonColumn = carbonTable.getColumnByName(carbonRelation.tableName, msrAtrr.name)
      val columnComment = getColumnComment(carbonColumn)
      fields(columnList.indexOf(carbonColumn.getColumnSchema)) =
        '`' + msrAtrr.name + '`' + ' ' + msrAtrr.dataType.catalogString + columnComment
    })
    fields.mkString(",")
  }

  /**
   * add escape prefix for delimiter
   *
   * @param delimiter A delimiter is a sequence of one or more characters
   * used to specify the boundary between separate
   * @return delimiter
   */
  def delimiterConverter4Udf(delimiter: String): String = delimiter match {
    case "|" | "*" | "." | ":" | "^" | "\\" | "$" | "+" | "?" | "(" | ")" | "{" | "}" | "[" | "]" =>
      "\\\\" + delimiter
    case _ =>
      delimiter
  }

  def getKeyOnPrefix(path: String): (String, String, String) = {
    val prefix = "spark.hadoop."
    val endPoint = prefix + ENDPOINT
    if (path.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
      (prefix + ACCESS_KEY, prefix + SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3N_PREFIX)) {
      (prefix + CarbonCommonConstants.S3N_ACCESS_KEY,
        prefix + CarbonCommonConstants.S3N_SECRET_KEY, endPoint)
    } else if (path.startsWith(CarbonCommonConstants.S3_PREFIX)) {
      (prefix + CarbonCommonConstants.S3_ACCESS_KEY,
        prefix + CarbonCommonConstants.S3_SECRET_KEY, endPoint)
    } else {
      throw new Exception("Incorrect Store Path")
    }
  }

  def getS3EndPoint(args: Array[String]): String = {
    if (args.length >= 4 && args(3).contains(".com")) args(3)
    else ""
  }

}
