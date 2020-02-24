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
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.types.{ArrayType, DataType, DataTypes, FloatType, MapType, StructField, StructType}
import org.apache.spark.SparkConf

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonColumn, ColumnSchema}

/**
 * carbon spark common methods
 */
object CarbonSparkUtil {

  def createCarbonRelation(tableInfo: TableInfo, tablePath: String): CarbonRelation = {
    val table = CarbonTable.buildFromTableInfo(tableInfo)
    CarbonRelation(
      tableInfo.getDatabaseName,
      tableInfo.getFactTable.getTableName,
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
      .filter(cSchema => !cSchema.isInvisible && cSchema.getSchemaOrdinal != -1 &&
                         !cSchema.isIndexColumn).sortWith(_.getSchemaOrdinal < _.getSchemaOrdinal)
    val columnList = columnSchemas.toList.asJava
    carbonRelation.dimensionsAttr.foreach(attr => {
      val carbonColumn = carbonTable.getColumnByName(attr.name)
      val columnComment = getColumnComment(carbonColumn)
      fields(columnList.indexOf(carbonColumn.getColumnSchema)) =
        '`' + attr.name + '`' + ' ' + attr.dataType.catalogString + columnComment
    })
    carbonRelation.measureAttr.foreach(msrAtrr => {
      val carbonColumn = carbonTable.getColumnByName(msrAtrr.name)
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

  def getSparkConfForS3(accessKey: String, secretKey: String, endpoint: String): SparkConf = {
    val sparkConf = new SparkConf(false)
    val prefix = "spark.hadoop."
    Seq(ACCESS_KEY, CarbonCommonConstants.S3N_ACCESS_KEY, CarbonCommonConstants.S3_ACCESS_KEY)
      .foreach(key => sparkConf.set(prefix + key, accessKey))
    Seq(SECRET_KEY, CarbonCommonConstants.S3N_SECRET_KEY, CarbonCommonConstants.S3_SECRET_KEY)
      .foreach(key => sparkConf.set(prefix + key, secretKey))
    sparkConf.set(prefix + ENDPOINT, endpoint)
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

  def updateStruct(struct: StructType): StructType = {
    struct.copy(fields = struct.map(f => updateField(f)).toArray)
  }

  def updateArray(array: ArrayType): ArrayType = {
    array.copy(elementType = updateDataType(array.elementType))
  }

  def updateMap(map: MapType): MapType = {
    map.copy(
      keyType = updateDataType(map.keyType),
      valueType = updateDataType(map.valueType)
    )
  }

  def updateField(field: StructField): StructField = {
    field.copy(name = field.name.toLowerCase, dataType = updateDataType(field.dataType))
  }

  def updateDataType(dataType: DataType): DataType = {
    dataType match {
      case _: FloatType => DataTypes.DoubleType
      case struct: StructType => updateStruct(struct)
      case array: ArrayType => updateArray(array)
      case map: MapType => updateMap(map)
      case _ => dataType
    }
  }
}
