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

package org.apache.spark.sql.execution.command

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.CarbonRelation
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.CarbonUtil

private[sql] case class CarbonDescribeFormattedCommand(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends RunnableCommand with SchemaProcessCommand {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processSchema(sparkSession)
  }

  private def getColumnGroups(dimensions: List[CarbonDimension]): Seq[(String, String, String)] = {
    var results: Seq[(String, String, String)] =
      Seq(("", "", ""), ("##Column Group Information", "", ""))
    val groupedDimensions = dimensions.groupBy(x => x.columnGroupId()).filter {
      case (groupId, _) => groupId != -1
    }.toSeq.sortBy(_._1)
    val groups = groupedDimensions.map(colGroups => {
      colGroups._2.map(dim => dim.getColName).mkString(", ")
    })
    var index = 1
    groups.foreach { x =>
      results = results :+ (s"Column Group $index", x, "")
      index = index + 1
    }
    results
  }

  override def processSchema(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
      .lookupRelation(tblIdentifier)(sparkSession).asInstanceOf[CarbonRelation]
    val mapper = new ObjectMapper()
    val colProps = StringBuilder.newBuilder
    val dims = relation.metaData.dims.map(x => x.toLowerCase)
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val fieldName = field.name.toLowerCase
      val colComment = field.getComment().getOrElse("null")
      val comment = if (dims.contains(fieldName)) {
        val dimension = relation.metaData.carbonTable.getDimensionByName(
          relation.carbonTable.getTableName, fieldName)
        if (null != dimension.getColumnProperties && !dimension.getColumnProperties.isEmpty) {
          colProps.append(fieldName).append(".")
            .append(mapper.writeValueAsString(dimension.getColumnProperties))
            .append(",")
        }
        if (dimension.hasEncoding(Encoding.DICTIONARY) &&
            !dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          "DICTIONARY, KEY COLUMN" + (if (dimension.hasEncoding(Encoding.INVERTED_INDEX)) {
            "".concat(",").concat(colComment)
          } else {
            ",NOINVERTEDINDEX".concat(",").concat(colComment)
          })
        } else {
          "KEY COLUMN" + (if (dimension.hasEncoding(Encoding.INVERTED_INDEX)) {
            "".concat(",").concat(colComment)
          } else {
            ",NOINVERTEDINDEX".concat(",").concat(colComment)
          })
        }
      } else {
        "MEASURE".concat(",").concat(colComment)
      }

      (field.name, field.dataType.simpleString, comment)
    }
    val colPropStr = if (colProps.toString().trim().length() > 0) {
      // drops additional comma at end
      colProps.toString().dropRight(1)
    } else {
      colProps.toString()
    }
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Database Name: ", relation.carbonTable.getDatabaseName, "")
    )
    results ++= Seq(("Table Name: ", relation.carbonTable.getTableName, ""))
    results ++= Seq(("CARBON Store Path: ", CarbonProperties.getStorePath, ""))
    val carbonTable = relation.carbonTable
    // Carbon table support table comment
    val tableComment = carbonTable.getTableInfo.getFactTable.getTableProperties
      .getOrDefault(CarbonCommonConstants.TABLE_COMMENT, "")
    results ++= Seq(("Comment: ", tableComment, ""))
    results ++= Seq(("Table Block Size : ", carbonTable.getBlockSizeInMB + " MB", ""))
    val dataIndexSize = CarbonUtil.calculateDataIndexSize(carbonTable)
    if (!dataIndexSize.isEmpty) {
      results ++= Seq((CarbonCommonConstants.TABLE_DATA_SIZE + ":",
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toString, ""))
      results ++= Seq((CarbonCommonConstants.TABLE_INDEX_SIZE + ":",
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toString, ""))
      results ++= Seq((CarbonCommonConstants.LAST_UPDATE_TIME + ":",
        dataIndexSize.get(CarbonCommonConstants.LAST_UPDATE_TIME).toString, ""))
    }
    results ++= Seq(("SORT_SCOPE", carbonTable.getTableInfo.getFactTable
      .getTableProperties.getOrDefault("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT), CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))
    results ++= Seq(("", "", ""), ("##Detailed Column property", "", ""))
    if (colPropStr.length() > 0) {
      results ++= Seq((colPropStr, "", ""))
    } else {
      results ++= Seq(("ADAPTIVE", "", ""))
    }
    results ++= Seq(("SORT_COLUMNS", relation.metaData.carbonTable.getSortColumns(
      relation.carbonTable.getTableName).asScala
      .map(column => column).mkString(","), ""))
    val dimension = carbonTable
      .getDimensionByTableName(relation.carbonTable.getTableName)
    results ++= getColumnGroups(dimension.asScala.toList)
    if (carbonTable.getPartitionInfo(carbonTable.getTableName) != null) {
      results ++=
      Seq(("Partition Columns: ", carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getColumnSchemaList.asScala.map(_.getColumnName).mkString(","), ""))
    }
    results.map {
      case (name, dataType, null) =>
        Row(f"$name%-36s", f"$dataType%-80s", null)
      case (name, dataType, comment) =>
        Row(f"$name%-36s", f"$dataType%-80s", f"$comment%-72s")
    }
  }
}
