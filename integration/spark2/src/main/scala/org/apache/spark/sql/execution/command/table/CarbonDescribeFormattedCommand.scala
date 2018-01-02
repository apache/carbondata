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

package org.apache.spark.sql.execution.command.table

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}

private[sql] case class CarbonDescribeFormattedCommand(
    child: SparkPlan,
    override val output: Seq[Attribute],
    tblIdentifier: TableIdentifier)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
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
    results ++= Seq(("Database Name", relation.carbonTable.getDatabaseName, "")
    )
    results ++= Seq(("Table Name", relation.carbonTable.getTableName, ""))
    results ++= Seq(("CARBON Store Path ", CarbonProperties.getStorePath, ""))
    val carbonTable = relation.carbonTable
    // Carbon table support table comment
    val tableComment = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse(CarbonCommonConstants.TABLE_COMMENT, "")
    results ++= Seq(("Comment", tableComment, ""))
    results ++= Seq(("Table Block Size ", carbonTable.getBlockSizeInMB + " MB", ""))
    val dataIndexSize = CarbonUtil.calculateDataIndexSize(carbonTable)
    if (!dataIndexSize.isEmpty) {
      results ++= Seq((CarbonCommonConstants.TABLE_DATA_SIZE,
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toString, ""))
      results ++= Seq((CarbonCommonConstants.TABLE_INDEX_SIZE,
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toString, ""))
      results ++= Seq((CarbonCommonConstants.LAST_UPDATE_TIME,
        dataIndexSize.get(CarbonCommonConstants.LAST_UPDATE_TIME).toString, ""))
    }
    results ++= Seq(("SORT_SCOPE", carbonTable.getTableInfo.getFactTable
      .getTableProperties.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT), CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))
    val isStreaming = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
      .getOrElse("streaming", "false")
    results ++= Seq(("Streaming", isStreaming, ""))

    val tblProps = carbonTable.getTableInfo.getFactTable.getTableProperties
    results ++= Seq(("SORT_SCOPE", tblProps.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT), CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT))

    // show table level compaction options
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE)) {
      results ++= Seq((CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE.toUpperCase
        , tblProps.get(CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE),
        CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE))
    }
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE)) {
      results ++= Seq((CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE.toUpperCase,
        tblProps.get(CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE),
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE))
    }
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD)) {
      results ++= Seq((CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD.toUpperCase,
        tblProps.get(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD),
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD))
    }
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS)) {
      results ++= Seq((CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS.toUpperCase,
        tblProps.get(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS),
        CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER))
    }
    if (tblProps.containsKey(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS)) {
      results ++= Seq((CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS.toUpperCase,
        tblProps.get(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS),
        CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT))
    }

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
    if (carbonTable.getPartitionInfo(carbonTable.getTableName) != null) {
      results ++=
      Seq(("Partition Columns", carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getColumnSchemaList.asScala.map(_.getColumnName).mkString(","), ""))
      results ++=
      Seq(("Partition Type", carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getPartitionType.toString, ""))
    }
    results.map {
      case (name, dataType, null) =>
        Row(f"$name%-36s", f"$dataType%-80s", null)
      case (name, dataType, comment) =>
        Row(f"$name%-36s", f"$dataType%-80s", f"$comment%-72s")
    }
  }
}
