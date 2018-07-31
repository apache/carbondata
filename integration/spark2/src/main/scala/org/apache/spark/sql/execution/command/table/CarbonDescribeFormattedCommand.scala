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
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.MetadataCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.codehaus.jackson.map.ObjectMapper

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.util.CarbonUtil

private[sql] case class CarbonDescribeFormattedCommand(
    child: SparkPlan,
    override val output: Seq[Attribute],
    partitionSpec: TablePartitionSpec,
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
    val carbonTable = relation.carbonTable
    results ++= Seq(("", "", ""), ("##Detailed Table Information", "", ""))
    results ++= Seq(("Database Name", relation.carbonTable.getDatabaseName, "")
    )
    results ++= Seq(("Table Name", relation.carbonTable.getTableName, ""))
    results ++= Seq(("CARBON Store Path ", carbonTable.getTablePath, ""))

    val tblProps = carbonTable.getTableInfo.getFactTable.getTableProperties

    // Carbon table support table comment
    val tableComment = tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_COMMENT, "")
    results ++= Seq(("Comment", tableComment, ""))
    results ++= Seq(("Table Block Size ", carbonTable.getBlockSizeInMB + " MB", ""))
    val dataIndexSize = CarbonUtil.calculateDataIndexSize(carbonTable, false)
    if (!dataIndexSize.isEmpty) {
      results ++= Seq((CarbonCommonConstants.TABLE_DATA_SIZE,
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toString, ""))
      results ++= Seq((CarbonCommonConstants.TABLE_INDEX_SIZE,
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toString, ""))
      results ++= Seq((CarbonCommonConstants.LAST_UPDATE_TIME,
        dataIndexSize.get(CarbonCommonConstants.LAST_UPDATE_TIME).toString, ""))
    }

    results ++= Seq(("SORT_SCOPE", tblProps.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT), tblProps.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT)))
    // add Cache Level property
    results ++= Seq(("CACHE_LEVEL", tblProps.asScala.getOrElse(CarbonCommonConstants.CACHE_LEVEL,
      CarbonCommonConstants.CACHE_LEVEL_DEFAULT_VALUE), ""))
    val isStreaming = tblProps.asScala.getOrElse("streaming", "false")
    results ++= Seq(("Streaming", isStreaming, ""))

    // longstring related info
    if (tblProps.containsKey(CarbonCommonConstants.LONG_STRING_COLUMNS)) {
      results ++= Seq((CarbonCommonConstants.LONG_STRING_COLUMNS.toUpperCase,
        tblProps.get(CarbonCommonConstants.LONG_STRING_COLUMNS), ""))
    }

    var isLocalDictEnabled = tblProps.asScala
      .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
    if (isLocalDictEnabled.isDefined) {
      val localDictEnabled = isLocalDictEnabled.get.split(",") { 0 }
      results ++= Seq(("Local Dictionary Enabled", localDictEnabled, ""))
      // if local dictionary is enabled, then only show other properties of local dictionary
      if (localDictEnabled.toBoolean) {
        var localDictThreshold = tblProps.asScala
          .getOrElse(CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD_DEFAULT)
        val localDictionaryThreshold = localDictThreshold.split(",")
        localDictThreshold = localDictionaryThreshold { 0 }
        results ++= Seq(("Local Dictionary Threshold", localDictThreshold, ""))
        val columns = carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala
        val builder = new StringBuilder
        columns.foreach { column =>
          if (column.isLocalDictColumn && !column.isInvisible) {
            builder.append(column.getColumnName).append(",")
          }
        }
        results ++=
        Seq(("Local Dictionary Include", getDictColumnString(builder.toString().split(",")), ""))
        if (tblProps.asScala
          .get(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE).isDefined) {
          val columns = carbonTable.getTableInfo.getFactTable.getListOfColumns.asScala
          val builder = new StringBuilder
          columns.foreach { column =>
            if (!column.isLocalDictColumn && !column.isInvisible &&
                (column.getDataType.equals(DataTypes.STRING) ||
                 column.getDataType.equals(DataTypes.VARCHAR))) {
              builder.append(column.getColumnName).append(",")
            }
          }
          results ++=
          Seq(("Local Dictionary Exclude", getDictColumnString(builder.toString().split(",")), ""))
        }
      }
    } else {
      results ++=
      Seq(("Local Dictionary Enabled", CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT, ""))
    }

    /**
     * return the string which has all comma separated columns
     * @param localDictColumns
     * @return
     */
    def getDictColumnString(localDictColumns: Array[String]): String = {
      val dictColumns: StringBuilder = new StringBuilder
      localDictColumns.foreach(column => dictColumns.append(column.trim).append(","))
      dictColumns.toString().patch(dictColumns.toString().lastIndexOf(","), "", 1)
    }


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
    if (tblProps.containsKey(CarbonCommonConstants.FLAT_FOLDER)) {
      results ++= Seq((CarbonCommonConstants.FLAT_FOLDER.toUpperCase,
        tblProps.get(CarbonCommonConstants.FLAT_FOLDER),
        CarbonCommonConstants.DEFAULT_FLAT_FOLDER))
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
    // add columns configured in column meta cache
    if (null != tblProps.get(CarbonCommonConstants.COLUMN_META_CACHE)) {
      results ++=
      Seq(("COLUMN_META_CACHE", carbonTable.getMinMaxCachedColumnsInCreateOrder().asScala
        .map(col => col).mkString(","), ""))
    }
    if (carbonTable.getPartitionInfo(carbonTable.getTableName) != null) {
      results ++=
      Seq(("#Partition Information", "", ""),
        ("#col_name", "data_type", "comment"))
      results ++= carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getColumnSchemaList.asScala.map {
        col => (col.getColumnName, col.getDataType.getName, "NULL")
      }
      results ++= Seq(("Partition Type", carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getPartitionType.toString, ""))
    }
    if (partitionSpec.nonEmpty) {
      val partitions = sparkSession.sessionState.catalog.getPartition(tblIdentifier, partitionSpec)
      results ++=
      Seq(("", "", ""),
        ("##Detailed Partition Information", "", ""),
        ("Partition Value:", partitions.spec.values.mkString("[", ",", "]"), ""),
        ("Database:", tblIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase), ""),
        ("Table:", tblIdentifier.table, ""))
      if (partitions.storage.locationUri.isDefined) {
        results ++= Seq(("Location:", partitions.storage.locationUri.get.toString, ""))
      }
      results ++= Seq(("Partition Parameters:", partitions.parameters.mkString(", "), ""))
    }
    results.map {
      case (name, dataType, null) =>
        Row(f"$name%-36s", f"$dataType%-80s", null)
      case (name, dataType, comment) =>
        Row(f"$name%-36s", f"$dataType%-80s", f"$comment%-72s")
    }
  }
}
