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

import org.apache.carbondata.common.Strings
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.datatype.DataTypes
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
    val carbonTable = relation.carbonTable
    val tblProps = carbonTable.getTableInfo.getFactTable.getTableProperties

    // Table Schema Information
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      (field.name, field.dataType.simpleString, field.getComment().getOrElse(""))
    }

    results ++= Seq(("", "", ""), ("### PROPERTY", "VALUE", "DEFAULT_VALUE"))

    // Table Basic Information
    results ++= Seq(("", "", ""), ("## Table Basic Information", "", ""))
    results ++= Seq(("Database Name", carbonTable.getDatabaseName, ""))
    results ++= Seq(("Table Name", carbonTable.getTableName, ""))
    results ++= Seq(("Table Path", carbonTable.getTablePath, ""))

    // Carbon table support table comment
    results ++= Seq(("Comment",
        tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_COMMENT, ""), ""))

    val dataIndexSize = CarbonUtil.calculateDataIndexSize(carbonTable, false)
    var tableDataSizeStr = "0B"
    var tableIndexSizeStr = "0B"
    var lastUpdateTimeStr = "NA"
    if (!dataIndexSize.isEmpty) {
      val tableDataSize =
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).toLong
      tableDataSizeStr = Strings.formatSize(tableDataSize)
      val tableIndexSize =
        dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).toLong
      tableIndexSizeStr = Strings.formatSize(tableIndexSize)
      val lastUpdateTime = dataIndexSize.get(CarbonCommonConstants.LAST_UPDATE_TIME).toLong
      lastUpdateTimeStr = if (lastUpdateTime > 0L) {
        new java.sql.Timestamp(lastUpdateTime).toString()
      } else {
        "NA"
      }
    }
    results ++= Seq((CarbonCommonConstants.TABLE_DATA_SIZE, tableDataSizeStr, ""))
    results ++= Seq((CarbonCommonConstants.TABLE_INDEX_SIZE, tableIndexSizeStr, ""))
    results ++= Seq((CarbonCommonConstants.LAST_UPDATE_TIME, lastUpdateTimeStr, ""))

    // Detailed Table Properties Information
    results ++= Seq(("", "", ""), ("## Detailed Table Properties Information", "", ""))
    results ++= Seq(("Table Block Size", carbonTable.getBlockSizeInMB + " MB",
        CarbonCommonConstants.BLOCK_SIZE_DEFAULT_VAL + " MB"))
    results ++= Seq(("SORT_SCOPE", tblProps.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT), tblProps.asScala.getOrElse("sort_scope", CarbonCommonConstants
      .LOAD_SORT_SCOPE_DEFAULT)))
    results ++= Seq(("SORT_COLUMNS", carbonTable.getSortColumns(
      carbonTable.getTableName).asScala
      .map(column => column).mkString(","), ""))
    results ++= Seq(("DICTIONARY_INCLUDE",
      tblProps.asScala.getOrElse(CarbonCommonConstants.DICTIONARY_INCLUDE, ""), ""))
    results ++= Seq(("NO_INVERTED_INDEX",
      tblProps.asScala.getOrElse(CarbonCommonConstants.NO_INVERTED_INDEX, ""), ""))
    results ++= Seq(("Streaming", tblProps.asScala.getOrElse("streaming", "false"), "false"))
    // add Cache Level property
    results ++= Seq(("CACHE_LEVEL", tblProps.asScala.getOrElse(CarbonCommonConstants.CACHE_LEVEL,
      CarbonCommonConstants.CACHE_LEVEL_DEFAULT_VALUE),
      CarbonCommonConstants.CACHE_LEVEL_DEFAULT_VALUE))
    // add columns configured in column meta cache
    results ++=
      Seq(("COLUMN_META_CACHE",
          tblProps.asScala.getOrElse(CarbonCommonConstants.COLUMN_META_CACHE, ""), ""))

    // show table level compaction options
    results ++= Seq((CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE,
          CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE),
          CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE))
    results ++= Seq((CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD,
          CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD),
          CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD))
    results ++= Seq((CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS,
          CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER),
          CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER))
    results ++= Seq((CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS,
          CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT),
          CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT))
    results ++= Seq((CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE,
          CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE) + " MB",
          CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE + " MB"))

    results ++= Seq((CarbonCommonConstants.FLAT_FOLDER.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.FLAT_FOLDER,
          CarbonCommonConstants.DEFAULT_FLAT_FOLDER),
          CarbonCommonConstants.DEFAULT_FLAT_FOLDER))

    // longstring related info
    results ++= Seq((CarbonCommonConstants.LONG_STRING_COLUMNS.toUpperCase,
      tblProps.asScala.getOrElse(CarbonCommonConstants.LONG_STRING_COLUMNS, ""), ""))

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

    // local dictionary properties
    var isLocalDictEnabled = tblProps.asScala
      .get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
    if (isLocalDictEnabled.isDefined) {
      val localDictEnabled = isLocalDictEnabled.get.split(",") { 0 }
      results ++= Seq(("Local Dictionary Enabled", localDictEnabled,
          CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT))
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
            Seq(("Local Dictionary Exclude",
                getDictColumnString(builder.toString().split(",")), ""))
        }
      }
    } else {
      results ++=
        Seq(("Local Dictionary Enabled", CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT,
          CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT))
    }

    val isExternalTable = tblProps.asScala.getOrElse("_external", "false").toBoolean
    results ++= Seq(("External Table", isExternalTable.toString(), "false"))
    // Location Path
    if (isExternalTable) {
      results ++= Seq(("External Path", carbonTable.getTablePath, ""))
    }

    // Partition Information
    if (carbonTable.getPartitionInfo(carbonTable.getTableName) != null) {
      results ++= Seq(("", "", ""), ("## Partition Information", "", ""))
      results ++= Seq(("Partition Type", carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getPartitionType.toString, ""))
      results ++= Seq(("", "", ""), ("# partition_col", "data_type", "comment"))
      results ++= carbonTable.getPartitionInfo(carbonTable.getTableName)
        .getColumnSchemaList.asScala.map {
        col => (col.getColumnName, col.getDataType.getName, "")
      }
    }

    // Detailed Partition Information
    if (partitionSpec.nonEmpty) {
      val partitions = sparkSession.sessionState.catalog.getPartition(tblIdentifier, partitionSpec)
      results ++=
      Seq(("", "", ""),
        ("## Detailed Partition Information", "", ""),
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
        Row(f"$name%-40s", f"$dataType%-120s", null)
      case (name, dataType, comment) =>
        Row(f"$name%-40s", f"$dataType%-120s", f"$comment%-60s")
    }
  }
}
