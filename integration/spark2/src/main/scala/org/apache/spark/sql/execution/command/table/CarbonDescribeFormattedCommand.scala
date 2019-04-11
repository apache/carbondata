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

import java.util.Date

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
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil

private[sql] case class CarbonDescribeFormattedCommand(
    child: SparkPlan,
    override val output: Seq[Attribute],
    partitionSpec: TablePartitionSpec,
    tblIdentifier: TableIdentifier)
  extends MetadataCommand {

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
      .lookupRelation(tblIdentifier)(sparkSession).asInstanceOf[CarbonRelation]
    setAuditTable(relation.databaseName, relation.tableName)
    var results: Seq[(String, String, String)] = child.schema.fields.map { field =>
      val colComment = field.getComment().getOrElse("null")
      (field.name, field.dataType.simpleString, colComment)
    }

    val carbonTable = relation.carbonTable
    val tblProps = carbonTable.getTableInfo.getFactTable.getTableProperties.asScala
    val sortScope = if (carbonTable.getNumberOfSortColumns == 0) {
      "NO_SORT"
    } else {
      tblProps.getOrElse("sort_scope", CarbonCommonConstants.LOAD_SORT_SCOPE_DEFAULT)
    }
    val streaming: String = if (carbonTable.isStreamingSink) {
      "sink"
    } else if (carbonTable.isStreamingSource) {
      "source"
    } else {
      "false"
    }

    val catalog = sparkSession.sessionState.catalog
    val catalogTable = catalog.getTableMetadata(tblIdentifier)

    //////////////////////////////////////////////////////////////////////////////
    // Table Basic Information
    //////////////////////////////////////////////////////////////////////////////
    results ++= Seq(
      ("", "", ""),
      ("## Detailed Table Information", "", ""),
      ("Database", catalogTable.database, ""),
      ("Table", catalogTable.identifier.table, ""),
      ("Owner", catalogTable.owner, ""),
      ("Created", new Date(catalogTable.createTime).toString, ""),
      ("Location ", carbonTable.getTablePath, ""),
      ("External", carbonTable.isExternalTable.toString, ""),
      ("Transactional", carbonTable.isTransactionalTable.toString, ""),
      ("Streaming", streaming, ""),
      ("Table Block Size ", carbonTable.getBlockSizeInMB + " MB", ""),
      ("Table Blocklet Size ", carbonTable.getBlockletSizeInMB + " MB", ""),
      ("Comment", tblProps.getOrElse(CarbonCommonConstants.TABLE_COMMENT, ""), ""),
      ("Bad Record Path", tblProps.getOrElse("bad_record_path", ""), ""),
      ("Min Input Per Node Per Load",
        Strings.formatSize(
          tblProps.getOrElse(CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB,
            CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT).toFloat), ""),
      ("Data File Compressor ", tblProps
        .getOrElse(CarbonCommonConstants.COMPRESSOR,
          CarbonCommonConstants.DEFAULT_COMPRESSOR), ""),
      //////////////////////////////////////////////////////////////////////////////
      //  Index Information
      //////////////////////////////////////////////////////////////////////////////

      ("", "", ""),
      ("## Index Information", "", ""),
      ("Sort Scope", sortScope, ""),
      ("Sort Columns", relation.metaData.carbonTable.getSortColumns.asScala.mkString(", "), ""),
      ("Inverted Index Columns", carbonTable.getInvertedIndexColumns.asScala
        .map(_.getColumnName).mkString(", "), ""),
      ("Cached Min/Max Index Columns",
        carbonTable.getMinMaxCachedColumnsInCreateOrder.asScala.mkString(", "), ""),
      ("Min/Max Index Cache Level",
        tblProps.getOrElse(CarbonCommonConstants.CACHE_LEVEL,
          CarbonCommonConstants.CACHE_LEVEL_DEFAULT_VALUE), "")
    )

    //////////////////////////////////////////////////////////////////////////////
    //  Encoding Information
    //////////////////////////////////////////////////////////////////////////////

    results ++= Seq(
      ("", "", ""),
      ("## Encoding Information", "", ""))
    results ++= getLocalDictDesc(carbonTable, tblProps.toMap)
    results ++= Seq(("Global Dictionary",
      tblProps.getOrElse(CarbonCommonConstants.DICTIONARY_INCLUDE, ""), ""))
    if (tblProps.contains(CarbonCommonConstants.LONG_STRING_COLUMNS)) {
      results ++= Seq((CarbonCommonConstants.LONG_STRING_COLUMNS.toUpperCase,
        tblProps.getOrElse(CarbonCommonConstants.LONG_STRING_COLUMNS, ""), ""))
    }

    //////////////////////////////////////////////////////////////////////////////
    // Compaction Information
    //////////////////////////////////////////////////////////////////////////////

    results ++= Seq(
      ("", "", ""),
      ("## Compaction Information", "", ""),
      (CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE.toUpperCase,
        tblProps.getOrElse(CarbonCommonConstants.TABLE_MAJOR_COMPACTION_SIZE,
        CarbonCommonConstants.DEFAULT_CARBON_MAJOR_COMPACTION_SIZE), ""),
      (CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE.toUpperCase,
        tblProps.getOrElse(CarbonCommonConstants.TABLE_AUTO_LOAD_MERGE,
        CarbonCommonConstants.DEFAULT_ENABLE_AUTO_LOAD_MERGE), ""),
      (CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD.toUpperCase,
        tblProps.getOrElse(CarbonCommonConstants.TABLE_COMPACTION_LEVEL_THRESHOLD,
        CarbonCommonConstants.DEFAULT_SEGMENT_LEVEL_THRESHOLD), ""),
      (CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS.toUpperCase,
        tblProps.getOrElse(CarbonCommonConstants.TABLE_COMPACTION_PRESERVE_SEGMENTS,
        CarbonCommonConstants.DEFAULT_PRESERVE_LATEST_SEGMENTS_NUMBER), ""),
      (CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS.toUpperCase,
        tblProps.getOrElse(CarbonCommonConstants.TABLE_ALLOWED_COMPACTION_DAYS,
        CarbonCommonConstants.DEFAULT_DAYS_ALLOWED_TO_COMPACT), "")
    )

    //////////////////////////////////////////////////////////////////////////////
    // Partition Information
    //////////////////////////////////////////////////////////////////////////////
    val partitionInfo = carbonTable.getPartitionInfo()
    if (partitionInfo != null) {
      results ++= Seq(
        ("", "", ""),
        ("## Partition Information", "", ""),
        ("Partition Type", partitionInfo.getPartitionType.toString, ""),
        ("Partition Columns",
          partitionInfo.getColumnSchemaList.asScala.map {
            col => s"${col.getColumnName}:${col.getDataType.getName}"}.mkString(", "), ""),
        ("Number of Partitions", partitionInfo.getNumPartitions.toString, ""),
        ("Partitions Ids", partitionInfo.getPartitionIds.asScala.mkString(","), "")
      )
      if (partitionInfo.getPartitionType == PartitionType.RANGE) {
        results ++= Seq(("Range", partitionInfo.getRangeInfo.asScala.mkString(", "), ""))
      } else if (partitionInfo.getPartitionType == PartitionType.LIST) {
        results ++= Seq(("List", partitionInfo.getListInfo.asScala.mkString(", "), ""))
      }
    }
    if (partitionSpec.nonEmpty) {
      val partitions = sparkSession.sessionState.catalog.getPartition(tblIdentifier, partitionSpec)
      results ++=
      Seq(("", "", ""),
        ("## Partition Information", "", ""),
        ("Partition Type", "Hive", ""),
        ("Partition Value:", partitions.spec.values.mkString("[", ",", "]"), ""),
        ("Database:", tblIdentifier.database.getOrElse(sparkSession.catalog.currentDatabase), ""),
        ("Table:", tblIdentifier.table, ""))
      if (partitions.storage.locationUri.isDefined) {
        results ++= Seq(("Location:", partitions.storage.locationUri.get.toString, ""))
      }
      results ++= Seq(("Partition Parameters:", partitions.parameters.mkString(", "), ""))
    }

    //////////////////////////////////////////////////////////////////////////////
    // Dynamic Information
    //////////////////////////////////////////////////////////////////////////////

    val dataIndexSize = CarbonUtil.calculateDataIndexSize(carbonTable, false)
    if (!dataIndexSize.isEmpty) {
      if (carbonTable.isTransactionalTable) {
        results ++= Seq(
          ("", "", ""),
          ("## Dynamic Information", "", ""),
          ("Table Data Size", Strings.formatSize(
            dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).floatValue()), ""),
          ("Table Index Size", Strings.formatSize(
            dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE).floatValue()), ""),
          ("Last Update",
            new Date(dataIndexSize.get(CarbonCommonConstants.LAST_UPDATE_TIME)).toString, "")
        )
      } else {
        results ++= Seq(
          ("", "", ""),
          ("## Dynamic Information", "", ""),
          ("Table Total Size", Strings.formatSize(
            dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE).floatValue()), "")
        )
      }
    }

    results.map{case (c1, c2, c3) => Row(c1, c2, c3)}
  }

  private def getLocalDictDesc(
      carbonTable: CarbonTable,
      tblProps: Map[String, String]): Seq[(String, String, String)] = {
    val isLocalDictEnabled = tblProps.get(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE)
    var results = Seq[(String, String, String)]()
    if (isLocalDictEnabled.isDefined) {
      val localDictEnabled = isLocalDictEnabled.get.split(",") { 0 }
      results ++= Seq(("Local Dictionary Enabled", localDictEnabled, ""))
      // if local dictionary is enabled, then only show other properties of local dictionary
      if (localDictEnabled.toBoolean) {
        var localDictThreshold = tblProps.getOrElse(
            CarbonCommonConstants.LOCAL_DICTIONARY_THRESHOLD,
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
        if (tblProps.get(CarbonCommonConstants.LOCAL_DICTIONARY_EXCLUDE).isDefined) {
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
     *
     * @param localDictColumns
     * @return
     */
    def getDictColumnString(localDictColumns: Array[String]): String = {
      val dictColumns: StringBuilder = new StringBuilder
      localDictColumns.foreach(column => dictColumns.append(column.trim).append(","))
      dictColumns.toString().patch(dictColumns.toString().lastIndexOf(","), "", 1)
    }

    results
  }

  override protected def opName: String = "DESC FORMATTED"
}
