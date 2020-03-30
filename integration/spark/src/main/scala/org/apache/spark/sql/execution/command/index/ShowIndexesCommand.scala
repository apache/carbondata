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

package org.apache.spark.sql.execution.command.index

import java.util

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.collection.mutable

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.Strings
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.DataMapStoreManager
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProperty
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Show indexes on the table
 */
case class ShowIndexesCommand(
    dbNameOp: Option[String],
    tableName: String)
  extends DataCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def output: Seq[Attribute] = {
    Seq(
      AttributeReference("Name", StringType, nullable = false)(),
      AttributeReference("Provider", StringType, nullable = false)(),
      AttributeReference("Indexed Columns", StringType, nullable = false)(),
      AttributeReference("Properties", StringType, nullable = false)(),
      AttributeReference("Status", StringType, nullable = false)(),
      AttributeReference("Sync Info", StringType, nullable = false)())
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    val carbonTable = CarbonEnv.getCarbonTable(dbNameOp, tableName)(sparkSession)
    setAuditTable(carbonTable)
    getIndexInfo(sparkSession, carbonTable)
  }

  private def getIndexInfo(sparkSession: SparkSession, carbonTable: CarbonTable): Seq[Row] = {
    CarbonInternalMetastore.refreshIndexInfo(
      carbonTable.getDatabaseName, tableName, carbonTable)(sparkSession)
    val indexesMap = CarbonIndexUtil.getIndexesMap(carbonTable)
    if (null != indexesMap) {
      val indexTableMap = indexesMap.asScala
      if (indexTableMap.nonEmpty) {
        val secondaryIndex = indexTableMap.get(CarbonIndexProvider.SI.getIndexProviderName)
        var finalIndexList: Seq[(String, String, String, String, String, String)] = Seq.empty

        if (secondaryIndex.isDefined && null != secondaryIndex.get) {
          val siIterator = secondaryIndex.get.entrySet().iterator()
          while (siIterator.hasNext) {
            val indexInfo = siIterator.next()
            try {
              val isSITableEnabled = sparkSession.sessionState.catalog
                .getTableMetadata(TableIdentifier(indexInfo.getKey, dbNameOp)).storage.properties
                .getOrElse("isSITableEnabled", "true").equalsIgnoreCase("true")
              if (isSITableEnabled) {
                finalIndexList = finalIndexList :+
                                 (indexInfo.getKey, "carbondata", indexInfo.getValue
                                   .get(CarbonCommonConstants.INDEX_COLUMNS), "NA", "enabled", "NA")
              } else {
                finalIndexList = finalIndexList :+
                                 (indexInfo.getKey, "carbondata", indexInfo.getValue
                                   .get(CarbonCommonConstants
                                     .INDEX_COLUMNS), "NA", "disabled", "NA")
              }
            } catch {
              case ex: Exception =>
                LOGGER.error(s"Access storage properties from hive failed for index table: ${
                  indexInfo.getKey
                }")
                finalIndexList = finalIndexList :+
                                 (indexInfo.getKey, "carbondata", indexInfo.getValue
                                   .get(CarbonCommonConstants.INDEX_COLUMNS), "NA", "UNKNOWN", "NA")
            }
          }
        }

        indexesMap.asScala
          .filter(map => !map._1.equalsIgnoreCase(CarbonIndexProvider.SI.getIndexProviderName))
          .values.foreach { index =>
          val indexIterator = index.entrySet().iterator()
          while (indexIterator.hasNext) {
            val indexInfo = indexIterator.next()
            val indexProperties = indexInfo.getValue
            val indexStatus =
              if (null != indexProperties.get(CarbonCommonConstants.INDEX_STATUS)) {
                indexProperties.get(CarbonCommonConstants.INDEX_STATUS)
              } else {
                "DISABLED"
              }
            val provider = indexProperties.get(CarbonCommonConstants.INDEX_PROVIDER)
            val indexColumns = indexProperties.get(CarbonCommonConstants.INDEX_COLUMNS)
            // ignore internal used property
            val properties = ListMap(indexProperties.asScala.filter(f =>
              !f._1.equalsIgnoreCase(DataMapProperty.DEFERRED_REBUILD) &&
              !f._1.equalsIgnoreCase(CarbonCommonConstants.INDEX_PROVIDER) &&
              !f._1.equalsIgnoreCase(CarbonCommonConstants.INDEX_STATUS)).toSeq.sortBy(_._1): _*)
              .map { p => "'" + p._1 + "'='" + p._2 + "'" }
              .toArray
            finalIndexList = finalIndexList :+
                             ((indexInfo.getKey, provider, indexColumns, Strings
                               .mkString(properties, ","), indexStatus, "NA"))
          }
        }

        finalIndexList.map { case (col1, col2, col3, col4, col5, col6) =>
          Row(col1, col2, col3, col4, col5, col6)
        }
      } else {
        Seq.empty
      }
    } else {
      Seq.empty
    }

  }

  override protected def opName: String = "SHOW INDEXES"
}
