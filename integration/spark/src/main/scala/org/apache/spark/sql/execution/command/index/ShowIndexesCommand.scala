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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.types.StringType

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.DataMapStoreManager
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
    getFileIndexInfo(carbonTable) ++ getSIInfo(sparkSession, carbonTable)
  }

  // get info for 'index datamap'
  private def getFileIndexInfo(carbonTable: CarbonTable): Seq[Row] = {
    val indexes = DataMapStoreManager.getInstance().getDataMapSchemasOfTable(carbonTable).asScala
    if (indexes != null && indexes.nonEmpty) {
      indexes.map { index =>
        Row(
          index.getDataMapName,
          index.getProviderName,
          index.getIndexColumns.mkString(","),
          index.getPropertiesAsString,
          index.getStatus.name(),
          index.getSyncStatus
        )
      }
    } else {
      Seq.empty
    }
  }

  // get info for SI
  private def getSIInfo(sparkSession: SparkSession, carbonTable: CarbonTable): Seq[Row] = {
    CarbonInternalMetastore.refreshIndexInfo(
      carbonTable.getDatabaseName, tableName, carbonTable)(sparkSession)
    val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
    if (null == indexesMap) {
      throw new Exception("Secondary index information is not loaded in main table")
    }
    val indexTableMap = indexesMap.asScala
    if (indexTableMap.nonEmpty) {
      val indexList = indexTableMap.map { indexInfo =>
        try {
          val isSITableEnabled = sparkSession.sessionState.catalog
            .getTableMetadata(TableIdentifier(indexInfo._1, dbNameOp)).storage.properties
            .getOrElse("isSITableEnabled", "true").equalsIgnoreCase("true")
          if (isSITableEnabled) {
            (indexInfo._1, indexInfo._2.asScala.mkString(","), "enabled")
          } else {
            (indexInfo._1, indexInfo._2.asScala.mkString(","), "disabled")
          }
        } catch {
          case ex: Exception =>
            LOGGER.error(s"Access storage properties from hive failed for index table: ${
              indexInfo._1
            }")
            (indexInfo._1, indexInfo._2.asScala.mkString(","), "UNKNOWN")
        }
      }
      indexList.map { case (indexTableName, columnNames, isSITableEnabled) =>
        Row(
          indexTableName,
          "carbondata",
          columnNames,
          "NA",
          isSITableEnabled,
          "NA"
        )
      }.toSeq
    } else {
      Seq.empty
    }
  }

  override protected def opName: String = "SHOW INDEXES"
}
