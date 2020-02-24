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

package org.apache.spark.sql.secondaryindex.command

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConverters._
import scala.language.implicitConversions

import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.indextable.{IndexMetadata, IndexTableInfo}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable

/**
 * Command to list the indexes for a table
 */
case class ShowIndexesCommand(
    databaseNameOp: Option[String],
    tableName: String) extends RunnableCommand {

  override def output: Seq[Attribute] = {
    Seq(AttributeReference("Index Table Name", StringType, nullable = false)(),
      AttributeReference("Index Status", StringType, nullable = false)(),
      AttributeReference("Indexed Columns", StringType, nullable = false)())
  }

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  override def run(sparkSession: SparkSession): Seq[Row] = {
    val databaseName = CarbonEnv.getDatabaseName(databaseNameOp)(sparkSession)
    // Here using checkSchemasModifiedTimeAndReloadTables in tableExists to reload metadata if
    // schema is changed by other process, so that tableInfoMap woulb be refilled.
    val catalog = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val identifier = TableIdentifier(tableName, databaseNameOp)
    val tableExists = catalog
      .tableExists(identifier)(sparkSession)
    if (!tableExists) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val carbonTable = catalog.lookupRelation(Some(databaseName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable
    CarbonInternalMetastore.refreshIndexInfo(databaseName, tableName, carbonTable)(sparkSession)
    if (carbonTable == null) {
      sys.error(s"$databaseName.$tableName is not found")
    }
    val indexesMap = CarbonInternalScalaUtil.getIndexesMap(carbonTable)
    if (null == indexesMap) {
      throw new Exception("Secondary index information is not loaded in main table")
    }
    val indexTableMap = indexesMap.asScala
    if (indexTableMap.nonEmpty) {
      val indexList = indexTableMap.map { indexInfo =>
        try {
        val isSITableEnabled = sparkSession.sessionState.catalog
          .getTableMetadata(TableIdentifier(indexInfo._1, Some(databaseName))).storage.properties
          .getOrElse("isSITableEnabled", "true").equalsIgnoreCase("true")
        if (isSITableEnabled) {
          (indexInfo._1, indexInfo._2.asScala.mkString(","), "enabled")
        } else {
          (indexInfo._1, indexInfo._2.asScala.mkString(","), "disabled")
        }
        } catch {
          case ex: Exception =>
            LOGGER.error(s"Access storage properties from hive failed for index table: ${
              indexInfo._1}")
            (indexInfo._1, indexInfo._2.asScala.mkString(","), "UNKNOWN")
        }
      }
      indexList.map { case (indexTableName, columnName, isSITableEnabled) =>
        Row(indexTableName, isSITableEnabled, columnName)
      }.toSeq
    } else {
      Seq.empty
    }
  }

}

object ShowIndexesCommand {
  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def refreshIndexInfo(dbName: String, tableName: String,
      carbonTable: CarbonTable, needLock: Boolean = true)(sparkSession: SparkSession): Unit = {
    val indexTableExists = CarbonInternalScalaUtil.isIndexTableExists(carbonTable)
    // tables created without property "indexTableExists", will return null, for those tables enter
    // into below block, gather the actual data from hive and then set this property to true/false
    // then once the property has a value true/false, make decision based on the property value
    if (null != carbonTable && (null == indexTableExists || indexTableExists.toBoolean)) {
      // When Index information is not loaded in main table, then it will fetch
      // index info from hivemetastore and set it in the carbon table.
      val indexTableMap = new ConcurrentHashMap[String, java.util.List[String]]
      try {
        val (isIndexTable, parentTableName, indexInfo, parentTablePath, parentTableId, schema) =
          CarbonInternalMetastore.indexInfoFromHive(dbName, tableName)(sparkSession)
        if (isIndexTable.equals("true")) {
          val indexMeta = new IndexMetadata(indexTableMap,
            parentTableName,
            true,
            parentTablePath,
            parentTableId)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMeta.serialize)
        } else {
          IndexTableInfo.fromGson(indexInfo)
            .foreach { indexTableInfo =>
              indexTableMap
                .put(indexTableInfo.getTableName, indexTableInfo.getIndexCols)
            }
          val indexMetadata = new IndexMetadata(indexTableMap,
            parentTableName,
            isIndexTable.toBoolean,
            parentTablePath, parentTableId)
          carbonTable.getTableInfo.getFactTable.getTableProperties
            .put(carbonTable.getCarbonTableIdentifier.getTableId, indexMetadata.serialize)
        }
        if (null == indexTableExists && !isIndexTable.equals("true")) {
          val indexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable)
          if (indexTables.isEmpty) {
            // modify the tableProperties of mainTable by adding "indexTableExists" property
            // to false as there is no index table for this table
            CarbonInternalScalaUtil
              .addOrModifyTableProperty(carbonTable,
                Map("indexTableExists" -> "false"), needLock)(sparkSession)
          } else {
            // modify the tableProperties of mainTable by adding "indexTableExists" property
            // to true as there are some index table for this table
            CarbonInternalScalaUtil
              .addOrModifyTableProperty(carbonTable,
                Map("indexTableExists" -> "true"), needLock)(sparkSession)
          }
        }
      } catch {
        case e: Exception =>
          // In case of creating a table, hivetable will not be available.
          LOGGER.error(e.getMessage, e)
      }
    }
  }
}
