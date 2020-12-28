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
import org.apache.spark.sql.execution.command.DataCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.index.CarbonIndexUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}

/**
 * Repair logic for reindex command on maintable/indextable
 */
case class IndexRepairCommand(
  indexnameOp: Option[String], tableIdentifier: TableIdentifier,
  dbName: String,
  segments: Option[List[String]])
extends DataCommand {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  def processData(sparkSession: SparkSession): Seq[Row] = {
    if (dbName == null) {
      // dbName is null, repair for index table or all the index table in main table
      val databaseName = if (tableIdentifier.database.isEmpty) {
        sparkSession.sessionState.catalog.getCurrentDatabase
      } else {
        tableIdentifier.database.get
      }
      triggerRepair(tableIdentifier.table, databaseName, indexnameOp, segments, sparkSession)
    } else {
      // repairing si for all  index tables in the mentioned database in the repair command
      sparkSession.sessionState.catalog.listTables(dbName).foreach {
        tableIdent =>
          triggerRepair(tableIdent.table, dbName, indexnameOp, segments, sparkSession)
      }
    }
    Seq.empty
  }

  def triggerRepair(tableName: String, databaseName: String,
    indexTableToRepair: Option[String], segments: Option[List[String]],
    sparkSession: SparkSession): Unit = {
    // when Si creation and load to main table are parallel, get the carbonTable from the
    // metastore which will have the latest index Info
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val mainCarbonTable = metaStore
      .lookupRelation(Some(databaseName), tableName)(sparkSession)
      .asInstanceOf[CarbonRelation].carbonTable

    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setDatabaseName(databaseName)
    carbonLoadModel.setTableName(tableName)
    carbonLoadModel.setTablePath(mainCarbonTable.getTablePath)
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(mainCarbonTable))
    val indexMetadata = mainCarbonTable.getIndexMetadata
    val secondaryIndexProvider = IndexType.SI.getIndexProviderName
    if (null != indexMetadata && null != indexMetadata.getIndexesMap &&
      null != indexMetadata.getIndexesMap.get(secondaryIndexProvider)) {
      val indexTables = indexMetadata.getIndexesMap
        .get(secondaryIndexProvider).keySet().asScala
      // if there are no index tables for a given fact table do not perform any action
      if (indexTables.nonEmpty) {
        if (indexTableToRepair.isEmpty) {
          indexTables.foreach {
            indexTableName =>
              CarbonIndexUtil.processSIRepair(indexTableName, mainCarbonTable, carbonLoadModel,
                indexMetadata, secondaryIndexProvider, Integer.MAX_VALUE, segments)(sparkSession)
          }
        } else {
          val indexTablesToRepair = indexTables.filter(indexTable => indexTable
            .equals(indexTableToRepair.get))
          indexTablesToRepair.foreach {
            indexTableName =>
              CarbonIndexUtil.processSIRepair(indexTableName, mainCarbonTable, carbonLoadModel,
                indexMetadata, secondaryIndexProvider, Integer.MAX_VALUE, segments)(sparkSession)
          }
          if (indexTablesToRepair.isEmpty) {
            throw new Exception("Unable to find index table" + indexTableToRepair.get)
          }
        }
      }
    }
  }

  override protected def opName: String = "REINDEX COMMAND"
}
