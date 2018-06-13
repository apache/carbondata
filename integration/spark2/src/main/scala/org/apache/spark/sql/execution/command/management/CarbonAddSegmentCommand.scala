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

package org.apache.spark.sql.execution.command.management

import java.util.UUID

import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{CarbonEnv, Row, SparkSession}
import org.apache.spark.sql.execution.command.AtomicRunnableCommand
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.util.FileUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadMetadataEvent
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

/**
 * support `alter table tableName add segment location 'path'` command.
 * It will create a segment and map the path of datafile to segment's storage
 */
case class CarbonAddSegmentCommand(
    dbNameOp: Option[String],
    tableName: String,
    filePathFromUser: String,
    var operationContext: OperationContext = new OperationContext) extends AtomicRunnableCommand {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
  var carbonTable: CarbonTable = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val dbName = CarbonEnv.getDatabaseName(dbNameOp)(sparkSession)
    carbonTable = {
      val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
      if (relation == null) {
        LOGGER.error(s"Add segment failed due to table $dbName.$tableName not found")
        throw new NoSuchTableException(dbName, tableName)
      }
      relation.carbonTable
    }

    if (carbonTable.isHivePartitionTable) {
      LOGGER.error("Ignore hive partition table for now")
    }

    operationContext.setProperty("isOverwrite", false)
    if (CarbonUtil.hasAggregationDataMap(carbonTable)) {
      val loadMetadataEvent = new LoadMetadataEvent(carbonTable, false)
      OperationListenerBus.getInstance().fireEvent(loadMetadataEvent, operationContext)
    }
    Seq.empty
  }

  // will just mapping external files to segment metadata
  override def processData(sparkSession: SparkSession): Seq[Row] = {
    // clean up invalid segment before creating a new entry
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(carbonTable, false, null)
    val currentLoadMetadataDetails = SegmentStatusManager.readLoadMetadata(
      CarbonTablePath.getMetadataPath(carbonTable.getTablePath))
    val newSegmentId = SegmentStatusManager.createNewSegmentId(currentLoadMetadataDetails).toString
    // create new segment folder in carbon store
    CarbonLoaderUtil.checkAndCreateCarbonDataLocation(newSegmentId, carbonTable)

    val factFilePath = FileUtils.getPaths(filePathFromUser)

    val uuid = if (carbonTable.isChildDataMap) {
      Option(operationContext.getProperty("uuid")).getOrElse("").toString
    } else if (carbonTable.hasAggregationDataMap) {
      UUID.randomUUID().toString
    } else {
      ""
    }
    // associate segment meta with file path, files are separated with comma
    val loadModel: CarbonLoadModel = new CarbonLoadModel
    loadModel.setSegmentId(newSegmentId)
    loadModel.setDatabaseName(carbonTable.getDatabaseName)
    loadModel.setTableName(carbonTable.getTableName)
    loadModel.setTablePath(carbonTable.getTablePath)
    loadModel.setCarbonTransactionalTable(carbonTable.isTransactionalTable)
    loadModel.readAndSetLoadMetadataDetails()
    loadModel.setFactTimeStamp(CarbonUpdateUtil.readCurrentTime())
    val loadSchema: CarbonDataLoadSchema = new CarbonDataLoadSchema(carbonTable)
    loadModel.setCarbonDataLoadSchema(loadSchema)

    val newLoadMetadataDetail: LoadMetadataDetails = new LoadMetadataDetails

    // for external datasource table, there are no index files, so no need to write segment file

    // update table status file
    newLoadMetadataDetail.setSegmentFile(null)
    newLoadMetadataDetail.setSegmentStatus(SegmentStatus.SUCCESS)
    newLoadMetadataDetail.setLoadStartTime(loadModel.getFactTimeStamp)
    newLoadMetadataDetail.setLoadEndTime(CarbonUpdateUtil.readCurrentTime())
    newLoadMetadataDetail.setIndexSize("1")
    newLoadMetadataDetail.setDataSize("1")
    newLoadMetadataDetail.setFileFormat(FileFormat.EXTERNAL)
    newLoadMetadataDetail.setFactFilePath(factFilePath)

    val done = CarbonLoaderUtil.recordNewLoadMetadata(newLoadMetadataDetail, loadModel, true,
      false, uuid)
    if (!done) {
      val errorMsg =
        s"""
           | Data load is failed due to table status update failure for
           | ${loadModel.getDatabaseName}.${loadModel.getTableName}
         """.stripMargin
      throw new Exception(errorMsg)
    } else {
      DataMapStatusManager.disableAllLazyDataMaps(carbonTable)
    }
    Seq.empty
  }
}