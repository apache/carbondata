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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession, SQLContext}
import org.apache.spark.sql.execution.command.{AlterTableModel, CompactionModel, DataProcessCommand, RunnableCommand}
import org.apache.spark.sql.hive.CarbonRelation

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.spark.util.CommonUtil

/**
 * Command for the compaction in alter table command
 */
case class AlterTableCompactionCommand(
    alterTableModel: AlterTableModel)
  extends RunnableCommand with DataProcessCommand {

  private val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)

  override def run(sparkSession: SparkSession): Seq[Row] = {
    processData(sparkSession)
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {

    val tableName = alterTableModel.tableName.toLowerCase
    val databaseName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    val relation =
      CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(databaseName), tableName)(sparkSession)
        .asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Table $databaseName.$tableName does not exist")
    }
    if (null == relation.carbonTable) {
      LOGGER.error(s"alter table failed. table not found: $databaseName.$tableName")
      sys.error(s"alter table failed. table not found: $databaseName.$tableName")
    }

    val carbonLoadModel = new CarbonLoadModel()

    val table = relation.carbonTable
    carbonLoadModel.setTableName(table.getTableName)
    val dataLoadSchema = new CarbonDataLoadSchema(table)
    // Need to fill dimension relation
    carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
    carbonLoadModel.setTableName(relation.carbonTable.getTableName)
    carbonLoadModel.setDatabaseName(relation.carbonTable.getDatabaseName)
    carbonLoadModel.setTablePath(relation.carbonTable.getTablePath)

    var storeLocation = CarbonProperties.getInstance
      .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir")
      )
    storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
    try {
      alterTableForCompaction(sparkSession.sqlContext,
          alterTableModel,
          carbonLoadModel,
          storeLocation
        )
    } catch {
      case e: Exception =>
        if (null != e.getMessage) {
          sys.error(s"Compaction failed. Please check logs for more info. ${ e.getMessage }")
        } else {
          sys.error("Exception in compaction. Please check logs for more info.")
        }
    }
    Seq.empty
  }

  private def alterTableForCompaction(sqlContext: SQLContext,
      alterTableModel: AlterTableModel,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String): Unit = {
    var compactionSize: Long = 0
    var compactionType: CompactionType = CompactionType.MINOR_COMPACTION
    if (alterTableModel.compactionType.equalsIgnoreCase("major")) {
      compactionSize = CarbonDataMergerUtil.getCompactionSize(CompactionType.MAJOR_COMPACTION)
      compactionType = CompactionType.MAJOR_COMPACTION
    } else if (alterTableModel.compactionType.equalsIgnoreCase(
      CompactionType.IUD_UPDDEL_DELTA_COMPACTION.toString)) {
      compactionType = CompactionType.IUD_UPDDEL_DELTA_COMPACTION
      if (alterTableModel.segmentUpdateStatusManager.isDefined) {
        carbonLoadModel.setSegmentUpdateStatusManager(
          alterTableModel.segmentUpdateStatusManager.get)
        carbonLoadModel.setLoadMetadataDetails(
          alterTableModel.segmentUpdateStatusManager.get.getLoadMetadataDetails.toList.asJava)
      }
    } else if (alterTableModel.compactionType.equalsIgnoreCase(
      CompactionType.SEGMENT_INDEX_COMPACTION.toString)) {
      compactionType = CompactionType.SEGMENT_INDEX_COMPACTION
    } else {
      compactionType = CompactionType.MINOR_COMPACTION
    }

    LOGGER.audit(s"Compaction request received for table " +
                 s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable

    if (null == carbonLoadModel.getLoadMetadataDetails) {
      CommonUtil.readLoadMetadataDetails(carbonLoadModel)
    }
    if (compactionType == CompactionType.SEGMENT_INDEX_COMPACTION) {
      // Just launch job to merge index and return
      CommonUtil.mergeIndexFiles(sqlContext.sparkContext,
        carbonLoadModel.getLoadMetadataDetails.asScala.map(_.getLoadName),
        carbonLoadModel.getTablePath,
        carbonTable)
      return
    }
    // reading the start time of data load.
    val loadStartTime : Long =
      if (alterTableModel.factTimeStamp.isEmpty) {
        CarbonUpdateUtil.readCurrentTime
      } else {
        alterTableModel.factTimeStamp.get
      }
    carbonLoadModel.setFactTimeStamp(loadStartTime)

    val isCompactionTriggerByDDl = true
    val compactionModel = CompactionModel(compactionSize,
      compactionType,
      carbonTable,
      isCompactionTriggerByDDl
    )

    val isConcurrentCompactionAllowed = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_CONCURRENT_COMPACTION,
        CarbonCommonConstants.DEFAULT_ENABLE_CONCURRENT_COMPACTION
      )
      .equalsIgnoreCase("true")

    // if system level compaction is enabled then only one compaction can run in the system
    // if any other request comes at this time then it will create a compaction request file.
    // so that this will be taken up by the compaction process which is executing.
    if (!isConcurrentCompactionAllowed) {
      LOGGER.info("System level compaction lock is enabled.")
      CarbonDataRDDFactory.handleCompactionForSystemLocking(sqlContext,
        carbonLoadModel,
        storeLocation,
        compactionType,
        carbonTable,
        compactionModel
      )
    } else {
      // normal flow of compaction
      val lock = CarbonLockFactory
        .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
          LockUsage.COMPACTION_LOCK
        )

      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the compaction lock for table" +
                    s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        try {
          CarbonDataRDDFactory.startCompactionThreads(sqlContext,
            carbonLoadModel,
            storeLocation,
            compactionModel,
            lock
          )
        } catch {
          case e: Exception =>
            LOGGER.error(s"Exception in start compaction thread. ${ e.getMessage }")
            lock.unlock()
            throw e
        }
      } else {
        LOGGER.audit("Not able to acquire the compaction lock for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.error(s"Not able to acquire the compaction lock for table" +
                     s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        sys.error("Table is already locked for compaction. Please try after some time.")
      }
    }
  }
}
