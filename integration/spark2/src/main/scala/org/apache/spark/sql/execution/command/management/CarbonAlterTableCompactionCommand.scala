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

import java.io.{File, IOException}

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonEnv, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{AlterTableModel, AtomicRunnableCommand, CarbonMergerMapping, CompactionModel}
import org.apache.spark.sql.hive.{CarbonRelation, CarbonSessionCatalog}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadMetadataEvent
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.spark.rdd.CarbonDataRDDFactory
import org.apache.carbondata.streaming.StreamHandoffRDD
import org.apache.carbondata.streaming.segment.StreamSegment

/**
 * Command for the compaction in alter table command
 */
case class CarbonAlterTableCompactionCommand(
    alterTableModel: AlterTableModel,
    tableInfoOp: Option[TableInfo] = None,
    val operationContext: OperationContext = new OperationContext ) extends AtomicRunnableCommand {

  var table: CarbonTable = _

  override def processMetadata(sparkSession: SparkSession): Seq[Row] = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableModel.tableName.toLowerCase
    val dbName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    table = if (tableInfoOp.isDefined) {
      CarbonTable.buildFromTableInfo(tableInfoOp.get)
    } else {
      val relation = CarbonEnv.getInstance(sparkSession).carbonMetastore
        .lookupRelation(Option(dbName), tableName)(sparkSession).asInstanceOf[CarbonRelation]
      if (relation == null) {
        throw new NoSuchTableException(dbName, tableName)
      }
      if (null == relation.carbonTable) {
        LOGGER.error(s"Data loading failed. table not found: $dbName.$tableName")
        throw new NoSuchTableException(dbName, tableName)
      }
      relation.carbonTable
    }
    if (CarbonUtil.hasAggregationDataMap(table) ||
        (table.isChildDataMap && null == operationContext.getProperty(table.getTableName))) {
      val loadMetadataEvent = new LoadMetadataEvent(table, true)
      OperationListenerBus.getInstance().fireEvent(loadMetadataEvent, operationContext)
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (SegmentStatusManager.isLoadInProgressInTable(table)) {
      throw new ConcurrentOperationException(table, "loading", "compaction")
    }
    operationContext.setProperty("compactionException", "true")
    var compactionType: CompactionType = null
    var compactionException = "true"
    try {
      compactionType = CompactionType.valueOf(alterTableModel.compactionType.toUpperCase)
    } catch {
      case _: Exception =>
        val alterTableCompactionExceptionEvent: AlterTableCompactionExceptionEvent =
          AlterTableCompactionExceptionEvent(sparkSession, table, alterTableModel)
        OperationListenerBus.getInstance
          .fireEvent(alterTableCompactionExceptionEvent, operationContext)
        compactionException = operationContext.getProperty("compactionException").toString
    }
    if (compactionException.equalsIgnoreCase("true") && null == compactionType) {
      throw new MalformedCarbonCommandException(
        "Unsupported alter operation on carbon table")
    } else if (compactionException.equalsIgnoreCase("false")) {
      Seq.empty
    } else {
      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(table.getTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(table.getTableName)
      carbonLoadModel.setDatabaseName(table.getDatabaseName)
      carbonLoadModel.setTablePath(table.getTablePath)

      var storeLocation = CarbonProperties.getInstance.getProperty(
        CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
        System.getProperty("java.io.tmpdir"))
      storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
      // trigger event for compaction
      val alterTableCompactionPreEvent: AlterTableCompactionPreEvent =
        AlterTableCompactionPreEvent(sparkSession, table, null, null)
      OperationListenerBus.getInstance.fireEvent(alterTableCompactionPreEvent, operationContext)
      try {
        alterTableForCompaction(
          sparkSession.sqlContext,
          alterTableModel,
          carbonLoadModel,
          storeLocation,
          operationContext)
      } catch {
        case e: Exception =>
          if (null != e.getMessage) {
            CarbonException.analysisException(
              s"Compaction failed. Please check logs for more info. ${ e.getMessage }")
          } else {
            CarbonException.analysisException(
              "Exception in compaction. Please check logs for more info.")
          }
      }
      // trigger event for compaction
      val alterTableCompactionPostEvent: AlterTableCompactionPostEvent =
        AlterTableCompactionPostEvent(sparkSession, table, null, null)
      OperationListenerBus.getInstance.fireEvent(alterTableCompactionPostEvent, operationContext)
      Seq.empty
    }
  }

  private def alterTableForCompaction(sqlContext: SQLContext,
      alterTableModel: AlterTableModel,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      operationContext: OperationContext): Unit = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    val compactionType = CompactionType.valueOf(alterTableModel.compactionType.toUpperCase)
    val compactionSize = CarbonDataMergerUtil.getCompactionSize(compactionType, carbonLoadModel)
    if (CompactionType.IUD_UPDDEL_DELTA == compactionType) {
      if (alterTableModel.segmentUpdateStatusManager.isDefined) {
        carbonLoadModel.setSegmentUpdateStatusManager(
          alterTableModel.segmentUpdateStatusManager.get)
        carbonLoadModel.setLoadMetadataDetails(
          alterTableModel.segmentUpdateStatusManager.get.getLoadMetadataDetails.toList.asJava)
      }
    }

    LOGGER.audit(s"Compaction request received for table " +
                 s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable

    if (null == carbonLoadModel.getLoadMetadataDetails) {
      carbonLoadModel.readAndSetLoadMetadataDetails()
    }

    if (compactionType == CompactionType.STREAMING) {
      StreamHandoffRDD.startStreamingHandoffThread(
        carbonLoadModel,
        sqlContext.sparkSession, true)
      return
    }

    if (compactionType == CompactionType.CLOSE_STREAMING) {
      closeStreamingTable(
        carbonLoadModel,
        sqlContext.sparkSession)
      return
    }

    // reading the start time of data load.
    val loadStartTime: Long =
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
      isCompactionTriggerByDDl,
      CarbonFilters.getCurrentPartitions(sqlContext.sparkSession,
        TableIdentifier(carbonTable.getTableName,
        Some(carbonTable.getDatabaseName)))
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
      CarbonDataRDDFactory.handleCompactionForSystemLocking(
        sqlContext,
        carbonLoadModel,
        storeLocation,
        compactionType,
        carbonTable,
        compactionModel,
        operationContext
      )
    } else {
      // normal flow of compaction
      val lock = CarbonLockFactory.getCarbonLockObj(
        carbonTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)

      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the compaction lock for table" +
                    s" ${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        try {
          CarbonDataRDDFactory.startCompactionThreads(
            sqlContext,
            carbonLoadModel,
            storeLocation,
            compactionModel,
            lock,
            operationContext
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
        CarbonException.analysisException(
          "Table is already locked for compaction. Please try after some time.")
      }
    }
  }

  def closeStreamingTable(
      carbonLoadModel: CarbonLoadModel,
      sparkSession: SparkSession
  ): Unit = {
    val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getName)
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // 1. acquire lock of streaming.lock
    val streamingLock = CarbonLockFactory.getCarbonLockObj(
      carbonTable.getTableInfo.getOrCreateAbsoluteTableIdentifier,
      LockUsage.STREAMING_LOCK)
    try {
      if (streamingLock.lockWithRetries()) {
        // 2. convert segment status from "streaming" to "streaming finish"
        StreamSegment.finishStreaming(carbonTable)
        // 3. iterate to handoff all streaming segment to batch segment
        StreamHandoffRDD.iterateStreamingHandoff(carbonLoadModel, sparkSession)
        val tableIdentifier =
          new TableIdentifier(carbonTable.getTableName, Option(carbonTable.getDatabaseName))
        // 4. modify table to normal table
        AlterTableUtil.modifyTableProperties(
          tableIdentifier,
          Map("streaming" -> "false"),
          Seq.empty,
          true)(sparkSession,
          sparkSession.sessionState.catalog.asInstanceOf[CarbonSessionCatalog])
        // 5. remove checkpoint
        FileFactory.deleteAllFilesOfDir(
          new File(CarbonTablePath.getStreamingCheckpointDir(carbonTable.getTablePath)))
        FileFactory.deleteAllFilesOfDir(
          new File(CarbonTablePath.getStreamingLogDir(carbonTable.getTablePath)))
      } else {
        val msg = "Failed to close streaming table, because streaming is locked for table " +
                  carbonTable.getDatabaseName() + "." + carbonTable.getTableName()
        LOGGER.error(msg)
        throw new IOException(msg)
      }
    } finally {
      if (streamingLock.unlock()) {
        LOGGER.info("Table unlocked successfully after streaming finished" +
                    carbonTable.getDatabaseName() + "." + carbonTable.getTableName())
      } else {
        LOGGER.error("Unable to unlock Table lock for table " +
                     carbonTable.getDatabaseName() + "." + carbonTable.getTableName() +
                     " during streaming finished")
      }
    }
  }
}
