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
import java.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, CarbonEnv, Row, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{AlterTableModel, AtomicRunnableCommand, CompactionModel}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.status.DataMapStatusManager
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.ColumnarFormatVersion
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, TableInfo}
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{FileFormat, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadMetadataEvent
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.rdd.{CarbonDataRDDFactory, StreamHandoffRDD}
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
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
    val tableName = alterTableModel.tableName.toLowerCase
    val dbName = alterTableModel.dbName.getOrElse(sparkSession.catalog.currentDatabase)
    setAuditTable(dbName, tableName)
    if (alterTableModel.customSegmentIds.nonEmpty) {
      setAuditInfo(Map("segmentIds" -> alterTableModel.customSegmentIds.get.mkString(", ")))
    }
    table = if (tableInfoOp.isDefined) {
      CarbonTable.buildFromTableInfo(tableInfoOp.get)
    } else {
      val relation = CarbonEnv.getInstance(sparkSession).carbonMetaStore
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
    if (!table.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    Seq.empty
  }

  override def processData(sparkSession: SparkSession): Seq[Row] = {
    if (SegmentStatusManager.isOverwriteInProgressInTable(table)) {
      throw new ConcurrentOperationException(table, "insert overwrite", "compaction")
    }
    var compactionType: CompactionType = null
    try {
      compactionType = CompactionType.valueOf(alterTableModel.compactionType.toUpperCase)
    } catch {
      case _: Exception =>
        throw new MalformedCarbonCommandException(
          "Unsupported alter operation on carbon table")
    }
    if (compactionType == CompactionType.UPGRADE_SEGMENT) {
      val tableStatusLock = CarbonLockFactory
        .getCarbonLockObj(table.getAbsoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
      try {
        if (tableStatusLock.lockWithRetries()) {
          val loadMetaDataDetails = SegmentStatusManager.readTableStatusFile(CarbonTablePath
            .getTableStatusFilePath(table.getTablePath))
          loadMetaDataDetails.foreach { loadMetaDataDetail =>
            // "0" check is added to reproduce a scenario similar to 1.1 store where the size
            // would be null. For test case in the new version it would be set to 0.
            if (loadMetaDataDetail.getIndexSize == null || loadMetaDataDetail.getDataSize == null
            || loadMetaDataDetail.getIndexSize == "0" || loadMetaDataDetail.getDataSize == "0") {
              CarbonLoaderUtil
                .addDataIndexSizeIntoMetaEntry(loadMetaDataDetail, loadMetaDataDetail.getLoadName,
                  table)
            }
          }
          SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
            .getTableStatusFilePath(table.getTablePath), loadMetaDataDetails)
        } else {
          throw new ConcurrentOperationException(table.getDatabaseName,
            table.getTableName, "table status updation", "upgrade segments")
        }
      } finally {
        tableStatusLock.unlock()
      }
      Seq.empty
    } else if (compactionType == CompactionType.SEGMENT_INDEX) {
      if (table.isStreamingSink) {
        throw new MalformedCarbonCommandException(
          "Unsupported alter operation on carbon table: Merge index is not supported on streaming" +
          " table")
      }
      val version = CarbonUtil.getFormatVersion(table)
      val isOlderVersion = version == ColumnarFormatVersion.V1 ||
                           version == ColumnarFormatVersion.V2
      if (isOlderVersion) {
        throw new MalformedCarbonCommandException(
          "Unsupported alter operation on carbon table: Merge index is not supported on V1 V2 " +
          "store segments")
      }

      val alterTableMergeIndexEvent: AlterTableMergeIndexEvent =
        AlterTableMergeIndexEvent(sparkSession, table, alterTableModel)
      OperationListenerBus.getInstance
        .fireEvent(alterTableMergeIndexEvent, operationContext)
      Seq.empty
    } else {

      if (compactionType != CompactionType.CUSTOM &&
        alterTableModel.customSegmentIds.isDefined) {
        throw new MalformedCarbonCommandException(
          s"Custom segments not supported when doing ${compactionType.toString} compaction")
      }
      if (compactionType == CompactionType.CUSTOM &&
        alterTableModel.customSegmentIds.isEmpty) {
        throw new MalformedCarbonCommandException(
          s"Segment ids should not be empty when doing ${compactionType.toString} compaction")
      }

      val carbonLoadModel = new CarbonLoadModel()
      carbonLoadModel.setTableName(table.getTableName)
      val dataLoadSchema = new CarbonDataLoadSchema(table)
      // Need to fill dimension relation
      carbonLoadModel.setCarbonDataLoadSchema(dataLoadSchema)
      carbonLoadModel.setTableName(table.getTableName)
      carbonLoadModel.setCarbonTransactionalTable(table.isTransactionalTable)
      carbonLoadModel.setDatabaseName(table.getDatabaseName)
      carbonLoadModel.setTablePath(table.getTablePath)
      val columnCompressor = table.getTableInfo.getFactTable.getTableProperties.asScala
        .getOrElse(CarbonCommonConstants.COMPRESSOR,
          CompressorFactory.getInstance().getCompressor.getName)
      carbonLoadModel.setColumnCompressor(columnCompressor)

      var storeLocation = System.getProperty("java.io.tmpdir")
      storeLocation = storeLocation + "/carbonstore/" + System.nanoTime()
      // trigger event for compaction
      val alterTableCompactionPreEvent: AlterTableCompactionPreEvent =
        AlterTableCompactionPreEvent(sparkSession, table, null, null)
      OperationListenerBus.getInstance.fireEvent(alterTableCompactionPreEvent, operationContext)
      val compactedSegments: java.util.List[String] = new util.ArrayList[String]()
      try {
        alterTableForCompaction(
          sparkSession.sqlContext,
          alterTableModel,
          carbonLoadModel,
          storeLocation,
          compactedSegments,
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
        AlterTableCompactionPostEvent(sparkSession, table, null, compactedSegments)
      OperationListenerBus.getInstance.fireEvent(alterTableCompactionPostEvent, operationContext)
      Seq.empty
    }
  }

  private def alterTableForCompaction(sqlContext: SQLContext,
      alterTableModel: AlterTableModel,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      compactedSegments: java.util.List[String],
      operationContext: OperationContext): Unit = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
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

    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable

    if (null == carbonLoadModel.getLoadMetadataDetails) {
      carbonLoadModel.readAndSetLoadMetadataDetails()
    }

    if (compactionType == CompactionType.STREAMING) {
      StreamHandoffRDD.startStreamingHandoffThread(
        carbonLoadModel,
        operationContext,
        sqlContext.sparkSession, true)
      return
    }

    if (compactionType == CompactionType.CLOSE_STREAMING) {
      closeStreamingTable(
        carbonLoadModel,
        operationContext,
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
    val segmentIds: Option[List[String]] = if (compactionType == CompactionType.CUSTOM &&
      alterTableModel.customSegmentIds.isDefined) {
      val ids = alterTableModel.customSegmentIds
      ids match {
        case Some(x) =>
          val loadMetadataDetails = carbonLoadModel.getLoadMetadataDetails.asScala
          val otherLoadDetails = loadMetadataDetails
            .exists(p => x.exists(a => a.equalsIgnoreCase(p.getLoadName)) && !p.isCarbonFormat)
          if (otherLoadDetails) {
            throw new AnalysisException(s"Custom compaction does not support other format segment")
          }
        case None =>
      }
      ids
    } else {
      None
    }
    val compactionModel = CompactionModel(compactionSize,
      compactionType,
      carbonTable,
      isCompactionTriggerByDDl,
      CarbonFilters.getCurrentPartitions(sqlContext.sparkSession,
      TableIdentifier(carbonTable.getTableName,
      Some(carbonTable.getDatabaseName))),
      segmentIds
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
        compactedSegments,
        compactionModel,
        operationContext
      )
    } else {
      // normal flow of compaction
      val lock = CarbonLockFactory.getCarbonLockObj(
        carbonTable.getAbsoluteTableIdentifier,
        LockUsage.COMPACTION_LOCK)
      val updateLock = CarbonLockFactory.getCarbonLockObj(carbonTable
        .getAbsoluteTableIdentifier, LockUsage.UPDATE_LOCK)
      try {
        // COMPACTION_LOCK and UPDATE_LOCK are already locked when start to execute update sql,
        // so it don't need to require locks again when compactionType is IUD_UPDDEL_DELTA.
        if (CompactionType.IUD_UPDDEL_DELTA != compactionType) {
          if (!updateLock.lockWithRetries(3, 3)) {
            throw new ConcurrentOperationException(carbonTable, "update", "compaction")
          }
          if (!lock.lockWithRetries()) {
            LOGGER.error(s"Not able to acquire the compaction lock for table " +
                         s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
            CarbonException.analysisException(
              "Table is already locked for compaction. Please try after some time.")
          } else {
            LOGGER.info("Acquired the compaction lock for table " +
                        s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
          }
        }
        CarbonDataRDDFactory.startCompactionThreads(
          sqlContext,
          carbonLoadModel,
          storeLocation,
          compactionModel,
          lock,
          compactedSegments,
          operationContext
        )
      } catch {
        case e: Exception =>
          LOGGER.error(s"Exception in start compaction thread.", e)
          if (CompactionType.IUD_UPDDEL_DELTA != compactionType) {
            lock.unlock()
          }
          throw e
      } finally {
        if (CompactionType.IUD_UPDDEL_DELTA != compactionType) {
          updateLock.unlock()
        }
        DataMapStatusManager.disableAllLazyDataMaps(carbonTable)
      }
    }
  }

  def closeStreamingTable(
      carbonLoadModel: CarbonLoadModel,
      operationContext: OperationContext,
      sparkSession: SparkSession
  ): Unit = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // 1. delete the lock of streaming.lock, forcing the stream to be closed
    val streamingLock = CarbonLockFactory.getCarbonLockObj(
      carbonTable.getTableInfo.getOrCreateAbsoluteTableIdentifier,
      LockUsage.STREAMING_LOCK)
    val lockFile =
      FileFactory.getCarbonFile(streamingLock.getLockFilePath, FileFactory.getConfiguration)
    if (lockFile.exists()) {
      if (!lockFile.delete()) {
        LOGGER.warn("failed to delete lock file: " + streamingLock.getLockFilePath)
      }
    }
    try {
      if (streamingLock.lockWithRetries()) {
        // 2. convert segment status from "streaming" to "streaming finish"
        StreamSegment.finishStreaming(carbonTable)
        // 3. iterate to handoff all streaming segment to batch segment
        StreamHandoffRDD.iterateStreamingHandoff(carbonLoadModel, operationContext, sparkSession)
        val tableIdentifier =
          new TableIdentifier(carbonTable.getTableName, Option(carbonTable.getDatabaseName))
        // 4. modify table to normal table
        AlterTableUtil.modifyTableProperties(
          tableIdentifier,
          Map("streaming" -> "false"),
          Seq.empty,
          true)(sparkSession,
          sparkSession.sessionState.catalog)
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

  override protected def opName: String = {
    s"ALTER TABLE COMPACTION ${alterTableModel.compactionType.toUpperCase}"
  }
}
