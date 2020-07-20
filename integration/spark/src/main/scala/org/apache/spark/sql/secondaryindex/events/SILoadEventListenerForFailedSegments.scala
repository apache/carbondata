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

package org.apache.spark.sql.secondaryindex.events

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.command.IndexModel
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostStatusUpdateEvent
import org.apache.carbondata.processing.loading.model.CarbonLoadModel

/**
 * This Listener is to load the data to failed segments of Secondary index table(s)
 */
class SILoadEventListenerForFailedSegments extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   */
  override protected def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case postStatusUpdateEvent: LoadTablePostStatusUpdateEvent =>
        LOGGER.info("Load post status update event-listener called")
        val loadTablePostStatusUpdateEvent = event.asInstanceOf[LoadTablePostStatusUpdateEvent]
        val carbonLoadModel = loadTablePostStatusUpdateEvent.getCarbonLoadModel
        val sparkSession = SparkSession.getActiveSession.get
        // when Si creation and load to main table are parallel, get the carbonTable from the
        // metastore which will have the latest index Info
        val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
        val carbonTable = metaStore
          .lookupRelation(Some(carbonLoadModel.getDatabaseName),
            carbonLoadModel.getTableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable
        val indexMetadata = carbonTable.getIndexMetadata
        val secondaryIndexProvider = IndexType.SI.getIndexProviderName
        if (null != indexMetadata && null != indexMetadata.getIndexesMap &&
            null != indexMetadata.getIndexesMap.get(secondaryIndexProvider)) {
          val indexTables = indexMetadata.getIndexesMap
            .get(secondaryIndexProvider).keySet().asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            val mainTableDetails =
              SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
            indexTables.foreach {
              indexTableName =>
                val isLoadSIForFailedSegments = sparkSession.sessionState.catalog
                  .getTableMetadata(TableIdentifier(indexTableName,
                    Some(carbonLoadModel.getDatabaseName))).storage.properties
                  .getOrElse("isSITableEnabled", "true").toBoolean
                val indexTable = metaStore
                  .lookupRelation(Some(carbonLoadModel.getDatabaseName), indexTableName)(
                    sparkSession)
                  .asInstanceOf[CarbonRelation]
                  .carbonTable

                val mainTblLoadMetadataDetails: Array[LoadMetadataDetails] =
                  SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
                val siTblLoadMetadataDetails: Array[LoadMetadataDetails] =
                  SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
                var segmentLocks: ListBuffer[ICarbonLock] = ListBuffer.empty
                if (!isLoadSIForFailedSegments
                    || !CarbonInternalLoaderUtil.checkMainTableSegEqualToSISeg(
                  mainTblLoadMetadataDetails,
                  siTblLoadMetadataDetails)) {
                  val indexColumns = indexMetadata.getIndexColumns(secondaryIndexProvider,
                    indexTableName)
                  val secondaryIndex = IndexModel(Some(carbonTable.getDatabaseName),
                    indexMetadata.getParentTableName,
                    indexColumns.split(",").toList,
                    indexTableName)

                  var details = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
                  // If it empty, then no need to do further computations because the
                  // tabletstatus might not have been created and hence next load will take care
                  if (details.isEmpty) {
                    return
                  }

                  val failedLoadMetadataDetails: java.util.List[LoadMetadataDetails] = new util
                  .ArrayList[LoadMetadataDetails]()

                  // read the details of SI table and get all the failed segments during SI
                  // creation which are MARKED_FOR_DELETE or invalid INSERT_IN_PROGRESS
                  details.collect {
                    case loadMetaDetail: LoadMetadataDetails =>
                      if (loadMetaDetail.getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE &&
                          checkIfMainTableLoadIsValid(mainTableDetails,
                            loadMetaDetail.getLoadName)) {
                          failedLoadMetadataDetails.add(loadMetaDetail)
                      } else if ((loadMetaDetail.getSegmentStatus ==
                                  SegmentStatus.INSERT_IN_PROGRESS ||
                                  loadMetaDetail.getSegmentStatus ==
                                  SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) &&
                                 checkIfMainTableLoadIsValid(mainTableDetails,
                                   loadMetaDetail.getLoadName)) {
                        val segmentLock = CarbonLockFactory
                          .getCarbonLockObj(indexTable.getAbsoluteTableIdentifier,
                            CarbonTablePath.addSegmentPrefix(loadMetaDetail.getLoadName) +
                            LockUsage.LOCK)
                        try {
                          if (segmentLock.lockWithRetries(1, 0)) {
                            LOGGER
                              .info("SIFailedLoadListener: Acquired segment lock on segment:" +
                                    loadMetaDetail.getLoadName)
                            failedLoadMetadataDetails.add(loadMetaDetail)
                          }
                        } finally {
                          segmentLock.unlock()
                          LOGGER
                            .info("SIFailedLoadListener: Released segment lock on segment:" +
                                  loadMetaDetail.getLoadName)
                        }
                      }
                  }
                  // check for the skipped segments. compare the main table and SI table table
                  // status file and get the skipped segments if any
                  CarbonInternalLoaderUtil.getListOfValidSlices(mainTableDetails).asScala
                    .foreach(metadataDetail => {
                      val detail = details
                        .filter(metadata => metadata.getLoadName.equals(metadataDetail))
                      val mainTableDetail = mainTableDetails
                        .filter(metadata => metadata.getLoadName.equals(metadataDetail))
                      if (null == detail || detail.length == 0) {
                        val newDetails = new LoadMetadataDetails
                        newDetails.setLoadName(metadataDetail)
                        LOGGER.error("Added in SILoadFailedSegment " + newDetails.getLoadName)
                        failedLoadMetadataDetails.add(newDetails)
                      } else if (detail != null && detail.length !=0 && metadataDetail != null
                                 && metadataDetail.length != 0) {
                        // If SI table has compacted segments and main table does not have
                        // compacted segments due to some failure while compaction, need to
                        // reload the original segments in this case.
                        if (detail(0).getSegmentStatus == SegmentStatus.COMPACTED &&
                            mainTableDetail(0).getSegmentStatus == SegmentStatus.SUCCESS) {
                          detail(0).setSegmentStatus(SegmentStatus.SUCCESS)
                          // in concurrent scenario, if a compaction is going on table, then SI
                          // segments are updated first in table status and then the main table
                          // segment, so in any load runs paralley this listener shouldn't consider
                          // those segments accidentally. So try to take the segment lock.
                          val segmentLockOfProbableOngngCompactionSeg = CarbonLockFactory
                            .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
                              CarbonTablePath.addSegmentPrefix(mainTableDetail(0).getLoadName) +
                              LockUsage.LOCK)
                          if (segmentLockOfProbableOngngCompactionSeg.lockWithRetries()) {
                            segmentLocks += segmentLockOfProbableOngngCompactionSeg
                            LOGGER.error("Added in SILoadFailedSegment " + detail(0).getLoadName)
                            failedLoadMetadataDetails.add(detail(0))
                          }
                        }
                      }
                    })
                  try {
                    if (!failedLoadMetadataDetails.isEmpty) {
                      // in the case when in SI table a segment is deleted and it's entry is
                      // deleted from the tablestatus file, the corresponding .segment file from
                      // the metadata folder should also be deleted as it contains the
                      // mergefilename which does not exist anymore as the segment is deleted.
                      deleteStaleSegmentFileIfPresent(carbonLoadModel,
                        indexTable,
                        failedLoadMetadataDetails)
                      CarbonIndexUtil
                        .LoadToSITable(sparkSession,
                          carbonLoadModel,
                          indexTableName,
                          isLoadToFailedSISegments = true,
                          secondaryIndex,
                          carbonTable, indexTable, failedLoadMetadataDetails)

                      // get the current load metadata details of the index table
                      details = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
                    }

                    // get updated main table segments and si table segments
                    val mainTblLoadMetadataDetails: Array[LoadMetadataDetails] =
                      SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
                    val siTblLoadMetadataDetails: Array[LoadMetadataDetails] =
                      SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)

                    // check if main table has load in progress and SI table has no load
                    // in progress entry, then no need to enable the SI table
                    // Only if the valid segments of maintable match the valid segments of SI
                    // table then we can enable the SI for query
                    if (CarbonInternalLoaderUtil
                          .checkMainTableSegEqualToSISeg(mainTblLoadMetadataDetails,
                            siTblLoadMetadataDetails)
                        && CarbonInternalLoaderUtil.checkInProgLoadInMainTableAndSI(carbonTable,
                      mainTblLoadMetadataDetails, siTblLoadMetadataDetails)) {
                      // enable the SI table if it was disabled earlier due to failure during SI
                      // creation time
                      sparkSession.sql(
                        s"""ALTER TABLE ${carbonLoadModel.getDatabaseName}.$indexTableName SET
                           |SERDEPROPERTIES ('isSITableEnabled' = 'true')""".stripMargin).collect()
                    }
                  } catch {
                    case ex: Exception =>
                      // in case of SI load only for for failed segments, catch the exception, but
                      // do not fail the main table load, as main table segments should be available
                      // for query
                      LOGGER.error(s"Load to SI table to $indexTableName is failed " +
                               s"or SI table ENABLE is failed. ", ex)
                      return
                  } finally {
                    segmentLocks.foreach {
                      segmentLock => segmentLock.unlock()
                    }
                  }
                }
            }
          }
        }
    }
  }

  def checkIfMainTableLoadIsValid(mainTableDetails: Array[LoadMetadataDetails],
    loadName: String): Boolean = {
    // in concurrent scenarios there can be cases when loadName is not present in the
    // mainTableDetails array. Added a check to see if the loadName is even present in the
    // mainTableDetails.
    val mainTableLoadDetail = mainTableDetails
      .filter(mainTableDetail => mainTableDetail.getLoadName.equals(loadName))
    if (mainTableLoadDetail.length == 0) {
      false
    } else {
      if (mainTableLoadDetail.head.getSegmentStatus ==
        SegmentStatus.MARKED_FOR_DELETE ||
        mainTableLoadDetail.head.getSegmentStatus == SegmentStatus.COMPACTED) {
        false
      } else {
        true
      }
    }
  }

  def deleteStaleSegmentFileIfPresent(carbonLoadModel: CarbonLoadModel, indexTable: CarbonTable,
    failedLoadMetaDataDetails: java.util.List[LoadMetadataDetails]): Unit = {
    failedLoadMetaDataDetails.asScala.map(failedLoadMetaData => {
      carbonLoadModel.getLoadMetadataDetails.asScala.map(loadMetaData => {
        if (failedLoadMetaData.getLoadName == loadMetaData.getLoadName) {
          val segmentFilePath = CarbonTablePath.getSegmentFilesLocation(indexTable.getTablePath) +
            CarbonCommonConstants.FILE_SEPARATOR + loadMetaData.getSegmentFile
          if (FileFactory.isFileExist(segmentFilePath)) {
            // delete the file if it exists
            FileFactory.deleteFile(segmentFilePath)
          }
        }
      })
    })
  }
}
