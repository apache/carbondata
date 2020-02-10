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

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.command.SecondaryIndex
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePostStatusUpdateEvent

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
        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        val mainTableDetails = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach {
              indexTableName =>
                val isLoadSIForFailedSegments = sparkSession.sessionState.catalog
                  .getTableMetadata(TableIdentifier(indexTableName,
                    Some(carbonLoadModel.getDatabaseName))).storage.properties
                  .getOrElse("isSITableEnabled", "true").toBoolean

                if (!isLoadSIForFailedSegments) {
                  val secondaryIndex = SecondaryIndex(Some(carbonTable.getDatabaseName),
                    indexMetadata.getParentTableName,
                    indexMetadata.getIndexesMap.get(indexTableName).asScala.toList,
                    indexTableName)

                  val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
                  val indexTable = metaStore
                    .lookupRelation(Some(carbonLoadModel.getDatabaseName),
                      indexTableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable

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
                      if (null == detail || detail.length == 0) {
                        val newDetails = new LoadMetadataDetails
                        newDetails.setLoadName(metadataDetail)
                        LOGGER.error("Added in SILoadFailedSegment " + newDetails.getLoadName)
                        failedLoadMetadataDetails.add(newDetails)
                      }
                    })
                  try {
                    if (!failedLoadMetadataDetails.isEmpty) {
                      CarbonInternalScalaUtil
                        .LoadToSITable(sparkSession,
                          carbonLoadModel,
                          indexTableName,
                          isLoadToFailedSISegments = true,
                          secondaryIndex,
                          carbonTable, indexTable, failedLoadMetadataDetails)

                      // get the current load metadata details of the index table
                      details = SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
                    }
                    // Only if the valid segments of maintable match the valid segments of SI
                    // table then we can enable the SI for query
                    if (CarbonInternalLoaderUtil
                      .checkMainTableSegEqualToSISeg(carbonTable.getMetadataPath,
                        indexTable.getMetadataPath)) {
                      // enable the SI table if it was disabled earlier due to failure during SI
                      // creation time
                      sparkSession.sql(
                        s"""ALTER TABLE ${carbonLoadModel.getDatabaseName}.$indexTableName SET
                           |SERDEPROPERTIES ('isSITableEnabled' = 'true')""".stripMargin)
                    }
                  } catch {
                    case ex: Exception =>
                      // in case of SI load only for for failed segments, catch the exception, but
                      // do not fail the main table load, as main table segments should be available
                      // for query
                      LOGGER.error(s"Load to SI table to $indexTableName is failed " +
                               s"or SI table ENABLE is failed. ", ex)
                      return
                  }
                }
            }
          }
        }
    }
  }

  def checkIfMainTableLoadIsValid(mainTableDetails: Array[LoadMetadataDetails],
    loadName: String): Boolean = {
    val mainTableLoadDetail = mainTableDetails
      .filter(mainTableDetail => mainTableDetail.getLoadName.equals(loadName)).head
    if (mainTableLoadDetail.getSegmentStatus ==
        SegmentStatus.MARKED_FOR_DELETE ||
        mainTableLoadDetail.getSegmentStatus == SegmentStatus.COMPACTED) {
      false
    } else {
      true
    }
  }
}
