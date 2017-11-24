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

package org.apache.carbondata.streaming

import org.apache.spark.sql.SQLContext

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus,
SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonStorePath
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.HandoffResultImpl
import org.apache.carbondata.spark.util.CommonUtil

object StreamHandoffUtil {

  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * start new thread to execute stream segment handoff
   */
  def startStreamingHandoffThread(
      carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      storeLocation: String
  ): Unit = {
    // start a new thread to execute streaming segment handoff
    val handoffThread = new Thread() {
      override def run(): Unit = {
        val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
        val identifier = carbonTable.getAbsoluteTableIdentifier
        val tablePath = CarbonStorePath.getCarbonTablePath(identifier)
        var continueHandoff = false
        // require handoff lock on table
        val lock = CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.HANDOFF_LOCK)
        try {
          if (lock.lockWithRetries()) {
            LOGGER.info("Acquired the handoff lock for table" +
                        s" ${ carbonTable.getDatabaseName }.${ carbonTable.getTableName }")
            // handoff streaming segment one by one
            do {
              val segmentStatusManager = new SegmentStatusManager(identifier)
              var loadMetadataDetails: Array[LoadMetadataDetails] = null
              val statusLock = segmentStatusManager.getTableStatusLock
              try {
                if (statusLock.lockWithRetries()) {
                  loadMetadataDetails = SegmentStatusManager.readLoadMetadata(
                    tablePath.getMetadataDirectoryPath)
                }
              } finally {
                if (null != statusLock) {
                  statusLock.unlock()
                }
              }
              if (null != loadMetadataDetails) {
                val streamSegments =
                  loadMetadataDetails.filter(_.getSegmentStatus == SegmentStatus.STREAMING_FINISH)

                continueHandoff = streamSegments.length > 0
                if (continueHandoff) {
                  // handoff a streaming segment
                  val loadMetadataDetail = streamSegments(0)
                  executeStreamingHandoff(
                    carbonLoadModel,
                    sqlContext,
                    storeLocation,
                    loadMetadataDetail.getLoadName
                  )
                }
              } else {
                continueHandoff = false
              }
            } while (continueHandoff)
          }
        } finally {
          if (null != lock) {
            lock.unlock()
          }
        }
      }
    }
    handoffThread.start()
  }

  /**
   * invoke StreamHandoffRDD to handoff streaming segment one bye one
   */
  def executeStreamingHandoff(
      carbonLoadModel: CarbonLoadModel,
      sqlContext: SQLContext,
      storeLocation: String,
      handoffSegmenId: String
  ): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var loadStatus = SegmentStatus.SUCCESS
    var errorMessage: String = "Handoff failure"
    try {
      // generate new columnar segment
      val newMetaEntry = new LoadMetadataDetails
      carbonLoadModel.setFactTimeStamp(System.currentTimeMillis())
      CarbonLoaderUtil.populateNewLoadMetaEntry(
        newMetaEntry,
        SegmentStatus.INSERT_IN_PROGRESS,
        carbonLoadModel.getFactTimeStamp,
        false)
      CarbonLoaderUtil.recordLoadMetadata(newMetaEntry, carbonLoadModel, true, false)
      // convert a streaming segment to columnar segment
      val status = new StreamHandoffRDD(
        sqlContext.sparkContext,
        new HandoffResultImpl(),
        carbonLoadModel,
        handoffSegmenId).collect()

      status.foreach { x =>
        if (!x._2) {
          loadStatus = SegmentStatus.LOAD_FAILURE
        }
      }
    } catch {
      case ex =>
        loadStatus = SegmentStatus.LOAD_FAILURE
        errorMessage = errorMessage + ": " + ex.getCause.getMessage
        LOGGER.error(errorMessage)
        LOGGER.error(ex, s"Handoff failed on streaming segment $handoffSegmenId")
    }

    if (loadStatus == SegmentStatus.LOAD_FAILURE) {
      CommonUtil.updateTableStatusForFailure(carbonLoadModel)
      LOGGER.info("********starting clean up**********")
      CarbonLoaderUtil.deleteSegment(carbonLoadModel, carbonLoadModel.getSegmentId.toInt)
      LOGGER.info("********clean up done**********")
      LOGGER.audit(s"Handoff is failed for " +
                   s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
      LOGGER.warn("Cannot write load metadata file as handoff failed")
      throw new Exception(errorMessage)
    }

    if (loadStatus == SegmentStatus.SUCCESS) {
      val done = updateLoadMetadata(handoffSegmenId, carbonLoadModel)
      if (!done) {
        val errorMessage = "Handoff failed due to failure in table status updation."
        LOGGER.audit("Handoff is failed for " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        LOGGER.error("Handoff failed due to failure in table status updation.")
        throw new Exception(errorMessage)
      }
      done
    }

  }

  /**
   * update streaming segment and new columnar segment
   */
  private def updateLoadMetadata(
      handoffSegmentId: String,
      loadModel: CarbonLoadModel
  ): Boolean = {
    var status = false
    val metaDataFilepath =
      loadModel.getCarbonDataLoadSchema().getCarbonTable().getMetaDataFilepath()
    val identifier =
      loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier()
    val carbonTablePath = CarbonStorePath.getCarbonTablePath(identifier)
    val metadataPath = carbonTablePath.getMetadataDirectoryPath()
    val fileType = FileFactory.getFileType(metadataPath)
    if (!FileFactory.isFileExist(metadataPath, fileType)) {
      FileFactory.mkdirs(metadataPath, fileType)
    }
    val tableStatusPath = carbonTablePath.getTableStatusFilePath()
    val segmentStatusManager = new SegmentStatusManager(identifier)
    val carbonLock = segmentStatusManager.getTableStatusLock()
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
          "Acquired lock for table" + loadModel.getDatabaseName() + "." + loadModel.getTableName()
          + " for table status updation")
        val listOfLoadFolderDetailsArray =
          SegmentStatusManager.readLoadMetadata(metaDataFilepath)

        // update new columnar segment to success status
        val newSegment =
          listOfLoadFolderDetailsArray.find(_.getLoadName.equals(loadModel.getSegmentId))
        if (newSegment.isEmpty) {
          throw new Exception("Failed to update table status for new segment")
        } else {
          newSegment.get.setSegmentStatus(SegmentStatus.SUCCESS)
          newSegment.get.setLoadEndTime(System.currentTimeMillis())
        }

        // update streaming segment to compacted status
        val streamSegment =
          listOfLoadFolderDetailsArray.find(_.getLoadName.equals(handoffSegmentId))
        if (streamSegment.isEmpty) {
          throw new Exception("Failed to update table status for streaming segment")
        } else {
          streamSegment.get.setSegmentStatus(SegmentStatus.COMPACTED)
        }

        // refresh table status file
        SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray)
        status = true
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + loadModel
          .getDatabaseName() + "." + loadModel.getTableName());
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" +
                    loadModel.getDatabaseName() + "." + loadModel.getTableName())
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + loadModel.getDatabaseName() +
                     "." + loadModel.getTableName() + " during table status updation")
      }
    }
    return status
  }
}
