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

import scala.collection.JavaConverters._

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.locks.{CarbonLockFactory, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.{CleanFilesPostEvent, Event, OperationContext, OperationEventListener}

class CleanFilesPostEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case cleanFilesPostEvent: CleanFilesPostEvent =>
        LOGGER.info("Clean files post event listener called")
        val carbonTable = cleanFilesPostEvent.carbonTable
        val indexTables = CarbonIndexUtil
          .getIndexCarbonTables(carbonTable, cleanFilesPostEvent.sparkSession)
        indexTables.foreach { indexTable =>
          val partitions: Option[Seq[PartitionSpec]] = CarbonFilters.getPartitions(
            Seq.empty[Expression],
            cleanFilesPostEvent.sparkSession,
            indexTable)
          SegmentStatusManager.deleteLoadsAndUpdateMetadata(
            indexTable, true, partitions.map(_.asJava).orNull)
          CarbonUpdateUtil.cleanUpDeltaFiles(indexTable, true)
          cleanUpUnwantedSegmentsOfSIAndUpdateMetadata(indexTable, carbonTable)
        }
    }
  }

  /**
   * This method added to clean the segments which are success in SI and may be compacted or marked
   * for delete in main table, which can happen in case of concurrent scenarios.
   */
  def cleanUpUnwantedSegmentsOfSIAndUpdateMetadata(indexTable: CarbonTable,
      mainTable: CarbonTable): Unit = {
    val mainTableStatusLock: ICarbonLock = CarbonLockFactory
      .getCarbonLockObj(mainTable.getAbsoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
    val indexTableStatusLock: ICarbonLock = CarbonLockFactory
      .getCarbonLockObj(indexTable.getAbsoluteTableIdentifier, LockUsage.TABLE_STATUS_LOCK)
    var mainTableLocked = false
    var indexTableLocked = false
    try {
      mainTableLocked = mainTableStatusLock.lockWithRetries()
      indexTableLocked = indexTableStatusLock.lockWithRetries()
      if (mainTableLocked && indexTableLocked) {
        val mainTableMetadataDetails =
          SegmentStatusManager.readLoadMetadata(mainTable.getMetadataPath).toSet ++
          SegmentStatusManager.readLoadHistoryMetadata(mainTable.getMetadataPath).toSet
        val indexTableMetadataDetails =
          SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath).toSet
        val segToStatusMap = mainTableMetadataDetails
          .map(detail => detail.getLoadName -> detail.getSegmentStatus).toMap

        val unnecessarySegmentsOfSI = indexTableMetadataDetails.filter { indexDetail =>
          indexDetail.getSegmentStatus.equals(SegmentStatus.SUCCESS) &&
          segToStatusMap.contains(indexDetail.getLoadName) &&
          (segToStatusMap(indexDetail.getLoadName).equals(SegmentStatus.COMPACTED) ||
           segToStatusMap(indexDetail.getLoadName).equals(SegmentStatus.MARKED_FOR_DELETE))
        }
        LOGGER.info(s"Unwanted SI segments are: $unnecessarySegmentsOfSI")
        unnecessarySegmentsOfSI.foreach { detail =>
          val carbonFile = FileFactory
            .getCarbonFile(CarbonTablePath
              .getSegmentPath(indexTable.getTablePath, detail.getLoadName))
          CarbonUtil.deleteFoldersAndFiles(carbonFile)
        }
        unnecessarySegmentsOfSI.foreach { detail =>
          detail.setSegmentStatus(segToStatusMap(detail.getLoadName))
          detail.setVisibility("false")
        }

        SegmentStatusManager.writeLoadDetailsIntoFile(
          indexTable.getMetadataPath + CarbonTablePath.TABLE_STATUS_FILE,
          unnecessarySegmentsOfSI.toArray)
      } else {
        LOGGER.error("Unable to get the lock file for main/Index table. Please try again later")
      }
    } catch {
      case ex: Exception =>
        LOGGER.error("clean up of unwanted SI segments failed", ex)
      // ignore the exception
    } finally {
      indexTableStatusLock.unlock()
      mainTableStatusLock.unlock()
    }
  }
}
