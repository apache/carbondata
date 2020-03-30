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

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.secondaryindex.events.{LoadTableSIPostExecutionEvent, LoadTableSIPreExecutionEvent}
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil
import org.apache.spark.sql.secondaryindex.util.SecondaryIndexUtil
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.apache.carbondata.core.metadata.{CarbonTableIdentifier, ColumnarFormatVersion}
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.events.{OperationContext, OperationListenerBus}

case class SIRebuildSegmentRunner(
    parentTable: CarbonTable,
    indexTable: CarbonTable,
    segments: Option[List[String]]) {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  def run(sparkSession: SparkSession): Unit = {
    processMetadata(sparkSession)
    processData(sparkSession)
  }

  private def processMetadata(sparkSession: SparkSession): Unit = {
    if (!indexTable.getTableInfo.isTransactionalTable) {
      throw new MalformedCarbonCommandException("Unsupported operation on non transactional table")
    }
    if (!indexTable.isIndexTable) {
      throw new UnsupportedOperationException("Unsupported operation on carbon table")
    }

    val version = CarbonUtil.getFormatVersion(indexTable)
    val isOlderVersion = version == ColumnarFormatVersion.V1 ||
                         version == ColumnarFormatVersion.V2
    if (isOlderVersion) {
      throw new MalformedCarbonCommandException(
        "Unsupported rebuild operation on carbon table: Merge data files is not supported on V1 " +
        "V2 store segments")
    }
    // check if the list of given segments in the command are valid
    val segmentIds = segments.getOrElse(List.empty)
    if (segmentIds.nonEmpty) {
      val segmentStatusManager = new SegmentStatusManager(indexTable.getAbsoluteTableIdentifier)
      val validSegments = segmentStatusManager.getValidAndInvalidSegments.getValidSegments.asScala
        .map(_.getSegmentNo)
      segmentIds.foreach { segmentId =>
        if (!validSegments.contains(segmentId)) {
          throw new RuntimeException(s"Refresh index by segment id is failed. " +
                                     s"Invalid ID: $segmentId")
        }
      }
    }
  }

  private def processData(sparkSession: SparkSession): Unit = {
    LOGGER.info( s"SI segment compaction request received for table " +
                 s"${ indexTable.getDatabaseName}.${indexTable.getTableName}")
    val lock = CarbonLockFactory.getCarbonLockObj(
      parentTable.getAbsoluteTableIdentifier,
      LockUsage.COMPACTION_LOCK)

    val segmentList = segments.orNull
    val segmentFileNameMap = new util.HashMap[String, String]()
    var segmentIdToLoadStartTimeMapping = mutable.Map[String, java.lang.Long]()

    try {
      if (lock.lockWithRetries()) {
        LOGGER.info("Acquired the compaction lock for table" +
                    s" ${parentTable.getDatabaseName}.${parentTable.getTableName}")

        val operationContext = new OperationContext
        val loadTableSIPreExecutionEvent: LoadTableSIPreExecutionEvent =
          LoadTableSIPreExecutionEvent(sparkSession,
            new CarbonTableIdentifier(indexTable.getDatabaseName, indexTable.getTableName, ""),
            null,
            indexTable)
        OperationListenerBus.getInstance.fireEvent(loadTableSIPreExecutionEvent, operationContext)

        SegmentStatusManager.readLoadMetadata(parentTable.getMetadataPath) collect {
          case loadDetails if null == segmentList ||
                              segmentList.contains(loadDetails.getLoadName) =>
            segmentFileNameMap.put(
              loadDetails.getLoadName, String.valueOf(loadDetails.getLoadStartTime))
        }

        val loadMetadataDetails = SegmentStatusManager
          .readLoadMetadata(indexTable.getMetadataPath)
          .filter(loadMetadataDetail =>
            (null == segmentList || segmentList.contains(loadMetadataDetail.getLoadName)) &&
            (loadMetadataDetail.getSegmentStatus == SegmentStatus.SUCCESS ||
             loadMetadataDetail.getSegmentStatus == SegmentStatus.LOAD_PARTIAL_SUCCESS))

        segmentIdToLoadStartTimeMapping = CarbonInternalLoaderUtil
          .getSegmentToLoadStartTimeMapping(loadMetadataDetails)
          .asScala

        val carbonLoadModelForMergeDataFiles =
          SecondaryIndexUtil.getCarbonLoadModel(
            indexTable,
            loadMetadataDetails.toList.asJava,
            System.currentTimeMillis(),
            CarbonIndexUtil.getCompressorForIndexTable(indexTable, parentTable))

        SecondaryIndexUtil.mergeDataFilesSISegments(segmentIdToLoadStartTimeMapping, indexTable,
          loadMetadataDetails.toList.asJava, carbonLoadModelForMergeDataFiles,
          isRebuildCommand = true)(sparkSession.sqlContext)

        val loadTableSIPostExecutionEvent: LoadTableSIPostExecutionEvent =
          LoadTableSIPostExecutionEvent(sparkSession,
            indexTable.getCarbonTableIdentifier,
            null,
            indexTable)
        OperationListenerBus.getInstance
          .fireEvent(loadTableSIPostExecutionEvent, operationContext)

        LOGGER.info(s"SI segment compaction request completed for table " +
                    s"${indexTable.getDatabaseName}.${indexTable.getTableName}")
      } else {
        LOGGER.error(s"Not able to acquire the compaction lock for table" +
                     s" ${indexTable.getDatabaseName}.${indexTable.getTableName}")
        CarbonException.analysisException(
          "Table is already locked for compaction. Please try after some time.")
      }
    } catch {
      case ex: Exception =>
        LOGGER.error(s"SI segment compaction request failed for table " +
                     s"${indexTable.getDatabaseName}.${indexTable.getTableName}")
      case ex: NoSuchTableException =>
        throw ex
    } finally {
      lock.unlock()
    }
  }

}
