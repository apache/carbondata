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
import scala.collection.mutable

import org.apache.log4j.Logger
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.CarbonEnv
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.command.SecondaryIndex
import org.apache.spark.sql.secondaryindex.load.Compactor
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.events.{AlterTableCompactionPreStatusUpdateEvent, Event, OperationContext, OperationEventListener}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}

class AlterTableCompactionPostEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case alterTableCompactionPostEvent: AlterTableCompactionPreStatusUpdateEvent =>
        LOGGER.info("post load event-listener called")
        val carbonLoadModel = alterTableCompactionPostEvent.carbonLoadModel
        val sQLContext = alterTableCompactionPostEvent.sparkSession.sqlContext
        val compactionType: CompactionType = alterTableCompactionPostEvent.carbonMergerMapping
          .campactionType
        if (compactionType.toString
          .equalsIgnoreCase(CompactionType.SEGMENT_INDEX.toString)) {
          val carbonMainTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
          val indexTablesList = CarbonInternalScalaUtil.getIndexesMap(carbonMainTable).asScala
          val loadFolderDetailsArray = SegmentStatusManager
            .readLoadMetadata(carbonMainTable.getMetadataPath)
          val segmentFileNameMap: java.util.Map[String, String] = new util.HashMap[String, String]()
          loadFolderDetailsArray.foreach(loadMetadataDetails => {
            segmentFileNameMap
              .put(loadMetadataDetails.getLoadName,
                String.valueOf(loadMetadataDetails.getLoadStartTime))
          })
          if (null != indexTablesList && indexTablesList.nonEmpty) {
            indexTablesList.foreach { indexTableAndColumns =>
              val secondaryIndex = SecondaryIndex(Some(carbonLoadModel.getDatabaseName),
                carbonLoadModel.getTableName,
                indexTableAndColumns._2.asScala.toList,
                indexTableAndColumns._1)
              val metastore = CarbonEnv.getInstance(sQLContext.sparkSession)
                .carbonMetaStore
              val indexCarbonTable = metastore
                .lookupRelation(Some(carbonLoadModel.getDatabaseName),
                  secondaryIndex.indexTableName)(sQLContext
                  .sparkSession).asInstanceOf[CarbonRelation].carbonTable

              val validSegments: mutable.Buffer[Segment] = CarbonDataMergerUtil.getValidSegmentList(
                carbonMainTable).asScala
              val validSegmentIds: mutable.Buffer[String] = mutable.Buffer[String]()
              validSegments.foreach { segment =>
                validSegmentIds += segment.getSegmentNo
              }
              // Just launch job to merge index for all index tables
              CarbonMergeFilesRDD.mergeIndexFiles(
                sQLContext.sparkSession,
                validSegmentIds,
                segmentFileNameMap,
                indexCarbonTable.getTablePath,
                indexCarbonTable,
                mergeIndexProperty = true)
            }
          }
        } else {
          val mergedLoadName = alterTableCompactionPostEvent.mergedLoadName
          val loadMetadataDetails = new LoadMetadataDetails
          loadMetadataDetails.setLoadName(mergedLoadName)
          val validSegments: Array[Segment] = alterTableCompactionPostEvent.carbonMergerMapping
            .validSegments
          val loadsToMerge: mutable.Buffer[String] = mutable.Buffer[String]()
          validSegments.foreach { segment =>
            loadsToMerge += segment.getSegmentNo
          }
          val loadName = mergedLoadName
            .substring(mergedLoadName.indexOf(CarbonCommonConstants.LOAD_FOLDER) +
                       CarbonCommonConstants.LOAD_FOLDER.length)
          val mergeLoadStartTime = CarbonUpdateUtil.readCurrentTime()

          val segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang
          .Long] = scala.collection.mutable.Map((loadName, mergeLoadStartTime))
          Compactor.createSecondaryIndexAfterCompaction(sQLContext,
            carbonLoadModel,
            List(loadName),
            loadsToMerge.toArray,
            segmentIdToLoadStartTimeMapping, forceAccessSegment = true)
        }
      case _ =>
    }
  }
}
