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

package org.apache.spark.sql.events

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datamap.{DataMapStoreManager, TableDataMap}
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.datamap.CarbonMergeBloomIndexFilesRDD
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil

class MergeBloomIndexEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    val sparkSession = SparkSession.getActiveSession.get
    event match {
      case loadPreStatusUpdateEvent: LoadTablePreStatusUpdateEvent =>
        LOGGER.info("LoadTablePreStatusUpdateEvent called for bloom index merging")
        // For loading process, segment can not be accessed at this time
        val loadModel = loadPreStatusUpdateEvent.getCarbonLoadModel
        val carbonTable = loadModel.getCarbonDataLoadSchema.getCarbonTable
        val segmentId = loadModel.getSegmentId

        // filter out bloom datamap, skip lazy datamap
        val bloomDatamaps = DataMapStoreManager.getInstance()
          .getAllDataMap(carbonTable, DataMapClassProvider.BLOOMFILTER, true).asScala.toList

        mergeBloomIndex(sparkSession, carbonTable, bloomDatamaps, Seq(segmentId))

      case compactPreStatusUpdateEvent: AlterTableCompactionPreStatusUpdateEvent =>
        LOGGER.info("AlterTableCompactionPreStatusUpdateEvent called for bloom index merging")
        // For compact process, segment can not be accessed at this time
        val carbonTable = compactPreStatusUpdateEvent.carbonTable
        val mergedLoadName = compactPreStatusUpdateEvent.mergedLoadName
        val segmentId = CarbonDataMergerUtil.getLoadNumberFromLoadName(mergedLoadName)

        // filter out bloom datamap, skip lazy datamap
        val bloomDatamaps = DataMapStoreManager.getInstance()
          .getAllDataMap(carbonTable, DataMapClassProvider.BLOOMFILTER, true).asScala.toList

        mergeBloomIndex(sparkSession, carbonTable, bloomDatamaps, Seq(segmentId))

      case datamapPostEvent: BuildDataMapPostExecutionEvent =>
        LOGGER.info("BuildDataMapPostExecutionEvent called for bloom index merging")
        // For rebuild datamap process, datamap is disabled when rebuilding
        if (!datamapPostEvent.isFromRebuild || null == datamapPostEvent.dmName) {
          // ignore datamapPostEvent from loading and compaction for bloom index merging
          // they use LoadTablePreStatusUpdateEvent and AlterTableCompactionPreStatusUpdateEvent
          LOGGER.info("Ignore BuildDataMapPostExecutionEvent from loading and compaction")
          return
        }

        val carbonTableIdentifier = datamapPostEvent.identifier
        val carbonTable = DataMapStoreManager.getInstance().getCarbonTable(carbonTableIdentifier)

        // filter out current rebuilt bloom datamap
        val bloomDatamaps = DataMapStoreManager.getInstance()
          .getAllDataMap(carbonTable, DataMapClassProvider.BLOOMFILTER, false).asScala
          .filter(_.getDataMapSchema.getDataMapName.equalsIgnoreCase(datamapPostEvent.dmName))
          .toList

        val segmentIds = datamapPostEvent.segmentIdList
        mergeBloomIndex(sparkSession, carbonTable, bloomDatamaps, segmentIds)
    }
  }


  private def mergeBloomIndex(sparkSession: SparkSession, carbonTable: CarbonTable,
      bloomDatamaps: List[TableDataMap], segmentIds: Seq[String]) = {
    if (bloomDatamaps.nonEmpty && segmentIds.nonEmpty) {
      // we extract bloom datamap name and index columns here
      // because TableDataMap is not serializable
      val bloomDMnames = ListBuffer.empty[String]
      val bloomIndexColumns = ListBuffer.empty[Seq[String]]
      bloomDatamaps.foreach(dm => {
        bloomDMnames += dm.getDataMapSchema.getDataMapName
        bloomIndexColumns += dm.getDataMapSchema.getIndexColumns.map(_.trim.toLowerCase)
      })
      LOGGER.info(
        String.format("Start to merge bloom index file for table %s. Datamaps=%s, SegmentIds=%s",
          carbonTable.getTableName, bloomDMnames.mkString("|"), segmentIds.mkString("|") ))
      new CarbonMergeBloomIndexFilesRDD(sparkSession, carbonTable,
        segmentIds, bloomDMnames, bloomIndexColumns).collect()
      LOGGER.info("Finish merging bloom index file for table " + carbonTable.getTableName)
    }
  }
}
