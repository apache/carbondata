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
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.index.CarbonIndexUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.util.CarbonProperties
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
          if (CarbonProperties.getInstance().isSIRepairEnabled(carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName)) {
          // when Si creation and load to main table are parallel, get the carbonTable from the
          // metastore which will have the latest index Info
          val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
          val carbonTable = metaStore
            .lookupRelation(Some(carbonLoadModel.getDatabaseName),
              carbonLoadModel.getTableName)(sparkSession)
            .asInstanceOf[CarbonRelation].carbonTable
          val indexMetadata = carbonTable.getIndexMetadata
          val secondaryIndexProvider = IndexType.SI.getIndexProviderName
          if (null != indexMetadata && null != indexMetadata.getIndexesMap &&
            null != indexMetadata.getIndexesMap.get(secondaryIndexProvider)) {
            val indexTables = indexMetadata.getIndexesMap
              .get(secondaryIndexProvider).keySet().asScala
            val maxSegmentRepairLimit = CarbonProperties.getInstance().getMaxSIRepairLimit(
              carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
            LOGGER.info("Number of segments to be repaired for table: " +
              carbonLoadModel.getTableName + " are : " + maxSegmentRepairLimit)
            // if there are no index tables for a given fact table do not perform any action
            if (indexTables.nonEmpty) {
              indexTables.foreach {
                indexTableName =>
                  CarbonIndexUtil.processSIRepair(indexTableName, carbonTable, carbonLoadModel,
                    indexMetadata, secondaryIndexProvider,
                    maxSegmentRepairLimit, isLoadOrCompaction = true)(sparkSession)
              }
            }
          }
        }
    }
  }
}
