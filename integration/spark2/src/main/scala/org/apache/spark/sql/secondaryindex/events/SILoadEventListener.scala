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
import org.apache.spark.sql.secondaryindex.command.SecondaryIndex
import org.apache.spark.sql.secondaryindex.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata
import org.apache.carbondata.events._
import org.apache.carbondata.processing.loading.events.LoadEvents.LoadTablePreStatusUpdateEvent

class SILoadEventListener extends OperationEventListener with Logging {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case _: LoadTablePreStatusUpdateEvent =>
        LOGGER.info("Load pre status update event-listener called")
        val loadTablePreStatusUpdateEvent = event.asInstanceOf[LoadTablePreStatusUpdateEvent]
        val carbonLoadModel = loadTablePreStatusUpdateEvent.getCarbonLoadModel
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
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach {
              indexTableName =>
                val secondaryIndex = SecondaryIndex(Some(carbonTable.getDatabaseName),
                  indexMetadata.getParentTableName,
                  indexMetadata.getIndexesMap.get(indexTableName).asScala.toList,
                  indexTableName)

                val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
                val indexTable = metaStore
                  .lookupRelation(Some(carbonLoadModel.getDatabaseName),
                    indexTableName)(sparkSession).asInstanceOf[CarbonRelation].carbonTable

                CarbonInternalScalaUtil
                  .LoadToSITable(sparkSession,
                    carbonLoadModel,
                    indexTableName,
                    isLoadToFailedSISegments = false,
                    secondaryIndex,
                    carbonTable, indexTable)
            }
          } else {
            logInfo(s"No index tables found for table: ${carbonTable.getTableName}")
          }
        } else {
          logInfo(s"Index information is null for table: ${carbonTable.getTableName}")
        }
    }
  }
}
