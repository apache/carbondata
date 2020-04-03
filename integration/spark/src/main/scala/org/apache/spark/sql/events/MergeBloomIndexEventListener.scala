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
import org.apache.carbondata.core.index.IndexStoreManager
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider
import org.apache.carbondata.datamap.CarbonMergeBloomIndexFilesRDD
import org.apache.carbondata.events._

class MergeBloomIndexEventListener extends OperationEventListener with Logging {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case datamapPostEvent: BuildIndexPostExecutionEvent =>
        LOGGER.info("Load post status event-listener called for merge bloom index")
        val carbonTableIdentifier = datamapPostEvent.identifier
        val carbonTable = IndexStoreManager.getInstance().getCarbonTable(carbonTableIdentifier)
        val tableDataMaps = IndexStoreManager.getInstance().getAllIndexes(carbonTable)
        val sparkSession = SparkSession.getActiveSession.get

        // filter out bloom indexSchema
        var bloomDatamaps = tableDataMaps.asScala.filter(
          _.getIndexSchema.getProviderName.equalsIgnoreCase(
            CarbonIndexProvider.BLOOMFILTER.getIndexProviderName))

        if (datamapPostEvent.isFromRebuild) {
          if (null != datamapPostEvent.indexName) {
            // for rebuild process
            bloomDatamaps = bloomDatamaps.filter(
              _.getIndexSchema.getIndexName.equalsIgnoreCase(datamapPostEvent.indexName))
          }
        } else {
          // for load process, skip lazy indexSchema
          bloomDatamaps = bloomDatamaps.filter(!_.getIndexSchema.isLazy)
        }

        val segmentIds = datamapPostEvent.segmentIdList
        if (bloomDatamaps.size > 0 && segmentIds.size > 0) {
          // we extract bloom indexSchema name and index columns here
          // because TableIndex is not serializable
          val bloomDMnames = ListBuffer.empty[String]
          val bloomIndexColumns = ListBuffer.empty[Seq[String]]
          bloomDatamaps.foreach( dm => {
            bloomDMnames += dm.getIndexSchema.getIndexName
            bloomIndexColumns += dm.getIndexSchema.getIndexColumns.map(_.trim.toLowerCase)
          })
          new CarbonMergeBloomIndexFilesRDD(sparkSession, carbonTable,
            segmentIds, bloomDMnames, bloomIndexColumns).collect()
        }
    }
  }

}
