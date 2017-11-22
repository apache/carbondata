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

package org.apache.carbondata.spark.rdd

import java.util.concurrent.ExecutorService

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.CompactionModel

import org.apache.carbondata.common.logging.{LogService, LogServiceFactory}
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil
import org.apache.carbondata.processing.util.CarbonLoaderUtil

abstract class Compactor(carbonLoadModel: CarbonLoadModel,
    compactionModel: CompactionModel,
    executor: ExecutorService,
    sqlContext: SQLContext,
    storeLocation: String) {

  val LOGGER: LogService = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def executeCompaction(): Unit

  def identifySegmentsToBeMerged(): java.util.List[LoadMetadataDetails] = {
    CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionModel.compactionSize,
        carbonLoadModel.getLoadMetadataDetails,
        compactionModel.compactionType)
  }

  def deletePartialLoadsInCompaction(): Unit = {
    // Deleting the any partially loaded data if present.
    // in some case the segment folder which is present in store will not have entry in
    // status.
    // so deleting those folders.
    try {
      CarbonLoaderUtil.deletePartialLoadDataIfExist(carbonLoadModel, true)
    } catch {
      case e: Exception =>
        LOGGER.error(s"Exception in compaction thread while clean up of stale segments" +
                     s" ${ e.getMessage }")
    }
  }

}
