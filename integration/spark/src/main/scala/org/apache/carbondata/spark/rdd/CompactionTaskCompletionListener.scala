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

import java.util

import org.apache.log4j.Logger
import org.apache.spark.TaskContext
import org.apache.spark.sql.carbondata.execution.datasources.tasklisteners.CarbonCompactionTaskCompletionListener
import org.apache.spark.sql.execution.command.management.CommonLoadUtils
import org.apache.spark.util.CollectionAccumulator

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{AbstractResultProcessor, CarbonCompactionExecutor, CarbonCompactionUtil}

class CompactionTaskCompletionListener(
    carbonLoadModel: CarbonLoadModel,
    exec: CarbonCompactionExecutor,
    processor: AbstractResultProcessor,
    rawResultIteratorMap: util.Map[String, util.List[RawResultIterator]],
    segmentMetaDataAccumulator: CollectionAccumulator[Map[String, SegmentMetaDataInfo]],
    queryStartTime: Long)
  extends CarbonCompactionTaskCompletionListener {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getName)

  override def onTaskCompletion(context: TaskContext): Unit = {
    deleteLocalDataFolders()
    // close all the query executor service and clean up memory acquired during query processing
    if (null != exec) {
      LOGGER.info("Cleaning up query resources acquired during compaction")
      exec.close(rawResultIteratorMap.get(CarbonCompactionUtil.UNSORTED_IDX), queryStartTime)
      exec.close(rawResultIteratorMap.get(CarbonCompactionUtil.SORTED_IDX), queryStartTime)
    }
    // clean up the resources for processor
    if (null != processor) {
      LOGGER.info("Closing compaction processor instance to clean up loading resources")
      processor.close()
    }
    // fill segment metadata to accumulator
    CommonLoadUtils.fillSegmentMetaDataInfoToAccumulator(carbonLoadModel.getTableName,
      carbonLoadModel.getSegmentId,
      segmentMetaDataAccumulator)
  }

  private def deleteLocalDataFolders(): Unit = {
    try {
      LOGGER.info("Deleting local folder store location")
      val isCompactionFlow = true
      TableProcessingOperations
        .deleteLocalDataLoadFolderLocation(carbonLoadModel, isCompactionFlow, false)
    } catch {
      case e: Exception =>
        LOGGER.error(e)
    }
  }

}
