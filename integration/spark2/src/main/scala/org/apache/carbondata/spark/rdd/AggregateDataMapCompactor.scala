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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonSession, SQLContext}
import org.apache.spark.sql.execution.command.CompactionModel
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.statusmanager.{SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.events.OperationContext
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}

/**
 * Used to perform compaction on Aggregate data map.
 */
class AggregateDataMapCompactor(carbonLoadModel: CarbonLoadModel,
    compactionModel: CompactionModel,
    executor: ExecutorService,
    sqlContext: SQLContext,
    storeLocation: String,
    operationContext: OperationContext)
  extends Compactor(carbonLoadModel, compactionModel, executor, sqlContext, storeLocation) {

  override def executeCompaction(): Unit = {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val loadMetaDataDetails = identifySegmentsToBeMerged()
    val segments = loadMetaDataDetails.asScala.map(_.getLoadName)
    if (segments.nonEmpty) {
      val mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadMetaDataDetails).split("_")(1)
      CarbonSession.threadSet(
        CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
        carbonLoadModel.getDatabaseName + "." +
        carbonLoadModel.getTableName,
        segments.mkString(","))
      CarbonSession.threadSet(
        CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
        carbonLoadModel.getDatabaseName + "." +
        carbonLoadModel.getTableName, "false")
      CarbonSession.updateSessionInfoToCurrentThread(sqlContext.sparkSession)
      val loadCommand = operationContext.getProperty(carbonTable.getTableName + "_Compaction")
        .asInstanceOf[CarbonLoadDataCommand]
      try {
        val newInternalOptions = loadCommand.internalOptions ++
                                 Map("mergedSegmentName" -> mergedLoadName)
        loadCommand.internalOptions = newInternalOptions
        loadCommand.dataFrame =
                  Some(PreAggregateUtil.getDataFrame(
                    sqlContext.sparkSession, loadCommand.logicalPlan.get))
        loadCommand.processData(sqlContext.sparkSession)
        val newLoadMetaDataDetails = SegmentStatusManager.readLoadMetadata(
          carbonTable.getMetadataPath)
        val updatedLoadMetaDataDetails = newLoadMetaDataDetails collect {
          case load if loadMetaDataDetails.contains(load) =>
            load.setMergedLoadName(mergedLoadName)
            load.setSegmentStatus(SegmentStatus.COMPACTED)
            load.setModificationOrdeletionTimesStamp(System.currentTimeMillis())
            load
          case other => other
        }
        SegmentStatusManager.writeLoadDetailsIntoFile(
          CarbonTablePath.getTableStatusFilePath(carbonLoadModel.getTablePath),
          updatedLoadMetaDataDetails)
        carbonLoadModel.setLoadMetadataDetails(updatedLoadMetaDataDetails.toList.asJava)
      } finally {
        // check if any other segments needs compaction on in case of MINOR_COMPACTION.
        // For example: after 8.1 creation 0.1, 4.1, 8.1 have to be merged to 0.2 if threshhold
        // allows it.
        if (!compactionModel.compactionType.equals(CompactionType.MAJOR)) {

          executeCompaction()
        }
        CarbonSession
          .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                       carbonLoadModel.getDatabaseName + "." +
                       carbonLoadModel.getTableName)
        CarbonSession.threadUnset(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                                  carbonLoadModel.getDatabaseName + "." +
                                  carbonLoadModel.getTableName)
      }
    }
  }
}
