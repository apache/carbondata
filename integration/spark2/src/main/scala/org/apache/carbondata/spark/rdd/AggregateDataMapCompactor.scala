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
import java.util.concurrent.ExecutorService

import scala.collection.JavaConverters._

import org.apache.spark.sql.{CarbonSession, SQLContext}
import org.apache.spark.sql.execution.command.CompactionModel
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.statusmanager.{SegmentDetailVO, SegmentManager, SegmentStatus, SegmentStatusManager}
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
    // If segmentFile name is specified in load details then segment is for partition table
    // therefore the segment file name should be loadName#segmentFileName.segment
    val segments = loadMetaDataDetails.asScala.map {
      loadDetail =>
        new Segment(loadDetail.getSegmentId, loadDetail.getSegmentFileName, null).toString
    }

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
      val loadCommand = operationContext.getProperty(carbonTable.getTableName + "_Compaction")
        .asInstanceOf[CarbonLoadDataCommand]
      val uuid = Option(loadCommand.operationContext.getProperty("uuid")).getOrElse("").toString
      try {
        val newInternalOptions = loadCommand.internalOptions ++
                                 Map("mergedSegmentName" -> mergedLoadName)
        loadCommand.internalOptions = newInternalOptions
        loadCommand.dataFrame =
                  Some(PreAggregateUtil.getDataFrame(
                    sqlContext.sparkSession, loadCommand.logicalPlan.get))
        CarbonSession.threadSet(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP,
          "true")
        loadCommand.processData(sqlContext.sparkSession)
        val updatedSegs = loadMetaDataDetails.asScala.map{seg =>
          new SegmentDetailVO().
            setSegmentId(seg.getSegmentId).
            setStatus(SegmentStatus.COMPACTED.toString).
            setModificationOrDeletionTimestamp(System.currentTimeMillis())
        }.toList.asJava
        new SegmentManager().updateSegments(carbonTable.getAbsoluteTableIdentifier,
          new util.ArrayList[SegmentDetailVO](updatedSegs))
      } finally {
        // check if any other segments needs compaction on in case of MINOR_COMPACTION.
        // For example: after 8.1 creation 0.1, 4.1, 8.1 have to be merged to 0.2 if threshhold
        // allows it.
        // Also as the load which will be fired for 2nd level compaction will read the
        // tablestatus file and not the tablestatus_UUID therefore we have to commit the
        // intermediate tablestatus file for 2nd level compaction to be successful.
        // This is required because:
        //  1. after doing 12 loads and a compaction after every 4 loads the table status file will
        //     have 0.1, 4.1, 8, 9, 10, 11 as Success segments. While tablestatus_UUID will have
        //     0.1, 4.1, 8.1.
        //  2. Now for 2nd level compaction 0.1, 8.1, 4.1 have to be merged to 0.2. therefore we
        //     need to read the tablestatus_UUID. But load flow should always read tablestatus file
        //     because it contains the actual In-Process status for the segments.
        //  3. If we read the tablestatus then 8, 9, 10, 11 will keep getting compacted into 8.1.
        //  4. Therefore tablestatus file will be committed in between multiple commits.
        if (!compactionModel.compactionType.equals(CompactionType.MAJOR) &&
          !compactionModel.compactionType.equals(CompactionType.CUSTOM)) {
          if (!identifySegmentsToBeMerged().isEmpty) {
            val uuidTableStaus = CarbonTablePath.getTableStatusFilePathWithUUID(
              carbonTable.getTablePath, uuid)
            val tableStatus = CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath)
            if (!uuidTableStaus.equalsIgnoreCase(tableStatus)) {
              FileFactory.getCarbonFile(uuidTableStaus).renameForce(tableStatus)
            }
            executeCompaction()
          }
        }
        CarbonSession
          .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                       carbonLoadModel.getDatabaseName + "." +
                       carbonLoadModel.getTableName)
        CarbonSession.threadUnset(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                                  carbonLoadModel.getDatabaseName + "." +
                                  carbonLoadModel.getTableName)
        LOGGER
          .info(s"Compaction request for datamap ${ carbonTable.getTableUniqueName } is successful")
        LOGGER
          .audit(s"Compaction request for datamap ${carbonTable.getTableUniqueName} is successful")
      }
    }
  }
}
