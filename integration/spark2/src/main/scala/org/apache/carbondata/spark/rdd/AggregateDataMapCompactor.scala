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

import org.apache.spark.sql.{CarbonUtils, SQLContext}
import org.apache.spark.sql.execution.command.CompactionModel
import org.apache.spark.sql.execution.command.management.CarbonLoadDataCommand
import org.apache.spark.sql.execution.command.preaaggregate.{CommitPreAggregateListener, PreAggregateUtil}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
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
    // If segmentFile name is specified in load details then segment is for partition table
    // therefore the segment file name should be loadName#segmentFileName.segment
    val segments = loadMetaDataDetails.asScala.map {
      loadDetail =>
        new Segment(loadDetail.getLoadName, loadDetail.getSegmentFile, null).toString
    }

    if (segments.nonEmpty) {
      val mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadMetaDataDetails).split("_")(1)
      CarbonUtils.threadSet(
        CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
        carbonLoadModel.getDatabaseName + "." +
        carbonLoadModel.getTableName,
        segments.mkString(","))
      CarbonUtils.threadSet(
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
        CarbonUtils.threadSet(CarbonCommonConstants.SUPPORT_DIRECT_QUERY_ON_DATAMAP,
          "true")
        loadCommand.processData(sqlContext.sparkSession)
        // After load is completed for child table the UUID table status will have 0.1 as success
        // and the table status file will have 0,1,2,3 as Success and 0.1 as In Progress.
        // Therefore we will read the table status and write 0,1,2,3 as Compacted as the commit
        // listener will take care of merging the UUID and the table status files.
        val newMetadataDetails = SegmentStatusManager.readLoadMetadata(
          carbonTable.getMetadataPath, uuid)
        val mergedContent = loadMetaDataDetails.asScala.map {
          segment => segment.setSegmentStatus(SegmentStatus.COMPACTED)
            segment.setMergedLoadName(mergedLoadName)
            segment.setModificationOrdeletionTimesStamp(CarbonUpdateUtil.readCurrentTime)
            segment
        } ++ newMetadataDetails
        SegmentStatusManager.writeLoadDetailsIntoFile(
          CarbonTablePath.getTableStatusFilePathWithUUID(carbonTable.getTablePath, uuid),
          mergedContent.toArray)
        carbonLoadModel.setLoadMetadataDetails((carbonLoadModel.getLoadMetadataDetails.asScala ++
        newMetadataDetails).asJava)
        // If isCompaction is true then it means that the compaction on aggregate table was
        // triggered by the maintable thus no need to commit the tablestatus file but if the
        // compaction was triggered directly for aggregate table then commit has to be fired as
        // the commit listener would not be called.
        val directAggregateCompactionCall = Option(operationContext
          .getProperty("isCompaction")).getOrElse("false").toString.toBoolean
        if (!directAggregateCompactionCall) {
          commitAggregateTableStatus(carbonTable, uuid)
        }
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
            commitAggregateTableStatus(carbonTable, uuid)
            executeCompaction()
          }
        }
        CarbonUtils
          .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                       carbonLoadModel.getDatabaseName + "." +
                       carbonLoadModel.getTableName)
        CarbonUtils.threadUnset(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                                  carbonLoadModel.getDatabaseName + "." +
                                  carbonLoadModel.getTableName)
        LOGGER
          .info(s"Compaction request for datamap ${ carbonTable.getTableUniqueName } is successful")
      }
    }
  }

  /**
   * Used to merge the contents of tablestatus and tablestatus_uuid files and write the new
   * details to tablestatus file. For Example:-
   * tablestatus contents are = 0(Success), 1(Success),2(Success),3(Success), 0.1(In Progress)
   * tablestatus_uuid contents are = 0(Compacted), 1(Compacted),2(Compacted),3(Compacted), 0.1
   * (Success).
   *
   * So after merging the tablestatus file will have: 0(Compacted), 1(Compacted),2(Compacted),
   * 3(Compacted), 0.1(Success).
   *
   * NOTE: This method will be called when direct compaction is fired on child aggregate table or
   * when there are anymore segments to be compacted and the intermediate state of the
   * tablestatus has to be committed for further compaction to pick other segments.
   */
  private def commitAggregateTableStatus(carbonTable: CarbonTable, uuid: String) {
    if (!CommitPreAggregateListener.mergeTableStatusContents(carbonTable, CarbonTablePath
      .getTableStatusFilePathWithUUID(carbonTable.getTablePath, uuid), CarbonTablePath
      .getTableStatusFilePathWithUUID(carbonTable.getTablePath, ""))) {
      throw new RuntimeException("Unable to acquire lock for table status updation")
    }
  }

}
