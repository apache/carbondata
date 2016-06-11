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

package org.carbondata.spark.rdd

import scala.collection.JavaConverters._

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, Partitioner}

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.load.LoadMetadataDetails
import org.carbondata.core.util.CarbonProperties
import org.carbondata.lcm.status.SegmentStatusManager
import org.carbondata.spark.load.{CarbonLoaderUtil, CarbonLoadModel}
import org.carbondata.spark.MergeResultImpl
import org.carbondata.spark.merger.CarbonDataMergerUtil

/**
 * Compactor class which handled the compaction cases.
 */
object Compactor {

  val logger = LogServiceFactory.getLogService(Compactor.getClass.getName)

  def triggerCompaction(hdfsStoreLocation: String,
    carbonLoadModel: CarbonLoadModel,
    partitioner: Partitioner,
    storeLocation: String,
    carbonTable: CarbonTable,
    kettleHomePath: String,
    cubeCreationTime: Long,
    loadsToMerge: java.util.List[LoadMetadataDetails],
    sc: SQLContext): Unit = {

    logger.info("loads about to merge are  " + loadsToMerge)
    val startTime = System.nanoTime();
    val mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
    var finalMergeStatus = true
    val schemaName: String = carbonLoadModel.getDatabaseName
    val factTableName = carbonLoadModel.getTableName
    val storePath = hdfsStoreLocation
    val validSegments: Array[String] = CarbonDataMergerUtil
      .getValidSegments(loadsToMerge).split(',')
    val mergeLoadStartTime = CarbonLoaderUtil.readCurrentTime();
    val carbonMergerMapping = CarbonMergerMapping(storeLocation,
      hdfsStoreLocation,
      partitioner,
      carbonTable.getMetaDataFilepath(),
      mergedLoadName,
      kettleHomePath,
      cubeCreationTime,
      schemaName,
      factTableName,
      validSegments,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId
    )
    carbonLoadModel.setStorePath(carbonMergerMapping.hdfsStoreLocation)
    val segmentStatusManager = new SegmentStatusManager(new AbsoluteTableIdentifier
    (CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
      new CarbonTableIdentifier(carbonLoadModel.getDatabaseName,
        carbonLoadModel.getTableName,
        carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId
      )
    )
    )
    carbonLoadModel.setLoadMetadataDetails(segmentStatusManager
      .readLoadMetadata(carbonTable.getMetaDataFilepath()).toList.asJava
    )

    val mergeStatus = new CarbonMergerRDD(
      sc.sparkContext,
      new MergeResultImpl(),
      carbonLoadModel,
      carbonMergerMapping
    ).collect

    mergeStatus.foreach { eachMergeStatus =>
      val state: Boolean = eachMergeStatus._2
      if (!state) {
        finalMergeStatus = false
      }
    }
    if (finalMergeStatus) {
      val endTime = System.nanoTime();
      logger.info("time taken to merge " + mergedLoadName + " is " + (endTime - startTime))
      CarbonDataMergerUtil
        .updateLoadMetadataWithMergeStatus(loadsToMerge, carbonTable.getMetaDataFilepath(),
          mergedLoadName, carbonLoadModel, mergeLoadStartTime
        )
    }
  }
}
