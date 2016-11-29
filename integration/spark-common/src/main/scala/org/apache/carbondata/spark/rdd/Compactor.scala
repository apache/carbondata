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

import scala.collection.JavaConverters._

import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel}

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.lcm.status.SegmentStatusManager
import org.apache.carbondata.spark.MergeResultImpl
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.merger.CarbonDataMergerUtil

/**
 * Compactor class which handled the compaction cases.
 */
object Compactor {

  val logger = LogServiceFactory.getLogService(Compactor.getClass.getName)

  def triggerCompaction(compactionCallableModel: CompactionCallableModel): Unit = {

    val storePath = compactionCallableModel.storePath
    val storeLocation = compactionCallableModel.storeLocation
    val carbonTable = compactionCallableModel.carbonTable
    val kettleHomePath = compactionCallableModel.kettleHomePath
    val cubeCreationTime = compactionCallableModel.cubeCreationTime
    val loadsToMerge = compactionCallableModel.loadsToMerge
    val sc = compactionCallableModel.sqlContext
    val carbonLoadModel = compactionCallableModel.carbonLoadModel
    val compactionType = compactionCallableModel.compactionType

    val startTime = System.nanoTime()
    val mergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
    var finalMergeStatus = false
    val schemaName: String = carbonLoadModel.getDatabaseName
    val factTableName = carbonLoadModel.getTableName
    val validSegments: Array[String] = CarbonDataMergerUtil
      .getValidSegments(loadsToMerge).split(',')
    val mergeLoadStartTime = CarbonLoaderUtil.readCurrentTime()
    val carbonMergerMapping = CarbonMergerMapping(storeLocation,
      storePath,
      carbonTable.getMetaDataFilepath,
      mergedLoadName,
      kettleHomePath,
      cubeCreationTime,
      schemaName,
      factTableName,
      validSegments,
      carbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
      maxSegmentColCardinality = null,
      maxSegmentColumnSchemaList = null
    )
    carbonLoadModel.setStorePath(carbonMergerMapping.storePath)
    carbonLoadModel.setLoadMetadataDetails(
      SegmentStatusManager.readLoadMetadata(carbonTable.getMetaDataFilepath).toList.asJava)
    var execInstance = "1"
    // in case of non dynamic executor allocation, number of executors are fixed.
    if (sc.sparkContext.getConf.contains("spark.executor.instances")) {
      execInstance = sc.sparkContext.getConf.get("spark.executor.instances")
      logger.info(s"spark.executor.instances property is set to = $execInstance")
    } // in case of dynamic executor allocation, taking the max executors of the dynamic allocation.
    else if (sc.sparkContext.getConf.contains("spark.dynamicAllocation.enabled")) {
      if (sc.sparkContext.getConf.get("spark.dynamicAllocation.enabled").trim
        .equalsIgnoreCase("true")) {
        execInstance = sc.sparkContext.getConf.get("spark.dynamicAllocation.maxExecutors")
        logger.info(s"spark.dynamicAllocation.maxExecutors property is set to = $execInstance")
      }
    }

    val mergeStatus = new CarbonMergerRDD(
      sc.sparkContext,
      new MergeResultImpl(),
      carbonLoadModel,
      carbonMergerMapping,
      execInstance
    ).collect

    if (mergeStatus.length == 0) {
      finalMergeStatus = false
    } else {
      finalMergeStatus = mergeStatus.forall(_._2)
    }

    if (finalMergeStatus) {
      val endTime = System.nanoTime()
      logger.info(s"time taken to merge $mergedLoadName is ${ endTime - startTime }")
      if (!CarbonDataMergerUtil
        .updateLoadMetadataWithMergeStatus(loadsToMerge, carbonTable.getMetaDataFilepath,
          mergedLoadName, carbonLoadModel, mergeLoadStartTime, compactionType
        )) {
        logger.audit(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        logger.error(s"Compaction request failed for table ${ carbonLoadModel.getDatabaseName }." +
                     s"${ carbonLoadModel.getTableName }")
        throw new Exception(s"Compaction failed to update metadata for table" +
                            s" ${ carbonLoadModel.getDatabaseName }." +
                            s"${ carbonLoadModel.getTableName }")
      } else {
        logger.audit(s"Compaction request completed for table " +
                     s"${ carbonLoadModel.getDatabaseName }.${ carbonLoadModel.getTableName }")
        logger.info("Compaction request completed for table ${ carbonLoadModel.getDatabaseName } " +
                    s".${ carbonLoadModel.getTableName }")
      }
    } else {
      logger.audit("Compaction request failed for table ${ carbonLoadModel.getDatabaseName } " +
                   s".${ carbonLoadModel.getTableName }"
      )
      logger.error("Compaction request failed for table ${ carbonLoadModel.getDatabaseName } " +
                   s".${ carbonLoadModel.getTableName }")
      throw new Exception("Compaction Failure in Merger Rdd.")
    }
  }
}
