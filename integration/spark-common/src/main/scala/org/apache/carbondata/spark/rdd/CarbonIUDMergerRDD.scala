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
import java.util.List

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.sql.execution.command.CarbonMergerMapping

import org.apache.carbondata.core.datastore.block.{Distributable, TableBlockInfo}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil
import org.apache.carbondata.spark.MergeResult

/**
 * IUD carbon merger RDD
 * */
class CarbonIUDMergerRDD[K, V](
    sc: SparkContext,
    result: MergeResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    carbonMergerMapping: CarbonMergerMapping,
    confExecutorsTemp: String)
  extends CarbonMergerRDD[K, V](sc,
    result,
    carbonLoadModel,
    carbonMergerMapping,
    confExecutorsTemp) {

  override def getPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(
      hdfsStoreLocation, new CarbonTableIdentifier(databaseName, factTableName, tableId)
    )
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    var defaultParallelism = sparkContext.defaultParallelism
    val result = new util.ArrayList[Partition](defaultParallelism)

    // mapping of the node and block list.
    var nodeMapping: util.Map[String, util.List[Distributable]] = new
        util.HashMap[String, util.List[Distributable]]

    var noOfBlocks = 0

    val taskInfoList = new util.ArrayList[Distributable]

    var blocksOfLastSegment: List[TableBlockInfo] = null

    CarbonTableInputFormat.setSegmentsToAccess(
      job.getConfiguration, carbonMergerMapping.validSegments.toList.asJava)

    // get splits
    val splits = format.getSplits(job)
    val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

    // group blocks by segment.
    val splitsGroupedMySegment = carbonInputSplits.groupBy(_.getSegmentId)

    var i = -1

    // No need to get a new SegmentUpdateStatus Manager as the Object is passed
    // in CarbonLoadModel.
    // val manager = new SegmentUpdateStatusManager(absoluteTableIdentifier)
    val updateStatusManager = carbonLoadModel.getSegmentUpdateStatusManager

    // make one spark partition for one segment
    val resultSplits = splitsGroupedMySegment.map(entry => {
      val (segName, splits) = (entry._1, entry._2)
      val invalidBlocks = updateStatusManager.getInvalidBlockList(segName)
      val validSplits = splits.filter( inputSplit =>
        CarbonDataMergerUtil
          .checkUpdateDeltaMatchBlock(segName, inputSplit.getBlockPath, updateStatusManager)
      )

      if (!validSplits.isEmpty) {
        val locations = validSplits(0).getLocations
        i += 1
        new CarbonSparkPartition(id, i,
          new CarbonMultiBlockSplit(absoluteTableIdentifier, validSplits.asJava, locations))
      }
      else {
        null
      }
    }
    ).filter( _ != null)

    // max segment cardinality is calculated in executor for each segment
    carbonMergerMapping.maxSegmentColCardinality = null
    carbonMergerMapping.maxSegmentColumnSchemaList = null

    // Log the distribution
    val noOfTasks = resultSplits.size
    logInfo(s"Identified  no.of.Blocks: $noOfBlocks,"
            + s"parallelism: $defaultParallelism , no.of.nodes: unknown, no.of.tasks: $noOfTasks"
    )
    logInfo("Time taken to identify Blocks to scan : " + (System
                                                            .currentTimeMillis() - startTime)
    )
    resultSplits.foreach { partition =>
      val cp = partition.asInstanceOf[CarbonSparkPartition]
      logInfo(s"Node : " + cp.multiBlockSplit.getLocations.toSeq.mkString(",")
              + ", No.Of Blocks : " + cp.multiBlockSplit.getLength
      )
    }
    resultSplits.toArray
  }
}
