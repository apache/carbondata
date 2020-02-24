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

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partition
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.CarbonMergerMapping

import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.api.CarbonInputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.CarbonDataMergerUtil
import org.apache.carbondata.spark.MergeResult

/**
 * IUD carbon merger RDD
 * */
class CarbonIUDMergerRDD[K, V](
    @transient private val ss: SparkSession,
    result: MergeResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    carbonMergerMapping: CarbonMergerMapping)
  extends CarbonMergerRDD[K, V](ss,
    result,
    carbonLoadModel,
    carbonMergerMapping) {

  override def internalGetPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
      tablePath, new CarbonTableIdentifier(databaseName, factTableName, tableId)
    )
    val jobConf: JobConf = new JobConf(FileFactory.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    val defaultParallelism = sparkContext.defaultParallelism
    val noOfBlocks = 0

    CarbonInputFormat.setSegmentsToAccess(
      job.getConfiguration, carbonMergerMapping.validSegments.toList.asJava)
    CarbonInputFormat.setTableInfo(
      job.getConfiguration,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)

    // get splits
    val splits = format.getSplits(job)
    val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

    // group blocks by segment.
    val splitsGroupedMySegment =
      carbonInputSplits.groupBy(_.getSegmentId)

    var i = -1

    // No need to get a new SegmentUpdateStatus Manager as the Object is passed
    // in CarbonLoadModel.
    // val manager = new SegmentUpdateStatusManager(absoluteTableIdentifier)
    val updateStatusManager = carbonLoadModel.getSegmentUpdateStatusManager

    // make one spark partition for one segment
    val resultSplits = splitsGroupedMySegment.map { entry =>
      val (segName, splits) = (entry._1, entry._2)
      val validSplits = splits.filter { inputSplit =>
        CarbonDataMergerUtil
          .checkUpdateDeltaMatchBlock(segName, inputSplit.getBlockPath, updateStatusManager)
      }

      if (validSplits.nonEmpty) {
        val locations = validSplits.head.getLocations
        i += 1
        new CarbonSparkPartition(id, i,
          new CarbonMultiBlockSplit(validSplits.asJava, locations))
      } else {
        null
      }
    }.filter( _ != null)

    // max segment cardinality is calculated in executor for each segment
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
      logInfo(s"Node : " + partition.multiBlockSplit.getLocations.toSeq.mkString(",")
              + ", No.Of Blocks : " + partition.multiBlockSplit.getLength
      )
    }
    resultSplits.toArray
  }
}
