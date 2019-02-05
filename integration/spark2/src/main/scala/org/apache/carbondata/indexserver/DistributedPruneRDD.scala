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

package org.apache.carbondata.indexserver

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.{InputSplit, Job, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkEnv, TaskContext, TaskKilledException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datamap.DistributableDataMapFormat
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.indexstore.ExtendedBlocklet
import org.apache.carbondata.spark.rdd.CarbonRDD
import org.apache.carbondata.spark.util.CarbonScalaUtil

private[indexserver] class DataMapRDDPartition(rddId: Int, idx: Int, val inputSplit: InputSplit)
  extends Partition {

  override def index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

private[indexserver] class DistributedPruneRDD(@transient private val ss: SparkSession,
    dataMapFormat: DistributableDataMapFormat)
  extends CarbonRDD[(String, ExtendedBlocklet)](ss, Nil) {

  val executorsList: Set[String] = DistributionUtil.getNodeList(ss.sparkContext).toSet

  @transient private val LOGGER = LogServiceFactory.getLogService(classOf[DistributedPruneRDD]
    .getName)

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    split.asInstanceOf[DataMapRDDPartition].inputSplit.getLocations.toSeq
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(String, ExtendedBlocklet)] = {
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(FileFactory.getConfiguration, attemptId)
    val inputSplit = split.asInstanceOf[DataMapRDDPartition].inputSplit
    val reader = dataMapFormat.createRecordReader(inputSplit, attemptContext)
    reader.initialize(inputSplit, attemptContext)
    val cacheSize = if (CacheProvider.getInstance().getCarbonCache != null) {
      CacheProvider.getInstance().getCarbonCache.getCurrentSize
    } else {
      0L
    }
    context.addTaskCompletionListener(_ => {
      if (reader != null) {
        reader.close()
      }
    })
    val iter: Iterator[(String, ExtendedBlocklet)] = new Iterator[(String, ExtendedBlocklet)] {

      private var havePair = false
      private var finished = false

      override def hasNext: Boolean = {
        if (context.isInterrupted) {
          throw new TaskKilledException
        }
        if (!finished && !havePair) {
          finished = !reader.nextKeyValue
          havePair = !finished
        }
        !finished
      }

      override def next(): (String, ExtendedBlocklet) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val executorIP = SparkEnv.get.blockManager.blockManagerId.host
        val value = (executorIP + "_" + cacheSize.toString, reader.getCurrentValue)
        value
      }
    }
    iter
  }

  override protected def internalGetPartitions: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val splits = dataMapFormat.getSplits(job).asScala
    if (dataMapFormat.isFallbackJob || splits.isEmpty) {
      splits.zipWithIndex.map {
        f => new DataMapRDDPartition(id, f._2, f._1)
      }.toArray
    } else {
      val (response, time) = CarbonScalaUtil.logTime {
        DistributedRDDUtils.getExecutors(splits.toArray, executorsList, dataMapFormat
          .getCarbonTable.getTableUniqueName, id)
      }
      LOGGER.debug(s"Time taken to assign executors to ${splits.length} is $time ms")
      response.toArray
    }
  }
}
