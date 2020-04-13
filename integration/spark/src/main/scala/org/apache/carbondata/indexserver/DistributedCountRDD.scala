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

import java.util
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration

import org.apache.hadoop.mapred.TaskAttemptID
import org.apache.hadoop.mapreduce.{InputSplit, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datamap.{SegmentProcessor, SegmentWorkStatus}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexInputFormat, IndexStoreManager}
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper
import org.apache.carbondata.core.util.{CarbonProperties, CarbonThreadFactory}
import org.apache.carbondata.spark.rdd.CarbonRDD

/**
 * An RDD which will get the count for the table.
 */
class DistributedCountRDD(@transient ss: SparkSession, indexInputFormat: IndexInputFormat)
  extends CarbonRDD[(String, String)](ss, Nil) {

  @transient private val LOGGER = LogServiceFactory.getLogService(classOf[DistributedPruneRDD]
    .getName)

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.asInstanceOf[IndexRDDPartition].getLocations != null) {
      split.asInstanceOf[IndexRDDPartition].getLocations.toSeq
    } else {
      Seq()
    }
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(String, String)] = {
    val attemptId = new TaskAttemptID(DistributedRDDUtils.generateTrackerId,
      id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(FileFactory.getConfiguration, attemptId)
    val inputSplits = split.asInstanceOf[IndexRDDPartition].inputSplit
    val numOfThreads = CarbonProperties.getInstance().getNumOfThreadsForExecutorPruning
    val service = Executors
      .newFixedThreadPool(numOfThreads, new CarbonThreadFactory("IndexPruningPool", true))
    implicit val ec: ExecutionContextExecutor = ExecutionContext
      .fromExecutor(service)
    if (indexInputFormat.ifAsyncCall()) {
      // to clear cache of invalid segments during pre-priming in index server
      IndexStoreManager.getInstance().clearInvalidSegments(indexInputFormat.getCarbonTable,
        indexInputFormat.getInvalidSegments)
    }
    val globalQueue = SegmentProcessor.getInstance()
    val futures = if (inputSplits.length <= numOfThreads) {
      inputSplits.map {
        split => generateFuture(Seq(split), globalQueue)
      }
    } else {
      DistributedRDDUtils.groupSplits(inputSplits, numOfThreads).map {
        splits => generateFuture(splits, globalQueue)
      }
    }
    // globalQueue.emptyQueue()
    // scalastyle:off awaitresult
    val results = Await.result(Future.sequence(futures), Duration.Inf).flatten
    // scalastyle:on awaitresult
    val executorIP = s"${ SparkEnv.get.blockManager.blockManagerId.host }_${
      SparkEnv.get.blockManager.blockManagerId.executorId
    }"
    val cacheSize = if (CacheProvider.getInstance().getCarbonCache != null) {
      CacheProvider.getInstance().getCarbonCache.getCurrentSize
    } else {
      0L
    }
    Iterator((executorIP + "_" + cacheSize.toString, results.map(_._2.toLong).sum.toString))
  }

  override protected def internalGetPartitions: Array[Partition] = {
    new DistributedPruneRDD(ss, indexInputFormat).partitions
  }

  private def generateFuture(split: Seq[InputSplit], globalQueue: SegmentProcessor)
    (implicit executionContext: ExecutionContext) = {
    Future {
      val segmentsWorkStatus = split.map { inputSplit =>
        val distributable = inputSplit.asInstanceOf[IndexInputSplitWrapper]
        distributable.getDistributable.getSegment
          .setReadCommittedScope(indexInputFormat.getReadCommittedScope)
        // global queue needs to syncronised here because, 2 different threads can return
        // ifProcessSegments as false.
        globalQueue.synchronized {
          val ifSegmentsToProcess = globalQueue.ifProcessSegment(distributable.getDistributable
            .getSegment.getSegmentNo, indexInputFormat.getCarbonTable.getTableId)
          val segmentWorkStatusList = new SegmentWorkStatus(distributable.getDistributable
            .getSegment, !ifSegmentsToProcess)
          // if ifprocesssegment = true, iswaiting = false and vice versa
          globalQueue.processSegment(segmentWorkStatusList,
            indexInputFormat.getCarbonTable.getTableId)
          segmentWorkStatusList
        }
      }
      val segmentsPositive = new util.HashSet[SegmentWorkStatus]()
      val segmentsNegative = new util.HashSet[SegmentWorkStatus]()

      segmentsWorkStatus.map { iter =>
        if (!iter.getWaiting) {
          segmentsPositive.add(iter)
        } else {
          segmentsNegative.add(iter)
        }
      }

      val defaultDataMap = IndexStoreManager.getInstance
        .getIndex(indexInputFormat.getCarbonTable, split.head
          .asInstanceOf[IndexInputSplitWrapper].getDistributable.getIndexSchema)
      val result = defaultDataMap.getBlockRowCount(segmentsPositive.asScala.map { iter =>
        iter.getSegment }.toList.asJava, indexInputFormat.getPartitions,
        defaultDataMap).asScala

      //  updating the waiting status in the global queue
      segmentsPositive.asScala.foreach { item =>
        globalQueue.updateWaitingStatus(item, defaultDataMap.getTable.getTableId)
      }

      // Checking and processing all the remaining segments till all the unprocessed segments are
      // finished processing
      while (segmentsNegative != null && !segmentsNegative.isEmpty) {
        val unProcessedSegmentsWorkStatus = segmentsNegative.asScala.map { iter =>
          globalQueue.synchronized {
            globalQueue.ifProcessSegment(iter.getSegment.getSegmentNo, defaultDataMap.getTable
            .getTableId)
            globalQueue.processSegment(iter, indexInputFormat.getCarbonTable.getTableId)
            iter
          }
        }.toSeq

        // clearing both the processed and unprocessed segments anf filling them again with
        // remaining segments
        segmentsNegative.clear()
        segmentsPositive.clear()
        unProcessedSegmentsWorkStatus.map { iter =>
          if (!iter.getWaiting) {
            segmentsPositive.add(iter)
          } else {
            segmentsNegative.add(iter)
          }
        }

        // Combining previous and the current result map.
        result.asJava.putAll(defaultDataMap
          .getBlockRowCount(segmentsPositive.asScala.map { iter => iter.getSegment }.toList.asJava,
            indexInputFormat.getPartitions, defaultDataMap ))
        segmentsPositive.asScala.map { iter =>
          globalQueue.updateWaitingStatus(iter, defaultDataMap.getTable.getTableId)
        }
      }
      result
    }
  }
}
