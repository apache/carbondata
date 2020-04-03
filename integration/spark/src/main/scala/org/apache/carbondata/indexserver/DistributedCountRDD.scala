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
    val futures = if (inputSplits.length <= numOfThreads) {
      inputSplits.map {
        split => generateFuture(Seq(split))
      }
    } else {
      DistributedRDDUtils.groupSplits(inputSplits, numOfThreads).map {
        splits => generateFuture(splits)
      }
    }
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

  private def generateFuture(split: Seq[InputSplit])
    (implicit executionContext: ExecutionContext) = {
    Future {
      val segments = split.map { inputSplit =>
        val distributable = inputSplit.asInstanceOf[IndexInputSplitWrapper]
        distributable.getDistributable.getSegment
          .setReadCommittedScope(indexInputFormat.getReadCommittedScope)
        distributable.getDistributable.getSegment
      }
      val defaultIndex = IndexStoreManager.getInstance
        .getIndex(indexInputFormat.getCarbonTable, split.head
          .asInstanceOf[IndexInputSplitWrapper].getDistributable.getIndexSchema)
      defaultIndex.getBlockRowCount(segments.toList.asJava, indexInputFormat
        .getPartitions, defaultIndex).asScala
    }
  }

}
