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

import org.apache.hadoop.mapred.{RecordReader, TaskAttemptID}
import org.apache.hadoop.mapreduce.{InputSplit, Job, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexInputFormat, IndexStoreManager}
import org.apache.carbondata.core.index.dev.expr.IndexInputSplitWrapper
import org.apache.carbondata.core.indexstore.{ExtendedBlocklet, ExtendedBlockletWrapper}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonThreadFactory}
import org.apache.carbondata.spark.rdd.CarbonRDD
import org.apache.carbondata.spark.util.CarbonScalaUtil

class IndexRDDPartition(rddId: Int,
    idx: Int,
    val inputSplit: Seq[InputSplit],
    location: Array[String])
  extends Partition {

  override def index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx

  def getLocations: Array[String] = {
    location
  }
}

private[indexserver] class DistributedPruneRDD(@transient private val ss: SparkSession,
    indexInputFormat: IndexInputFormat)
  extends CarbonRDD[(String, ExtendedBlockletWrapper)](ss, Nil) {

  @transient private val LOGGER = LogServiceFactory.getLogService(classOf[DistributedPruneRDD]
    .getName)

  var readers: scala.collection.Iterator[RecordReader[Void, ExtendedBlocklet]] = _

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(String, ExtendedBlockletWrapper)] = {
    val attemptId = new TaskAttemptID(DistributedRDDUtils.generateTrackerId,
      id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(FileFactory.getConfiguration, attemptId)
    val inputSplits = split.asInstanceOf[IndexRDDPartition].inputSplit
    val executorIP = s"${ SparkEnv.get.blockManager.blockManagerId.host }_${
      SparkEnv.get.blockManager.blockManagerId.executorId
    }"
    if (indexInputFormat.isJobToClearIndexes) {
      // if job is to clear datamaps just clear datamaps from cache and pass empty iterator
      IndexStoreManager.getInstance().clearInvalidIndex(indexInputFormat.getCarbonTable,
        inputSplits.map(_
          .asInstanceOf[IndexInputSplitWrapper].getDistributable.getSegment.getSegmentNo)
          .toList.asJava,
        indexInputFormat
          .getIndexToClear)
      val cacheSize = if (CacheProvider.getInstance().getCarbonCache != null) {
        CacheProvider.getInstance().getCarbonCache.getCurrentSize
      } else {
        0L
      }
      val executorIP = s"${ SparkEnv.get.blockManager.blockManagerId.host }_${
        SparkEnv.get.blockManager.blockManagerId.executorId
      }"
      Iterator((executorIP + "_" + cacheSize, new ExtendedBlockletWrapper()))
    } else {
      if (indexInputFormat.getInvalidSegments.size > 0) {
        // clear the segmentMap and from cache in executor when there are invalid segments
        IndexStoreManager.getInstance().clearInvalidSegments(indexInputFormat.getCarbonTable,
          indexInputFormat.getInvalidSegments)
      }
      val startTime = System.currentTimeMillis()
      val numOfThreads = CarbonProperties.getInstance().getNumOfThreadsForExecutorPruning
      indexInputFormat.createIndexChooser()
      val service = Executors
        .newFixedThreadPool(numOfThreads, new CarbonThreadFactory("IndexPruningPool", true))
      implicit val ec: ExecutionContextExecutor = ExecutionContext
        .fromExecutor(service)

      val futures = if (inputSplits.length <= numOfThreads) {
        inputSplits.map {
          split => generateFuture(Seq(split), attemptContext)
        }
      } else {
        DistributedRDDUtils.groupSplits(inputSplits, numOfThreads).map {
          splits => generateFuture(splits, attemptContext)
        }
      }
      // scalastyle:off
      val f = Await.result(Future.sequence(futures), Duration.Inf).flatten
      // scalastyle:on
      service.shutdownNow()
      val LOGGER = LogServiceFactory.getLogService(classOf[DistributedPruneRDD].getName)
      LOGGER.info(s"Time taken to collect ${ inputSplits.size } blocklets : " +
                  (System.currentTimeMillis() - startTime))
      val cacheSize = if (CacheProvider.getInstance().getCarbonCache != null) {
        CacheProvider.getInstance().getCarbonCache.getCurrentSize
      } else {
        0L
      }
      val executorIP = s"${ SparkEnv.get.blockManager.blockManagerId.host }_${
        SparkEnv.get.blockManager.blockManagerId.executorId
      }"
      val value = (executorIP + "_" + cacheSize.toString, new ExtendedBlockletWrapper(f.toList
        .asJava, indexInputFormat.getCarbonTable.getTablePath, indexInputFormat.getQueryId,
        indexInputFormat.isWriteToFile, indexInputFormat.isCountStarJob))
      Iterator(value)
    }
  }

  private def generateFuture(split: Seq[InputSplit], attemptContextImpl: TaskAttemptContextImpl)
    (implicit executionContext: ExecutionContext): Future[Seq[ExtendedBlocklet]] = {
    Future {
      split.flatMap { inputSplit =>
        val blocklets = new java.util.ArrayList[ExtendedBlocklet]()
        val reader = indexInputFormat.createRecordReader(inputSplit, attemptContextImpl)
        reader.initialize(inputSplit, attemptContextImpl)
        while (reader.nextKeyValue()) {
          blocklets.add(reader.getCurrentValue)
        }
        blocklets.asScala
      }
    }
  }

  override protected def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.asInstanceOf[IndexRDDPartition].getLocations != null) {
      split.asInstanceOf[IndexRDDPartition].getLocations.toSeq
    } else {
      Seq()
    }
  }

  override protected def internalGetPartitions: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val splits = indexInputFormat.getSplits(job).asScala
    val isDistributedPruningEnabled = CarbonProperties.getInstance()
      .isDistributedPruningEnabled(indexInputFormat.getCarbonTable.getDatabaseName,
        indexInputFormat.getCarbonTable.getTableName)
    if (!isDistributedPruningEnabled || indexInputFormat.isFallbackJob || splits.isEmpty) {
      splits.zipWithIndex.map {
        f => new IndexRDDPartition(id, f._2, List(f._1), f._1.getLocations)
      }.toArray
    } else {
      val executorsList: Map[String, Seq[String]] = DistributionUtil
        .getExecutors(ss.sparkContext)
      val (response, time) = CarbonScalaUtil.logTime {
        DistributedRDDUtils.getExecutors(splits.toArray, executorsList, indexInputFormat
          .getCarbonTable.getTableUniqueName, id)
      }
      LOGGER.debug(s"Time taken to assign executors to ${ splits.length } is $time ms")
      response.toArray
    }
  }
}
