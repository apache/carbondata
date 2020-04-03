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

package org.apache.spark.sql.secondaryindex.Jobs

import java.{lang, util}
import java.text.SimpleDateFormat
import java.util.concurrent.{Callable, Executors, ExecutorService, TimeUnit}
import java.util.Date

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{InputSplit, Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, TaskContext, TaskKilledException}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder
import org.apache.carbondata.core.index.{AbstractIndexJob, IndexInputFormat, IndexStoreManager}
import org.apache.carbondata.core.index.dev.CacheableIndex
import org.apache.carbondata.core.indexstore.{BlockletIndexWrapper, TableBlockIndexUniqueIdentifier, TableBlockIndexUniqueIdentifierWrapper}
import org.apache.carbondata.core.indexstore.blockletindex.BlockIndex
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.spark.rdd.CarbonRDD

class SparkBlockletIndexLoaderJob extends AbstractIndexJob {
  private val LOGGER = LogServiceFactory
    .getLogService(classOf[SparkBlockletIndexLoaderJob].getName)
  override def execute(carbonTable: CarbonTable,
      dataMapFormat: FileInputFormat[Void, BlockletIndexWrapper]): Unit = {
    val loader: BlockletIndexInputFormat = dataMapFormat
      .asInstanceOf[BlockletIndexInputFormat]
    val dataMapIndexWrappers = new IndexLoaderRDD(SparkSQLUtil.getSparkSession, loader).collect()
    val cacheableDataMap = IndexStoreManager.getInstance.getDefaultIndex(carbonTable)
      .getIndexFactory.asInstanceOf[CacheableIndex]
    val tableBlockIndexUniqueIdentifiers = dataMapIndexWrappers.map {
      case (tableBlockIndexUniqueIdentifier, _) => tableBlockIndexUniqueIdentifier
    }
    val groupBySegment = tableBlockIndexUniqueIdentifiers.toSet.groupBy[String](x => x.getSegmentId)
      .map(a => (a._1, a._2.asJava)).asJava
    cacheableDataMap.updateSegmentIndex(groupBySegment)
    // add segmentProperties in single thread if carbon table schema is not modified
    if (!carbonTable.getTableInfo.isSchemaModified) {
      addSegmentProperties(carbonTable, dataMapIndexWrappers)
    }
    val executorService: ExecutorService = Executors.newFixedThreadPool(3)
    try {
      dataMapIndexWrappers.toList.foreach { dataMapIndexWrapper =>
        executorService
          .submit(new IndexCacher(cacheableDataMap,
            dataMapIndexWrapper,
            carbonTable))
      }
    } finally {
      loader.invalidate()
      executorService.shutdown()
      executorService.awaitTermination(10, TimeUnit.MINUTES)
    }
  }

  private def addSegmentProperties(carbonTable: CarbonTable,
      dataMapIndexWrappers: Array[(TableBlockIndexUniqueIdentifier,
        BlockletIndexDetailsWithSchema)]): Unit = {
    // use the carbon table schema only as this flow is called when schema is not modified
    val tableColumnSchema = CarbonUtil
      .getColumnSchemaList(carbonTable.getVisibleDimensions,
        carbonTable.getVisibleMeasures)
    // add segmentProperties in the segmentPropertyCache
    dataMapIndexWrappers.foreach { entry =>
      val segmentId = entry._1.getSegmentId
      val wrapper = SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(carbonTable, tableColumnSchema, segmentId)
      entry._2.getBlockletIndexWrapper.getDataMaps.asScala
        .foreach(_.setSegmentPropertiesWrapper(wrapper))
    }
  }

  override def executeCountJob(dataMapFormat: IndexInputFormat): lang.Long = 0L
}

class IndexCacher(
    cacheableIndex: CacheableIndex,
    dataMapIndexWrapper: (TableBlockIndexUniqueIdentifier, BlockletIndexDetailsWithSchema),
    carbonTable: CarbonTable) extends Callable[Unit] {
  override def call(): Unit = {
    // if schema is modified then populate the segmentProperties cache
    if (carbonTable.getTableInfo.isSchemaModified) {
      val dataMaps: util.List[BlockIndex] = dataMapIndexWrapper._2.getBlockletIndexWrapper
        .getDataMaps
      val wrapper = SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(carbonTable,
          dataMapIndexWrapper._2.getColumnSchemaList,
          dataMapIndexWrapper._1.getSegmentId)
      // update all dataMaps with new segmentPropertyIndex
      dataMaps.asScala.foreach { dataMap =>
        dataMap.setSegmentPropertiesWrapper(wrapper)
      }
    }
    // create identifier wrapper object
    val tableBlockIndexUniqueIdentifierWrapper: TableBlockIndexUniqueIdentifierWrapper = new
        TableBlockIndexUniqueIdentifierWrapper(
          dataMapIndexWrapper._1,
          carbonTable)
    // add dataMap to cache
    cacheableIndex
      .cache(tableBlockIndexUniqueIdentifierWrapper,
        dataMapIndexWrapper._2.getBlockletIndexWrapper)
  }
}

class IndexLoaderPartition(rddId: Int, idx: Int, val inputSplit: InputSplit)
  extends Partition {
  override def index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * This RDD is used to load the dataMaps of a segment
 *
 * @param ss
 * @param dataMapFormat
 */
class IndexLoaderRDD(
    @transient ss: SparkSession,
    dataMapFormat: BlockletIndexInputFormat)
  extends CarbonRDD[(TableBlockIndexUniqueIdentifier, BlockletIndexDetailsWithSchema)](ss, Nil) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def internalGetPartitions: Array[Partition] = {
    val job = Job.getInstance(new Configuration())
    val splits = dataMapFormat.getSplits(job)
    splits.asScala.zipWithIndex.map(f => new IndexLoaderPartition(id, f._2, f._1)).toArray
  }

  override def internalCompute(split: Partition, context: TaskContext):
  Iterator[(TableBlockIndexUniqueIdentifier, BlockletIndexDetailsWithSchema)] = {
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val inputSplit = split.asInstanceOf[IndexLoaderPartition].inputSplit
    val reader = dataMapFormat.createRecordReader(inputSplit, attemptContext)
    val iter = new Iterator[(TableBlockIndexUniqueIdentifier, BlockletIndexDetailsWithSchema)] {
      // in case of success, failure or cancelation clear memory and stop execution
      context.addTaskCompletionListener { _ =>
        reader.close()
      }
      reader.initialize(inputSplit, attemptContext)

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

      override def next(): (TableBlockIndexUniqueIdentifier, BlockletIndexDetailsWithSchema) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val value = reader.getCurrentValue
        val key = reader.getCurrentKey
        (key, value)
      }
    }
    iter
  }
}
