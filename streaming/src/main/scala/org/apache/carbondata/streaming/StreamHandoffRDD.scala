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

package org.apache.carbondata.streaming

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{Job, TaskAttemptID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.{Partition, SerializableWritable, SparkContext, TaskContext}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow

import org.apache.carbondata.core.datastore.block.SegmentProperties
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonProjection}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.streaming.{CarbonStreamInputFormat, CarbonStreamRecordReader}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger.{CompactionResultSortProcessor, CompactionType}
import org.apache.carbondata.spark.HandoffResult
import org.apache.carbondata.spark.rdd.CarbonRDD

/**
 * partition of the handoff segment
 */
class HandoffPartition(
    val rddId: Int,
    val idx: Int,
    @transient val inputSplit: CarbonInputSplit
) extends Partition {

  val split = new SerializableWritable[CarbonInputSplit](inputSplit)

  override val index: Int = idx

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

/**
 * package the record reader of the handoff segment to RawResultIterator
 */
class StreamingRawResultIterator(
    recordReader: CarbonStreamRecordReader
) extends RawResultIterator {

  override def hasNext: Boolean = {
    recordReader.nextKeyValue()
  }

  override def next(): Array[Object] = {
    recordReader
      .getCurrentValue
      .asInstanceOf[GenericInternalRow]
      .values
      .asInstanceOf[Array[Object]]
  }
}

/**
 * execute streaming segment handoff
 */
class StreamHandoffRDD[K, V](
    sc: SparkContext,
    result: HandoffResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    handOffSegmentId: String
) extends CarbonRDD[(K, V)](sc, Nil) {

  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }

  override def internalCompute(split: Partition,
      context: TaskContext): Iterator[(K, V)] = {
    carbonLoadModel.setPartitionId("0")
    carbonLoadModel.setTaskNo("" + split.index)
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val iteratorList = prepareInputIterator(split, carbonTable)
    val processor = prepareHandoffProcessor(carbonTable)
    val status = processor.execute(iteratorList)

    new Iterator[(K, V)] {
      private var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey("" + split.index, status)
      }
    }
  }

  /**
   * prepare input iterator by basing CarbonStreamRecordReader
   */
  private def prepareInputIterator(
      split: Partition,
      carbonTable: CarbonTable
  ): util.ArrayList[RawResultIterator] = {
    val inputSplit = split.asInstanceOf[HandoffPartition].split.value
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val hadoopConf = new Configuration()
    CarbonTableInputFormat.setDatabaseName(hadoopConf, carbonTable.getDatabaseName)
    CarbonTableInputFormat.setTableName(hadoopConf, carbonTable.getTableName)
    CarbonTableInputFormat.setTablePath(hadoopConf, carbonTable.getTablePath)
    val projection = new CarbonProjection
    val dataFields = carbonTable.getStreamStorageOrderColumn(carbonTable.getTableName)
    (0 until dataFields.size()).foreach { index =>
      projection.addColumn(dataFields.get(index).getColName)
    }
    CarbonTableInputFormat.setColumnProjection(hadoopConf, projection)
    val attemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId)
    val format = new CarbonTableInputFormat[Array[Object]]()
    val model = format.getQueryModel(inputSplit, attemptContext)
    val inputFormat = new CarbonStreamInputFormat
    val streamReader = inputFormat.createRecordReader(inputSplit, attemptContext)
      .asInstanceOf[CarbonStreamRecordReader]
    streamReader.setVectorReader(false)
    streamReader.setQueryModel(model)
    streamReader.setUseRawRow(true)
    streamReader.initialize(inputSplit, attemptContext)
    val iteratorList = new util.ArrayList[RawResultIterator](1)
    iteratorList.add(new StreamingRawResultIterator(streamReader))
    iteratorList
  }

  private def prepareHandoffProcessor(
      carbonTable: CarbonTable
  ): CompactionResultSortProcessor = {
    val wrapperColumnSchemaList = CarbonUtil.getColumnSchemaList(
      carbonTable.getDimensionByTableName(carbonTable.getTableName),
      carbonTable.getMeasureByTableName(carbonTable.getTableName))
    val dimLensWithComplex = new Array[Int](wrapperColumnSchemaList.size())
    for (i <- 0 until dimLensWithComplex.length) {
      dimLensWithComplex(i) = Integer.MAX_VALUE
    }
    val dictionaryColumnCardinality =
      CarbonUtil.getFormattedCardinality(dimLensWithComplex, wrapperColumnSchemaList)
    val segmentProperties =
      new SegmentProperties(wrapperColumnSchemaList, dictionaryColumnCardinality)

    new CompactionResultSortProcessor(
      carbonLoadModel,
      carbonTable,
      segmentProperties,
      CompactionType.STREAMING,
      carbonTable.getTableName
    )
  }

  /**
   * get the partitions of the handoff segment
   */
  override protected def getPartitions: Array[Partition] = {
    val job = Job.getInstance(FileFactory.getConfiguration)
    val inputFormat = new CarbonTableInputFormat[Array[Object]]()
    val segmentList = new util.ArrayList[String](1)
    segmentList.add(handOffSegmentId)
    val splits = inputFormat.getSplitsOfStreaming(
      job,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getAbsoluteTableIdentifier,
      segmentList
    )

    (0 until splits.size()).map { index =>
      new HandoffPartition(id, index, splits.get(index).asInstanceOf[CarbonInputSplit])
    }.toArray[Partition]
  }
}
