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

import java.util.ArrayList

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.command.AlterPartitionModel
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.sql.util.CarbonException
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.PartitionUtils

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.datastore.block.{Distributable, SegmentProperties, TaskBlockInfo}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.column.{CarbonDimension, CarbonMeasure}
import org.apache.carbondata.core.scan.result.iterator.PartitionSpliterRawResultIterator
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper
import org.apache.carbondata.core.util.{ByteUtil, DataTypeUtil}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.merger.CarbonCompactionUtil
import org.apache.carbondata.processing.partition.spliter.CarbonSplitExecutor
import org.apache.carbondata.processing.util.CarbonLoaderUtil


/**
 * This RDD is used in alter table partition statement to get data of target partitions,
 * then repartition data according to new partitionInfo
 * @param alterPartitionModel
 * @param absoluteTableIdentifier
 * @param partitionIds  the ids of target partition to be scanned
 * @param bucketId
 */
class CarbonScanPartitionRDD(alterPartitionModel: AlterPartitionModel,
    absoluteTableIdentifier: AbsoluteTableIdentifier,
    partitionIds: Seq[String],
    bucketId: Int)
  extends CarbonRDD[(AnyRef, Array[AnyRef])](alterPartitionModel.sqlContext.sparkSession, Nil) {

  private val queryId = alterPartitionModel.sqlContext.sparkContext.getConf
    .get("queryId", System.nanoTime() + "")
  val segmentId = alterPartitionModel.segmentId
  val carbonLoadModel = alterPartitionModel.carbonLoadModel
  val oldPartitionIdList = alterPartitionModel.oldPartitionIds
  var storeLocation: String = null
  var splitStatus: Boolean = false
  var blockId: String = null
  val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
  val dimensions = carbonTable.getAllDimensions.asScala
  val measures = carbonTable.getAllMeasures.asScala
  val partitionInfo = carbonTable
    .getPartitionInfo(absoluteTableIdentifier.getCarbonTableIdentifier.getTableName)
  val partitionColumn = partitionInfo.getColumnSchemaList().get(0)
  val partitionDataType = partitionColumn.getDataType
  val partitionColumnName = partitionColumn.getColumnName
  var isDimension: Boolean = false
  val encodingList = partitionColumn.getEncodingList
  var dimension: CarbonDimension = null
  var measure: CarbonMeasure = null
  val noDictionaryIndexGroup: ArrayBuffer[Int] = new ArrayBuffer[Int]()
  val dictionaryIndexGroup: ArrayBuffer[Int] = new ArrayBuffer[Int]()
  val measureIndexGroup: ArrayBuffer[Int] = new ArrayBuffer[Int]()

  override def internalGetPartitions: Array[Partition] = {
    val parallelism = sparkContext.defaultParallelism
    val jobConf = new JobConf(FileFactory.getConfiguration)
    val job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonTableInputFormat(absoluteTableIdentifier,
      partitionIds.toList.asJava, job)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    job.getConfiguration.set("query.id", queryId)

    val splits = format.getSplitsOfOneSegment(job, segmentId,
      oldPartitionIdList.map(_.asInstanceOf[Integer]).asJava, partitionInfo)
    var partition_num = 0
    val result = new ArrayList[Partition](parallelism)
    val blockList = splits.asScala
      .filter(_.asInstanceOf[CarbonInputSplit].getBucketId.toInt == bucketId)
      .map(_.asInstanceOf[Distributable])
    if (!blockList.isEmpty) {
      val activeNodes = DistributionUtil.ensureExecutorsAndGetNodeList(blockList, sparkContext)
      val nodeBlockMapping = CarbonLoaderUtil.nodeBlockTaskMapping(blockList.asJava, -1,
        parallelism, activeNodes.toList.asJava)
      nodeBlockMapping.asScala.foreach { case (node, blockList) =>
        blockList.asScala.foreach { blocksPerTask =>
          val splits = blocksPerTask.asScala.map(_.asInstanceOf[CarbonInputSplit])
          if (blocksPerTask.size() != 0) {
            val multiBlockSplit =
              new CarbonMultiBlockSplit(splits.asJava, Array(node))
            val partition = new CarbonSparkPartition(id, partition_num, multiBlockSplit)
            result.add(partition)
            partition_num += 1
          }
        }
      }
    }
    result.toArray(new Array[Partition](result.size()))
  }

  override def internalCompute(split: Partition, context: TaskContext):
  Iterator[(AnyRef, Array[AnyRef])] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
      var exec : CarbonSplitExecutor = null
      val rows : java.util.List[(AnyRef, Array[AnyRef])] = new ArrayList[(AnyRef, Array[AnyRef])]()
      try {
        val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
        val splits = inputSplit.getAllSplits.asScala
        val tableBlockInfoList = CarbonInputSplit.createBlocks(splits.asJava)
        val segmentMapping: java.util.Map[String, TaskBlockInfo] =
          CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList)
        val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
        var result : java.util.List[PartitionSpliterRawResultIterator] = null
        try {
          exec = new CarbonSplitExecutor(segmentMapping, carbonTable)
          result = exec.processDataBlocks(segmentId, new SparkDataTypeConverterImpl(),
            FileFactory.getConfiguration)
        } catch {
          case e: Throwable =>
            LOGGER.error(e)
            if (null != e.getMessage) {
              CarbonException.analysisException(
                s"Exception occurred in query execution :: ${e.getMessage}")
            } else {
              CarbonException.analysisException(
                "Exception occurred in query execution. Please check logs.")
            }
        }
        val segmentProperties = PartitionUtils.getSegmentProperties(absoluteTableIdentifier,
          segmentId, partitionIds.toList, oldPartitionIdList, partitionInfo, carbonTable)
        val partColIdx = getPartitionColumnIndex(partitionColumnName, segmentProperties)
        indexInitialise()
        for (iterator <- result.asScala) {
          while (iterator.hasNext) {
            val row = iterator.next()
            val partitionColumnValue = getPartitionColumnValue(row, partColIdx,
              segmentProperties)
            rows.add((partitionColumnValue, row))
          }
        }
      } catch {
        case e: Exception =>
          LOGGER.error(e)
          throw e
      } finally {
        if (null != exec) {
          exec.finish
        }
      }
    val iter = rows.iterator().asScala
    iter
  }

  def getPartitionColumnValue(row: Array[AnyRef], partColIdx: Int,
      segmentProperties: SegmentProperties): AnyRef = {
    val dims: Array[Byte] = row(0).asInstanceOf[ByteArrayWrapper].getDictionaryKey
    val keyGen = segmentProperties.getDimensionKeyGenerator
    val keyArray: Array[Long] = keyGen.getKeyArray(dims)
    val encodings = partitionColumn.getEncodingList
    val partitionType = partitionInfo.getPartitionType
    var partitionValue: AnyRef = null
    val factor = 1000L
    if (isDimension) {
      // direct dictionary
      if (encodings.contains(Encoding.DIRECT_DICTIONARY)) {
        val directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(partitionDataType)
        val dictionaryIndex = dictionaryIndexGroup.indexOf(partColIdx)
        val surrogateValue = (keyArray(dictionaryIndex) / factor).toInt
        partitionValue = directDictionaryGenerator.getValueFromSurrogate(surrogateValue)
      } else if (!encodings.contains(Encoding.DICTIONARY)) {
        // no dictionary
        val byteArray = row(0).asInstanceOf[ByteArrayWrapper].getNoDictionaryKeys
        val index = noDictionaryIndexGroup.indexOf(partColIdx)
        partitionValue = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(byteArray(index)
          , partitionDataType)
        if (partitionValue.isInstanceOf[UTF8String]) {
          partitionValue = partitionValue.toString
        }
      } else {  // normal dictionary
        val dict = CarbonLoaderUtil.getDictionary(absoluteTableIdentifier,
          dimension.getColumnIdentifier, partitionDataType)
        if (partitionDataType == DataTypes.STRING) {
          if (partitionType == PartitionType.RANGE) {
            partitionValue = ByteUtil.
              toBytes(dict.getDictionaryValueForKey(keyArray(partColIdx).toInt))
          } else {
            partitionValue = dict.getDictionaryValueForKey(keyArray(partColIdx).toInt)
          }
        } else {
          partitionValue = dict.getDictionaryValueForKey(keyArray(partColIdx).toInt)
        }

      }
    } else {
      partitionValue = row(measureIndexGroup(partColIdx))
    }
    partitionValue
  }

  def indexInitialise(): Unit = {
      for (dim: CarbonDimension <- dimensions) {
        if (!dim.getEncoder.contains(Encoding.DICTIONARY)) {
          noDictionaryIndexGroup.append(dimensions.indexOf(dim))
        } else {
          dictionaryIndexGroup.append(dimensions.indexOf(dim))
        }
      }
      for (msr: CarbonMeasure <- measures) {
        // index of measure in row
        measureIndexGroup.append(measures.indexOf(msr) + 1)
      }
  }

  /**
   * get the index of partition column in dimension/measure
   * @param partitionColumnName
   * @param segmentProperties
   * @return
   */
  def getPartitionColumnIndex(partitionColumnName: String,
      segmentProperties: SegmentProperties): Int = {
    val dimensions = segmentProperties.getDimensions
    val measures = segmentProperties.getMeasures
    val columns = dimensions.asScala.map(_.getColName) ++ measures.asScala.map(_.getColName)
    var index = 0
    for (i <- 0 until columns.size) {
      if (columns(i) == partitionColumnName) {
        index = i
      }
    }
    if (index < dimensions.size()) {
      isDimension = true
      dimension = dimensions.get(index)
    } else {
      index = index - dimensions.size()
      measure = measures.get(index)
    }
    index
  }
}
