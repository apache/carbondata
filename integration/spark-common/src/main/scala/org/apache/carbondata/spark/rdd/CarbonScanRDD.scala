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

import java.text.SimpleDateFormat
import java.util.{ArrayList, Date, List}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.Distributable
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.stats.{QueryStatistic, QueryStatisticsConstants, QueryStatisticsRecorder}
import org.apache.carbondata.core.statusmanager.FileFormat
import org.apache.carbondata.core.util._
import org.apache.carbondata.hadoop._
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.streaming.{CarbonStreamInputFormat, CarbonStreamRecordReader}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.InitInputMetrics
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl

/**
 * This RDD is used to perform query on CarbonData file. Before sending tasks to scan
 * CarbonData file, this RDD will leverage CarbonData's index information to do CarbonData file
 * level filtering in driver side.
 */
class CarbonScanRDD(
    @transient sc: SparkContext,
    val columnProjection: CarbonProjection,
    var filterExpression: Expression,
    identifier: AbsoluteTableIdentifier,
    @transient serializedTableInfo: Array[Byte],
    @transient tableInfo: TableInfo,
    inputMetricsStats: InitInputMetrics,
    @transient val partitionNames: Seq[String])
  extends CarbonRDDWithTableInfo[InternalRow](sc, Nil, serializedTableInfo) {

  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }
  private var vectorReader = false

  private val readSupport = SparkReadSupport.readSupportClass

  private val bucketedTable = tableInfo.getFactTable.getBucketingInfo

  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def getPartitions: Array[Partition] = {
    val job = Job.getInstance(new Configuration())
    val format = prepareInputFormatForDriver(job.getConfiguration)

    // initialise query_id for job
    job.getConfiguration.set("query.id", queryId)

    // get splits
    val splits = format.getSplits(job)

    // separate split
    // 1. for batch splits, invoke distributeSplits method to create partitions
    // 2. for stream splits, create partition for each split by default
    val columnarSplits = new ArrayList[InputSplit]()
    val streamSplits = new ArrayBuffer[InputSplit]()
    splits.asScala.foreach { split =>
      val carbonInputSplit = split.asInstanceOf[CarbonInputSplit]
      if (FileFormat.ROW_V1 == carbonInputSplit.getFileFormat) {
        streamSplits += split
      } else {
        columnarSplits.add(split)
      }
    }
    val batchPartitions = distributeColumnarSplits(columnarSplits)
    if (streamSplits.isEmpty) {
      batchPartitions.toArray
    } else {
      val index = batchPartitions.length
      val streamPartitions: mutable.Buffer[Partition] =
        streamSplits.zipWithIndex.map { splitWithIndex =>
          val multiBlockSplit =
            new CarbonMultiBlockSplit(identifier,
              Seq(splitWithIndex._1.asInstanceOf[CarbonInputSplit]).asJava,
              splitWithIndex._1.getLocations,
              FileFormat.ROW_V1)
          new CarbonSparkPartition(id, splitWithIndex._2 + index, multiBlockSplit)
        }
      if (batchPartitions.isEmpty) {
        streamPartitions.toArray
      } else {
        // should keep the order by index of partition
        batchPartitions.appendAll(streamPartitions)
        batchPartitions.toArray
      }
    }
  }

  private def distributeColumnarSplits(splits: List[InputSplit]): mutable.Buffer[Partition] = {
    // this function distributes the split based on following logic:
    // 1. based on data locality, to make split balanced on all available nodes
    // 2. if the number of split for one

    var statistic = new QueryStatistic()
    val statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    val parallelism = sparkContext.defaultParallelism
    val result = new ArrayList[Partition](parallelism)
    var noOfBlocks = 0
    var noOfNodes = 0
    var noOfTasks = 0

    if (!splits.isEmpty) {

      statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis)
      statisticRecorder.recordStatisticsForDriver(statistic, queryId)
      statistic = new QueryStatistic()

      // If bucketing is enabled on table then partitions should be grouped based on buckets.
      if (bucketedTable != null) {
        var i = 0
        val bucketed =
          splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).groupBy(f => f.getBucketId)
        (0 until bucketedTable.getNumberOfBuckets).map { bucketId =>
          val bucketPartitions = bucketed.getOrElse(bucketId.toString, Nil)
          val multiBlockSplit =
            new CarbonMultiBlockSplit(identifier,
              bucketPartitions.asJava,
              bucketPartitions.flatMap(_.getLocations).toArray)
          val partition = new CarbonSparkPartition(id, i, multiBlockSplit)
          i += 1
          result.add(partition)
        }
      } else if (CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION,
          CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION_DEFAULT).toBoolean) {
        // create a list of block based on split
        val blockList = splits.asScala.map(_.asInstanceOf[Distributable])

        // get the list of executors and map blocks to executors based on locality
        val activeNodes = DistributionUtil.ensureExecutorsAndGetNodeList(blockList, sparkContext)

        // divide the blocks among the tasks of the nodes as per the data locality
        val nodeBlockMapping = CarbonLoaderUtil.nodeBlockTaskMapping(blockList.asJava, -1,
          parallelism, activeNodes.toList.asJava)
        var i = 0
        // Create Spark Partition for each task and assign blocks
        nodeBlockMapping.asScala.foreach { case (node, blockList) =>
          blockList.asScala.foreach { blocksPerTask =>
            val splits = blocksPerTask.asScala.map(_.asInstanceOf[CarbonInputSplit])
            if (blocksPerTask.size() != 0) {
              val multiBlockSplit =
                new CarbonMultiBlockSplit(identifier, splits.asJava, Array(node))
              val partition = new CarbonSparkPartition(id, i, multiBlockSplit)
              result.add(partition)
              i += 1
            }
          }
        }
        noOfNodes = nodeBlockMapping.size
      } else {
        if (CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_USE_BLOCKLET_DISTRIBUTION,
            CarbonCommonConstants.CARBON_USE_BLOCKLET_DISTRIBUTION_DEFAULT).toBoolean) {
          // Use blocklet distribution
          // Randomize the blocklets for better shuffling
          Random.shuffle(splits.asScala).zipWithIndex.foreach { splitWithIndex =>
            val multiBlockSplit =
              new CarbonMultiBlockSplit(identifier,
                Seq(splitWithIndex._1.asInstanceOf[CarbonInputSplit]).asJava,
                splitWithIndex._1.getLocations)
            val partition = new CarbonSparkPartition(id, splitWithIndex._2, multiBlockSplit)
            result.add(partition)
          }
        } else {
          // Use block distribution
          splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).
            groupBy(f => f.getBlockPath).values.zipWithIndex.foreach { splitWithIndex =>
            val multiBlockSplit =
              new CarbonMultiBlockSplit(identifier,
                splitWithIndex._1.asJava,
                splitWithIndex._1.flatMap(f => f.getLocations).distinct.toArray)
            val partition = new CarbonSparkPartition(id, splitWithIndex._2, multiBlockSplit)
            result.add(partition)
          }
        }
      }

      noOfBlocks = splits.size
      noOfTasks = result.size()

      statistic.addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION,
        System.currentTimeMillis)
      statisticRecorder.recordStatisticsForDriver(statistic, queryId)
      statisticRecorder.logStatisticsAsTableDriver()
    }
    logInfo(
      s"""
         | Identified no.of.blocks: $noOfBlocks,
         | no.of.tasks: $noOfTasks,
         | no.of.nodes: $noOfNodes,
         | parallelism: $parallelism
       """.stripMargin)
    result.asScala
  }

  override def internalCompute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val queryStartTime = System.currentTimeMillis
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties"
      )
    }

    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val format = prepareInputFormatForExecutor(attemptContext.getConfiguration)
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    TaskMetricsMap.getInstance().registerThreadCallback()
    inputMetricsStats.initBytesReadCallback(context, inputSplit)
    val iterator = if (inputSplit.getAllSplits.size() > 0) {
      val model = format.getQueryModel(inputSplit, attemptContext)
      // get RecordReader by FileFormat
      val reader: RecordReader[Void, Object] = inputSplit.getFileFormat match {
        case FileFormat.ROW_V1 =>
          // create record reader for row format
          DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
          val inputFormat = new CarbonStreamInputFormat
          val streamReader = inputFormat.createRecordReader(inputSplit, attemptContext)
            .asInstanceOf[CarbonStreamRecordReader]
          streamReader.setVectorReader(vectorReader)
          model.setStatisticsRecorder(
            CarbonTimeStatisticsFactory.createExecutorRecorder(model.getQueryId))
          streamReader.setQueryModel(model)
          streamReader
        case _ =>
          // create record reader for CarbonData file format
          if (vectorReader) {
            val carbonRecordReader = createVectorizedCarbonRecordReader(model, inputMetricsStats)
            if (carbonRecordReader == null) {
              new CarbonRecordReader(model,
                format.getReadSupportClass(attemptContext.getConfiguration), inputMetricsStats)
            } else {
              carbonRecordReader
            }
          } else {
            new CarbonRecordReader(model,
              format.getReadSupportClass(attemptContext.getConfiguration),
              inputMetricsStats)
          }
      }

      reader.initialize(inputSplit, attemptContext)

      new Iterator[Any] {
        private var havePair = false
        private var finished = false

        context.addTaskCompletionListener { _ =>
          reader.close()
          close()
          logStatistics(queryStartTime, model.getStatisticsRecorder)
        }

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

        override def next(): Any = {
          if (!hasNext) {
            throw new java.util.NoSuchElementException("End of stream")
          }
          havePair = false
          val value = reader.getCurrentValue
          inputMetricsStats.updateByValue(value)
          value
        }

        private def close() {
          TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId)
          inputMetricsStats.updateAndClose()
        }
      }
    } else {
      new Iterator[Any] {
        override def hasNext: Boolean = false

        override def next(): Any = throw new java.util.NoSuchElementException("End of stream")
      }
    }


    iterator.asInstanceOf[Iterator[InternalRow]]
  }

  def prepareInputFormatForDriver(conf: Configuration): CarbonTableInputFormat[Object] = {
    CarbonTableInputFormat.setTableInfo(conf, tableInfo)
    CarbonTableInputFormat.setDatabaseName(conf, tableInfo.getDatabaseName)
    CarbonTableInputFormat.setTableName(conf, tableInfo.getFactTable.getTableName)
    if (partitionNames != null) {
      CarbonTableInputFormat.setPartitionsToPrune(conf, partitionNames.asJava)
    }
    createInputFormat(conf)
  }

  private def prepareInputFormatForExecutor(conf: Configuration): CarbonTableInputFormat[Object] = {
    CarbonTableInputFormat.setCarbonReadSupport(conf, readSupport)
    val tableInfo1 = getTableInfo
    CarbonTableInputFormat.setTableInfo(conf, tableInfo1)
    CarbonTableInputFormat.setDatabaseName(conf, tableInfo1.getDatabaseName)
    CarbonTableInputFormat.setTableName(conf, tableInfo1.getFactTable.getTableName)
    CarbonTableInputFormat.setDataTypeConverter(conf, new SparkDataTypeConverterImpl)
    createInputFormat(conf)
  }

  private def createInputFormat(conf: Configuration): CarbonTableInputFormat[Object] = {
    val format = new CarbonTableInputFormat[Object]
    CarbonTableInputFormat.setTablePath(conf,
      identifier.appendWithLocalPrefix(identifier.getTablePath))
    CarbonTableInputFormat.setQuerySegment(conf, identifier)
    CarbonTableInputFormat.setFilterPredicates(conf, filterExpression)
    CarbonTableInputFormat.setColumnProjection(conf, columnProjection)
    if (CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP,
        CarbonCommonConstants.USE_DISTRIBUTED_DATAMAP_DEFAULT).toBoolean) {
      CarbonTableInputFormat.setDataMapJob(conf, new SparkDataMapJob)
    }

    // when validate segments is disabled in thread local update it to CarbonTableInputFormat
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (carbonSessionInfo != null) {
      CarbonTableInputFormat.setValidateSegmentsToAccess(conf, carbonSessionInfo.getSessionParams
          .getProperty(CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                       identifier.getCarbonTableIdentifier.getDatabaseName + "." +
                       identifier.getCarbonTableIdentifier.getTableName, "true").toBoolean)
    }
    format
  }

  def logStatistics(queryStartTime: Long, recorder: QueryStatisticsRecorder): Unit = {
    val queryStatistic = new QueryStatistic()
    queryStatistic.addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
      System.currentTimeMillis - queryStartTime)
    recorder.recordStatistics(queryStatistic)
    // print executor query statistics for each task_id
    recorder.logStatisticsAsTableExecutor()
  }

  /**
   * Get the preferred locations where to launch this task.
   */
  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkPartition]
    val firstOptionLocation = theSplit.split.value.getLocations.filter(_ != "localhost")
    firstOptionLocation
  }

  def createVectorizedCarbonRecordReader(queryModel: QueryModel,
      inputMetricsStats: InputMetricsStats): RecordReader[Void, Object] = {
    val name = "org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader"
    try {
      val cons = Class.forName(name).getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance(queryModel, inputMetricsStats).asInstanceOf[RecordReader[Void, Object]]
    } catch {
      case e: Exception =>
        LOGGER.error(e)
        null
    }
  }

  // TODO find the better way set it.
  def setVectorReaderSupport(boolean: Boolean): Unit = {
    vectorReader = boolean
  }
}
