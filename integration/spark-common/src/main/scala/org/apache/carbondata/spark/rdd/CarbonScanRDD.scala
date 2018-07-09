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
import scala.reflect.ClassTag
import scala.util.Random
import scala.util.control.Breaks.{break, breakable}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.hive.DistributionUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.profiler.{GetPartition, Profiler, QueryTaskEnd}
import org.apache.spark.sql.util.SparkSQLUtil.sessionState

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.{CarbonCommonConstants, CarbonCommonConstantsInternal}
import org.apache.carbondata.core.datastore.block.Distributable
import org.apache.carbondata.core.indexstore.PartitionSpec
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.TableInfo
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.filter.FilterUtil
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.stats.{QueryStatistic, QueryStatisticsConstants, QueryStatisticsRecorder}
import org.apache.carbondata.core.statusmanager.FileFormat
import org.apache.carbondata.core.util._
import org.apache.carbondata.hadoop._
import org.apache.carbondata.hadoop.api.{CarbonFileInputFormat, CarbonInputFormat}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.InitInputMetrics
import org.apache.carbondata.spark.util.{SparkDataTypeConverterImpl, Util}
import org.apache.carbondata.streaming.{CarbonStreamInputFormat, CarbonStreamRecordReader}

/**
 * This RDD is used to perform query on CarbonData file. Before sending tasks to scan
 * CarbonData file, this RDD will leverage CarbonData's index information to do CarbonData file
 * level filtering in driver side.
 */
class CarbonScanRDD[T: ClassTag](
    @transient spark: SparkSession,
    val columnProjection: CarbonProjection,
    var filterExpression: Expression,
    identifier: AbsoluteTableIdentifier,
    @transient serializedTableInfo: Array[Byte],
    @transient tableInfo: TableInfo,
    inputMetricsStats: InitInputMetrics,
    @transient val partitionNames: Seq[PartitionSpec],
    val dataTypeConverterClz: Class[_ <: DataTypeConverter] = classOf[SparkDataTypeConverterImpl],
    val readSupportClz: Class[_ <: CarbonReadSupport[_]] = SparkReadSupport.readSupportClass)
  extends CarbonRDDWithTableInfo[T](spark.sparkContext, Nil, serializedTableInfo) {

  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }
  private var vectorReader = false

  private val bucketedTable = tableInfo.getFactTable.getBucketingInfo

  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def getPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    var partitions: Array[Partition] = Array.empty[Partition]
    var getSplitsStartTime: Long = -1
    var getSplitsEndTime: Long = -1
    var distributeStartTime: Long = -1
    var distributeEndTime: Long = -1
    val tablePath = tableInfo.getOrCreateAbsoluteTableIdentifier().getTablePath
    var numSegments = 0
    var numStreamSegments = 0
    var numBlocks = 0

    try {
      val conf = new Configuration()
      val jobConf = new JobConf(conf)
      SparkHadoopUtil.get.addCredentials(jobConf)
      val job = Job.getInstance(jobConf)
      val fileLevelExternal = tableInfo.getFactTable().getTableProperties().get("_filelevelformat")
      val format = if (fileLevelExternal != null && fileLevelExternal.equalsIgnoreCase("true")) {
        prepareFileInputFormatForDriver(job.getConfiguration)
      } else {
        prepareInputFormatForDriver(job.getConfiguration)
      }
      // initialise query_id for job
      job.getConfiguration.set("query.id", queryId)

      // get splits
      getSplitsStartTime = System.currentTimeMillis()
      val splits = format.getSplits(job)
      getSplitsEndTime = System.currentTimeMillis()
      if ((splits == null) && format.isInstanceOf[CarbonFileInputFormat[Object]]) {
        throw new SparkException(
          "CarbonData file not exist in the segment_null (SDK writer Output) path")
      }
      numSegments = format.getNumSegments
      numStreamSegments = format.getNumStreamSegments
      numBlocks = format.getNumBlocks

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
      distributeStartTime = System.currentTimeMillis()
      val batchPartitions = distributeColumnarSplits(columnarSplits)
      distributeEndTime = System.currentTimeMillis()
      // check and remove InExpression from filterExpression
      checkAndRemoveInExpressinFromFilterExpression(batchPartitions)
      if (streamSplits.isEmpty) {
        partitions = batchPartitions.toArray
      } else {
        val index = batchPartitions.length
        val streamPartitions: mutable.Buffer[Partition] =
          streamSplits.zipWithIndex.map { splitWithIndex =>
            val multiBlockSplit =
              new CarbonMultiBlockSplit(
                Seq(splitWithIndex._1.asInstanceOf[CarbonInputSplit]).asJava,
                splitWithIndex._1.getLocations,
                FileFormat.ROW_V1)
            new CarbonSparkPartition(id, splitWithIndex._2 + index, multiBlockSplit)
          }
        if (batchPartitions.isEmpty) {
          partitions = streamPartitions.toArray
        } else {
          logInfo(
            s"""
               | Identified no.of Streaming Blocks: ${ streamPartitions.size },
          """.stripMargin)
          // should keep the order by index of partition
          batchPartitions.appendAll(streamPartitions)
          partitions = batchPartitions.toArray
        }
      }
      partitions
    } finally {
      Profiler.invokeIfEnable {
        val endTime = System.currentTimeMillis()
        val executionId = spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
        if (executionId != null) {
          Profiler.send(
            GetPartition(
              executionId.toLong,
              tableInfo.getDatabaseName + "." + tableInfo.getFactTable.getTableName,
              tablePath,
              queryId,
              partitions.length,
              startTime,
              endTime,
              getSplitsStartTime,
              getSplitsEndTime,
              numSegments,
              numStreamSegments,
              numBlocks,
              distributeStartTime,
              distributeEndTime,
              if (filterExpression == null) "" else filterExpression.getStatement,
              if (columnProjection == null) "" else columnProjection.getAllColumns.mkString(",")
            )
          )
        }
      }
    }
  }

  private def distributeColumnarSplits(splits: List[InputSplit]): mutable.Buffer[Partition] = {
    // this function distributes the split based on following logic:
    // 1. based on data locality, to make split balanced on all available nodes
    // 2. if the number of split for one

    var statistic = new QueryStatistic()
    val statisticRecorder = CarbonTimeStatisticsFactory.createDriverRecorder()
    var parallelism = sparkContext.defaultParallelism
    val result = new ArrayList[Partition](parallelism)
    var noOfBlocks = 0
    var noOfNodes = 0
    var noOfTasks = 0

    if (!splits.isEmpty) {

      statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis)
      statisticRecorder.recordStatisticsForDriver(statistic, queryId)
      statistic = new QueryStatistic()
      val carbonDistribution = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION,
        CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_DEFAULT)
      // If bucketing is enabled on table then partitions should be grouped based on buckets.
      if (bucketedTable != null) {
        var i = 0
        val bucketed =
          splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).groupBy(f => f.getBucketId)
        (0 until bucketedTable.getNumOfRanges).map { bucketId =>
          val bucketPartitions = bucketed.getOrElse(bucketId.toString, Nil)
          val multiBlockSplit =
            new CarbonMultiBlockSplit(
              bucketPartitions.asJava,
              bucketPartitions.flatMap(_.getLocations).toArray)
          val partition = new CarbonSparkPartition(id, i, multiBlockSplit)
          i += 1
          result.add(partition)
        }
      } else {
        val useCustomDistribution =
          CarbonProperties.getInstance().getProperty(
            CarbonCommonConstants.CARBON_CUSTOM_BLOCK_DISTRIBUTION,
            "false").toBoolean ||
          carbonDistribution.equalsIgnoreCase(CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_CUSTOM)
        val enableSearchMode = CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE,
          CarbonCommonConstants.CARBON_SEARCH_MODE_ENABLE_DEFAULT).toBoolean
        if (useCustomDistribution || enableSearchMode) {
          if (enableSearchMode) {
            // force to assign only one task contains multiple splits each node
            parallelism = 0
          }
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
                  new CarbonMultiBlockSplit(splits.asJava, Array(node))
                val partition = new CarbonSparkPartition(id, i, multiBlockSplit)
                result.add(partition)
                i += 1
              }
            }
          }
          noOfNodes = nodeBlockMapping.size
        } else if (carbonDistribution.equalsIgnoreCase(
            CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_BLOCKLET)) {
          // Use blocklet distribution
          // Randomize the blocklets for better shuffling
          Random.shuffle(splits.asScala).zipWithIndex.foreach { splitWithIndex =>
            val multiBlockSplit =
              new CarbonMultiBlockSplit(
                Seq(splitWithIndex._1.asInstanceOf[CarbonInputSplit]).asJava,
                splitWithIndex._1.getLocations)
            val partition = new CarbonSparkPartition(id, splitWithIndex._2, multiBlockSplit)
            result.add(partition)
          }
        } else if (carbonDistribution.equalsIgnoreCase(
            CarbonCommonConstants.CARBON_TASK_DISTRIBUTION_MERGE_FILES)) {

          // sort blocks in reverse order of length
          val blockSplits = splits
            .asScala
            .map(_.asInstanceOf[CarbonInputSplit])
            .groupBy(f => f.getBlockPath)
            .map { blockSplitEntry =>
              new CarbonMultiBlockSplit(
                blockSplitEntry._2.asJava,
                blockSplitEntry._2.flatMap(f => f.getLocations).distinct.toArray)
            }.toArray.sortBy(_.getLength)(implicitly[Ordering[Long]].reverse)

          val defaultMaxSplitBytes = sessionState(spark).conf.filesMaxPartitionBytes
          val openCostInBytes = sessionState(spark).conf.filesOpenCostInBytes
          val defaultParallelism = spark.sparkContext.defaultParallelism
          val totalBytes = blockSplits.map(_.getLength + openCostInBytes).sum
          val bytesPerCore = totalBytes / defaultParallelism

          val maxSplitBytes = Math
            .min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerCore))
          LOGGER.info(s"Planning scan with bin packing, max size: $maxSplitBytes bytes, " +
                      s"open cost is considered as scanning $openCostInBytes bytes.")

          val currentFiles = new ArrayBuffer[CarbonMultiBlockSplit]
          var currentSize = 0L

          def closePartition(): Unit = {
            if (currentFiles.nonEmpty) {
              result.add(combineSplits(currentFiles, currentSize, result.size()))
            }
            currentFiles.clear()
            currentSize = 0
          }

          blockSplits.foreach { file =>
            if (currentSize + file.getLength > maxSplitBytes) {
              closePartition()
            }
            // Add the given file to the current partition.
            currentSize += file.getLength + openCostInBytes
            currentFiles += file
          }
          closePartition()
        } else {
          // Use block distribution
          splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).groupBy { f =>
            f.getSegmentId.concat(f.getBlockPath)
          }.values.zipWithIndex.foreach { splitWithIndex =>
            val multiBlockSplit =
              new CarbonMultiBlockSplit(
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

  def combineSplits(
      splits: ArrayBuffer[CarbonMultiBlockSplit],
      size: Long,
      partitionId: Int
  ): CarbonSparkPartition = {
    val carbonInputSplits = splits.flatMap(_.getAllSplits.asScala)

    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    splits.foreach { split =>
      split.getLocations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + split.getLength
      }
    }
    // Takes the first 3 hosts with the most data to be retrieved
    val locations = hostToNumBytes
      .toSeq
      .sortBy(_._2)(implicitly[Ordering[Long]].reverse)
      .take(3)
      .map(_._1)
      .toArray

    val multiBlockSplit = new CarbonMultiBlockSplit(carbonInputSplits.asJava, locations)
    new CarbonSparkPartition(id, partitionId, multiBlockSplit)
  }

  override def internalCompute(split: Partition, context: TaskContext): Iterator[T] = {
    val queryStartTime = System.currentTimeMillis
    val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
    if (null == carbonPropertiesFilePath) {
      System.setProperty("carbon.properties.filepath",
        System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties"
      )
    }
    val executionId = context.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    val taskId = split.index
    val attemptId = new TaskAttemptID(jobTrackerId, id, TaskType.MAP, split.index, 0)
    val attemptContext = new TaskAttemptContextImpl(new Configuration(), attemptId)
    val format = prepareInputFormatForExecutor(attemptContext.getConfiguration)
    val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
    TaskMetricsMap.getInstance().registerThreadCallback()
    inputMetricsStats.initBytesReadCallback(context, inputSplit)
    val iterator = if (inputSplit.getAllSplits.size() > 0) {
      val model = format.createQueryModel(inputSplit, attemptContext)
      // one query id per table
      model.setQueryId(queryId)
      // get RecordReader by FileFormat
      var reader: RecordReader[Void, Object] = inputSplit.getFileFormat match {
        case FileFormat.ROW_V1 =>
          // create record reader for row format
          DataTypeUtil.setDataTypeConverter(dataTypeConverterClz.newInstance())
          val inputFormat = new CarbonStreamInputFormat
          val streamReader = inputFormat.createRecordReader(inputSplit, attemptContext)
            .asInstanceOf[CarbonStreamRecordReader]
          streamReader.setVectorReader(vectorReader)
          streamReader.setInputMetricsStats(inputMetricsStats)
          model.setStatisticsRecorder(
            CarbonTimeStatisticsFactory.createExecutorRecorder(model.getQueryId))
          streamReader.setQueryModel(model)
          streamReader
        case _ =>
          // create record reader for CarbonData file format
          if (vectorReader) {
            val carbonRecordReader = createVectorizedCarbonRecordReader(model,
              inputMetricsStats,
              "true")
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

      val closeReader = () => {
        if (reader != null) {
          try {
            reader.close()
          } catch {
            case e: Exception =>
              LogServiceFactory.getLogService(this.getClass.getCanonicalName).error(e)
          }
          reader = null
        }
      }

      // add task completion before calling initialize as initialize method will internally call
      // for usage of unsafe method for processing of one blocklet and if there is any exception
      // while doing that the unsafe memory occupied for that task will not get cleared
      context.addTaskCompletionListener { _ =>
        closeReader.apply()
        close()
        logStatistics(executionId, taskId, queryStartTime, model.getStatisticsRecorder, split)
      }
      // create a statistics recorder
      val recorder = CarbonTimeStatisticsFactory.createExecutorRecorder(model.getQueryId())
      model.setStatisticsRecorder(recorder)
      // initialize the reader
      reader.initialize(inputSplit, attemptContext)

      new Iterator[Any] {
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
          if (finished) {
            closeReader.apply()
          }
          !finished
        }

        override def next(): Any = {
          if (!hasNext) {
            throw new java.util.NoSuchElementException("End of stream")
          }
          havePair = false
          val value = reader.getCurrentValue
          value
        }

      }
    } else {
      new Iterator[Any] {
        override def hasNext: Boolean = false

        override def next(): Any = throw new java.util.NoSuchElementException("End of stream")
      }
    }

    iterator.asInstanceOf[Iterator[T]]
  }

  private def close() {
    TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId)
    inputMetricsStats.updateAndClose()
  }

  def prepareInputFormatForDriver(conf: Configuration): CarbonTableInputFormat[Object] = {
    CarbonInputFormat.setTableInfo(conf, tableInfo)
    CarbonInputFormat.setDatabaseName(conf, tableInfo.getDatabaseName)
    CarbonInputFormat.setTableName(conf, tableInfo.getFactTable.getTableName)
    if (partitionNames != null) {
      CarbonInputFormat.setPartitionsToPrune(conf, partitionNames.asJava)
    }

    CarbonInputFormat.setTransactionalTable(conf, tableInfo.isTransactionalTable)
    createInputFormat(conf)
  }

  def prepareFileInputFormatForDriver(conf: Configuration): CarbonFileInputFormat[Object] = {
    CarbonInputFormat.setTableInfo(conf, tableInfo)
    CarbonInputFormat.setDatabaseName(conf, tableInfo.getDatabaseName)
    CarbonInputFormat.setTableName(conf, tableInfo.getFactTable.getTableName)
    if (partitionNames != null) {
      CarbonInputFormat.setPartitionsToPrune(conf, partitionNames.asJava)
    }
    createFileInputFormat(conf)
  }

  private def prepareInputFormatForExecutor(conf: Configuration): CarbonInputFormat[Object] = {
    CarbonInputFormat.setCarbonReadSupport(conf, readSupportClz)
    val tableInfo1 = getTableInfo
    CarbonInputFormat.setTableInfo(conf, tableInfo1)
    CarbonInputFormat.setDatabaseName(conf, tableInfo1.getDatabaseName)
    CarbonInputFormat.setTableName(conf, tableInfo1.getFactTable.getTableName)
    CarbonInputFormat.setDataTypeConverter(conf, dataTypeConverterClz)
    createInputFormat(conf)
  }

  private def createFileInputFormat(conf: Configuration): CarbonFileInputFormat[Object] = {
    val format = new CarbonFileInputFormat[Object]
    CarbonInputFormat.setTablePath(conf,
      identifier.appendWithLocalPrefix(identifier.getTablePath))
    CarbonInputFormat.setQuerySegment(conf, identifier)
    CarbonInputFormat.setFilterPredicates(conf, filterExpression)
    CarbonInputFormat.setColumnProjection(conf, columnProjection)
    CarbonInputFormatUtil.setDataMapJobIfConfigured(conf)

    // when validate segments is disabled in thread local update it to CarbonTableInputFormat
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (carbonSessionInfo != null) {
      val tableUniqueKey = identifier.getDatabaseName + "." + identifier.getTableName
      val validateInputSegmentsKey = CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                                     tableUniqueKey
      CarbonInputFormat.setValidateSegmentsToAccess(conf, carbonSessionInfo.getThreadParams
        .getProperty(validateInputSegmentsKey, "true").toBoolean)
      val queryOnPreAggStreamingKey = CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING +
                                      tableUniqueKey
      val queryOnPreAggStreaming = carbonSessionInfo.getThreadParams
        .getProperty(queryOnPreAggStreamingKey, "false").toBoolean
      val inputSegmentsKey = CarbonCommonConstants.CARBON_INPUT_SEGMENTS + tableUniqueKey
      CarbonInputFormat.setValidateSegmentsToAccess(conf, carbonSessionInfo.getThreadParams
        .getProperty(validateInputSegmentsKey, "true").toBoolean)
      CarbonInputFormat
        .setQuerySegment(conf,
          carbonSessionInfo.getThreadParams
            .getProperty(inputSegmentsKey,
              CarbonProperties.getInstance().getProperty(inputSegmentsKey, "*")))
      if (queryOnPreAggStreaming) {
        CarbonInputFormat.setAccessStreamingSegments(conf, queryOnPreAggStreaming)
        carbonSessionInfo.getThreadParams.removeProperty(queryOnPreAggStreamingKey)
        carbonSessionInfo.getThreadParams.removeProperty(inputSegmentsKey)
        carbonSessionInfo.getThreadParams.removeProperty(validateInputSegmentsKey)
      }
    }
    format
  }


  private def createInputFormat(conf: Configuration): CarbonTableInputFormat[Object] = {
    val format = new CarbonTableInputFormat[Object]
    CarbonInputFormat.setTablePath(conf,
      identifier.appendWithLocalPrefix(identifier.getTablePath))
    CarbonInputFormat.setQuerySegment(conf, identifier)
    CarbonInputFormat.setFilterPredicates(conf, filterExpression)
    CarbonInputFormat.setColumnProjection(conf, columnProjection)
    CarbonInputFormatUtil.setDataMapJobIfConfigured(conf)
    // when validate segments is disabled in thread local update it to CarbonTableInputFormat
    val carbonSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (carbonSessionInfo != null) {
      val tableUniqueKey = identifier.getDatabaseName + "." + identifier.getTableName
      val validateInputSegmentsKey = CarbonCommonConstants.VALIDATE_CARBON_INPUT_SEGMENTS +
                                     tableUniqueKey
      CarbonInputFormat.setValidateSegmentsToAccess(conf, carbonSessionInfo.getThreadParams
        .getProperty(validateInputSegmentsKey, "true").toBoolean)
      val queryOnPreAggStreamingKey = CarbonCommonConstantsInternal.QUERY_ON_PRE_AGG_STREAMING +
                                      tableUniqueKey
      val queryOnPreAggStreaming = carbonSessionInfo.getThreadParams
        .getProperty(queryOnPreAggStreamingKey, "false").toBoolean
      val inputSegmentsKey = CarbonCommonConstants.CARBON_INPUT_SEGMENTS + tableUniqueKey
      CarbonInputFormat.setValidateSegmentsToAccess(conf, carbonSessionInfo.getThreadParams
        .getProperty(validateInputSegmentsKey, "true").toBoolean)
      CarbonInputFormat
        .setQuerySegment(conf,
          carbonSessionInfo.getThreadParams
            .getProperty(inputSegmentsKey,
              CarbonProperties.getInstance().getProperty(inputSegmentsKey, "*")))
      if (queryOnPreAggStreaming) {
        CarbonInputFormat.setAccessStreamingSegments(conf, queryOnPreAggStreaming)
        carbonSessionInfo.getThreadParams.removeProperty(queryOnPreAggStreamingKey)
        carbonSessionInfo.getThreadParams.removeProperty(inputSegmentsKey)
        carbonSessionInfo.getThreadParams.removeProperty(validateInputSegmentsKey)
      }
    }
    format
  }

  def logStatistics(
      executionId: String,
      taskId: Long,
      queryStartTime: Long,
      recorder: QueryStatisticsRecorder,
      split: Partition
  ): Unit = {
    if (null != recorder) {
      val queryStatistic = new QueryStatistic()
      queryStatistic.addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
        System.currentTimeMillis - queryStartTime)
      recorder.recordStatistics(queryStatistic)
      // print executor query statistics for each task_id
      val statistics = recorder.statisticsForTask(taskId, queryStartTime)
      if (statistics != null && executionId != null) {
        Profiler.invokeIfEnable {
          val inputSplit = split.asInstanceOf[CarbonSparkPartition].split.value
          inputSplit.calculateLength()
          val size = inputSplit.getLength
          val files = inputSplit.getAllSplits.asScala.map { s =>
            s.getSegmentId + "/" + s.getPath.getName
          }.toArray[String]
          Profiler.send(
            QueryTaskEnd(
              executionId.toLong,
              queryId,
              statistics.getValues,
              size,
              files
            )
          )
        }
      }
      recorder.logStatisticsForTask(statistics)
    }
  }

  /**
   * This method will check and remove InExpression from filterExpression to prevent the List
   * Expression values from serializing and deserializing on executor
   *
   * @param identifiedPartitions
   */
  private def checkAndRemoveInExpressinFromFilterExpression(
      identifiedPartitions: mutable.Buffer[Partition]) = {
    if (null != filterExpression) {
      if (identifiedPartitions.nonEmpty &&
          !checkForBlockWithoutBlockletInfo(identifiedPartitions)) {
        FilterUtil.removeInExpressionNodeWithPositionIdColumn(filterExpression)
      }
    }
  }

  /**
   * This method will check for presence of any block from old store (version 1.1). If any of the
   * blocks identified does not contain the blocklet info that means that block is from old store
   *
   * @param identifiedPartitions
   * @return
   */
  private def checkForBlockWithoutBlockletInfo(
      identifiedPartitions: mutable.Buffer[Partition]): Boolean = {
    var isBlockWithoutBlockletInfoPresent = false
    breakable {
      identifiedPartitions.foreach { value =>
        val inputSplit = value.asInstanceOf[CarbonSparkPartition].split.value
        val splitList = if (inputSplit.isInstanceOf[CarbonMultiBlockSplit]) {
          inputSplit.asInstanceOf[CarbonMultiBlockSplit].getAllSplits
        } else {
          new java.util.ArrayList().add(inputSplit.asInstanceOf[CarbonInputSplit])
        }.asInstanceOf[java.util.List[CarbonInputSplit]]
        // check for block from old store (version 1.1 and below)
        if (Util.isBlockWithoutBlockletInfoExists(splitList)) {
          isBlockWithoutBlockletInfoPresent = true
          break
        }
      }
    }
    isBlockWithoutBlockletInfoPresent
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
      inputMetricsStats: InputMetricsStats, enableBatch: String): RecordReader[Void, Object] = {
    val name = "org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader"
    try {
      val cons = Class.forName(name).getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance(queryModel, inputMetricsStats, enableBatch)
        .asInstanceOf[RecordReader[Void, Object]]
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
