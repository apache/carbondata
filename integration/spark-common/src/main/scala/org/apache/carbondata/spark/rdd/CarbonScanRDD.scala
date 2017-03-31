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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block.Distributable
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.expression.Expression
import org.apache.carbondata.core.scan.model.QueryModel
import org.apache.carbondata.core.stats.{QueryStatistic, QueryStatisticsConstants, QueryStatisticsRecorder}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonTimeStatisticsFactory}
import org.apache.carbondata.hadoop._
import org.apache.carbondata.spark.load.CarbonLoaderUtil


/**
 * This RDD is used to perform query on CarbonData file. Before sending tasks to scan
 * CarbonData file, this RDD will leverage CarbonData's index information to do CarbonData file
 * level filtering in driver side.
 */
class CarbonScanRDD(
    @transient sc: SparkContext,
    columnProjection: CarbonProjection,
    filterExpression: Expression,
    identifier: AbsoluteTableIdentifier,
    @transient carbonTable: CarbonTable)
  extends RDD[InternalRow](sc, Nil) {

  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  private val jobTrackerId: String = {
    val formatter = new SimpleDateFormat("yyyyMMddHHmm")
    formatter.format(new Date())
  }
  private var vectorReader = false

  private val readSupport = SparkReadSupport.readSupportClass

  private val bucketedTable = carbonTable.getBucketingInfo(carbonTable.getFactTableName)

  @transient private val jobId = new JobID(jobTrackerId, id)
  @transient val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)

  override def getPartitions: Array[Partition] = {
    val job = Job.getInstance(new Configuration())
    val format = prepareInputFormatForDriver(job.getConfiguration)

    // initialise query_id for job
    job.getConfiguration.set("query.id", queryId)

    // get splits
    val splits = format.getSplits(job)
    val result = distributeSplits(splits)
    result
  }

  private def distributeSplits(splits: List[InputSplit]): Array[Partition] = {
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
        splits.asScala.zipWithIndex.foreach { splitWithIndex =>
          val multiBlockSplit =
            new CarbonMultiBlockSplit(identifier,
              Seq(splitWithIndex._1.asInstanceOf[CarbonInputSplit]).asJava,
              splitWithIndex._1.getLocations)
          val partition = new CarbonSparkPartition(id, splitWithIndex._2, multiBlockSplit)
          result.add(partition)
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
    result.toArray(new Array[Partition](result.size()))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
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
    val iterator = if (inputSplit.getAllSplits.size() > 0) {
      val model = format.getQueryModel(inputSplit, attemptContext)
      val reader = {
        if (vectorReader) {
          val carbonRecordReader = createVectorizedCarbonRecordReader(model)
          if (carbonRecordReader == null) {
            new CarbonRecordReader(model,
              format.getReadSupportClass(attemptContext.getConfiguration))
          } else {
            carbonRecordReader
          }
        } else {
          new CarbonRecordReader(model, format.getReadSupportClass(attemptContext.getConfiguration))
        }
      }

      reader.initialize(inputSplit, attemptContext)
      val queryStartTime = System.currentTimeMillis

      new Iterator[Any] {
        private var havePair = false
        private var finished = false
        private var count = 0

        context.addTaskCompletionListener { context =>
          logStatistics(queryStartTime, count, model.getStatisticsRecorder)
          reader.close()
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
          count += 1
          value
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

  private def prepareInputFormatForDriver(conf: Configuration): CarbonInputFormat[Object] = {
    CarbonInputFormat.setCarbonTable(conf, carbonTable)
    createInputFormat(conf)
  }

  private def prepareInputFormatForExecutor(conf: Configuration): CarbonInputFormat[Object] = {
    CarbonInputFormat.setCarbonReadSupport(conf, readSupport)
    createInputFormat(conf)
  }

  private def createInputFormat(conf: Configuration): CarbonInputFormat[Object] = {
    val format = new CarbonInputFormat[Object]
    CarbonInputFormat.setTablePath(conf, identifier.appendWithLocalPrefix(identifier.getTablePath))
    CarbonInputFormat.setFilterPredicates(conf, filterExpression)
    CarbonInputFormat.setColumnProjection(conf, columnProjection)
    format
  }

  def logStatistics(queryStartTime: Long, recordCount: Int,
      recorder: QueryStatisticsRecorder): Unit = {
    var queryStatistic = new QueryStatistic()
    queryStatistic.addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
      System.currentTimeMillis - queryStartTime)
    recorder.recordStatistics(queryStatistic)
    // result size
    queryStatistic = new QueryStatistic()
    queryStatistic.addCountStatistic(QueryStatisticsConstants.RESULT_SIZE, recordCount)
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

  def createVectorizedCarbonRecordReader(queryModel: QueryModel): RecordReader[Void, Object] = {
    val name = "org.apache.carbondata.spark.vectorreader.VectorizedCarbonRecordReader"
    try {
      val cons = Class.forName(name).getDeclaredConstructors
      cons.head.setAccessible(true)
      cons.head.newInstance(queryModel).asInstanceOf[RecordReader[Void, Object]]
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
