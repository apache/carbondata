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

import java.util

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.CarbonIterator
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.dictionary.Dictionary
import org.apache.carbondata.core.carbon.datastore.block.{BlockletInfos, TableBlockInfo}
import org.apache.carbondata.core.carbon.querystatistics.{QueryStatistic, QueryStatisticsConstants, QueryStatisticsRecorder}
import org.apache.carbondata.core.util.CarbonTimeStatisticsFactory
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.apache.carbondata.scan.executor.QueryExecutorFactory
import org.apache.carbondata.scan.expression.Expression
import org.apache.carbondata.scan.model.QueryModel
import org.apache.carbondata.scan.result.BatchResult
import org.apache.carbondata.scan.result.iterator.ChunkRowIterator
import org.apache.carbondata.spark.RawValue
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.util.QueryPlanUtil

class CarbonSparkPartition(rddId: Int, val idx: Int,
    val locations: Array[String],
    val tableBlockInfos: util.List[TableBlockInfo])
  extends Partition {

  override val index: Int = idx

  // val serializableHadoopSplit = new SerializableWritable[Array[String]](locations)
  override def hashCode(): Int = {
    41 * (41 + rddId) + idx
  }
}

/**
 * This RDD is used to perform query on CarbonData file. Before sending tasks to scan
 * CarbonData file, this RDD will leverage CarbonData's index information to do CarbonData file
 * level filtering in driver side.
 */
class CarbonScanRDD[V: ClassTag](
    sc: SparkContext,
    queryModel: QueryModel,
    filterExpression: Expression,
    keyClass: RawValue[V],
    @transient conf: Configuration,
    tableCreationTime: Long,
    schemaLastUpdatedTime: Long,
    baseStoreLocation: String)
  extends RDD[V](sc, Nil) with Logging {


  override def getPartitions: Array[Partition] = {
    var defaultParallelism = sparkContext.defaultParallelism
    val statisticRecorder = CarbonTimeStatisticsFactory.getQueryStatisticsRecorderInstance()
    val (carbonInputFormat: CarbonInputFormat[Array[Object]], job: Job) =
      QueryPlanUtil.createCarbonInputFormat(queryModel.getAbsoluteTableIdentifier)

    val result = new util.ArrayList[Partition](defaultParallelism)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    // set filter resolver tree
    try {
      // before applying filter check whether segments are available in the table.
      val splits = carbonInputFormat.getSplits(job)
      if (!splits.isEmpty) {
        val filterResolver = carbonInputFormat
          .getResolvedFilter(job.getConfiguration, filterExpression)
        CarbonInputFormat.setFilterPredicates(job.getConfiguration, filterResolver)
        queryModel.setFilterExpressionResolverTree(filterResolver)
      }
    }
    catch {
      case e: Exception =>
        LOGGER.error(e)
        sys.error("Exception occurred in query execution :: " + e.getMessage)
    }
    // get splits
    val splits = carbonInputFormat.getSplits(job)
    if (!splits.isEmpty) {
      val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

      val blockListTemp = carbonInputSplits.map(inputSplit =>
        new TableBlockInfo(inputSplit.getPath.toString,
          inputSplit.getStart, inputSplit.getSegmentId,
          inputSplit.getLocations, inputSplit.getLength,
          new BlockletInfos(inputSplit.getNumberOfBlocklets, 0, inputSplit.getNumberOfBlocklets)
        )
      )
      var activeNodes = Array[String]()
      if(blockListTemp.nonEmpty) {
         activeNodes = DistributionUtil
          .ensureExecutorsAndGetNodeList(blockListTemp.toArray, sparkContext)
      }
      defaultParallelism = sparkContext.defaultParallelism
      val blockList = CarbonLoaderUtil.
        distributeBlockLets(blockListTemp.asJava, defaultParallelism).asScala

      if (blockList.nonEmpty) {
        var statistic = new QueryStatistic()
        // group blocks to nodes, tasks
        val nodeBlockMapping =
          CarbonLoaderUtil.nodeBlockTaskMapping(blockList.asJava, -1, defaultParallelism,
            activeNodes.toList.asJava
          )
        statistic.addStatistics(QueryStatisticsConstants.BLOCK_ALLOCATION, System.currentTimeMillis)
        statisticRecorder.recordStatisticsForDriver(statistic, queryModel.getQueryId())
        statistic = new QueryStatistic()
        var i = 0
        // Create Spark Partition for each task and assign blocks
        nodeBlockMapping.asScala.foreach { entry =>
          entry._2.asScala.foreach { blocksPerTask => {
            val tableBlockInfo = blocksPerTask.asScala.map(_.asInstanceOf[TableBlockInfo])
            if (blocksPerTask.size() != 0) {
              result
                .add(new CarbonSparkPartition(id, i, Seq(entry._1).toArray, tableBlockInfo.asJava))
              i += 1
            }
          }
          }
        }
        val noOfBlocks = blockList.size
        val noOfNodes = nodeBlockMapping.size
        val noOfTasks = result.size()
        logInfo(s"Identified  no.of.Blocks: $noOfBlocks,"
                + s"parallelism: $defaultParallelism , " +
                s"no.of.nodes: $noOfNodes, no.of.tasks: $noOfTasks"
        )
        statistic.addStatistics(QueryStatisticsConstants.BLOCK_IDENTIFICATION,
          System.currentTimeMillis)
        statisticRecorder.recordStatisticsForDriver(statistic, queryModel.getQueryId())
        statisticRecorder.logStatisticsAsTableDriver()
        result.asScala.foreach { r =>
          val cp = r.asInstanceOf[CarbonSparkPartition]
          logInfo(s"Node : " + cp.locations.toSeq.mkString(",")
                  + ", No.Of Blocks : " + cp.tableBlockInfos.size()
          )
        }
      } else {
        logInfo("No blocks identified to scan")
      }
    }
    else {
      logInfo("No valid segments found to scan")
    }
    result.toArray(new Array[Partition](result.size()))
  }

  override def compute(thepartition: Partition, context: TaskContext): Iterator[V] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[V] {
      var rowIterator: CarbonIterator[Array[Any]] = _
      var queryStartTime: Long = 0
      try {
        val carbonSparkPartition = thepartition.asInstanceOf[CarbonSparkPartition]
        if(!carbonSparkPartition.tableBlockInfos.isEmpty) {
          queryModel.setQueryId(queryModel.getQueryId + "_" + carbonSparkPartition.idx)
          // fill table block info
          queryModel.setTableBlockInfos(carbonSparkPartition.tableBlockInfos)
          queryStartTime = System.currentTimeMillis

          val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
          logInfo("*************************" + carbonPropertiesFilePath)
          if (null == carbonPropertiesFilePath) {
            System.setProperty("carbon.properties.filepath",
              System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties")
          }
          // execute query
          rowIterator = new ChunkRowIterator(
            QueryExecutorFactory.getQueryExecutor(queryModel).execute(queryModel).
              asInstanceOf[CarbonIterator[BatchResult]]).asInstanceOf[CarbonIterator[Array[Any]]]

        }
      } catch {
        case e: Exception =>
          LOGGER.error(e)
          if (null != e.getMessage) {
            sys.error("Exception occurred in query execution :: " + e.getMessage)
          } else {
            sys.error("Exception occurred in query execution.Please check logs.")
          }
      }

      var havePair = false
      var finished = false
      var recordCount = 0

      override def hasNext: Boolean = {
        if (!finished && !havePair) {
          finished = (null == rowIterator) || (!rowIterator.hasNext)
          havePair = !finished
        }
        if (finished) {
          clearDictionaryCache(queryModel.getColumnToDictionaryMapping)
          if (null != queryModel.getStatisticsRecorder) {
            var queryStatistic = new QueryStatistic()
            queryStatistic
              .addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
                System.currentTimeMillis - queryStartTime
              )
            queryModel.getStatisticsRecorder.recordStatistics(queryStatistic)
            // result size
            queryStatistic = new QueryStatistic()
            queryStatistic.addCountStatistic(QueryStatisticsConstants.RESULT_SIZE, recordCount)
            queryModel.getStatisticsRecorder.recordStatistics(queryStatistic)
            // print executor query statistics for each task_id
            queryModel.getStatisticsRecorder.logStatisticsAsTableExecutor()
          }
        }
        !finished
      }

      override def next(): V = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        recordCount += 1
        if (queryModel.getLimit != -1 && recordCount >= queryModel.getLimit) {
          clearDictionaryCache(queryModel.getColumnToDictionaryMapping)
          if (null != queryModel.getStatisticsRecorder) {
            var queryStatistic = new QueryStatistic()
            queryStatistic
              .addFixedTimeStatistic(QueryStatisticsConstants.EXECUTOR_PART,
                System.currentTimeMillis - queryStartTime
              )
            queryModel.getStatisticsRecorder.recordStatistics(queryStatistic)
            // result size
            queryStatistic = new QueryStatistic()
            queryStatistic.addCountStatistic(QueryStatisticsConstants.RESULT_SIZE, recordCount)
            queryModel.getStatisticsRecorder.recordStatistics(queryStatistic)
            // print executor query statistics for each task_id
            queryModel.getStatisticsRecorder.logStatisticsAsTableExecutor()
          }
        }
        keyClass.getValue(rowIterator.next())
      }
      def clearDictionaryCache(columnToDictionaryMap: java.util.Map[String, Dictionary]) = {
        if (null != columnToDictionaryMap) {
          org.apache.carbondata.spark.util.CarbonQueryUtil
            .clearColumnDictionaryCache(columnToDictionaryMap)
        }
      }
    }
    iter
  }

  /**
   * Get the preferred locations where to launch this task.
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val theSplit = partition.asInstanceOf[CarbonSparkPartition]
    val firstOptionLocation = theSplit.locations.filter(_ != "localhost")
    val tableBlocks = theSplit.tableBlockInfos
    // node name and count mapping
    val blockMap = new util.LinkedHashMap[String, Integer]()

    tableBlocks.asScala.foreach(tableBlock => tableBlock.getLocations.foreach(
      location => {
        if (!firstOptionLocation.exists(location.equalsIgnoreCase(_))) {
          val currentCount = blockMap.get(location)
          if (currentCount == null) {
            blockMap.put(location, 1)
          } else {
            blockMap.put(location, currentCount + 1)
          }
        }
      }
    )
    )

    val sortedList = blockMap.entrySet().asScala.toSeq.sortWith((nodeCount1, nodeCount2) => {
      nodeCount1.getValue > nodeCount2.getValue
    }
    )

    val sortedNodesList = sortedList.map(nodeCount => nodeCount.getKey).take(2)
    firstOptionLocation ++ sortedNodesList
  }
}
