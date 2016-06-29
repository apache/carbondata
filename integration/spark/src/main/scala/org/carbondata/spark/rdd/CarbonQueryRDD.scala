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


package org.carbondata.spark.rdd

import java.util

import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.cache.dictionary.Dictionary
import org.carbondata.core.carbon.datastore.block.TableBlockInfo
import org.carbondata.core.iterator.CarbonIterator
import org.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.carbondata.query.carbon.executor.QueryExecutorFactory
import org.carbondata.query.carbon.model.QueryModel
import org.carbondata.query.carbon.result.RowResult
import org.carbondata.query.expression.Expression
import org.carbondata.query.filter.resolver.FilterResolverIntf
import org.carbondata.spark.KeyVal
import org.carbondata.spark.load.CarbonLoaderUtil
import org.carbondata.spark.util.QueryPlanUtil

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
  * This RDD is used to perform query.
  */
class CarbonQueryRDD[K, V](
  sc: SparkContext,
  queryModel: QueryModel,
  filterExpression: Expression,
  keyClass: KeyVal[K, V],
  @transient conf: Configuration,
  cubeCreationTime: Long,
  schemaLastUpdatedTime: Long,
  baseStoreLocation: String)
  extends RDD[(K, V)](sc, Nil) with Logging {

  val defaultParallelism = sc.defaultParallelism

  override def getPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val (carbonInputFormat: CarbonInputFormat[RowResult], job: Job) =
      QueryPlanUtil.createCarbonInputFormat(queryModel.getAbsoluteTableIdentifier)

    val result = new util.ArrayList[Partition](defaultParallelism)
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    // set filter resolver tree
    try {
      // before applying filter check whether segments are available in the table.
      val splits = carbonInputFormat.getSplits(job)
      if (!splits.isEmpty) {
        var filterResolver = carbonInputFormat
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

      val blockList = carbonInputSplits.map(inputSplit =>
        new TableBlockInfo(inputSplit.getPath.toString,
          inputSplit.getStart, inputSplit.getSegmentId,
          inputSplit.getLocations, inputSplit.getLength
        )
      )
      if (blockList.nonEmpty) {
        // group blocks to nodes, tasks
        val nodeBlockMapping =
          CarbonLoaderUtil.nodeBlockTaskMapping(blockList.asJava, -1, defaultParallelism)

        var i = 0
        // Create Spark Partition for each task and assign blocks
        nodeBlockMapping.asScala.foreach { entry =>
          entry._2.asScala.foreach { blocksPerTask =>
            if (blocksPerTask.size() != 0) {
              result.add(new CarbonSparkPartition(id, i, Seq(entry._1).toArray, blocksPerTask))
              i += 1
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
        logInfo("Time taken to identify Blocks to scan : " +
          (System.currentTimeMillis() - startTime)
        )
        result.asScala.foreach { r =>
          val cp = r.asInstanceOf[CarbonSparkPartition]
          logInfo(s"Node : " + cp.locations.toSeq.mkString(",")
            + ", No.Of Blocks : " + cp.tableBlockInfos.size()
          )
        }
      } else {
        logInfo("No blocks identified to scan")
        val nodesPerBlock = new util.ArrayList[TableBlockInfo]()
        result.add(new CarbonSparkPartition(id, 0, Seq("").toArray, nodesPerBlock))
      }
    }
    else {
      logInfo("No valid segments found to scan")
      val nodesPerBlock = new util.ArrayList[TableBlockInfo]()
      result.add(new CarbonSparkPartition(id, 0, Seq("").toArray, nodesPerBlock))
    }
    result.toArray(new Array[Partition](result.size()))
  }


  override def compute(thepartition: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      var rowIterator: CarbonIterator[_] = _
      var queryStartTime: Long = 0
      try {
        val carbonSparkPartition = thepartition.asInstanceOf[CarbonSparkPartition]
        if (!carbonSparkPartition.tableBlockInfos.isEmpty) {
          queryModel.setQueryId(queryModel.getQueryId + "_" + carbonSparkPartition.idx)
          // fill table block info
          queryModel.setTableBlockInfos(carbonSparkPartition.tableBlockInfos)
          queryStartTime = System.currentTimeMillis

          val carbonPropertiesFilePath = System.getProperty("carbon.properties.filepath", null)
          logInfo("*************************" + carbonPropertiesFilePath)
          if (null == carbonPropertiesFilePath) {
            System.setProperty("carbon.properties.filepath",
              System.getProperty("user.dir") + '/' + "conf" + '/' + "carbon.properties"
            )
          }
          // execute query
          rowIterator = QueryExecutorFactory.getQueryExecutor(queryModel).execute(queryModel)
            .asInstanceOf[CarbonIterator[RowResult]]
        }
        // TODOi
        // : CarbonQueryUtil.isQuickFilter quick filter from dictionary needs to support
      } catch {
        case e: Throwable =>
          clearDictionaryCache(queryModel.getColumnToDictionaryMapping)
          LOGGER.error(e)
          // updateCubeAndLevelCacheStatus(levelCacheKeys)
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
        }
        !finished
      }

      override def next(): (K, V) = {
        if (!hasNext) {
          throw new java.util.NoSuchElementException("End of stream")
        }
        havePair = false
        val row = rowIterator.next()
        val key = row.asInstanceOf[RowResult].getKey()
        val value = row.asInstanceOf[RowResult].getValue()
        recordCount += 1
        if (queryModel.getLimit != -1 && recordCount >= queryModel.getLimit) {
          clearDictionaryCache(queryModel.getColumnToDictionaryMapping)
        }
        keyClass.getKey(key, value)
      }

      def clearDictionaryCache(columnToDictionaryMap: java.util.Map[String, Dictionary]) = {
        if (null != columnToDictionaryMap) {
          org.carbondata.spark.util.CarbonQueryUtil
            .clearColumnDictionaryCache(columnToDictionaryMap)
        }
      }

      logInfo("*************************** Total Time Taken to execute the query in Carbon Side: " +
        (System.currentTimeMillis - queryStartTime)
      )
    }
    iter
  }


   /**
    * Get the preferred locations where to launch this task.
    */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val theSplit = partition.asInstanceOf[CarbonSparkPartition]
    theSplit.locations.filter(_ != "localhost")
  }
}
