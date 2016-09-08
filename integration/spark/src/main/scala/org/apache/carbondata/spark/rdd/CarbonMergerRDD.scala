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
import java.util.{Collections, List}

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.CarbonContext
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, NodeInfo}
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.carbon.datastore.block.{Distributable, SegmentProperties, TableBlockInfo, TableTaskInfo, TaskBlockInfo}
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.carbon.path.CarbonTablePath
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.apache.carbondata.integration.spark.merger.{CarbonCompactionExecutor, CarbonCompactionUtil, RowResultMerger}
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.scan.result.iterator.RawResultIterator
import org.apache.carbondata.spark.MergeResult
import org.apache.carbondata.spark.load.{CarbonLoaderUtil, CarbonLoadModel}
import org.apache.carbondata.spark.splits.TableSplit
import org.apache.carbondata.spark.util.QueryPlanUtil


class CarbonMergerRDD[K, V](
  sc: SparkContext,
  result: MergeResult[K, V],
  carbonLoadModel: CarbonLoadModel,
  carbonMergerMapping : CarbonMergerMapping,
  confExecutorsTemp: String)
  extends RDD[(K, V)](sc, Nil) with Logging {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")
  sc.setLocalProperty("spark.job.interruptOnCancel", "true")

  var storeLocation: String = null
  val hdfsStoreLocation = carbonMergerMapping.hdfsStoreLocation
  val metadataFilePath = carbonMergerMapping.metadataFilePath
  val mergedLoadName = carbonMergerMapping.mergedLoadName
  val databaseName = carbonMergerMapping.databaseName
  val factTableName = carbonMergerMapping.factTableName
  val tableId = carbonMergerMapping.tableId
  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {

      carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
      val tempLocationKey: String = CarbonCommonConstants
        .COMPACTION_KEY_WORD + '_' + carbonLoadModel
        .getDatabaseName + '_' + carbonLoadModel
        .getTableName + '_' + carbonLoadModel.getTaskNo

      // this property is used to determine whether temp location for carbon is inside
      // container temp dir or is yarn application directory.
      val carbonUseLocalDir = CarbonProperties.getInstance()
        .getProperty("carbon.use.local.dir", "false")

      if(carbonUseLocalDir.equalsIgnoreCase("true")) {

        val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != storeLocations && storeLocations.length > 0) {
          storeLocation = storeLocations(Random.nextInt(storeLocations.length))
        }
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
      }
      else {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
      storeLocation = storeLocation + '/' + System.nanoTime() + '/' + theSplit.index
      CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation)
      LOGGER.info("Temp storeLocation taken is " + storeLocation)
      var mergeStatus = false
      var mergeNumber = ""
      try {
        var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
        val carbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]

        // sorting the table block info List.
        var tableBlockInfoList = carbonSparkPartition.tableBlockInfos

        Collections.sort(tableBlockInfoList)

        val segmentMapping: java.util.Map[String, TaskBlockInfo] =
          CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList)

        val dataFileMetadataSegMapping: java.util.Map[String, List[DataFileFooter]] =
          CarbonCompactionUtil.createDataFileFooterMappingForSegments(tableBlockInfoList)

        carbonLoadModel.setStorePath(hdfsStoreLocation)

        // taking the last table block info for getting the segment properties.
        val listMetadata = dataFileMetadataSegMapping.get(tableBlockInfoList.get
        (tableBlockInfoList.size() - 1).getSegmentId()
        )

        val colCardinality: Array[Int] = listMetadata.get(listMetadata.size() - 1).getSegmentInfo
          .getColumnCardinality

        val segmentProperties = new SegmentProperties(
          listMetadata.get(listMetadata.size() - 1).getColumnInTable,
          colCardinality
        )

        val exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties, databaseName,
          factTableName, hdfsStoreLocation, carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
          dataFileMetadataSegMapping
        )

        // fire a query and get the results.
        var result2: util.List[RawResultIterator] = null
        try {
          result2 = exec.processTableBlocks()
        } catch {
          case e: Throwable =>
            exec.clearDictionaryFromQueryModel
            LOGGER.error(e)
            if (null != e.getMessage) {
              sys.error("Exception occurred in query execution :: " + e.getMessage)
            } else {
              sys.error("Exception occurred in query execution.Please check logs.")
            }
        }

        mergeNumber = mergedLoadName
          .substring(mergedLoadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER) +
            CarbonCommonConstants.LOAD_FOLDER.length(), mergedLoadName.length()
          )

        val tempStoreLoc = CarbonDataProcessorUtil.getLocalDataFolderLocation(databaseName,
          factTableName,
          carbonLoadModel.getTaskNo,
          "0",
          mergeNumber,
          true
        )

        carbonLoadModel.setSegmentId(mergeNumber)
        carbonLoadModel.setPartitionId("0")
        val merger =
          new RowResultMerger(result2,
            databaseName,
            factTableName,
            segmentProperties,
            tempStoreLoc,
            carbonLoadModel,
            colCardinality
          )
        mergeStatus = merger.mergerSlice()

      }
      catch {
        case e: Exception =>
          LOGGER.error(e)
          throw e
      }
      finally {
        // delete temp location data
        val newSlice = CarbonCommonConstants.LOAD_FOLDER + mergeNumber
        try {
          val isCompactionFlow = true
          CarbonLoaderUtil
            .deleteLocalDataLoadFolderLocation(carbonLoadModel, isCompactionFlow)
        } catch {
          case e: Exception =>
            LOGGER.error(e)
        }
      }

      var finished = false

      override def hasNext: Boolean = {
        if (!finished) {
          finished = true
          finished
        }
        else {
          !finished
        }
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(0, mergeStatus)
      }

    }
    iter
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkPartition]
    theSplit.locations.filter(_ != "localhost")
  }

  override def getPartitions: Array[Partition] = {

    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(
      hdfsStoreLocation, new CarbonTableIdentifier(databaseName, factTableName, tableId)
    )
    val (carbonInputFormat: CarbonInputFormat[Array[Object]], job: Job) =
      QueryPlanUtil.createCarbonInputFormat(absoluteTableIdentifier)
    var defaultParallelism = sparkContext.defaultParallelism
    val result = new util.ArrayList[Partition](defaultParallelism)

    // mapping of the node and block list.
    var nodeMapping: util.Map[String, util.List[Distributable]] = new
        util.HashMap[String, util.List[Distributable]]

    var noOfBlocks = 0

    val taskInfoList = new util.ArrayList[Distributable]

    // for each valid segment.
    for (eachSeg <- carbonMergerMapping.validSegments) {

      // map for keeping the relation of a task and its blocks.
      val taskIdMapping: util.Map[String, util.List[TableBlockInfo]] = new
          util.HashMap[String, util.List[TableBlockInfo]]

      job.getConfiguration.set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, eachSeg)

      // get splits
      val splits = carbonInputFormat.getSplits(job)
      val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

      // take the blocks of one segment.
      val blocksOfOneSegment = carbonInputSplits.map(inputSplit =>
        new TableBlockInfo(inputSplit.getPath.toString,
          inputSplit.getStart, inputSplit.getSegmentId,
          inputSplit.getLocations, inputSplit.getLength
        )
      )

      // populate the task and its block mapping.
      blocksOfOneSegment.foreach(tableBlockInfo => {
        val taskNo = CarbonTablePath.DataFileUtil.getTaskNo(tableBlockInfo.getFilePath)
        val blockList = taskIdMapping.get(taskNo)
        if (null == blockList) {
          val blockListTemp = new util.ArrayList[TableBlockInfo]()
          blockListTemp.add(tableBlockInfo)
          taskIdMapping.put(taskNo, blockListTemp)
        }
        else {
          blockList.add(tableBlockInfo)
        }
      }
      )

      noOfBlocks += blocksOfOneSegment.size
      taskIdMapping.asScala.foreach(
        entry =>
          taskInfoList.add(new TableTaskInfo(entry._1, entry._2).asInstanceOf[Distributable])
      )
    }
    // send complete list of blocks to the mapping util.
    nodeMapping = CarbonLoaderUtil.nodeBlockMapping(taskInfoList, -1)

    val confExecutors = confExecutorsTemp.toInt
    val requiredExecutors = if (nodeMapping.size > confExecutors) {
      confExecutors
    } else { nodeMapping.size() }
    CarbonContext.ensureExecutors(sparkContext, requiredExecutors)
    logInfo("No.of Executors required=" + requiredExecutors
      + " , spark.executor.instances=" + confExecutors
      + ", no.of.nodes where data present=" + nodeMapping.size())
    var nodes = DistributionUtil.getNodeList(sparkContext)
    var maxTimes = 30
    while (nodes.length < requiredExecutors && maxTimes > 0) {
      Thread.sleep(500)
      nodes = DistributionUtil.getNodeList(sparkContext)
      maxTimes = maxTimes - 1
    }
    logInfo("Time taken to wait for executor allocation is =" + ((30 - maxTimes) * 500) + "millis")
    defaultParallelism = sparkContext.defaultParallelism
    var i = 0

    val nodeTaskBlocksMap: util.Map[String, util.List[NodeInfo]] = new util.HashMap[String, util
    .List[NodeInfo]]()

    // Create Spark Partition for each task and assign blocks
    nodeMapping.asScala.foreach { entry =>

      val taskBlockList: List[NodeInfo] = new util.ArrayList[NodeInfo](0)
      nodeTaskBlocksMap.put(entry._1, taskBlockList)

      val list = new util.ArrayList[TableBlockInfo]
      entry._2.asScala.foreach(taskInfo => {
         val blocksPerNode = taskInfo.asInstanceOf[TableTaskInfo]
         list.addAll(blocksPerNode.getTableBlockInfoList)
        taskBlockList
          .add(new NodeInfo(blocksPerNode.getTaskId, blocksPerNode.getTableBlockInfoList.size))
       })
      if (list.size() != 0) {
           result.add(new CarbonSparkPartition(id, i, Seq(entry._1).toArray, list))
           i += 1
         }
    }

    // print the node info along with task and number of blocks for the task.

    nodeTaskBlocksMap.asScala.foreach((entry : (String, List[NodeInfo])) => {
      logInfo(s"for the node ${entry._1}" )
      for (elem <- entry._2.asScala) {
        logInfo("Task ID is " + elem.TaskId + "no. of blocks is " + elem.noOfBlocks)
      }
    } )

    val noOfNodes = nodes.length
    val noOfTasks = result.size
    logInfo(s"Identified  no.of.Blocks: $noOfBlocks,"
            + s"parallelism: $defaultParallelism , no.of.nodes: $noOfNodes, no.of.tasks: $noOfTasks"
    )
    logInfo("Time taken to identify Blocks to scan : " + (System
                                                            .currentTimeMillis() - startTime)
    )
    for (j <- 0 until result.size ) {
      val cp = result.get(j).asInstanceOf[CarbonSparkPartition]
      logInfo(s"Node : " + cp.locations.toSeq.mkString(",")
              + ", No.Of Blocks : " + cp.tableBlockInfos.size
      )
    }
    result.toArray(new Array[Partition](result.size))
  }

}

class CarbonLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}
