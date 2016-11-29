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
import scala.collection.mutable
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, NodeInfo}
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.carbon.datastore.block.{Distributable, SegmentProperties, TaskBlockInfo}
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, CarbonUtilException}
import org.apache.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.integration.spark.merger.{CarbonCompactionExecutor, CarbonCompactionUtil, RowResultMerger}
import org.apache.carbondata.processing.model.CarbonLoadModel
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.scan.result.iterator.RawResultIterator
import org.apache.carbondata.spark.MergeResult
import org.apache.carbondata.spark.load.CarbonLoaderUtil
import org.apache.carbondata.spark.splits.TableSplit

class CarbonMergerRDD[K, V](
    sc: SparkContext,
    result: MergeResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    carbonMergerMapping: CarbonMergerMapping,
    confExecutorsTemp: String)
  extends RDD[(K, V)](sc, Nil) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")
  sc.setLocalProperty("spark.job.interruptOnCancel", "true")

  var storeLocation: String = null
  val storePath = carbonMergerMapping.storePath
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

      if (carbonUseLocalDir.equalsIgnoreCase("true")) {

        val storeLocations = CarbonLoaderUtil.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != storeLocations && storeLocations.nonEmpty) {
          storeLocation = storeLocations(Random.nextInt(storeLocations.length))
        }
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
      } else {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
      storeLocation = storeLocation + '/' + System.nanoTime() + '/' + theSplit.index
      CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation)
      LOGGER.info(s"Temp storeLocation taken is $storeLocation")
      var mergeStatus = false
      var mergeNumber = ""
      var exec: CarbonCompactionExecutor = null
      try {
        val carbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]

        // get destination segment properties as sent from driver which is of last segment.

        val segmentProperties = new SegmentProperties(
          carbonMergerMapping.maxSegmentColumnSchemaList.asJava,
          carbonMergerMapping.maxSegmentColCardinality)

        // sorting the table block info List.
        val splitList = carbonSparkPartition.split.value.getAllSplits
        val tableBlockInfoList = CarbonInputSplit.createBlocks(splitList)

        Collections.sort(tableBlockInfoList)

        val segmentMapping: java.util.Map[String, TaskBlockInfo] =
          CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList)

        val dataFileMetadataSegMapping: java.util.Map[String, List[DataFileFooter]] =
          CarbonCompactionUtil.createDataFileFooterMappingForSegments(tableBlockInfoList)

        carbonLoadModel.setStorePath(storePath)

        exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties, databaseName,
          factTableName, storePath, carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
          dataFileMetadataSegMapping
        )

        // fire a query and get the results.
        var result2: util.List[RawResultIterator] = null
        try {
          result2 = exec.processTableBlocks()
        } catch {
          case e: Throwable =>
            if (null != exec) {
              exec.finish()
            }
            LOGGER.error(e)
            if (null != e.getMessage) {
              sys.error(s"Exception occurred in query execution :: ${ e.getMessage }")
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
            carbonMergerMapping.maxSegmentColCardinality
          )
        mergeStatus = merger.mergerSlice()

      } catch {
        case e: Exception =>
          LOGGER.error(e)
          throw e
      } finally {
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
        if (null != exec) {
          exec.finish
        }
      }

      var finished = false

      override def hasNext: Boolean = {
        if (!finished) {
          finished = true
          finished
        } else {
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
    theSplit.split.value.getLocations.filter(_ != "localhost")
  }

  override def getPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(
      storePath, new CarbonTableIdentifier(databaseName, factTableName, tableId)
    )
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    var defaultParallelism = sparkContext.defaultParallelism
    val result = new util.ArrayList[Partition](defaultParallelism)

    // mapping of the node and block list.
    var nodeBlockMapping: util.Map[String, util.List[Distributable]] = new
        util.HashMap[String, util.List[Distributable]]

    val noOfBlocks = 0
    var carbonInputSplits = mutable.Seq[CarbonInputSplit]()

    // for each valid segment.
    for (eachSeg <- carbonMergerMapping.validSegments) {

      // map for keeping the relation of a task and its blocks.
      job.getConfiguration.set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, eachSeg)

      // get splits
      val splits = format.getSplits(job)
      carbonInputSplits ++:= splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
    }

    // prepare the details required to extract the segment properties using last segment.
    if (null != carbonInputSplits && carbonInputSplits.nonEmpty) {
      val carbonInputSplit = carbonInputSplits.last
      var dataFileFooter: DataFileFooter = null

      try {
        dataFileFooter = CarbonUtil.readMetadatFile(carbonInputSplit.getPath.toString(),
          carbonInputSplit.getStart, carbonInputSplit.getLength)
      } catch {
        case e: CarbonUtilException =>
          logError("Exception in preparing the data file footer for compaction " + e.getMessage)
          throw e
      }

      carbonMergerMapping.maxSegmentColCardinality = dataFileFooter.getSegmentInfo
        .getColumnCardinality
      carbonMergerMapping.maxSegmentColumnSchemaList = dataFileFooter.getColumnInTable.asScala
        .toList
    }
    // send complete list of blocks to the mapping util.
    nodeBlockMapping = CarbonLoaderUtil.nodeBlockMapping(
      carbonInputSplits.map(_.asInstanceOf[Distributable]).asJava, -1)

    val confExecutors = confExecutorsTemp.toInt
    val requiredExecutors = if (nodeBlockMapping.size > confExecutors) {
      confExecutors
    } else { nodeBlockMapping.size() }
    DistributionUtil.ensureExecutors(sparkContext, requiredExecutors)
    logInfo("No.of Executors required=" + requiredExecutors +
            " , spark.executor.instances=" + confExecutors +
            ", no.of.nodes where data present=" + nodeBlockMapping.size())
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

    val nodeTaskBlocksMap = new util.HashMap[String, util.List[NodeInfo]]()

    // Create Spark Partition for each task and assign blocks
    nodeBlockMapping.asScala.foreach { case (nodeName, blockList) =>
      val taskBlockList = new util.ArrayList[NodeInfo](0)
      nodeTaskBlocksMap.put(nodeName, taskBlockList)
      var blockletCount = 0
      blockList.asScala.foreach { taskInfo =>
        val blocksPerNode = taskInfo.asInstanceOf[CarbonInputSplit]
        blockletCount = blockletCount + blocksPerNode.getNumberOfBlocklets
        taskBlockList.add(
          NodeInfo(blocksPerNode.taskId, blocksPerNode.getNumberOfBlocklets))
      }
      if (blockletCount != 0) {
        val multiBlockSplit = new CarbonMultiBlockSplit(absoluteTableIdentifier,
          carbonInputSplits.asJava, nodeName)
        result.add(new CarbonSparkPartition(id, i, multiBlockSplit))
        i += 1
      }
    }

    // print the node info along with task and number of blocks for the task.

    nodeTaskBlocksMap.asScala.foreach((entry: (String, List[NodeInfo])) => {
      logInfo(s"for the node ${ entry._1 }")
      for (elem <- entry._2.asScala) {
        logInfo("Task ID is " + elem.TaskId + "no. of blocks is " + elem.noOfBlocks)
      }
    })

    val noOfNodes = nodes.length
    val noOfTasks = result.size
    logInfo(s"Identified  no.of.Blocks: $noOfBlocks," +
            s"parallelism: $defaultParallelism , no.of.nodes: $noOfNodes, no.of.tasks: $noOfTasks")
    logInfo("Time taken to identify Blocks to scan : " + (System.currentTimeMillis() - startTime))
    for (j <- 0 until result.size ) {
      val multiBlockSplit = result.get(j).asInstanceOf[CarbonSparkPartition].split.value
      val splitList = multiBlockSplit.getAllSplits
      logInfo(s"Node: ${multiBlockSplit.getLocations.mkString(",")}, No.Of Blocks: " +
              s"${CarbonInputSplit.createBlocks(splitList).size}")
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
