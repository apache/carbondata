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

import java.io.IOException
import java.util
import java.util.{Collections, List}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Random

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, NodeInfo}
import org.apache.spark.sql.hive.DistributionUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.block._
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema
import org.apache.carbondata.core.mutate.UpdateVO
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil, DataTypeUtil}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.util.{CarbonInputFormatUtil, CarbonInputSplitTaskInfo}
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger._
import org.apache.carbondata.processing.splits.TableSplit
import org.apache.carbondata.processing.util.{CarbonDataProcessorUtil, CarbonLoaderUtil}
import org.apache.carbondata.spark.MergeResult
import org.apache.carbondata.spark.util.{SparkDataTypeConverterImpl, Util}

class CarbonMergerRDD[K, V](
    sc: SparkContext,
    result: MergeResult[K, V],
    carbonLoadModel: CarbonLoadModel,
    carbonMergerMapping: CarbonMergerMapping,
    confExecutorsTemp: String)
  extends CarbonRDD[(K, V)](sc, Nil) {

  sc.setLocalProperty("spark.scheduler.pool", "DDL")
  sc.setLocalProperty("spark.job.interruptOnCancel", "true")

  private val queryId = sparkContext.getConf.get("queryId", System.nanoTime() + "")
  var storeLocation: String = null
  var mergeResult: String = null
  val hdfsStoreLocation = carbonMergerMapping.hdfsStoreLocation
  val metadataFilePath = carbonMergerMapping.metadataFilePath
  val mergedLoadName = carbonMergerMapping.mergedLoadName
  val databaseName = carbonMergerMapping.databaseName
  val factTableName = carbonMergerMapping.factTableName
  val tableId = carbonMergerMapping.tableId

  override def internalCompute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val carbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]
      if (carbonTable.isPartitionTable) {
        carbonLoadModel.setTaskNo(String.valueOf(carbonSparkPartition.partitionId))
      } else {
        carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
      }
      // this property is used to determine whether temp location for carbon is inside
      // container temp dir or is yarn application directory.
      val carbonUseLocalDir = CarbonProperties.getInstance()
        .getProperty("carbon.use.local.dir", "false")

      if (carbonUseLocalDir.equalsIgnoreCase("true")) {

        val storeLocations = Util.getConfiguredLocalDirs(SparkEnv.get.conf)
        if (null != storeLocations && storeLocations.nonEmpty) {
          storeLocation = storeLocations(Random.nextInt(storeLocations.length))
        }
        if (storeLocation == null) {
          storeLocation = System.getProperty("java.io.tmpdir")
        }
      } else {
        storeLocation = System.getProperty("java.io.tmpdir")
      }
      storeLocation = storeLocation + '/' + System.nanoTime() + '_' + theSplit.index
      var mergeStatus = false
      var mergeNumber = ""
      var exec: CarbonCompactionExecutor = null
      try {


        // sorting the table block info List.
        val splitList = carbonSparkPartition.split.value.getAllSplits
        val tableBlockInfoList = CarbonInputSplit.createBlocks(splitList)

        Collections.sort(tableBlockInfoList)

        // During UPDATE DELTA COMPACTION case all the blocks received in compute belongs to
        // one segment, so max cardinality will be calculated from first block of segment
        if(carbonMergerMapping.campactionType == CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
          var dataFileFooter: DataFileFooter = null
          try {
            // As the tableBlockInfoList is sorted take the ColCardinality from the last
            // Block of the sorted list as it will have the last updated cardinality.
            // Blocks are sorted by order of updation using TableBlockInfo.compare method so
            // the last block after the sort will be the latest one.
            dataFileFooter = CarbonUtil
              .readMetadatFile(tableBlockInfoList.get(tableBlockInfoList.size() - 1))
          } catch {
            case e: IOException =>
              logError("Exception in preparing the data file footer for compaction " + e.getMessage)
              throw e
          }
          // target load name will be same as source load name in case of update data compaction
          carbonMergerMapping.mergedLoadName = tableBlockInfoList.get(0).getSegmentId
          carbonMergerMapping.maxSegmentColCardinality = dataFileFooter.getSegmentInfo
            .getColumnCardinality
          carbonMergerMapping.maxSegmentColumnSchemaList = dataFileFooter.getColumnInTable.asScala
            .toList
        }
        mergeNumber = if (carbonMergerMapping.campactionType ==
                              CompactionType.IUD_UPDDEL_DELTA_COMPACTION) {
          tableBlockInfoList.get(0).getSegmentId
        }
        else {
          mergedLoadName
            .substring(mergedLoadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER) +
                       CarbonCommonConstants.LOAD_FOLDER.length(), mergedLoadName.length()
            )
        }
        carbonLoadModel.setSegmentId(mergeNumber)
        val tempLocationKey = CarbonDataProcessorUtil
          .getTempStoreLocationKey(carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName,
            carbonLoadModel.getSegmentId,
            carbonLoadModel.getTaskNo,
            true,
            false)
        CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation)
        LOGGER.info(s"Temp storeLocation taken is $storeLocation")
        // get destination segment properties as sent from driver which is of last segment.
        val segmentProperties = new SegmentProperties(
          carbonMergerMapping.maxSegmentColumnSchemaList.asJava,
          carbonMergerMapping.maxSegmentColCardinality)

        val segmentMapping: java.util.Map[String, TaskBlockInfo] =
          CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList)

        val dataFileMetadataSegMapping: java.util.Map[String, List[DataFileFooter]] =
          CarbonCompactionUtil.createDataFileFooterMappingForSegments(tableBlockInfoList)

        carbonLoadModel.setStorePath(hdfsStoreLocation)
        // check for restructured block
        // TODO: only in case of add and drop this variable should be true
        val restructuredBlockExists: Boolean = CarbonCompactionUtil
          .checkIfAnyRestructuredBlockExists(segmentMapping,
            dataFileMetadataSegMapping,
            carbonTable.getTableLastUpdatedTime)
        DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
        exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties,
          carbonTable, dataFileMetadataSegMapping, restructuredBlockExists)

        // fire a query and get the results.
        var result2: java.util.List[RawResultIterator] = null
        try {
          result2 = exec.processTableBlocks()
        } catch {
          case e: Throwable =>
            LOGGER.error(e)
            if (null != e.getMessage) {
              sys.error(s"Exception occurred in query execution :: ${ e.getMessage }")
            } else {
              sys.error("Exception occurred in query execution.Please check logs.")
            }
        }

        val tempStoreLoc = CarbonDataProcessorUtil.getLocalDataFolderLocation(databaseName,
          factTableName,
          carbonLoadModel.getTaskNo,
          "0",
          mergeNumber,
          true,
          false
        )

        carbonLoadModel.setPartitionId("0")
        var processor: AbstractResultProcessor = null
        if (restructuredBlockExists) {
          processor = new CompactionResultSortProcessor(carbonLoadModel, carbonTable,
            segmentProperties,
            carbonMergerMapping.campactionType,
            factTableName
          )
        } else {
          processor =
            new RowResultMergerProcessor(
              databaseName,
              factTableName,
              segmentProperties,
              tempStoreLoc,
              carbonLoadModel,
              carbonMergerMapping.campactionType
            )
        }
        mergeStatus = processor.execute(result2)
        mergeResult = tableBlockInfoList.get(0).getSegmentId + ',' + mergeNumber

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
            .deleteLocalDataLoadFolderLocation(carbonLoadModel, isCompactionFlow, false)
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
        !finished
      }

      override def next(): (K, V) = {
        finished = true
        result.getKey(mergeResult, mergeStatus)
      }
    }
    iter
  }

  override def getPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = new AbsoluteTableIdentifier(
      hdfsStoreLocation, new CarbonTableIdentifier(databaseName, factTableName, tableId)
    )
    val updateStatusManager: SegmentUpdateStatusManager = new SegmentUpdateStatusManager(
      absoluteTableIdentifier)
    val jobConf: JobConf = new JobConf(new Configuration)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    CarbonTableInputFormat.setTableInfo(job.getConfiguration,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
    var updateDetails: UpdateVO = null
    // initialise query_id for job
    job.getConfiguration.set("query.id", queryId)
    var defaultParallelism = sparkContext.defaultParallelism
    val result = new java.util.ArrayList[Partition](defaultParallelism)
    var taskPartitionNo = 0
    var carbonPartitionId = 0;
    var columnSize = 0
    var noOfBlocks = 0

    // mapping of the node and block list.
    var nodeBlockMapping: java.util.Map[String, java.util.List[Distributable]] = new
        java.util.HashMap[String, java.util.List[Distributable]]

    val taskInfoList = new java.util.ArrayList[Distributable]
    var carbonInputSplits = mutable.Seq[CarbonInputSplit]()

    var splitsOfLastSegment: List[CarbonInputSplit] = null
    // map for keeping the relation of a task and its blocks.
    val taskIdMapping: java.util.Map[String, java.util.List[CarbonInputSplit]] = new
        java.util.HashMap[String, java.util.List[CarbonInputSplit]]

    // for each valid segment.
    for (eachSeg <- carbonMergerMapping.validSegments) {

      // map for keeping the relation of a task and its blocks.
      job.getConfiguration.set(CarbonTableInputFormat.INPUT_SEGMENT_NUMBERS, eachSeg)

      if (updateStatusManager.getUpdateStatusDetails.length != 0) {
         updateDetails = updateStatusManager.getInvalidTimestampRange(eachSeg)
      }

      var updated: Boolean = updateStatusManager.getUpdateStatusDetails.length != 0
      // get splits
      val splits = format.getSplits(job)

      // keep on assigning till last one is reached.
      if (null != splits && splits.size > 0) {
        splitsOfLastSegment = splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).toList.asJava
      }

      carbonInputSplits ++:= splits.asScala.map(_.asInstanceOf[CarbonInputSplit]).filter(entry => {
        val blockInfo = new TableBlockInfo(entry.getPath.toString,
          entry.getStart, entry.getSegmentId,
          entry.getLocations, entry.getLength, entry.getVersion,
          updateStatusManager.getDeleteDeltaFilePath(entry.getPath.toString)
        )
        ((!updated) || ((updated) && (!CarbonUtil
          .isInvalidTableBlock(blockInfo.getSegmentId, blockInfo.getFilePath,
            updateDetails, updateStatusManager))))
      })
    }

    // prepare the details required to extract the segment properties using last segment.
    if (null != splitsOfLastSegment && splitsOfLastSegment.size() > 0) {
      val carbonInputSplit = splitsOfLastSegment.get(0)
      var dataFileFooter: DataFileFooter = null

      try {
        dataFileFooter = CarbonUtil.readMetadatFile(
          CarbonInputSplit.getTableBlockInfo(carbonInputSplit))
      } catch {
        case e: IOException =>
          logError("Exception in preparing the data file footer for compaction " + e.getMessage)
          throw e
      }
    }

    val columnToCardinalityMap = new util.HashMap[java.lang.String, Integer]()

    carbonInputSplits.foreach(splits => {
      val taskNo = splits.taskId
      var dataFileFooter: DataFileFooter = null

      val splitList = taskIdMapping.get(taskNo)
      noOfBlocks += 1
      if (null == splitList) {
        val splitTempList = new util.ArrayList[CarbonInputSplit]()
        splitTempList.add(splits)
        taskIdMapping.put(taskNo, splitTempList)
      } else {
        splitList.add(splits)
      }

      // Check the cardinality of each columns and set the highest.
      try {
        dataFileFooter = CarbonUtil.readMetadatFile(
          CarbonInputSplit.getTableBlockInfo(splits))
      } catch {
        case e: IOException =>
          logError("Exception in preparing the data file footer for compaction " + e.getMessage)
          throw e
      }
      // add all the column and cardinality to the map
      CarbonCompactionUtil
        .addColumnCardinalityToMap(columnToCardinalityMap,
          dataFileFooter.getColumnInTable,
          dataFileFooter.getSegmentInfo.getColumnCardinality)
    }
    )
    val updatedMaxSegmentColumnList = new util.ArrayList[ColumnSchema]()
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    // update cardinality and column schema list according to master schema
    val cardinality = CarbonCompactionUtil
      .updateColumnSchemaAndGetCardinality(columnToCardinalityMap,
        carbonTable,
        updatedMaxSegmentColumnList)
    carbonMergerMapping.maxSegmentColumnSchemaList = updatedMaxSegmentColumnList.asScala.toList
    // Set cardinality for new segment.
    carbonMergerMapping.maxSegmentColCardinality = cardinality

    taskIdMapping.asScala.foreach(
      entry =>
        taskInfoList
          .add(new CarbonInputSplitTaskInfo(entry._1, entry._2).asInstanceOf[Distributable])
    )

    val nodeBlockMap = CarbonLoaderUtil.nodeBlockMapping(taskInfoList, -1)

    val nodeTaskBlocksMap = new java.util.HashMap[String, java.util.List[NodeInfo]]()

    val confExecutors = confExecutorsTemp.toInt
    val requiredExecutors = if (nodeBlockMap.size > confExecutors) {
      confExecutors
    } else { nodeBlockMap.size() }
    DistributionUtil.ensureExecutors(sparkContext, requiredExecutors, taskInfoList.size)
    logInfo("No.of Executors required=" + requiredExecutors +
            " , spark.executor.instances=" + confExecutors +
            ", no.of.nodes where data present=" + nodeBlockMap.size())
    var nodes = DistributionUtil.getNodeList(sparkContext)
    var maxTimes = 30
    while (nodes.length < requiredExecutors && maxTimes > 0) {
      Thread.sleep(500)
      nodes = DistributionUtil.getNodeList(sparkContext)
      maxTimes = maxTimes - 1
    }
    logInfo("Time taken to wait for executor allocation is =" + ((30 - maxTimes) * 500) + "millis")
    defaultParallelism = sparkContext.defaultParallelism

    val isPartitionTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.isPartitionTable
    // Create Spark Partition for each task and assign blocks
    nodeBlockMap.asScala.foreach { case (nodeName, splitList) =>
      val taskSplitList = new java.util.ArrayList[NodeInfo](0)
      nodeTaskBlocksMap.put(nodeName, taskSplitList)
      var blockletCount = 0
      splitList.asScala.foreach { splitInfo =>
        val splitsPerNode = splitInfo.asInstanceOf[CarbonInputSplitTaskInfo]
        blockletCount = blockletCount + splitsPerNode.getCarbonInputSplitList.size()
        taskSplitList.add(
          NodeInfo(splitsPerNode.getTaskId, splitsPerNode.getCarbonInputSplitList.size()))

        if (blockletCount != 0) {
          val taskInfo = splitInfo.asInstanceOf[CarbonInputSplitTaskInfo]
          val multiBlockSplit = new CarbonMultiBlockSplit(absoluteTableIdentifier,
            taskInfo.getCarbonInputSplitList,
            Array(nodeName))
          if (isPartitionTable) {
            carbonPartitionId = Integer.parseInt(taskInfo.getTaskId)
          }
          result.add(
            new CarbonSparkPartition(id, taskPartitionNo, multiBlockSplit, carbonPartitionId))
          taskPartitionNo += 1
        }
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
    for (j <- 0 until result.size) {
      val multiBlockSplit = result.get(j).asInstanceOf[CarbonSparkPartition].split.value
      val splitList = multiBlockSplit.getAllSplits
      logInfo(s"Node: ${ multiBlockSplit.getLocations.mkString(",") }, No.Of Blocks: " +
              s"${ CarbonInputSplit.createBlocks(splitList).size }")
    }
    result.toArray(new Array[Partition](result.size))
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val theSplit = split.asInstanceOf[CarbonSparkPartition]
    theSplit.split.value.getLocations.filter(_ != "localhost")
  }
}


class CarbonLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}

case class SplitTaskInfo (splits: List[CarbonInputSplit]) extends CarbonInputSplit{
}
