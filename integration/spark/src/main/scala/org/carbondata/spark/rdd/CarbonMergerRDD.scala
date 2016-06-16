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
import java.util.{Collections, List}

import scala.collection.JavaConverters._

import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.execution.command.CarbonMergerMapping

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.carbondata.core.carbon.datastore.block.{SegmentProperties, TableBlockInfo, TaskBlockInfo}
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.util.CarbonProperties
import org.carbondata.hadoop.{CarbonInputFormat, CarbonInputSplit}
import org.carbondata.integration.spark.merger.{CarbonCompactionExecutor, CarbonCompactionUtil, RowResultMerger}
import org.carbondata.processing.util.CarbonDataProcessorUtil
import org.carbondata.query.carbon.result.RowResult
import org.carbondata.query.carbon.result.iterator.RawResultIterator
import org.carbondata.spark.MergeResult
import org.carbondata.spark.load.{CarbonLoaderUtil, CarbonLoadModel}
import org.carbondata.spark.merger.CarbonDataMergerUtil
import org.carbondata.spark.splits.TableSplit
import org.carbondata.spark.util.QueryPlanUtil


class CarbonMergerRDD[K, V](
  sc: SparkContext,
  result: MergeResult[K, V],
  carbonLoadModel: CarbonLoadModel,
  carbonMergerMapping : CarbonMergerMapping)
  extends RDD[(K, V)](sc, Nil) with Logging {

  val defaultParallelism = sc.defaultParallelism
  sc.setLocalProperty("spark.scheduler.pool", "DDL")

  val storeLocation = carbonMergerMapping.storeLocation
  val hdfsStoreLocation = carbonMergerMapping.hdfsStoreLocation
  val metadataFilePath = carbonMergerMapping.metadataFilePath
  val mergedLoadName = carbonMergerMapping.mergedLoadName
  val schemaName = carbonMergerMapping.schemaName
  val factTableName = carbonMergerMapping.factTableName
  val tableId = carbonMergerMapping.tableId
  override def compute(theSplit: Partition, context: TaskContext): Iterator[(K, V)] = {
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter = new Iterator[(K, V)] {
      var dataloadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
      carbonLoadModel.setTaskNo(String.valueOf(theSplit.index))
      val carbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]

      val tempLocationKey: String = carbonLoadModel.getDatabaseName + '_' + carbonLoadModel
        .getTableName + carbonLoadModel.getTaskNo
      CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation)

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
      (tableBlockInfoList.size()-1).getSegmentId())

      val colCardinality: Array[Int] = listMetadata.get(listMetadata.size() - 1).getSegmentInfo
        .getColumnCardinality

      val segmentProperties = new SegmentProperties(
        listMetadata.get(listMetadata.size() - 1).getColumnInTable,
        colCardinality
      )

      val exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties, schemaName,
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

      val mergeNumber = mergedLoadName
        .substring(mergedLoadName.lastIndexOf(CarbonCommonConstants.LOAD_FOLDER) +
          CarbonCommonConstants.LOAD_FOLDER.length(), mergedLoadName.length()
        )

      val tempStoreLoc = CarbonDataProcessorUtil.getLocalDataFolderLocation(schemaName,
        factTableName,
        carbonLoadModel.getTaskNo,
        "0",
        mergeNumber
      )

      carbonLoadModel.setSegmentId(mergeNumber)
      carbonLoadModel.setPartitionId("0")
      val merger =
        new RowResultMerger(result2,
        schemaName,
        factTableName,
        segmentProperties,
        tempStoreLoc,
        carbonLoadModel,
        colCardinality
      )
      val mergeStatus = merger.mergerSlice()

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
      hdfsStoreLocation, new CarbonTableIdentifier(schemaName, factTableName, tableId)
    )
    val (carbonInputFormat: CarbonInputFormat[RowResult], job: Job) =
      QueryPlanUtil.createCarbonInputFormat(absoluteTableIdentifier)
    val result = new util.ArrayList[Partition](defaultParallelism)
    val mapsOfNodeBlockMapping: util.List[util.Map[String, util.List[TableBlockInfo]]] = new
        java.util.ArrayList[util.Map[String, util.List[TableBlockInfo]]]()
    var noOfBlocks = 0
    for (eachSeg <- carbonMergerMapping.validSegments) {

      job.getConfiguration.set(CarbonInputFormat.INPUT_SEGMENT_NUMBERS, eachSeg)

      // get splits
      val splits = carbonInputFormat.getSplits(job)
      val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

      val blocksOfOneSegment = carbonInputSplits.map(inputSplit =>
        new TableBlockInfo(inputSplit.getPath.toString,
          inputSplit.getStart, inputSplit.getSegmentId,
          inputSplit.getLocations, inputSplit.getLength
        )
      )
      noOfBlocks += blocksOfOneSegment.size
      mapsOfNodeBlockMapping.add(CarbonLoaderUtil.nodeBlockMapping(blocksOfOneSegment.asJava, -1))
    }

    // combining the segments output i.e combining list of maps.
    val nodeBlockMapping: util.Map[String, util.List[TableBlockInfo]] =
      CarbonDataMergerUtil.combineNodeBlockMaps(mapsOfNodeBlockMapping)


    var i = 0
    // Create Spark Partition for each task and assign blocks
    nodeBlockMapping.asScala.foreach { entry =>
      val blocksPerNode = entry._2
      if (blocksPerNode.size() != 0) {
        result.add(new CarbonSparkPartition(id, i, Seq(entry._1).toArray, blocksPerNode))
        i += 1
      }
    }
   // val noOfBlocks = blockList.size
    val noOfNodes = nodeBlockMapping.size
    val noOfTasks = result.size()
    logInfo(s"Identified  no.of.Blocks: $noOfBlocks,"
      + s"parallelism: $defaultParallelism , no.of.nodes: $noOfNodes, no.of.tasks: $noOfTasks"
    )
    logInfo("Time taken to identify Blocks to scan : " + (System
      .currentTimeMillis() - startTime)
    )
    for (j <- 0 until result.size() - 1) {
      val cp = result.get(j).asInstanceOf[CarbonSparkPartition]
      logInfo(s"Node : " + cp.locations.toSeq.mkString(",")
        + ", No.Of Blocks : " + cp.tableBlockInfos.size()
      )
    }
    result.toArray(new Array[Partition](result.size()))
  }
}

class CarbonLoadPartition(rddId: Int, val idx: Int, @transient val tableSplit: TableSplit)
  extends Partition {

  override val index: Int = idx
  val serializableHadoopSplit = new SerializableWritable[TableSplit](tableSplit)

  override def hashCode(): Int = 41 * (41 + rddId) + idx
}
