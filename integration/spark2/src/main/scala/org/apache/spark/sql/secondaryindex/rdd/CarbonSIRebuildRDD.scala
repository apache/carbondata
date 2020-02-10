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

package org.apache.spark.sql.secondaryindex.rdd

import java.io.IOException
import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.execution.command.CarbonMergerMapping
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.util.{CarbonInternalScalaUtil, SecondaryIndexUtil}
import org.apache.spark.sql.util.CarbonException

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.converter.SparkDataTypeConverterImpl
import org.apache.carbondata.core.constants.{CarbonCommonConstants, SortScopeOptions}
import org.apache.carbondata.core.datastore.block.{SegmentProperties, TaskBlockInfo}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.scan.result.iterator.RawResultIterator
import org.apache.carbondata.core.util.{CarbonUtil, DataTypeUtil}
import org.apache.carbondata.hadoop.{CarbonInputSplit, CarbonMultiBlockSplit}
import org.apache.carbondata.hadoop.api.CarbonInputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.TableProcessingOperations
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.merger._
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil
import org.apache.carbondata.spark.MergeResult
import org.apache.carbondata.spark.rdd.{CarbonRDD, CarbonSparkPartition}
import org.apache.carbondata.spark.util.{CarbonScalaUtil, CommonUtil}


/**
 * SI segment merger / rebuild RDD
 * */
class CarbonSIRebuildRDD[K, V](
  @transient private val ss: SparkSession,
  result: MergeResult[K, V],
  carbonLoadModel: CarbonLoadModel,
  carbonMergerMapping: CarbonMergerMapping) extends CarbonRDD[((K, V), String)](ss, Nil) {

  ss.sparkContext.setLocalProperty("spark.scheduler.pool", "DDL")
  ss.sparkContext.setLocalProperty("spark.job.interruptOnCancel", "true")

  var mergeResult: String = _
  val indexTablePath: String = carbonMergerMapping.hdfsStoreLocation
  val databaseName: String = carbonMergerMapping.databaseName
  val indexTableName: String = carbonMergerMapping.factTableName
  val indexTableId: String = carbonMergerMapping.tableId

  override def internalGetPartitions: Array[Partition] = {
    val startTime = System.currentTimeMillis()
    val absoluteTableIdentifier: AbsoluteTableIdentifier = AbsoluteTableIdentifier.from(
      indexTablePath, new CarbonTableIdentifier(databaseName, indexTableName, indexTableId)
    )
    val jobConf: JobConf = new JobConf(FileFactory.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    val defaultParallelism = sparkContext.defaultParallelism
    val noOfBlocks = 0

    CarbonInputFormat.setSegmentsToAccess(
      job.getConfiguration, carbonMergerMapping.validSegments.toList.asJava)
    CarbonInputFormat.setTableInfo(
      job.getConfiguration,
      carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable.getTableInfo)
    CarbonInputFormat.setValidateSegmentsToAccess(job.getConfiguration, false)

    // get splits
    val splits = format.getSplits(job)
    val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])

    // group blocks by segment.
    val splitsGroupedMySegment =
      carbonInputSplits.groupBy(_.getSegmentId)

    var i = -1

    // take the merge size as the block size
    val mergeSize =
    getTableBlockSizeInMb(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable)(ss) * 1024 *
     1024

    val resultSplits: java.util.List[CarbonSparkPartition] = new java.util.ArrayList()
    splitsGroupedMySegment.foreach { entry =>
      if (entry._2.nonEmpty) {
        val (_, splits) = (entry._1, entry._2)
        val locations = splits.head.getLocations
        val blocksToBeMerged = SecondaryIndexUtil
          .identifyBlocksToBeMerged(splits.asJava, mergeSize)
        blocksToBeMerged.asScala.foreach(blocks => {
          i += 1
          resultSplits.add(new CarbonSparkPartition(id, i,
            new CarbonMultiBlockSplit(blocks, locations)))
        })
      }
    }

    carbonMergerMapping.maxSegmentColumnSchemaList = null

    // Log the distribution
    val noOfTasks = resultSplits.size
    logInfo(s"Total no.of.Blocks: $noOfBlocks,"
            + s"parallelism: $defaultParallelism , Identified no.of.tasks: $noOfTasks")

    logInfo("Time taken to identify Blocks and Tasks : " + (System.currentTimeMillis() - startTime))
    resultSplits.asScala.toArray
  }

  /**
   * Get the table block size from the index table, if not found in SI table, check main table
   * If main table also not set with table block size then fall back to default block size set
   *
   */
  def getTableBlockSizeInMb(indexTable: CarbonTable)(sparkSession: SparkSession): Long = {
    var tableBlockSize: String = null
    var tableProperties = indexTable.getTableInfo.getFactTable.getTableProperties
    if (null != tableProperties) {
      tableBlockSize = tableProperties.get(CarbonCommonConstants.TABLE_BLOCKSIZE)
    }
    if (null == tableBlockSize) {
      val metaStore = CarbonEnv.getInstance(sparkSession)
        .carbonMetaStore
      val mainTable = metaStore
        .lookupRelation(Some(indexTable.getDatabaseName),
          CarbonInternalScalaUtil.getParentTableName(indexTable))(sparkSession)
        .asInstanceOf[CarbonRelation]
        .carbonTable
      tableProperties = mainTable.getTableInfo.getFactTable.getTableProperties
      if (null != tableProperties) {
        tableBlockSize = tableProperties.get(CarbonCommonConstants.TABLE_BLOCKSIZE)
      }
      if (null == tableBlockSize) {
        tableBlockSize = CarbonCommonConstants.TABLE_BLOCK_SIZE_DEFAULT
      }
    }
    tableBlockSize.toLong
  }


  override def internalCompute(theSplit: Partition,
    context: TaskContext): Iterator[((K, V), String)] = {
    val queryStartTime = System.currentTimeMillis()
    val LOGGER = LogServiceFactory.getLogService(this.getClass.getName)
    val iter: Iterator[((K, V), String)] = new Iterator[((K, V), String)] {
      val carbonSparkPartition: CarbonSparkPartition = theSplit.asInstanceOf[CarbonSparkPartition]
      val carbonLoadModelCopy: CarbonLoadModel = SecondaryIndexUtil
        .getCarbonLoadModel(carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
          carbonLoadModel.getLoadMetadataDetails,
          carbonLoadModel.getFactTimeStamp,
          carbonLoadModel.getColumnCompressor)
      val indexTable = carbonLoadModelCopy.getCarbonDataLoadSchema.getCarbonTable
      carbonLoadModelCopy.setTablePath(indexTable.getTablePath)
      carbonLoadModelCopy.setTaskNo(String.valueOf(theSplit.index))

      var mergeStatus = false
      var mergeNumber = ""
      var exec: CarbonCompactionExecutor = _
      var processor: AbstractResultProcessor = _
      var rawResultIteratorMap: util.Map[String, util.List[RawResultIterator]] = _
      var segmentId: String = _
      try {
        // sorting the table block info List.
        val splitList = carbonSparkPartition.split.value.getAllSplits
        val tableBlockInfoList = CarbonInputSplit.createBlocks(splitList)
        segmentId = tableBlockInfoList.get(0).getSegmentId

        Collections.sort(tableBlockInfoList)

        // max cardinality will be calculated from first block of segment
        var dataFileFooter: DataFileFooter = null
        try {
          // As the tableBlockInfoList is sorted take the ColCardinality from the last
          // Block of the sorted list as it will have the last updated cardinality.
          // Blocks are sorted by order of updation using TableBlockInfo.compare method so
          // the last block after the sort will be the latest one.
          dataFileFooter = CarbonUtil
            .readMetadataFile(tableBlockInfoList.get(tableBlockInfoList.size() - 1))
        } catch {
          case e: IOException =>
            logError("Exception in preparing the data file footer for compaction " + e.getMessage)
            throw e
        }
        // target load name will be same as source load name in case of update data compaction
        carbonMergerMapping.mergedLoadName = tableBlockInfoList.get(0).getSegmentId
        carbonMergerMapping.maxSegmentColumnSchemaList = dataFileFooter.getColumnInTable.asScala
          .toList
        mergeNumber = tableBlockInfoList.get(0).getSegment.toString
        carbonLoadModelCopy.setSegmentId(mergeNumber)

        if (indexTable.isHivePartitionTable) {
          carbonLoadModelCopy.setTaskNo(
            CarbonScalaUtil.generateUniqueNumber(
              theSplit.index,
              mergeNumber.replace(".", ""), 0L))
        }

        CommonUtil.setTempStoreLocation(theSplit.index,
          carbonLoadModelCopy,
          isCompactionFlow = true,
          isAltPartitionFlow = false)

        // get destination segment properties as sent from driver which is of last segment.
        val segmentProperties = new SegmentProperties(
          carbonMergerMapping.maxSegmentColumnSchemaList.asJava)

        val segmentMapping: java.util.Map[String, TaskBlockInfo] =
          CarbonCompactionUtil.createMappingForSegments(tableBlockInfoList)

        val dataFileMetadataSegMapping: java.util.Map[String, util.List[DataFileFooter]] =
          CarbonCompactionUtil.createDataFileFooterMappingForSegments(tableBlockInfoList,
            indexTable.getSortScope != SortScopeOptions.SortScope.NO_SORT)

        carbonLoadModelCopy.setTablePath(indexTablePath)
        // check for restructured block
        // TODO: only in case of add and drop this variable should be true
        val restructuredBlockExists: Boolean = CarbonCompactionUtil
          .checkIfAnyRestructuredBlockExists(segmentMapping,
            dataFileMetadataSegMapping,
            indexTable.getTableLastUpdatedTime)
        LOGGER.info(s"Restructured block exists: $restructuredBlockExists")
        DataTypeUtil.setDataTypeConverter(new SparkDataTypeConverterImpl)
        exec = new CarbonCompactionExecutor(segmentMapping, segmentProperties,
          indexTable, dataFileMetadataSegMapping, restructuredBlockExists,
          new SparkDataTypeConverterImpl)

        // add task completion listener to clean up the resources
        context.addTaskCompletionListener { _ =>
          close()
        }
        try {
          // fire a query and get the results.
          rawResultIteratorMap = exec.processTableBlocks(FileFactory.getConfiguration, null)
        } catch {
          case e: Throwable =>
            LOGGER.error(e)
            if (null != e.getMessage) {
              CarbonException.analysisException(
                s"Exception occurred in query execution :: ${ e.getMessage }")
            } else {
              CarbonException.analysisException(
                "Exception occurred in query execution.Please check logs.")
            }
        }

        val tempStoreLoc = CarbonDataProcessorUtil.getLocalDataFolderLocation(
          indexTable, carbonLoadModelCopy.getTaskNo, mergeNumber, true, false)

        if (indexTable.getSortScope == SortScopeOptions.SortScope.NO_SORT ||
            rawResultIteratorMap.get(CarbonCompactionUtil.UNSORTED_IDX).size() == 0) {

          LOGGER.info("RowResultMergerProcessor flow is selected")
          processor = new RowResultMergerProcessor(
            databaseName,
            indexTableName,
            segmentProperties,
            tempStoreLoc,
            carbonLoadModelCopy,
            carbonMergerMapping.campactionType,
            null)

        } else {

          LOGGER.info("CompactionResultSortProcessor flow is selected")
          processor = new CompactionResultSortProcessor(
            carbonLoadModelCopy,
            indexTable,
            segmentProperties,
            carbonMergerMapping.campactionType,
            indexTableName,
            null)

        }

        mergeStatus = processor.execute(
          rawResultIteratorMap.get(CarbonCompactionUtil.UNSORTED_IDX),
          rawResultIteratorMap.get(CarbonCompactionUtil.SORTED_IDX))
        mergeResult = tableBlockInfoList.get(0).getSegmentId + ',' + mergeNumber
      } catch {
        case e: Exception =>
          LOGGER.error("Compaction Failed ", e)
          throw e
      }

      private def close(): Unit = {
        deleteLocalDataFolders()
        // close all the query executor service and clean up memory acquired during query processing
        if (null != exec) {
          LOGGER.info("Cleaning up query resources acquired during compaction")
          exec.close(rawResultIteratorMap.get(CarbonCompactionUtil.UNSORTED_IDX), queryStartTime)
          exec.close(rawResultIteratorMap.get(CarbonCompactionUtil.SORTED_IDX), queryStartTime)
        }
        // clean up the resources for processor
        if (null != processor) {
          LOGGER.info("Closing compaction processor instance to clean up loading resources")
          processor.close()
        }
      }

      private def deleteLocalDataFolders(): Unit = {
        try {
          LOGGER.info("Deleting local folder store location")
          val isCompactionFlow = true
          TableProcessingOperations
            .deleteLocalDataLoadFolderLocation(carbonLoadModelCopy, isCompactionFlow, false)
        } catch {
          case e: Exception =>
            LOGGER.error(e)
        }
      }

      var finished = false

      override def hasNext: Boolean = {
        !finished
      }

      override def next(): ((K, V), String) = {
        finished = true
        (result.getKey(mergeResult, mergeStatus), segmentId)
      }
    }
    iter
  }

}
