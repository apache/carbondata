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
package org.apache.spark.util

import java.io.{File, IOException}
import java.text.SimpleDateFormat
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.command.AlterPartitionModel

import org.apache.carbondata.core.datastore.block.{SegmentProperties, TableBlockInfo}
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.spark.util.CommonUtil

object PartitionUtils {

  def getListInfo(originListInfo: String): List[List[String]] = {
    var listInfo = ListBuffer[List[String]]()
    var templist = ListBuffer[String]()
    val arr = originListInfo.split(",")
      .map(_.trim())
    var groupEnd = true
    val iter = arr.iterator
    while (iter.hasNext) {
      val value = iter.next()
      if (value.startsWith("(")) {
        templist += value.replace("(", "").trim()
        groupEnd = false
      } else if (value.endsWith(")")) {
        templist += value.replace(")", "").trim()
        listInfo += templist.toList
        templist.clear()
        groupEnd = true
      } else {
        if (groupEnd) {
          templist += value
          listInfo += templist.toList
          templist.clear()
        } else {
          templist += value
        }
      }
    }
    listInfo.toList
  }

  /**
   * verify the add/split information and update the partitionInfo:
   *  1. update rangeInfo/listInfo
   *  2. update partitionIds
   */
  def updatePartitionInfo(partitionInfo: PartitionInfo, partitionIdList: List[Int],
      partitionId: Int, splitInfo: List[String], timestampFormatter: SimpleDateFormat,
      dateFormatter: SimpleDateFormat): Unit = {
    val columnDataType = partitionInfo.getColumnSchemaList.get(0).getDataType
    val index = partitionIdList.indexOf(partitionId)
    if (index < 0) {
      throw new IllegalArgumentException("Invalid Partition Id " + partitionId +
        "\n Use show partitions table_name to get the list of valid partitions")
    }
    if (partitionInfo.getPartitionType == PartitionType.RANGE) {
      val rangeInfo = partitionInfo.getRangeInfo.asScala.toList
      val newRangeInfo = partitionId match {
        case 0 => rangeInfo ++ splitInfo
        case _ => rangeInfo.take(index - 1) ++ splitInfo ++
          rangeInfo.takeRight(rangeInfo.size - index)
      }
      CommonUtil.validateRangeInfo(newRangeInfo, columnDataType,
        timestampFormatter, dateFormatter)
      partitionInfo.setRangeInfo(newRangeInfo.asJava)
    } else if (partitionInfo.getPartitionType == PartitionType.LIST) {
      val originList = partitionInfo.getListInfo.asScala.map(_.asScala.toList).toList
      if (partitionId != 0) {
        val targetListInfo = partitionInfo.getListInfo.get(index - 1)
        CommonUtil.validateSplitListInfo(targetListInfo.asScala.toList, splitInfo, originList)
      } else {
        CommonUtil.validateAddListInfo(splitInfo, originList)
      }
      val addListInfo = PartitionUtils.getListInfo(splitInfo.mkString(","))
      val newListInfo = partitionId match {
        case 0 => originList ++ addListInfo
        case _ => originList.take(index - 1) ++ addListInfo ++
          originList.takeRight(originList.size - index)
      }
      partitionInfo.setListInfo(newListInfo.map(_.asJava).asJava)
    }

    if (partitionId == 0) {
      partitionInfo.addPartition(splitInfo.size)
    } else {
      partitionInfo.splitPartition(index, splitInfo.size)
    }
  }

  /**
   * Used for alter table partition commands to get segmentProperties in spark node
   * @param identifier
   * @param segmentId
   * @param oldPartitionIdList   Task id group before partition info is changed
   * @return
   */
  def getSegmentProperties(identifier: AbsoluteTableIdentifier, segmentId: String,
      partitionIds: List[String], oldPartitionIdList: List[Int],
      partitionInfo: PartitionInfo,
      carbonTable: CarbonTable): SegmentProperties = {
    val tableBlockInfoList =
      getPartitionBlockList(
        identifier,
        segmentId,
        partitionIds,
        oldPartitionIdList,
        partitionInfo,
        carbonTable)
    val footer = CarbonUtil.readMetadatFile(tableBlockInfoList.get(0))
    val segmentProperties = new SegmentProperties(footer.getColumnInTable,
      footer.getSegmentInfo.getColumnCardinality)
    segmentProperties
  }

  def getPartitionBlockList(identifier: AbsoluteTableIdentifier, segmentId: String,
      partitionIds: List[String], oldPartitionIdList: List[Int],
      partitionInfo: PartitionInfo,
      carbonTable: CarbonTable): java.util.List[TableBlockInfo] = {
    val jobConf = new JobConf(new Configuration)
    val job = new Job(jobConf)
    val format = CarbonInputFormatUtil
      .createCarbonTableInputFormat(identifier, partitionIds.asJava, job)
    CarbonTableInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
    val splits = format.getSplitsOfOneSegment(job, segmentId,
      oldPartitionIdList.map(_.asInstanceOf[Integer]).asJava, partitionInfo)
    val blockList = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
    val tableBlockInfoList = CarbonInputSplit.createBlocks(blockList.asJava)
    tableBlockInfoList
  }

  @throws(classOf[IOException])
  def deleteOriginalCarbonFile(alterPartitionModel: AlterPartitionModel,
      identifier: AbsoluteTableIdentifier,
      partitionIds: List[String], dbName: String, tableName: String,
      partitionInfo: PartitionInfo): Unit = {
    val carbonLoadModel = alterPartitionModel.carbonLoadModel
    val segmentId = alterPartitionModel.segmentId
    val oldPartitionIds = alterPartitionModel.oldPartitionIds
    val newTime = carbonLoadModel.getFactTimeStamp
    val tablePath = carbonLoadModel.getTablePath
    val tableBlockInfoList =
      getPartitionBlockList(identifier, segmentId, partitionIds, oldPartitionIds,
        partitionInfo, carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable).asScala
    val pathList: util.List[String] = new util.ArrayList[String]()
    tableBlockInfoList.foreach{ tableBlockInfo =>
      val path = tableBlockInfo.getFilePath
      val timestamp = CarbonTablePath.DataFileUtil.getTimeStampFromFileName(path)
      if (timestamp.toLong != newTime) {
        // add carbondata file
        pathList.add(path)
        // add index file
        val version = tableBlockInfo.getVersion
        val taskNo = CarbonTablePath.DataFileUtil.getTaskNo(path)
        val batchNo = CarbonTablePath.DataFileUtil.getBatchNoFromTaskNo(taskNo)
        val taskId = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(taskNo)
        val bucketNumber = CarbonTablePath.DataFileUtil.getBucketNo(path)
        val indexFilePath = CarbonTablePath.getCarbonIndexFilePath(
          tablePath, String.valueOf(taskId), segmentId, batchNo, String.valueOf(bucketNumber),
          timestamp, version)
        // indexFilePath could be duplicated when multiple data file related to one index file
        if (indexFilePath != null && !pathList.contains(indexFilePath)) {
          pathList.add(indexFilePath)
        }
      }
    }
    val files: util.List[File] = new util.ArrayList[File]()
    for (path <- pathList.asScala) {
      val file = new File(path)
      files.add(file)
    }
    CarbonUtil.deleteFiles(files.asScala.toArray)
  }
}
