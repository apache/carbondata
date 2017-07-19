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
import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job

import org.apache.carbondata.core.datastore.block.{SegmentProperties, TableBlockInfo}
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil

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
   * Used for alter table partition commands to get segmentProperties in spark node
   * @param identifier
   * @param segmentId
   * @param oldPartitionIdList   Task id group before partition info is changed
   * @return
   */
  def getSegmentProperties(identifier: AbsoluteTableIdentifier, segmentId: String,
      partitionIds: List[String], oldPartitionIdList: List[Int]): SegmentProperties = {
    val tableBlockInfoList =
      getPartitionBlockList(identifier, segmentId, partitionIds, oldPartitionIdList)
    val footer = CarbonUtil.readMetadatFile(tableBlockInfoList.get(0))
    val segmentProperties = new SegmentProperties(footer.getColumnInTable,
      footer.getSegmentInfo.getColumnCardinality)
    segmentProperties
  }

  def getPartitionBlockList(identifier: AbsoluteTableIdentifier, segmentId: String,
      partitionIds: List[String], oldPartitionIdList: List[Int]): java.util.List[TableBlockInfo] = {
    val jobConf = new JobConf(new Configuration)
    val job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(identifier, partitionIds.asJava, job)
    val splits = format.getSplitsOfOneSegment(job, segmentId,
      oldPartitionIdList.map(_.asInstanceOf[Integer]).asJava)
    val blockList = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
    val tableBlockInfoList = CarbonInputSplit.createBlocks(blockList.asJava)
    tableBlockInfoList
  }

  @throws(classOf[IOException])
  def deleteOriginalCarbonFile(identifier: AbsoluteTableIdentifier, segmentId: String,
      partitionIds: List[String], oldPartitionIdList: List[Int], storePath: String,
      dbName: String, tableName: String): Unit = {
    val tableBlockInfoList =
      getPartitionBlockList(identifier, segmentId, partitionIds, oldPartitionIdList).asScala
    val pathList: util.List[String] = new util.ArrayList[String]()
    val carbonTablePath = new CarbonTablePath(storePath, dbName, tableName)
    tableBlockInfoList.foreach{ tableBlockInfo =>
      val path = tableBlockInfo.getFilePath
      // add carbondata file
      pathList.add(path)
      // add index file
      val version = tableBlockInfo.getVersion
      val timestamp = CarbonTablePath.DataFileUtil.getTimeStampFromFileName(path)
      val taskNo = CarbonTablePath.DataFileUtil.getTaskNo(path)
      val batchNo = CarbonTablePath.DataFileUtil.getBatchNoFromTaskNo(taskNo)
      val taskId = CarbonTablePath.DataFileUtil.getTaskIdFromTaskNo(taskNo)
      val bucketNumber = CarbonTablePath.DataFileUtil.getBucketNo(path)
      val indexFilePath = carbonTablePath.getCarbonIndexFilePath(String.valueOf(taskId), "0",
        segmentId, batchNo, String.valueOf(bucketNumber), timestamp, version)
      // indexFilePath could be duplicated when multiple data file related to one index file
      if (indexFilePath != null && !pathList.contains(indexFilePath)) {
        pathList.add(indexFilePath)
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
