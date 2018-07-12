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
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.execution.command.{AlterPartitionModel, DataMapField, Field, PartitionerField}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.Segment
import org.apache.carbondata.core.datastore.block.{SegmentProperties, TableBlockInfo}
import org.apache.carbondata.core.datastore.filesystem.CarbonFile
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.{AbsoluteTableIdentifier, CarbonTableIdentifier, SegmentFileStore}
import org.apache.carbondata.core.metadata.schema.PartitionInfo
import org.apache.carbondata.core.metadata.schema.partition.PartitionType
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope
import org.apache.carbondata.core.statusmanager.SegmentStatusManager
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableInputFormat}
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
    val jobConf = new JobConf(FileFactory.getConfiguration)
    val job = new Job(jobConf)
    val format = CarbonInputFormatUtil
      .createCarbonTableInputFormat(identifier, partitionIds.asJava, job)
    CarbonInputFormat.setTableInfo(job.getConfiguration, carbonTable.getTableInfo)
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
        val indexFilePath =
          new Path(new Path(path).getParent,
            CarbonTablePath.getCarbonIndexFileName(taskId,
            bucketNumber.toInt,
            batchNo,
            timestamp,
            segmentId)).toString
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
    if (!files.isEmpty) {
      val carbonTable = alterPartitionModel.carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
      val updatedSegFile: String = mergeAndUpdateSegmentFile(alterPartitionModel,
        identifier,
        segmentId,
        carbonTable,
        files.asScala)

      val segmentFiles = Seq(new Segment(alterPartitionModel.segmentId, updatedSegFile, null))
        .asJava
      if (!CarbonUpdateUtil.updateTableMetadataStatus(
        new util.HashSet[Segment](Seq(new Segment(alterPartitionModel.segmentId,
          null, null)).asJava),
        carbonTable,
        alterPartitionModel.carbonLoadModel.getFactTimeStamp.toString,
        true,
        new util.ArrayList[Segment](0),
        new util.ArrayList[Segment](segmentFiles), "")) {
        throw new IOException("Data update failed due to failure in table status updation.")
      }
    }
  }

  /**
   * Used to extract PartitionerFields for aggregation datamaps.
   * This method will keep generating partitionerFields until the sequence of
   * partition column is broken.
   *
   * For example: if x,y,z are partition columns in main table then child tables will be
   * partitioned only if the child table has List("x,y,z", "x,y", "x") as the projection columns.
   *
   *
   */
  def getPartitionerFields(allPartitionColumn: Seq[String],
      fieldRelations: mutable.LinkedHashMap[Field, DataMapField]): Seq[PartitionerField] = {

    def generatePartitionerField(partitionColumn: List[String],
        partitionerFields: Seq[PartitionerField]): Seq[PartitionerField] = {
      partitionColumn match {
        case head :: tail =>
          // Collect the first relation which matched the condition
          val validRelation = fieldRelations.zipWithIndex.collectFirst {
            case ((field, dataMapField), index) if
            dataMapField.columnTableRelationList.getOrElse(Seq()).nonEmpty &&
            head.equals(dataMapField.columnTableRelationList.get.head.parentColumnName) &&
            dataMapField.aggregateFunction.isEmpty =>
              (PartitionerField(field.name.get,
                field.dataType,
                field.columnComment), allPartitionColumn.indexOf(head))
          }
          if (validRelation.isDefined) {
            val (partitionerField, index) = validRelation.get
            // if relation is found then check if the partitionerFields already found are equal
            // to the index of this element.
            // If x with index 1 is found then there should be exactly 1 element already found.
            // If z with index 2 comes directly after x then this check will be false are 1
            // element is skipped in between and index would be 2 and number of elements found
            // would be 1. In that case return empty sequence so that the aggregate table is not
            // partitioned on any column.
            if (index == partitionerFields.length) {
              generatePartitionerField(tail, partitionerFields :+ partitionerField)
            } else {
              Seq.empty
            }
          } else {
            // if not found then countinue search for the rest of the elements. Because the rest
            // of the elements can also decide if the table has to be partitioned or not.
            generatePartitionerField(tail, partitionerFields)
          }
        case Nil =>
          // if end of list then return fields.
          partitionerFields
      }
    }

    generatePartitionerField(allPartitionColumn.toList, Seq.empty)
  }


  private def mergeAndUpdateSegmentFile(alterPartitionModel: AlterPartitionModel,
      identifier: AbsoluteTableIdentifier,
      segmentId: String,
      carbonTable: CarbonTable, filesToBeDelete: Seq[File]) = {
    val metadataDetails =
      SegmentStatusManager.readTableStatusFile(
        CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath))
    val segmentFile =
      metadataDetails.find(_.getLoadName.equals(segmentId)).get.getSegmentFile
    var allSegmentFiles: Seq[CarbonFile] = Seq.empty[CarbonFile]
    val file = SegmentFileStore.writeSegmentFile(
      carbonTable,
      alterPartitionModel.segmentId,
      System.currentTimeMillis().toString)
    if (segmentFile != null) {
      allSegmentFiles ++= FileFactory.getCarbonFile(
        SegmentFileStore.getSegmentFilePath(carbonTable.getTablePath, segmentFile)) :: Nil
    }
    val updatedSegFile = {
      val carbonFile = FileFactory.getCarbonFile(
        SegmentFileStore.getSegmentFilePath(carbonTable.getTablePath, file))
      allSegmentFiles ++= carbonFile :: Nil

      val mergedSegFileName = SegmentFileStore.genSegmentFileName(
        segmentId,
        alterPartitionModel.carbonLoadModel.getFactTimeStamp.toString)
      val tmpFile = mergedSegFileName + "_tmp"
      val segmentStoreFile = SegmentFileStore.mergeSegmentFiles(
        tmpFile,
        CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath),
        allSegmentFiles.toArray)
      val indexFiles = segmentStoreFile.getLocationMap.values().asScala.head.getFiles
      filesToBeDelete.foreach(f => indexFiles.remove(f.getName))
      SegmentFileStore.writeSegmentFile(
        segmentStoreFile,
        CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath) +
        CarbonCommonConstants.FILE_SEPARATOR + mergedSegFileName + CarbonTablePath.SEGMENT_EXT)
      carbonFile.delete()
      FileFactory.getCarbonFile(
        SegmentFileStore.getSegmentFilePath(
          carbonTable.getTablePath, tmpFile + CarbonTablePath.SEGMENT_EXT)).delete()
      mergedSegFileName + CarbonTablePath.SEGMENT_EXT
    }
    updatedSegFile
  }
}
