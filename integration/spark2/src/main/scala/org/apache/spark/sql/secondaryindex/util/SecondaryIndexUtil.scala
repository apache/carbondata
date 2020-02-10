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
package org.apache.spark.sql.secondaryindex.util

import java.io.IOException
import java.util
import java.util.{Collections, Comparator, List}

import scala.collection.JavaConverters._
import scala.util.control.Breaks

import org.apache.log4j.Logger
import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.{CarbonEnv, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel}
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.secondaryindex.rdd.CarbonSIRebuildRDD
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datamap.{DataMapStoreManager, Segment}
import org.apache.carbondata.core.datastore.block.{TableBlockInfo, TaskBlockInfo}
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.datatype.DataType
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.MergeResultImpl

object SecondaryIndexUtil {

  val LOGGER: Logger = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Merge the data files of the SI segments in case of small files
   *
   */
  def mergeDataFilesSISegments(segmentIdToLoadStartTimeMapping: scala.collection.mutable
  .Map[String, java.lang.Long],
    indexCarbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails],
    carbonLoadModel: CarbonLoadModel,
    isRebuildCommand: Boolean = false)
    (sqlContext: SQLContext): Set[String] = {
    var rebuildSegmentProperty = false
    try {
      rebuildSegmentProperty = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE,
        CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE_DEFAULT).toBoolean

    } catch {
      case _: Exception =>
        rebuildSegmentProperty = CarbonCommonConstants.CARBON_SI_SEGMENT_MERGE_DEFAULT
          .toBoolean
    }
    try {
      // in case of manual rebuild command, no need to consider the carbon property
      if (rebuildSegmentProperty || isRebuildCommand) {
        scanSegmentsAndSubmitJob(segmentIdToLoadStartTimeMapping,
          indexCarbonTable,
          loadsToMerge, carbonLoadModel)(sqlContext)
      } else {
        Set.empty
      }
    } catch {
      case ex: Exception =>
        throw ex
    }
  }

  /**
   * This will submit the loads to be merged into the executor.
   */
  def scanSegmentsAndSubmitJob(
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long],
    indexCarbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails], carbonLoadModel: CarbonLoadModel)
    (sqlContext: SQLContext): Set[String] = {
    loadsToMerge.asScala.foreach { seg =>
      LOGGER.info("loads identified for merge is " + seg.getLoadName)
    }
    if (loadsToMerge.isEmpty) {
      Set.empty
    } else {
      val compactionCallableModel = CompactionCallableModel(
        carbonLoadModel,
        indexCarbonTable,
        loadsToMerge,
        sqlContext,
        null,
        CarbonFilters.getCurrentPartitions(sqlContext.sparkSession,
          TableIdentifier(indexCarbonTable.getTableName,
            Some(indexCarbonTable.getDatabaseName))),
        null)
      triggerCompaction(compactionCallableModel, segmentIdToLoadStartTimeMapping)(sqlContext)
    }
  }

  /**
   * Get a new CarbonLoadModel
   *
   */
  def getCarbonLoadModel(indexCarbonTable: CarbonTable,
    loadsToMerge: util.List[LoadMetadataDetails],
    factTimeStamp: Long,
    columnCompressor: String): CarbonLoadModel = {
    val carbonLoadModel = new CarbonLoadModel
    carbonLoadModel.setCarbonDataLoadSchema(new CarbonDataLoadSchema(indexCarbonTable))
    carbonLoadModel.setTableName(indexCarbonTable.getTableName)
    carbonLoadModel.setDatabaseName(indexCarbonTable.getDatabaseName)
    carbonLoadModel.setLoadMetadataDetails(loadsToMerge)
    carbonLoadModel.setTablePath(indexCarbonTable.getTablePath)
    carbonLoadModel.setFactTimeStamp(factTimeStamp)
    carbonLoadModel.setColumnCompressor(columnCompressor)
    carbonLoadModel
  }

  private def triggerCompaction(compactionCallableModel: CompactionCallableModel,
    segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long])
    (sqlContext: SQLContext): Set[String] = {
    val indexCarbonTable = compactionCallableModel.carbonTable
    val sc = compactionCallableModel.sqlContext
    val carbonLoadModel = compactionCallableModel.carbonLoadModel
    val compactionType = compactionCallableModel.compactionType
    val partitions = compactionCallableModel.currentPartitions
    val tablePath = indexCarbonTable.getTablePath
    val startTime = System.nanoTime()
    var finalMergeStatus = false
    val databaseName: String = indexCarbonTable.getDatabaseName
    val factTableName = indexCarbonTable.getTableName
    val validSegments: util.List[Segment] = CarbonDataMergerUtil
      .getValidSegments(compactionCallableModel.loadsToMerge)
    val mergedLoadName: String = ""
    val carbonMergerMapping = CarbonMergerMapping(
      tablePath,
      indexCarbonTable.getMetadataPath,
      mergedLoadName,
      databaseName,
      factTableName,
      validSegments.asScala.toArray,
      indexCarbonTable.getAbsoluteTableIdentifier.getCarbonTableIdentifier.getTableId,
      compactionType,
      maxSegmentColumnSchemaList = null,
      currentPartitions = partitions)
    carbonLoadModel.setTablePath(carbonMergerMapping.hdfsStoreLocation)
    carbonLoadModel.setLoadMetadataDetails(
      SegmentStatusManager.readLoadMetadata(indexCarbonTable.getMetadataPath).toList.asJava)

    val mergedSegments: util.Set[LoadMetadataDetails] = new util.HashSet[LoadMetadataDetails]()
    var rebuiltSegments: Set[String] = Set[String]()
    val segmentIdToLoadStartTimeMap: util.Map[String, String] = new util.HashMap()

    try {
      val mergeStatus =
        new CarbonSIRebuildRDD(
          sc.sparkSession,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping
        ).collect
      if (null != mergeStatus && mergeStatus.length == 0) {
        finalMergeStatus = true
      } else {
        finalMergeStatus = mergeStatus.forall(_._1._2)
        rebuiltSegments = mergeStatus.map(_._2).toSet
        compactionCallableModel.loadsToMerge.asScala.foreach(metadataDetails => {
          if (rebuiltSegments.contains(metadataDetails.getLoadName)) {
            mergedSegments.add(metadataDetails)
            segmentIdToLoadStartTimeMap
              .put(metadataDetails.getLoadName, String.valueOf(metadataDetails.getLoadStartTime))
          }
        })
      }
      if (finalMergeStatus) {
        if (null != mergeStatus && mergeStatus.length != 0) {
          mergedSegments.asScala.map { seg =>
            val file = SegmentFileStore.writeSegmentFile(
              indexCarbonTable,
              seg.getLoadName,
              segmentIdToLoadStartTimeMapping(seg.getLoadName).toString,
              carbonLoadModel.getFactTimeStamp.toString,
              null)
            val segment = new Segment(seg.getLoadName, file)
            SegmentFileStore.updateTableStatusFile(indexCarbonTable,
              seg.getLoadName,
              file,
              indexCarbonTable.getCarbonTableIdentifier.getTableId,
              new SegmentFileStore(tablePath, segment.getSegmentFileName))
            segment
          }

          val endTime = System.currentTimeMillis()
          val loadMetadataDetails = SegmentStatusManager
            .readLoadMetadata(indexCarbonTable.getMetadataPath)
          loadMetadataDetails.foreach(loadMetadataDetail => {
            if (rebuiltSegments.contains(loadMetadataDetail.getLoadName)) {
              loadMetadataDetail.setLoadStartTime(carbonLoadModel.getFactTimeStamp)
              loadMetadataDetail.setLoadEndTime(endTime)
              CarbonLoaderUtil
                .addDataIndexSizeIntoMetaEntry(loadMetadataDetail,
                  loadMetadataDetail.getLoadName,
                  indexCarbonTable)
            }
          })

          SegmentStatusManager
            .writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(tablePath),
              loadMetadataDetails)

          // clear the datamap cache for the merged segments, as the index files and
          // data files are rewritten after compaction
          if (mergedSegments.size > 0) {

            // merge index files for merged segments
            CarbonMergeFilesRDD.mergeIndexFiles(sc.sparkSession,
              rebuiltSegments.toSeq,
              segmentIdToLoadStartTimeMap,
              indexCarbonTable.getTablePath,
              indexCarbonTable, mergeIndexProperty = false
            )

            if (CarbonProperties.getInstance()
              .isDistributedPruningEnabled(indexCarbonTable.getDatabaseName,
                indexCarbonTable.getTableName)) {
              try {
                IndexServer.getClient
                  .invalidateSegmentCache(indexCarbonTable,
                    rebuiltSegments.toArray,
                    SparkSQLUtil.getTaskGroupId(sc.sparkSession))
              } catch {
                case _: Exception =>
              }
            }

            DataMapStoreManager.getInstance
              .clearInvalidSegments(indexCarbonTable, rebuiltSegments.toList.asJava)
          }
        }

        val endTime = System.nanoTime()
        LOGGER.info(s"Time taken to merge is(in nano) ${endTime - startTime}")
        LOGGER.info(s"Merge data files request completed for table " +
                    s"${indexCarbonTable.getDatabaseName}.${indexCarbonTable.getTableName}")
        rebuiltSegments
      } else {
        LOGGER.error(s"Merge data files request failed for table " +
                     s"${indexCarbonTable.getDatabaseName}.${indexCarbonTable.getTableName}")
        throw new Exception("Merge data files Failure in Merger Rdd.")
      }
    } catch {
      case e: Exception =>
        LOGGER.error(s"Merge data files request failed for table " +
                     s"${indexCarbonTable.getDatabaseName}.${indexCarbonTable.getTableName}")
        throw new Exception("Merge data files Failure in Merger Rdd.", e)
    }
  }

  /**
   * Identifies the group of blocks to be merged based on the merge size.
   * This should be per segment grouping.
   *
   * @return List of List of blocks(grouped based on the size)
   */
  def identifyBlocksToBeMerged(splits: util.List[CarbonInputSplit], mergeSize: Long):
  java.util.List[java.util.List[CarbonInputSplit]] = {
    val blockGroupsToMerge: java.util.List[java.util.List[CarbonInputSplit]] = new util.ArrayList()
    var totalSize: Long = 0L
    var blocksSelected: java.util.List[CarbonInputSplit] = new util.ArrayList()
    // sort the splits based on the block size and then make groups based on the threshold
    Collections.sort(splits, new Comparator[CarbonInputSplit]() {
      def compare(split1: CarbonInputSplit, split2: CarbonInputSplit): Int = {
        (split1.getLength - split2.getLength).toInt
      }
    })
    for (i <- 0 until splits.size()) {
      val block = splits.get(i)
      val blockFileSize = block.getLength
      blocksSelected.add(block)
      totalSize += blockFileSize
      if (totalSize >= mergeSize) {
        if (!blocksSelected.isEmpty) {
          blockGroupsToMerge.add(blocksSelected)
        }
        totalSize = 0L
        blocksSelected = new util.ArrayList()
      }
    }
    if (!blocksSelected.isEmpty) {
      blockGroupsToMerge.add(blocksSelected)
    }
    // check if all the groups are having only one split, then ignore rebuilding that segment
    if (blockGroupsToMerge.size() == splits.size()) {
      new util.ArrayList[util.List[CarbonInputSplit]]()
    } else {
      blockGroupsToMerge
    }
  }

  /**
   * To create a mapping of task and block
   *
   */
  def createTaskAndBlockMapping(tableBlockInfoList: util.List[TableBlockInfo]): TaskBlockInfo = {
    val taskBlockInfo = new TaskBlockInfo
    tableBlockInfoList.asScala.foreach { info =>
      val taskNo = CarbonTablePath.DataFileUtil.getTaskNo(info.getFilePath)
      groupCorrespodingInfoBasedOnTask(info, taskBlockInfo, taskNo)
    }
    taskBlockInfo
  }

  /**
   * Grouping the taskNumber and list of TableBlockInfo.
   *
   */
  private def groupCorrespodingInfoBasedOnTask(info: TableBlockInfo,
      taskBlockMapping: TaskBlockInfo,
      taskNo: String): Unit = {
    // get the corresponding list from task mapping.
    var blockLists = taskBlockMapping.getTableBlockInfoList(taskNo)
    if (null != blockLists) {
      blockLists.add(info)
    } else {
      blockLists = new util.ArrayList[TableBlockInfo](CarbonCommonConstants.DEFAULT_COLLECTION_SIZE)
      blockLists.add(info)
      taskBlockMapping.addTableBlockInfoList(taskNo, blockLists)
    }
  }

  /**
   * This method will read the file footer of given block
   *
   */
  def readFileFooter(tableBlockInfo: TableBlockInfo): DataFileFooter = {
    val dataFileFooter = try {
      CarbonUtil.readMetadataFile(tableBlockInfo)
    } catch {
      case e: IOException =>
        throw new IOException(
          "Problem reading the file footer during secondary index creation: " + e.getMessage)
    }
    dataFileFooter
  }

  /**
   * In case of secondary index table all the columns participate in SORT. So,
   * only for SI table sorting all the no dictionary data types are needed.
   *
   */
  def getNoDictDataTypes(carbonTable: CarbonTable): Array[DataType] = {
    val dimensions = carbonTable.getVisibleDimensions
    val dataTypeList = new util.ArrayList[DataType]
    for (i <- 0 until dimensions.size) {
      if (!dimensions.get(i).hasEncoding(Encoding.DICTIONARY)) {
        dataTypeList.add(dimensions
          .get(i)
          .getDataType)
      }
    }
    dataTypeList.toArray(new Array[DataType](dataTypeList.size))
  }

  // For the compacted segment the timestamp in the main table segment
  // and index table segment may be different. This method updates the
  // correct timestamp for index segment while updating the tablestatus
  // in the metadata of index table.
  def updateTimeStampForIndexTable(mainTableLoad: Array[LoadMetadataDetails],
      indexTableLoads: Array[LoadMetadataDetails]): Array[LoadMetadataDetails] = {
    for {
      indexLoad <- indexTableLoads
      mainTableLoad <- mainTableLoad
      if indexLoad.getLoadName.equalsIgnoreCase(mainTableLoad.getLoadName)
    } yield {
      mainTableLoad.setSegmentFile(indexLoad.getSegmentFile)
      mainTableLoad
    }
  }

  /**
   * This method will update the deletion status for all the index tables
   *
   */
  def updateTableStatusForIndexTables(parentCarbonTable: CarbonTable,
      indexTables: java.util.List[CarbonTable]): Unit = {
    val loadFolderDetailsArrayMainTable =
      SegmentStatusManager.readLoadMetadata(parentCarbonTable.getMetadataPath)
    indexTables.asScala.foreach { indexTable =>
      val tableStatusFilePath = CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath)
      if (CarbonUtil.isFileExists(tableStatusFilePath)) {
        val loadFolderDetailsArray = SegmentStatusManager.readLoadMetadata(indexTable
          .getMetadataPath);
        if (null != loadFolderDetailsArray && loadFolderDetailsArray.nonEmpty) {
          try {
            SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath),
              updateTimeStampForIndexTable(loadFolderDetailsArrayMainTable,
                loadFolderDetailsArray))
          } catch {
            case ex: Exception =>
              LOGGER.error(ex.getMessage);
          }
        }
      } else {
        LOGGER.info(
          "Table status file does not exist for index table: " + indexTable.getTableUniqueName)
      }
    }
  }

  /**
   * This method will return fact table to index table column mapping
   *
   */
  def prepareColumnMappingOfFactToIndexTable(carbonTable: CarbonTable,
      indexTable: CarbonTable,
      isDictColsAlone: Boolean): Array[Int] = {
    val loop = new Breaks
    val factTableDimensions = carbonTable.getVisibleDimensions
    val indexTableDimensions = indexTable.getVisibleDimensions
    val dims = new util.ArrayList[Integer]
    loop.breakable {
      for (indexTableDimension <- indexTableDimensions.asScala) {
        for (i <- 0 until factTableDimensions.size) {
          val dim = factTableDimensions.get(i)
          if (dim.getColumnId == indexTableDimension.getColumnId) {
            if (isDictColsAlone && dim.hasEncoding(Encoding.DICTIONARY)) {
              dims.add(i)
            } else if (!isDictColsAlone) {
              dims.add(i)
            }
            loop.break()
          }
        }
      }
    }
    val sortedDims = new util.ArrayList[Integer](dims.size)
    sortedDims.addAll(dims)
    Collections.sort(sortedDims)
    val dimsCount = sortedDims.size
    val indexToFactColMapping = new Array[Int](dimsCount)
    for (i <- 0 until dimsCount) {
      indexToFactColMapping(sortedDims.indexOf(dims.get(i))) = i
    }
    indexToFactColMapping
  }

  /**
   * Identifies all segments which can be merged for compaction type - CUSTOM.
   *
   * @param sparkSession
   * @param tableName
   * @param dbName
   * @param customSegments
   * @return list of LoadMetadataDetails
   * @throws UnsupportedOperationException   if customSegments is null or empty
   */
  def identifySegmentsToBeMergedCustom(sparkSession: SparkSession,
      tableName: String,
      dbName: String,
      customSegments: util.List[String]): util.List[LoadMetadataDetails] = {
    val (carbonLoadModel: CarbonLoadModel, compactionSize: Long, segments:
      Array[LoadMetadataDetails]) = getSegmentDetails(
      sparkSession,
      tableName,
      dbName,
      CompactionType.CUSTOM)
    if (customSegments.equals(null) || customSegments.isEmpty) {
      throw new UnsupportedOperationException("Custom Segments cannot be null or empty")
    }
    val identifiedSegments = CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionSize,
        segments.toList.asJava,
        CompactionType.CUSTOM,
        customSegments)
    if (identifiedSegments.size().equals(1)) {
      return new util.ArrayList[LoadMetadataDetails]()
    }
    identifiedSegments
  }

  /**
   * Returns the Merged Load Name for given list of segments
   *
   * @param list
   * @return Merged Load Name
   * @throws UnsupportedOperationException if list of segments is less than 1
   */
  def getMergedLoadName(list: util.List[LoadMetadataDetails]): String = {
    if (list.size() > 1) {
      val sortedSegments: java.util.List[LoadMetadataDetails] =
        new java.util.ArrayList[LoadMetadataDetails](list)
      CarbonDataMergerUtil.sortSegments(sortedSegments)
      CarbonDataMergerUtil.getMergedLoadName(sortedSegments)
    } else {
      throw new UnsupportedOperationException(
        "Compaction requires atleast 2 segments to be merged.But the input list size is " +
        list.size())
    }
  }

  private def getSegmentDetails(sparkSession: SparkSession,
      tableName: String,
      dbName: String,
      compactionType: CompactionType): (CarbonLoadModel, Long, Array[LoadMetadataDetails]) = {
    val carbonLoadModel = new CarbonLoadModel
    val carbonTable = CarbonEnv.getCarbonTable(Some(dbName), tableName)(sparkSession)
    val carbonDataLoadSchema = new CarbonDataLoadSchema(carbonTable)
    carbonLoadModel.setCarbonDataLoadSchema(carbonDataLoadSchema)
    val compactionSize = CarbonDataMergerUtil.getCompactionSize(compactionType, carbonLoadModel)
    val segments = SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
    (carbonLoadModel, compactionSize, segments)
  }

  /**
   * Identifies all segments which can be merged with compaction type - MAJOR.
   *
   * @return list of LoadMetadataDetails
   */
  def identifySegmentsToBeMerged(sparkSession: SparkSession,
      tableName: String,
      dbName: String): util.List[LoadMetadataDetails] = {
    val (carbonLoadModel: CarbonLoadModel, compactionSize: Long, segments:
      Array[LoadMetadataDetails]) = getSegmentDetails(
      sparkSession,
      tableName,
      dbName,
      CompactionType.MAJOR)
    val identifiedSegments = CarbonDataMergerUtil
      .identifySegmentsToBeMerged(carbonLoadModel,
        compactionSize,
        segments.toList.asJava,
        CompactionType.MAJOR,
        new util.ArrayList[String]())
    if (identifiedSegments.size().equals(1)) {
      return new util.ArrayList[LoadMetadataDetails]()
    }
    identifiedSegments
  }

}
