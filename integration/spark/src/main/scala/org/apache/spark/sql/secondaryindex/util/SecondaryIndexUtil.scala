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
import java.util.{Collections, Comparator}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.log4j.Logger
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.rdd.CarbonMergeFilesRDD
import org.apache.spark.sql.{CarbonEnv, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{CarbonMergerMapping, CompactionCallableModel}
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.index.CarbonIndexUtil
import org.apache.spark.sql.optimizer.CarbonFilters
import org.apache.spark.sql.secondaryindex.rdd.{CarbonSIRebuildRDD, SecondaryIndexCreator}
import org.apache.spark.sql.util.SparkSQLUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.constants.SortScopeOptions.SortScope
import org.apache.carbondata.core.datastore.block.{TableBlockInfo, TaskBlockInfo}
import org.apache.carbondata.core.datastore.filesystem.{CarbonFile, CarbonFileFilter}
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.index.{IndexStoreManager, Segment}
import org.apache.carbondata.core.locks.CarbonLockUtil
import org.apache.carbondata.core.metadata.SegmentFileStore
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter
import org.apache.carbondata.core.metadata.datatype.{DataType, StructField, StructType}
import org.apache.carbondata.core.metadata.encoder.Encoding
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatusManager}
import org.apache.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.hadoop.api.{CarbonInputFormat, CarbonTableOutputFormat}
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil
import org.apache.carbondata.indexserver.IndexServer
import org.apache.carbondata.processing.loading.FailureCauses
import org.apache.carbondata.processing.loading.model.{CarbonDataLoadSchema, CarbonLoadModel}
import org.apache.carbondata.processing.merger.{CarbonDataMergerUtil, CompactionType}
import org.apache.carbondata.processing.util.CarbonLoaderUtil
import org.apache.carbondata.spark.MergeResultImpl
import org.apache.carbondata.spark.load.DataLoadProcessBuilderOnSpark

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
      val mergeStatus = if (SortScope.GLOBAL_SORT == indexCarbonTable.getSortScope &&
      !indexCarbonTable.getSortColumns.isEmpty) {
        mergeSISegmentDataFiles(sc.sparkSession, carbonLoadModel, carbonMergerMapping)
      } else {
        new CarbonSIRebuildRDD(sc.sparkSession,
          new MergeResultImpl(),
          carbonLoadModel,
          carbonMergerMapping).collect
      }
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
          val validSegmentsToUse = validSegments.asScala
            .filter(segment => mergeStatus.map(_._2).toSet.contains(segment.getSegmentNo))
          deleteOldIndexOrMergeIndexFiles(
            carbonLoadModel.getFactTimeStamp,
            validSegmentsToUse.toList.asJava,
            indexCarbonTable)
          if (SortScope.GLOBAL_SORT == indexCarbonTable.getSortScope &&
            !indexCarbonTable.getSortColumns.isEmpty) {
            deleteOldCarbonDataFiles(carbonLoadModel.getFactTimeStamp,
              validSegmentsToUse.toList.asJava,
              indexCarbonTable)
          }
          mergedSegments.asScala.map { seg =>
            val file = SegmentFileStore.writeSegmentFile(
              indexCarbonTable,
              seg.getLoadName,
              carbonLoadModel.getFactTimeStamp.toString,
              null,
              null)
            val segment = new Segment(seg.getLoadName, file)
            SegmentFileStore.updateTableStatusFile(indexCarbonTable,
              seg.getLoadName,
              file,
              indexCarbonTable.getCarbonTableIdentifier.getTableId,
              new SegmentFileStore(tablePath, segment.getSegmentFileName))
            segment
          }

          val statusLock =
            new SegmentStatusManager(indexCarbonTable.getAbsoluteTableIdentifier).getTableStatusLock
          try {
            val retryCount = CarbonLockUtil.getLockProperty(CarbonCommonConstants
              .NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT)
            val maxTimeout = CarbonLockUtil.getLockProperty(CarbonCommonConstants
              .MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT)
            if (statusLock.lockWithRetries(retryCount, maxTimeout)) {
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
            } else {
              throw new RuntimeException(
                "Not able to acquire the lock for table status updation for table " + databaseName +
                "." + indexCarbonTable.getTableName)
            }
          } finally {
            if (statusLock != null) {
              statusLock.unlock()
            }
          }
          // clear the indexSchema cache for the merged segments, as the index files and
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

            IndexStoreManager.getInstance
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
   * This method deletes the old index files or merge index file after data files merge
   */
  private def deleteOldIndexOrMergeIndexFiles(
      factTimeStamp: Long,
      validSegments: util.List[Segment],
      indexCarbonTable: CarbonTable): Unit = {
    // delete the index/merge index carbonFile of old data files
    validSegments.asScala.foreach { segment =>
      SegmentFileStore.getIndexFilesListForSegment(segment, indexCarbonTable.getTablePath)
        .asScala
        .foreach { indexFile =>
          if (DataFileUtil.getTimeStampFromFileName(indexFile).toLong < factTimeStamp) {
            FileFactory.getCarbonFile(indexFile).delete()
          }
        }
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
      groupCorrespondingInfoBasedOnTask(info, taskBlockInfo, taskNo)
    }
    taskBlockInfo
  }

  /**
   * Grouping the taskNumber and list of TableBlockInfo.
   *
   */
  private def groupCorrespondingInfoBasedOnTask(info: TableBlockInfo,
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
      val statusLock =
        new SegmentStatusManager(indexTable.getAbsoluteTableIdentifier).getTableStatusLock
      try {
        if (statusLock.lockWithRetries()) {
          val tableStatusFilePath = CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath)
          if (CarbonUtil.isFileExists(tableStatusFilePath)) {
            val loadFolderDetailsArray = SegmentStatusManager.readLoadMetadata(indexTable
              .getMetadataPath);
            if (null != loadFolderDetailsArray && loadFolderDetailsArray.nonEmpty) {
              SegmentStatusManager.writeLoadDetailsIntoFile(
                CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath),
                updateTimeStampForIndexTable(loadFolderDetailsArrayMainTable,
                  loadFolderDetailsArray))
            }
          } else {
            LOGGER.info(
              "Table status file does not exist for index table: " + indexTable.getTableUniqueName)
          }
        }
      } catch {
        case ex: Exception =>
          LOGGER.error(ex.getMessage);
      } finally {
        if (statusLock != null) {
          statusLock.unlock()
        }
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
            if (isDictColsAlone && indexTableDimension.hasEncoding(Encoding.DICTIONARY)) {
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
        "Compaction requires at least 2 segments to be merged.But the input list size is " +
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

  /**
   * This method deletes the old carbondata files.
   */
  private def deleteOldCarbonDataFiles(factTimeStamp: Long,
              validSegments: util.List[Segment],
              indexCarbonTable: CarbonTable): Unit = {
    validSegments.asScala.foreach { segment =>
      val segmentPath = CarbonTablePath.getSegmentPath(indexCarbonTable.getTablePath,
        segment.getSegmentNo)
      val dataFiles = FileFactory.getCarbonFile(segmentPath).listFiles(new CarbonFileFilter {
        override def accept(file: CarbonFile): Boolean = {
          file.getName.endsWith(CarbonTablePath.CARBON_DATA_EXT)
        }})
      dataFiles.foreach(dataFile =>
      if (DataFileUtil.getTimeStampFromFileName(dataFile.getAbsolutePath).toLong < factTimeStamp) {
        dataFile.delete()
      })
    }
  }

  def mergeSISegmentDataFiles(sparkSession: SparkSession,
      carbonLoadModel: CarbonLoadModel,
      carbonMergerMapping: CarbonMergerMapping): Array[((String, Boolean), String)] = {
    val validSegments = carbonMergerMapping.validSegments.toList
    val indexCarbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    val absoluteTableIdentifier = indexCarbonTable.getAbsoluteTableIdentifier
    val jobConf: JobConf = new JobConf(FileFactory.getConfiguration)
    SparkHadoopUtil.get.addCredentials(jobConf)
    val job: Job = new Job(jobConf)
    val format = CarbonInputFormatUtil.createCarbonInputFormat(absoluteTableIdentifier, job)
    CarbonInputFormat.setTableInfo(job.getConfiguration, indexCarbonTable.getTableInfo)
    var mergeStatus = ArrayBuffer[((String, Boolean), String)]()
    val mergeSize = getTableBlockSizeInMb(indexCarbonTable)(sparkSession) * 1024 * 1024
    val header = indexCarbonTable.getCreateOrderColumn.asScala.map(_.getColName).toArray
    val outputModel = getLoadModelForGlobalSort(sparkSession, indexCarbonTable)
    CarbonIndexUtil.initializeSILoadModel(outputModel, header)
    outputModel.setFactTimeStamp(carbonLoadModel.getFactTimeStamp)
    val segmentMetaDataAccumulator = sparkSession.sqlContext
      .sparkContext
      .collectionAccumulator[Map[String, SegmentMetaDataInfo]]
    validSegments.foreach { segment =>
      outputModel.setSegmentId(segment.getSegmentNo)
      val dataFrame = SparkSQLUtil.createInputDataFrame(
        sparkSession,
        indexCarbonTable)
      SecondaryIndexCreator.findCarbonScanRDD(dataFrame.rdd, null)
      val segList : java.util.List[Segment] = new util.ArrayList[Segment]()
      segList.add(segment)
      CarbonInputFormat.setSegmentsToAccess(job.getConfiguration, segList)
      CarbonInputFormat.setValidateSegmentsToAccess(job.getConfiguration, false)
      val splits = format.getSplits(job)
      val carbonInputSplits = splits.asScala.map(_.asInstanceOf[CarbonInputSplit])
      outputModel.setGlobalSortPartitions(identifyGlobalSortPartitions(carbonInputSplits.asJava,
        mergeSize))
      DataLoadProcessBuilderOnSpark.loadDataUsingGlobalSort(sparkSession,
        Option(dataFrame),
        outputModel,
        SparkSQLUtil.sessionState(sparkSession).newHadoopConf(),
        segmentMetaDataAccumulator)
        .map{ row =>
          ((row._1, FailureCauses.NONE == row._2._2.failureCauses), segment.getSegmentNo)}
        .foreach(status => mergeStatus += status)
    }
    mergeStatus.toArray
  }

  /**
   * create CarbonLoadModel for global_sort of SI segment data files merge
   */
  def getLoadModelForGlobalSort(sparkSession: SparkSession,
      carbonTable: CarbonTable): CarbonLoadModel = {
    val conf = SparkSQLUtil.sessionState(sparkSession).newHadoopConf()
    CarbonTableOutputFormat.setDatabaseName(conf, carbonTable.getDatabaseName)
    CarbonTableOutputFormat.setTableName(conf, carbonTable.getTableName)
    CarbonTableOutputFormat.setCarbonTable(conf, carbonTable)
    val fieldList = carbonTable.getCreateOrderColumn
      .asScala
      .map { column =>
        new StructField(column.getColName, column.getDataType)
      }
    CarbonTableOutputFormat.setInputSchema(conf, new StructType(fieldList.asJava))
    val loadModel = CarbonTableOutputFormat.getLoadModel(conf)
    loadModel
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
          CarbonIndexUtil.getParentTableName(indexTable))(sparkSession)
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

  /**
   * Identifies number of global sort partitions used for SI segment data file merge load
   * For eg: if block is 1MB and number of splits are 6 having size as follows.
   * (100KB, 200KB, 300KB, 400KB, 500KB, 600KB), Then,
   * (100KB + 200KB + 300KB + 400KB) >= 1MB, so it gives first partition
   * (500KB + 600KB) >= 1MB, and it gives second partition
   *
   */
  def identifyGlobalSortPartitions(splits: util.List[CarbonInputSplit], mergeSize: Long):
  String = {
    var partitions: Long = 0L
    var totalSize: Long = 0L
    // sort the splits based on the block size and then make groups based on the threshold
    Collections.sort(splits, new Comparator[CarbonInputSplit]() {
      def compare(split1: CarbonInputSplit, split2: CarbonInputSplit): Int = {
        (split1.getLength - split2.getLength).toInt
      }
    })
    for (i <- 0 until splits.size()) {
      val block = splits.get(i)
      val blockFileSize = block.getLength
      totalSize += blockFileSize
      if (totalSize >= mergeSize) {
        partitions = partitions + 1
        totalSize = 0L
      }
    }
    if (totalSize > 0) {
      partitions = partitions + 1
    }
    partitions.toString
  }

}
