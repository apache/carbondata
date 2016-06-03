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

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

import org.apache.hadoop.conf.{Configurable, Configuration}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.spark.{Logging, Partition, SparkContext}
import org.apache.spark.sql.{CarbonEnv, CarbonRelation, SQLContext}
import org.apache.spark.sql.execution.command.Partitioner
import org.apache.spark.util.{FileUtils, SplitUtils}

import org.carbondata.common.logging.LogServiceFactory
import org.carbondata.core.carbon.{AbsoluteTableIdentifier, CarbonDataLoadSchema, CarbonTableIdentifier}
import org.carbondata.core.carbon.datastore.block.TableBlockInfo
import org.carbondata.core.carbon.metadata.CarbonMetadata
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable
import org.carbondata.core.constants.CarbonCommonConstants
import org.carbondata.core.datastorage.store.impl.FileFactory
import org.carbondata.core.load.{BlockDetails, LoadMetadataDetails}
import org.carbondata.core.locks.{CarbonLockFactory, LockUsage}
import org.carbondata.core.util.{CarbonProperties, CarbonUtil}
import org.carbondata.lcm.status.SegmentStatusManager
import org.carbondata.processing.util.CarbonDataProcessorUtil
import org.carbondata.query.scanner.impl.{CarbonKey, CarbonValue}
import org.carbondata.spark._
import org.carbondata.spark.load._
import org.carbondata.spark.merger.CarbonDataMergerUtil
import org.carbondata.spark.splits.TableSplit
import org.carbondata.spark.util.{CarbonQueryUtil, LoadMetadataUtil}

/**
 * This is the factory class which can create different RDD depends on user needs.
 *
 */
object CarbonDataRDDFactory extends Logging {

  val logger = LogServiceFactory.getLogService(CarbonDataRDDFactory.getClass.getName)

  // scalastyle:off
  def partitionCarbonData(sc: SparkContext,
      schemaName: String,
      cubeName: String,
      sourcePath: String,
      targetFolder: String,
      requiredColumns: Array[String],
      headers: String,
      delimiter: String,
      quoteChar: String,
      escapeChar: String,
      multiLine: Boolean,
      partitioner: Partitioner): String = {
    // scalastyle:on
    val status = new
        CarbonDataPartitionRDD(sc, new PartitionResultImpl(), schemaName, cubeName, sourcePath,
          targetFolder, requiredColumns, headers, delimiter, quoteChar, escapeChar, multiLine,
          partitioner).collect
    CarbonDataProcessorUtil
      .renameBadRecordsFromInProgressToNormal("partition/" + schemaName + '/' + cubeName)
    var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
    status.foreach {
      case (key, value) =>
        if (value) {
          loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
        }
    }
    loadStatus
  }

  def mergeCarbonData(
      sc: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      hdfsStoreLocation: String,
      partitioner: Partitioner) {
    val cube = CarbonMetadata.getInstance()
      .getCarbonTable(carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName)
    val metaDataPath: String = cube.getMetaDataFilepath
    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
  }

  def deleteLoadByDate(
      sqlContext: SQLContext,
      schema: CarbonDataLoadSchema,
      schemaName: String,
      cubeName: String,
      tableName: String,
      hdfsStoreLocation: String,
      dateField: String,
      dateFieldActualName: String,
      dateValue: String,
      partitioner: Partitioner) {

    val sc = sqlContext
    // Delete the records based on data
    val cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(schemaName + "_" + cubeName)

    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(cube.getMetaDataFilepath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    var segmentStatusManager = new SegmentStatusManager(
      new AbsoluteTableIdentifier(
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier(schemaName, tableName)));
    val loadMetadataDetailsArray = segmentStatusManager.readLoadMetadata(cube.getMetaDataFilepath())
      .toList
    val resultMap = new CarbonDeleteLoadByDateRDD(
      sc.sparkContext,
      new DeletedLoadResultImpl(),
      schemaName,
      cube.getDatabaseName,
      dateField,
      dateFieldActualName,
      dateValue,
      partitioner,
      cube.getFactTableName,
      tableName,
      hdfsStoreLocation,
      loadMetadataDetailsArray,
      currentRestructNumber).collect.groupBy(_._1)

    var updatedLoadMetadataDetailsList = new ListBuffer[LoadMetadataDetails]()
    if (resultMap.nonEmpty) {
      if (resultMap.size == 1) {
        if (resultMap.contains("")) {
          logError("Delete by Date request is failed")
          sys.error("Delete by Date request is failed, potential causes " +
                    "Empty store or Invalid column type, For more details please refer logs.")
        }
      }
      val updatedloadMetadataDetails = loadMetadataDetailsArray.map { elem => {
        var statusList = resultMap.get(elem.getLoadName)
        // check for the merged load folder.
        if (statusList.isEmpty && null != elem.getMergedLoadName) {
          statusList = resultMap.get(elem.getMergedLoadName)
        }

        if (statusList.isDefined) {
          elem.setModificationOrdeletionTimesStamp(CarbonLoaderUtil.readCurrentTime())
          // if atleast on CarbonCommonConstants.MARKED_FOR_UPDATE status exist,
          // use MARKED_FOR_UPDATE
          if (statusList.get
            .forall(status => status._2 == CarbonCommonConstants.MARKED_FOR_DELETE)) {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE)
          } else {
            elem.setLoadStatus(CarbonCommonConstants.MARKED_FOR_UPDATE)
            updatedLoadMetadataDetailsList += elem
          }
          elem
        } else {
          elem
        }
      }

      }

      // Save the load metadata
      val carbonLock = CarbonLockFactory
        .getCarbonLockObj(cube.getMetaDataFilepath, LockUsage.METADATA_LOCK)
      try {
        if (carbonLock.lockWithRetries()) {
          logInfo("Successfully got the cube metadata file lock")
          if (updatedLoadMetadataDetailsList.nonEmpty) {
            LoadAggregateTabAfterRetention(schemaName, cube.getFactTableName, cube.getFactTableName,
              sqlContext, schema, updatedLoadMetadataDetailsList)
          }

          // write
          CarbonLoaderUtil.writeLoadMetadata(
            schema,
            schemaName,
            cube.getDatabaseName,
            updatedloadMetadataDetails.asJava)
        }
      } finally {
        if (carbonLock.unlock()) {
          logInfo("unlock the cube metadata file successfully")
        } else {
          logError("Unable to unlock the metadata lock")
        }
      }
    } else {
      logError("Delete by Date request is failed")
      logger.audit("The delete load by date is failed.")
      sys.error("Delete by Date request is failed, potential causes " +
                "Empty store or Invalid column type, For more details please refer logs.")
    }
  }

  def LoadAggregateTabAfterRetention(
      schemaName: String,
      cubeName: String,
      factTableName: String,
      sqlContext: SQLContext,
      schema: CarbonDataLoadSchema,
      list: ListBuffer[LoadMetadataDetails]) {
    val relation = CarbonEnv.getInstance(sqlContext).carbonCatalog.lookupRelation1(
      Option(schemaName),
      cubeName,
      None)(sqlContext).asInstanceOf[CarbonRelation]
    if (relation == null) {
      sys.error(s"Cube $schemaName.$cubeName does not exist")
    }
    val carbonLoadModel = new CarbonLoadModel()
    carbonLoadModel.setTableName(cubeName)
    carbonLoadModel.setDatabaseName(schemaName)
    val table = relation.cubeMeta.carbonTable
    val aggTables = schema.getCarbonTable.getAggregateTablesName
    if (null != aggTables && !aggTables.isEmpty) {
      carbonLoadModel.setRetentionRequest(true)
      carbonLoadModel.setLoadMetadataDetails(list.asJava)
      carbonLoadModel.setTableName(table.getFactTableName)
      carbonLoadModel
        .setCarbonDataLoadSchema(new CarbonDataLoadSchema(relation.cubeMeta.carbonTable))
      // TODO: need to fill dimension relation from data load sql command
      var storeLocation = CarbonProperties.getInstance
        .getProperty(CarbonCommonConstants.STORE_LOCATION_TEMP_PATH,
          System.getProperty("java.io.tmpdir"))
      storeLocation = storeLocation + "/carbonstore/" + System.currentTimeMillis()
      val columinar = sqlContext.getConf("carbon.is.columnar.storage", "true").toBoolean
      var kettleHomePath = sqlContext.getConf("carbon.kettle.home", null)
      if (null == kettleHomePath) {
        kettleHomePath = CarbonProperties.getInstance.getProperty("carbon.kettle.home")
      }
      if (kettleHomePath == null) {
        sys.error(s"carbon.kettle.home is not set")
      }
      CarbonDataRDDFactory.loadCarbonData(
        sqlContext,
        carbonLoadModel,
        storeLocation,
        relation.cubeMeta.dataPath,
        kettleHomePath,
        relation.cubeMeta.partitioner, columinar, isAgg = true)
    }
  }

  def loadCarbonData(sc: SQLContext,
      carbonLoadModel: CarbonLoadModel,
      storeLocation: String,
      hdfsStoreLocation: String,
      kettleHomePath: String,
      partitioner: Partitioner,
      columinar: Boolean,
      isAgg: Boolean,
      partitionStatus: String = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS) {
    val carbonTable = carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable
    var currentRestructNumber = -1
    try {
      logger.audit("The data load request has been received.")

      currentRestructNumber = CarbonUtil
        .checkAndReturnCurrentRestructFolderNumber(carbonTable.getMetaDataFilepath, "RS_", false)
      if (-1 == currentRestructNumber) {
        currentRestructNumber = 0
      }

      // Check if any load need to be deleted before loading new data
      deleteLoadsAndUpdateMetadata(carbonLoadModel, carbonTable, partitioner,
        hdfsStoreLocation, false,
        currentRestructNumber)
      if (null == carbonLoadModel.getLoadMetadataDetails) {
        readLoadMetadataDetails(carbonLoadModel, hdfsStoreLocation)
      }

      var currentLoadCount = -1
      val convLoadDetails = carbonLoadModel.getLoadMetadataDetails.asScala
      if (convLoadDetails.nonEmpty) {
        convLoadDetails.foreach { l =>
          val loadCount = Integer.parseInt(l.getLoadName)
          if (currentLoadCount < loadCount) {
            currentLoadCount = loadCount
          }
        }
        currentLoadCount += 1
        CarbonLoaderUtil
          .deletePartialLoadDataIfExist(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, carbonLoadModel.getTableName, hdfsStoreLocation,
            currentRestructNumber, currentLoadCount)
      } else {
        currentLoadCount += 1
      }

      // reading the start time of data load.
      val loadStartTime = CarbonLoaderUtil.readCurrentTime()
      carbonLoadModel.setFactTimeStamp(loadStartTime)
      val cubeCreationTime = CarbonEnv.getInstance(sc).carbonCatalog
        .getCubeCreationTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
      val schemaLastUpdatedTime = CarbonEnv.getInstance(sc).carbonCatalog
        .getSchemaLastUpdatedTime(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)

      // get partition way from configuration
      // val isTableSplitPartition = CarbonProperties.getInstance().getProperty(
      // CarbonCommonConstants.TABLE_SPLIT_PARTITION,
      // CarbonCommonConstants.TABLE_SPLIT_PARTITION_DEFAULT_VALUE).toBoolean
      val isTableSplitPartition = false
      var blocksGroupBy: Array[(String, Array[BlockDetails])] = null
      isTableSplitPartition match {
        case true =>
          /*
           * when data handle by table split partition
           * 1) get partition files, direct load or not will get the different files path
           * 2) get files blocks by using SplitUtils
           * 3) output Array[(partitionID,Array[BlockDetails])] to blocksGroupBy
           */
          var splits = Array[TableSplit]()
          if (carbonLoadModel.isDirectLoad) {
            // get all table Splits, this part means files were divide to different partitions
            splits = CarbonQueryUtil.getTableSplitsForDirectLoad(carbonLoadModel.getFactFilePath,
              partitioner.nodeList, partitioner.partitionCount)
            // get all partition blocks from file list
            blocksGroupBy = splits.map {
              split =>
                val pathBuilder = new StringBuilder()
                for (path <- split.getPartition.getFilesPath.asScala) {
                  pathBuilder.append(path).append(",")
                }
                if (pathBuilder.nonEmpty) {
                  pathBuilder.substring(0, pathBuilder.size - 1)
                }
                (split.getPartition.getUniqueID, SplitUtils.getSplits(pathBuilder.toString(),
                  sc.sparkContext))
            }
          } else {
            // get all table Splits,when come to this, means data have been partition
            splits = CarbonQueryUtil.getTableSplits(carbonLoadModel.getDatabaseName,
              carbonLoadModel.getTableName, null, partitioner)
            // get all partition blocks from factFilePath/uniqueID/
            blocksGroupBy = splits.map {
              split =>
                val pathBuilder = new StringBuilder()
                pathBuilder.append(carbonLoadModel.getFactFilePath)
                if (!carbonLoadModel.getFactFilePath.endsWith("/")
                    && !carbonLoadModel.getFactFilePath.endsWith("\\")) {
                  pathBuilder.append("/")
                }
                pathBuilder.append(split.getPartition.getUniqueID).append("/")
                (split.getPartition.getUniqueID,
                  SplitUtils.getSplits(pathBuilder.toString, sc.sparkContext))
            }
          }

        case false =>
          /*
           * when data load handle by node partition
           * 1)clone the hadoop configuration,and set the file path to the configuration
           * 2)use NewHadoopRDD to get split,size:Math.max(minSize, Math.min(maxSize, blockSize))
           * 3)use DummyLoadRDD to group blocks by host,and let spark balance the block location
           * 4)DummyLoadRDD output (host,Array[BlockDetails])as the parameter to CarbonDataLoadRDD
           *   which parititon by host
           */
          val hadoopConfiguration = new Configuration(sc.sparkContext.hadoopConfiguration)
          // FileUtils will skip file which is no csv, and return all file path which split by ','
          val filePaths = FileUtils.getPaths(carbonLoadModel.getFactFilePath)
          hadoopConfiguration.set("mapreduce.input.fileinputformat.inputdir", filePaths)
          hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")
          val defaultParallelism =
            if (sc.sparkContext.defaultParallelism < 1) 1 else sc.sparkContext.defaultParallelism
          val spaceConsumed = FileUtils.getSpaceOccupied(filePaths)
          val blockSize =
            hadoopConfiguration.getLong("dfs.blocksize", CarbonCommonConstants.CARBON_256MB)
          logInfo("[Block Distribution]")
          // calculate new block size to allow use all the parallelism
          if (spaceConsumed < defaultParallelism * blockSize) {
            var newSplitSize: Long = spaceConsumed / defaultParallelism
            if (newSplitSize < CarbonCommonConstants.CARBON_16MB) {
              newSplitSize = CarbonCommonConstants.CARBON_16MB
            }
            hadoopConfiguration.set(
              "mapreduce.input.fileinputformat.split.maxsize", newSplitSize.toString
            )
            logInfo("totalInputSpaceConsumed : " + spaceConsumed +
              " , defaultParallelism : " + defaultParallelism
            )
            logInfo("mapreduce.input.fileinputformat.split.maxsize : " + newSplitSize.toString)
          }

          val inputFormat = new org.apache.hadoop.mapreduce.lib.input.TextInputFormat
          inputFormat match {
            case configurable: Configurable =>
              configurable.setConf(hadoopConfiguration)
            case _ =>
          }
          val jobContext = new Job(hadoopConfiguration)
          val rawSplits = inputFormat.getSplits(jobContext).toArray
          val result = new Array[Partition](rawSplits.size)
          val blockList = rawSplits.map(inputSplit => {
            val fileSplit = inputSplit.asInstanceOf[FileSplit]
            new TableBlockInfo(fileSplit.getPath.toString,
              fileSplit.getStart, 1,
              fileSplit.getLocations, fileSplit.getLength
            )
          }
          )
          // group blocks to nodes, tasks
          val nodeBlockMapping =
            CarbonLoaderUtil.nodeBlockMapping(blockList.toSeq.asJava, -1).asScala.toSeq
          logInfo("Total no of blocks : " + blockList.size
            + ", No.of Nodes : " + nodeBlockMapping.size
          )
          var str = ""
          nodeBlockMapping.foreach(entry => {
            val tableBlock = entry._2
            str = str + "#Node: " + entry._1 + " no.of.blocks: " + tableBlock.size()
            tableBlock.asScala.foreach(tableBlockInfo =>
              if (!tableBlockInfo.getLocations.exists(hostentry =>
                hostentry.equalsIgnoreCase(entry._1)
              )) {
                str = str + " , mismatch locations: " + tableBlockInfo.getLocations
                  .foldLeft("")((a, b) => a + "," + b)
              }
            )
            str = str + "\n"
          }
          )
          logInfo(str)
          blocksGroupBy = nodeBlockMapping.map(entry => {
            val blockDetailsList =
              entry._2.asScala.map(tableBlock =>
                new BlockDetails(tableBlock.getFilePath,
                  tableBlock.getBlockOffset, tableBlock.getBlockLength
                )
              ).toArray
            (entry._1, blockDetailsList)
          }
          ).toArray
      }

      CarbonLoaderUtil.checkAndCreateCarbonDataLocation(hdfsStoreLocation,
        carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName,
        partitioner.partitionCount, currentLoadCount
      )
      val status = new
          CarbonDataLoadRDD(sc.sparkContext, new ResultImpl(), carbonLoadModel, storeLocation,
            hdfsStoreLocation, kettleHomePath, partitioner, columinar, currentRestructNumber,
            currentLoadCount, cubeCreationTime, schemaLastUpdatedTime, blocksGroupBy,
            isTableSplitPartition
          ).collect()
      val newStatusMap = scala.collection.mutable.Map.empty[String, String]
      status.foreach { eachLoadStatus =>
        val state = newStatusMap.get(eachLoadStatus._2.getPartitionCount)
        state match {
          case Some(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) =>
            newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
          case Some(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS)
            if eachLoadStatus._2.getLoadStatus == CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS =>
            newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
          case _ =>
            newStatusMap.put(eachLoadStatus._2.getPartitionCount, eachLoadStatus._2.getLoadStatus)
        }
      }

      var loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
      newStatusMap.foreach {
        case (key, value) =>
          if (value == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
            loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_FAILURE
          } else if (value == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS &&
            !loadStatus.equals(CarbonCommonConstants.STORE_LOADSTATUS_FAILURE)) {
            loadStatus = CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
          }
      }

      if (loadStatus != CarbonCommonConstants.STORE_LOADSTATUS_FAILURE &&
        partitionStatus == CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) {
        loadStatus = partitionStatus
      }

      if (loadStatus == CarbonCommonConstants.STORE_LOADSTATUS_FAILURE) {
        var message: String = ""
        logInfo("********starting clean up**********")
        if (isAgg) {
          CarbonLoaderUtil.deleteTable(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, carbonLoadModel.getAggTableName, hdfsStoreLocation,
            currentRestructNumber)
          message = "Aggregate table creation failure"
        } else {
          val (result, _) = status(0)
          val newSlice = CarbonCommonConstants.LOAD_FOLDER + result
          CarbonLoaderUtil.deleteSlice(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
            carbonLoadModel.getTableName, carbonLoadModel.getTableName, hdfsStoreLocation,
            currentRestructNumber, newSlice)
          val aggTables = carbonTable.getAggregateTablesName
          if (null != aggTables && !aggTables.isEmpty) {
            aggTables.asScala.foreach { aggTableName =>
              CarbonLoaderUtil
                .deleteSlice(partitioner.partitionCount, carbonLoadModel.getDatabaseName,
                  carbonLoadModel.getTableName, aggTableName, hdfsStoreLocation,
                  currentRestructNumber, newSlice)
            }
          }
          message = "Dataload failure"
        }
        logInfo("********clean up done**********")
        logger.audit("The data loading is failed.")
        logWarning("Unable to write load metadata file")
        throw new Exception(message)
      } else {
        val (result, metadataDetails) = status(0)
        if (!isAgg) {
          CarbonLoaderUtil
            .recordLoadMetadata(result, metadataDetails, carbonLoadModel, loadStatus, loadStartTime)
        } else if (!carbonLoadModel.isRetentionRequest) {
          // TODO : Handle it
          logInfo("********schema updated**********")
        }
        logger.audit("The data loading is successful.")
        if (CarbonDataMergerUtil
          .checkIfLoadMergingRequired(carbonTable.getMetaDataFilepath, carbonLoadModel,
            hdfsStoreLocation, partitioner.partitionCount, currentRestructNumber)) {

          val loadsToMerge = CarbonDataMergerUtil.getLoadsToMergeFromHDFS(
            hdfsStoreLocation, FileFactory.getFileType(hdfsStoreLocation),
            carbonTable.getMetaDataFilepath, carbonLoadModel, currentRestructNumber,
            partitioner.partitionCount)

          if (loadsToMerge.size() == 2) {
            val MergedLoadName = CarbonDataMergerUtil.getMergedLoadName(loadsToMerge)
            var finalMergeStatus = true
            val mergeStatus = new CarbonMergerRDD(
              sc.sparkContext,
              new MergeResultImpl(),
              carbonLoadModel,
              storeLocation,
              hdfsStoreLocation,
              partitioner,
              currentRestructNumber,
              carbonTable.getMetaDataFilepath,
              loadsToMerge,
              MergedLoadName,
              kettleHomePath,
              cubeCreationTime).collect

            mergeStatus.foreach { eachMergeStatus =>
              val state = eachMergeStatus._2
              if (!state) {
                finalMergeStatus = false
              }
            }
            if (finalMergeStatus) {
              CarbonDataMergerUtil
                .updateLoadMetadataWithMergeStatus(loadsToMerge, carbonTable.getMetaDataFilepath,
                  MergedLoadName, carbonLoadModel)
            }
          }
        }
      }
    }

  }

  def readLoadMetadataDetails(model: CarbonLoadModel, hdfsStoreLocation: String): Unit = {
    val metadataPath = model.getCarbonDataLoadSchema.getCarbonTable.getMetaDataFilepath
    var segmentStatusManager = new SegmentStatusManager(
      new AbsoluteTableIdentifier(
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
        new CarbonTableIdentifier(model.getDatabaseName, model.getTableName)));
    val details = segmentStatusManager.readLoadMetadata(metadataPath)
    model.setLoadMetadataDetails(details.toList.asJava)
  }

  def deleteLoadsAndUpdateMetadata(
      carbonLoadModel: CarbonLoadModel,
      cube: CarbonTable, partitioner: Partitioner,
      hdfsStoreLocation: String,
      isForceDeletion: Boolean,
      currentRestructNumber: Integer) {
    if (LoadMetadataUtil.isLoadDeletionRequired(carbonLoadModel)) {
      val loadMetadataFilePath = CarbonLoaderUtil
        .extractLoadMetadataFileLocation(carbonLoadModel)
      var segmentStatusManager = new SegmentStatusManager(
        new AbsoluteTableIdentifier(
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION),
          new CarbonTableIdentifier(carbonLoadModel.getDatabaseName, carbonLoadModel.getTableName)
        )
      );
      val details = segmentStatusManager
        .readLoadMetadata(loadMetadataFilePath)

      // Delete marked loads
      val isUpdationRequired = DeleteLoadFolders
        .deleteLoadFoldersFromFileSystem(carbonLoadModel, hdfsStoreLocation,
          partitioner.partitionCount, isForceDeletion, details)

      if (isUpdationRequired) {
        // Update load metadate file after cleaning deleted nodes
        CarbonLoaderUtil.writeLoadMetadata(
          carbonLoadModel.getCarbonDataLoadSchema,
          carbonLoadModel.getDatabaseName,
          carbonLoadModel.getTableName, details.toList.asJava)
      }
    }
// TODO:Need to hanlde this scanario for data compaction
 //   CarbonDataMergerUtil
 //     .cleanUnwantedMergeLoadFolder(carbonLoadModel, partitioner.partitionCount,
 //    hdfsStoreLocation, isForceDeletion, currentRestructNumber)*/

  }

  def dropAggregateTable(
      sc: SparkContext,
      schema: String,
      cube: String,
      partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    new CarbonDropAggregateTableRDD(sc, kv, schema, cube, partitioner).collect
  }

  def dropCube(
      sc: SparkContext,
      schema: String,
      cube: String,
      partitioner: Partitioner) {
    val kv: KeyVal[CarbonKey, CarbonValue] = new KeyValImpl()
    new CarbonDropCubeRDD(sc, kv, schema, cube, partitioner).collect
  }

  def cleanFiles(
      sc: SparkContext,
      carbonLoadModel: CarbonLoadModel,
      hdfsStoreLocation: String,
      partitioner: Partitioner) {
    val cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance
      .getCarbonTable(carbonLoadModel.getDatabaseName + "_" + carbonLoadModel.getTableName)
    val metaDataPath: String = cube.getMetaDataFilepath
    var currentRestructNumber = CarbonUtil
      .checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false)
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0
    }
    var carbonLock = CarbonLockFactory
      .getCarbonLockObj(cube.getMetaDataFilepath, LockUsage.METADATA_LOCK)
    try {
      if (carbonLock.lockWithRetries()) {
        deleteLoadsAndUpdateMetadata(carbonLoadModel,
          cube,
          partitioner,
          hdfsStoreLocation,
          isForceDeletion = true,
          currentRestructNumber)
      }
    }
    finally {
      if (carbonLock.unlock()) {
        logInfo("unlock the cube metadata file successfully")
      } else {
        logError("Unable to unlock the metadata lock")
      }
    }
  }

}
