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

package org.apache.spark.sql.index

import java.util

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{CarbonDatasourceHadoopRelation, CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.CarbonRelation
import org.apache.spark.sql.secondaryindex.command.{IndexModel, SecondaryIndexModel}
import org.apache.spark.sql.secondaryindex.hive.CarbonInternalMetastore
import org.apache.spark.sql.secondaryindex.load.CarbonInternalLoaderUtil
import org.apache.spark.sql.secondaryindex.rdd.SecondaryIndexCreator
import org.apache.spark.util.AlterTableUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.compression.CompressorFactory
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.locks.{CarbonLockFactory, CarbonLockUtil, ICarbonLock, LockUsage}
import org.apache.carbondata.core.metadata.index.IndexType
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata
import org.apache.carbondata.core.metadata.schema.indextable.IndexTableInfo
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.mutate.CarbonUpdateUtil
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.processing.loading.constants.DataLoadProcessorConstants
import org.apache.carbondata.processing.loading.model.CarbonLoadModel
import org.apache.carbondata.processing.util.TableOptionConstant

/**
 * Carbon Index util
 */
object CarbonIndexUtil {

  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  def addIndexTableInfo(
      indexProvider: String,
      carbonTable: CarbonTable,
      tableName: String,
      indexProperties: java.util.Map[String, String]): Unit = {
    val indexMetadata = carbonTable.getIndexMetadata
    if (null != indexMetadata) {
        indexMetadata.addIndexTableInfo(indexProvider, tableName, indexProperties)
    }
  }

  def removeIndexTableInfo(carbonTable: CarbonTable, tableName: String): Unit = {
    val indexMetadata = carbonTable.getIndexMetadata
    if (null != indexMetadata) {
      indexMetadata.removeIndexTableInfo(tableName)
    }
  }

  /**
   * Check if the index meta data for the table exists or not
   *
   * @param carbonTable
   * @return
   */
  def isIndexTableExists(carbonTable: CarbonTable): String = {
    carbonTable.getTableInfo.getFactTable.getTableProperties
      .get("indextableexists")
  }

  def isIndexExists(carbonTable: CarbonTable): String = {
    carbonTable.getTableInfo.getFactTable.getTableProperties
      .get("indexexists")
  }

  def getSecondaryIndexes(carbonTable: CarbonTable): java.util.List[String] = {
    val indexMetadata = carbonTable.getIndexMetadata
    val secondaryIndexesTables = if (null != indexMetadata) {
     indexMetadata.getIndexTables(IndexType.SI.getIndexProviderName)
    } else {
      new java.util.ArrayList[String]
    }
    secondaryIndexesTables
  }

  def getCGAndFGIndexes(carbonTable: CarbonTable): java.util.Map[String,
    util.Map[String, util.Map[String, String]]] = {
    val indexMetadata = carbonTable.getIndexMetadata
    val cgAndFgIndexes = if (null != indexMetadata && null != indexMetadata.getIndexesMap) {
      val indexesMap = indexMetadata.getIndexesMap
      indexesMap.asScala.filter(provider =>
        !provider._1.equalsIgnoreCase(IndexType.SI.getIndexProviderName)).asJava
    } else {
      new util.HashMap[String, util.Map[String, util.Map[String, String]]]()
    }
    cgAndFgIndexes
  }

  def getParentTableName(carbonTable: CarbonTable): String = {
    val indexMetadata = carbonTable.getIndexMetadata
    val indexesTables = if (null != indexMetadata) {
      indexMetadata.getParentTableName
    } else {
      null
    }
    indexesTables
  }

  def getSecondaryIndexesMap(carbonTable: CarbonTable): scala.collection.mutable
  .Map[String, Array[String]] = {
    val indexes = scala.collection.mutable.Map[String, Array[String]]()
    val indexInfo = carbonTable.getIndexInfo(IndexType.SI.getIndexProviderName)
    if (null != indexInfo) {
      IndexTableInfo.fromGson(indexInfo).foreach { indexTableInfo =>
        indexes.put(indexTableInfo.getTableName, indexTableInfo.getIndexCols.asScala.toArray)
      }
    }
    indexes
  }

  /**
   * For a given index table this method will prepare the table status details
   *
   */
  def getTableStatusDetailsForIndexTable(factLoadMetadataDetails: util.List[LoadMetadataDetails],
      indexTable: CarbonTable,
      newSegmentDetailsObject: util.List[LoadMetadataDetails]): util.List[LoadMetadataDetails] = {
    val indexTableDetailsList: util.List[LoadMetadataDetails] = new util
    .ArrayList[LoadMetadataDetails](
      factLoadMetadataDetails.size)
    val indexTableStatusDetailsArray: Array[LoadMetadataDetails] = SegmentStatusManager
      .readLoadMetadata(indexTable.getMetadataPath)
    if (null !=
        indexTableStatusDetailsArray) {
      for (loadMetadataDetails <- indexTableStatusDetailsArray) {
        indexTableDetailsList.add(loadMetadataDetails)
      }
    }
    indexTableDetailsList.addAll(newSegmentDetailsObject)
    val iterator: util.Iterator[LoadMetadataDetails] = indexTableDetailsList.iterator
    // synchronize the index table status file with its parent table
    while ( { iterator.hasNext }) {
      val indexTableDetails: LoadMetadataDetails = iterator.next
      var found: Boolean = false
      for (factTableDetails <- factLoadMetadataDetails.asScala) {
        // null check is added because in case of auto load, load end time will be null
        // for the last entry
        if (0L != factTableDetails.getLoadEndTime &&
            indexTableDetails.getLoadName == factTableDetails.getLoadName) {
          indexTableDetails.setLoadStartTime(factTableDetails.getLoadStartTime)
          //          indexTableDetails.setLoadStatus(factTableDetails.getLoadStatus)
          indexTableDetails.setMajorCompacted(factTableDetails.isMajorCompacted)
          indexTableDetails.setMergedLoadName(factTableDetails.getMergedLoadName)
          indexTableDetails
            .setModificationOrDeletionTimestamp(factTableDetails
              .getModificationOrDeletionTimestamp)
          indexTableDetails.setLoadEndTime(factTableDetails.getLoadEndTime)
          indexTableDetails.setVisibility(factTableDetails.getVisibility)
          found = true
          // TODO: make it breakable
        } else if (indexTableDetails.getLoadName == factTableDetails.getLoadName) {
          indexTableDetails.setLoadStartTime(factTableDetails.getLoadStartTime)
          //          indexTableDetails.setLoadStatus(CarbonCommonConstants
          // .STORE_LOADSTATUS_SUCCESS)
          indexTableDetails.setLoadEndTime(CarbonUpdateUtil.readCurrentTime)
          found = true
          // TODO: make it breakable
        }
      }
      // in case there is some inconsistency between fact table index file and index table
      // status file, it can resolved here by removing unwanted segments
      if (!found) {
        iterator.remove()
      }
    }
    indexTableDetailsList
  }

  def checkIsIndexTable(plan: LogicalPlan): Boolean = {
    plan match {
      case Aggregate(_, _, plan) if (isIndexTablesJoin(plan)) => true
      case _ => false
    }
  }

  /**
   * Collect all logical relation and check for if plan contains index table join
   *
   * @param plan
   * @return false if there are no index tables found in the plan or if logical relation is empty.
   */
  def isIndexTablesJoin(plan: LogicalPlan): Boolean = {
    val allRelations = plan.collect { case logicalRelation: LogicalRelation => logicalRelation }
    allRelations.nonEmpty && allRelations.forall(x =>
      x.relation.isInstanceOf[CarbonDatasourceHadoopRelation]
      && x.relation.asInstanceOf[CarbonDatasourceHadoopRelation].carbonTable.isIndexTable)
  }

  /**
   * Get the column compressor for the index table. Check first in the index table properties
   * and then fall back to main table at last to the default compressor
   */
  def getCompressorForIndexTable(
      indexTable: CarbonTable,
      parentTable: CarbonTable) : String = {
    // get the compressor from the index table (table properties)
    var columnCompressor = indexTable.getTableInfo.getFactTable.getTableProperties.get(
      CarbonCommonConstants.COMPRESSOR)
    if (null == columnCompressor) {
      // if nothing is set to index table then fall to the main table compressor
      columnCompressor = parentTable.getTableInfo.getFactTable.getTableProperties.get(
        CarbonCommonConstants.COMPRESSOR)
      if (null == columnCompressor) {
        // if main table compressor is also not set then choose the default compressor
        columnCompressor = CompressorFactory.getInstance.getCompressor.getName
      }
    }
    columnCompressor
  }


  def getIndexCarbonTables(carbonTable: CarbonTable,
      sparkSession: SparkSession): Seq[CarbonTable] = {
    val indexMetadata = carbonTable.getIndexMetadata
    val siIndexesMap = if (null != indexMetadata && null != indexMetadata.getIndexesMap) {
      indexMetadata.getIndexesMap.get(IndexType.SI.getIndexProviderName)
    } else {
      new util.HashMap[String, util.Map[String, util.Map[String, String]]]()
    }
    if (null != siIndexesMap) {
      siIndexesMap.keySet().asScala.map {
        indexTable =>
          CarbonEnv.getCarbonTable(Some(carbonTable.getDatabaseName), indexTable)(sparkSession);
      }.toSeq
    } else {
      Seq.empty
    }
  }

  /**
   * This method loads data to SI table, if isLoadToFailedSISegments is true, then load to only
   * failed segments, if false, just load the data to current segment of main table load
   */
  def LoadToSITable(sparkSession: SparkSession,
    carbonLoadModel: CarbonLoadModel,
    indexTableName: String,
    isLoadToFailedSISegments: Boolean,
    secondaryIndex: IndexModel,
    carbonTable: CarbonTable,
    indexTable: CarbonTable,
    isInsertOverWrite: Boolean = false,
    failedLoadMetaDataDetils: java.util.List[LoadMetadataDetails] = null): Unit = {

    var segmentIdToLoadStartTimeMapping: scala.collection.mutable.Map[String, java.lang.Long] =
      scala.collection.mutable.Map()

    val segmentsToReload: scala.collection.mutable.ListBuffer[String] = scala
      .collection
      .mutable.ListBuffer[String]()

    if (isLoadToFailedSISegments && null != failedLoadMetaDataDetils) {
      val metadata = CarbonInternalLoaderUtil
        .getListOfValidSlices(SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath))
      segmentIdToLoadStartTimeMapping = CarbonInternalLoaderUtil
        .getSegmentToLoadStartTimeMapping(carbonLoadModel.getLoadMetadataDetails.asScala.toArray)
        .asScala
      failedLoadMetaDataDetils.asScala.foreach(loadMetaDetail => {
        // check whether this segment is valid or invalid, if it is present in the valid list
        // then don't consider it for reloading.
        // Also main table should have this as a valid segment for reloading.
        if (!metadata.contains(loadMetaDetail.getLoadName) &&
            segmentIdToLoadStartTimeMapping.contains(loadMetaDetail.getLoadName)) {
          segmentsToReload.append(loadMetaDetail.getLoadName)
        }
      })
      LOGGER.info(
        s"SI segments to be reloaded for index table: ${
          indexTable.getTableUniqueName} are: ${segmentsToReload}")
    } else {
      segmentIdToLoadStartTimeMapping = scala.collection.mutable
        .Map((carbonLoadModel.getSegmentId, carbonLoadModel.getFactTimeStamp))
    }
    val header = indexTable.getCreateOrderColumn.asScala.map(_.getColName).toArray
    if (isInsertOverWrite) {
      val loadMetadataDetails = carbonLoadModel.getLoadMetadataDetails.asScala
      loadMetadataDetails.foreach { loadMetadata =>
        segmentIdToLoadStartTimeMapping.put(loadMetadata.getLoadName,
          loadMetadata.getLoadStartTime)
      }
    }
    initializeSILoadModel(carbonLoadModel, header)
    val secondaryIndexModel = if (isLoadToFailedSISegments) {
      SecondaryIndexModel(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        segmentsToReload.toList,
        segmentIdToLoadStartTimeMapping)
    } else {
      SecondaryIndexModel(
        sparkSession.sqlContext,
        carbonLoadModel,
        carbonLoadModel.getCarbonDataLoadSchema.getCarbonTable,
        secondaryIndex,
        List(carbonLoadModel.getSegmentId),
        segmentIdToLoadStartTimeMapping)
    }

    SecondaryIndexCreator
      .createSecondaryIndex(secondaryIndexModel,
        new java.util.HashMap[String, String](),
        indexTable,
        forceAccessSegment = true,
        isCompactionCall = false,
        isLoadToFailedSISegments, isInsertOverWrite)
  }

  /**
   * This method add/modify the table properties.
   *
   */
  def addOrModifyTableProperty(carbonTable: CarbonTable,
    properties: Map[String, String],
    needLock: Boolean = true)
    (sparkSession: SparkSession): Unit = {
    val tableName = carbonTable.getTableName
    val dbName = carbonTable.getDatabaseName
    val locksToBeAcquired = List(LockUsage.METADATA_LOCK, LockUsage.COMPACTION_LOCK)
    val locks: java.util.List[ICarbonLock] = new java.util.ArrayList[ICarbonLock]
    try {
      try {
        if (needLock) {
          locksToBeAcquired.foreach { lock =>
            locks.add(CarbonLockUtil.getLockObject(carbonTable.getAbsoluteTableIdentifier, lock))
          }
        }
      } catch {
        case e: Exception =>
          AlterTableUtil.releaseLocks(locks.asScala.toList)
          throw e
      }
      val lowerCasePropertiesMap: mutable.Map[String, String] = mutable.Map.empty
      // convert all the keys to lower case
      properties.foreach { entry =>
        lowerCasePropertiesMap.put(entry._1.toLowerCase, entry._2)
      }
      val thriftTable = AlterTableUtil.readLatestTableSchema(carbonTable)(sparkSession)
      val tblPropertiesMap = thriftTable.fact_table.getTableProperties.asScala

      // This overrides/add the newProperties of thriftTable
      lowerCasePropertiesMap.foreach { property =>
        if (tblPropertiesMap.get(property._1) != null) {
          tblPropertiesMap.put(property._1, property._2)
        }
      }
      val tableIdentifier = AlterTableUtil.updateSchemaInfo(
        carbonTable = carbonTable,
        thriftTable = thriftTable)(sparkSession)
      // remove from the cache so that the table will be loaded again with the new table properties
      CarbonInternalMetastore
        .removeTableFromMetadataCache(carbonTable.getDatabaseName, tableName)(sparkSession)
      // refresh the parent table relation
      sparkSession.catalog.refreshTable(tableIdentifier.quotedString)

      LOGGER.info(s"Adding/Modifying tableProperties is successful for table $dbName.$tableName")
    } catch {
      case e: Exception =>
        sys.error(s"Adding/Modifying tableProperties operation failed: ${e.getMessage}")
    } finally {
      // release lock after command execution completion
      AlterTableUtil.releaseLocks(locks.asScala.toList)
    }
  }

  def processSIRepair(indexTableName: String, carbonTable: CarbonTable,
      carbonLoadModel: CarbonLoadModel, indexMetadata: IndexMetadata,
      secondaryIndexProvider: String, repairLimit: Int,
      segments: Option[List[String]] = Option.empty,
      isLoadOrCompaction: Boolean = false)(sparkSession: SparkSession): Unit = {
    // when Si creation and load to main table are parallel, get the carbonTable from the
    // metastore which will have the latest index Info
    val metaStore = CarbonEnv.getInstance(sparkSession).carbonMetaStore
    val indexTable = metaStore
      .lookupRelation(Some(carbonLoadModel.getDatabaseName), indexTableName)(
        sparkSession)
      .asInstanceOf[CarbonRelation]
      .carbonTable

    var segmentLocks: ListBuffer[ICarbonLock] = ListBuffer.empty
    val compactionLock = CarbonLockFactory.getCarbonLockObj(
      carbonTable.getAbsoluteTableIdentifier,
      LockUsage.COMPACTION_LOCK)
    try {
      // In some cases, SI table segment might be in COMPACTED state and main table
      // compaction might be still in progress. In those cases, we can try to take compaction lock
      // on main table and then compare and add SI segments to failedLoads, to avoid repair
      // SI SUCCESS loads.
      if (compactionLock.lockWithRetries()) {
        var mainTableDetails = try {
          SegmentStatusManager.readTableStatusFile(CarbonTablePath.getTableStatusFilePath(
            carbonTable.getTablePath))
        } catch {
          case exception: Exception =>
            if (!isLoadOrCompaction) {
              throw exception
            }
            return;
        }
        carbonLoadModel.setLoadMetadataDetails(mainTableDetails.toList.asJava)
        if (segments.isDefined) {
          mainTableDetails = mainTableDetails.filter(
            loadMetaDataDetails => segments.get.contains(loadMetaDataDetails.getLoadName))
        }
        val siTblLoadMetadataDetails: Array[LoadMetadataDetails] =
          SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)
        if (!CarbonInternalLoaderUtil.checkMainTableSegEqualToSISeg(
          mainTableDetails,
          siTblLoadMetadataDetails)) {
          val indexColumns = indexMetadata.getIndexColumns(secondaryIndexProvider,
            indexTableName)
          val indexModel = IndexModel(Some(carbonTable.getDatabaseName),
            indexMetadata.getParentTableName,
            indexColumns.split(",").toList,
            indexTableName)

          // If it empty, then no need to do further computations because the
          // tabletstatus might not have been created and hence next load will take care
          if (siTblLoadMetadataDetails.isEmpty) {
            Seq.empty
          }

          val failedLoadMetadataDetails: java.util.List[LoadMetadataDetails] = new util
          .ArrayList[LoadMetadataDetails]()

          // read the details of SI table and get all the failed segments during SI
          // creation which are MARKED_FOR_DELETE or invalid INSERT_IN_PROGRESS
          siTblLoadMetadataDetails.foreach {
            case loadMetaDetail: LoadMetadataDetails =>
              val isMainTableLoadValid = checkIfMainTableLoadIsValid(mainTableDetails,
                loadMetaDetail.getLoadName)
              if (loadMetaDetail.getSegmentStatus == SegmentStatus.MARKED_FOR_DELETE &&
                  isMainTableLoadValid && repairLimit > failedLoadMetadataDetails.size()) {
                failedLoadMetadataDetails.add(loadMetaDetail)
              } else if ((loadMetaDetail.getSegmentStatus ==
                          SegmentStatus.INSERT_IN_PROGRESS ||
                          loadMetaDetail.getSegmentStatus ==
                          SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) &&
                         isMainTableLoadValid && repairLimit > failedLoadMetadataDetails.size()) {
                val segmentLock = CarbonLockFactory
                  .getCarbonLockObj(indexTable.getAbsoluteTableIdentifier,
                    CarbonTablePath.addSegmentPrefix(loadMetaDetail.getLoadName) +
                    LockUsage.LOCK)
                try {
                  if (segmentLock.lockWithRetries(1, 0)) {
                    LOGGER
                      .info("SIFailedLoadListener: Acquired segment lock on segment:" +
                            loadMetaDetail.getLoadName)
                    failedLoadMetadataDetails.add(loadMetaDetail)
                  }
                } finally {
                  segmentLock.unlock()
                  LOGGER
                    .info("SIFailedLoadListener: Released segment lock on segment:" +
                          loadMetaDetail.getLoadName)
                }
              }
          }

          // check for the skipped segments. compare the main table and SI table table
          // status file and get the skipped segments if any
          CarbonInternalLoaderUtil.getListOfValidSlices(mainTableDetails).asScala
            .foreach(metadataDetail => {
              if (repairLimit > failedLoadMetadataDetails.size()) {
                val detail = siTblLoadMetadataDetails
                  .filter(metadata => metadata.getLoadName.equals(metadataDetail))
                val mainTableDetail = mainTableDetails
                  .filter(metadata => metadata.getLoadName.equals(metadataDetail))
                if (null == detail || detail.length == 0) {
                  val newDetails = new LoadMetadataDetails
                  newDetails.setLoadName(metadataDetail)
                  LOGGER.error(
                    "Added in SILoadFailedSegment " + newDetails.getLoadName + " for SI" +
                    " table " + indexTableName + "." + carbonTable.getTableName)
                  failedLoadMetadataDetails.add(newDetails)
                } else if (detail != null && detail.length != 0 && metadataDetail != null
                           && metadataDetail.length != 0) {
                  // If SI table has compacted segments and main table does not have
                  // compacted segments due to some failure while compaction, need to
                  // reload the original segments in this case.
                  if (detail(0).getSegmentStatus == SegmentStatus.COMPACTED &&
                      mainTableDetail(0).getSegmentStatus == SegmentStatus.SUCCESS) {
                    detail(0).setSegmentStatus(SegmentStatus.SUCCESS)
                    // in concurrent scenario, if a compaction is going on table, then SI
                    // segments are updated first in table status and then the main table
                    // segment, so in any load runs parallel this listener shouldn't consider
                    // those segments accidentally. So try to take the segment lock.
                    val segmentLockOfProbableOnCompactionSeg = CarbonLockFactory
                      .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier,
                        CarbonTablePath.addSegmentPrefix(mainTableDetail(0).getLoadName) +
                        LockUsage.LOCK)
                    if (segmentLockOfProbableOnCompactionSeg.lockWithRetries()) {
                      segmentLocks += segmentLockOfProbableOnCompactionSeg
                      LOGGER.error(
                        "Added in SILoadFailedSegment " + detail(0).getLoadName + " for SI "
                        + "table " + indexTableName + "." + carbonTable.getTableName)
                      failedLoadMetadataDetails.add(detail(0))
                    }
                  }
                }
              }
            })

          try {
            if (!failedLoadMetadataDetails.isEmpty) {
              // in the case when in SI table a segment is deleted and it's entry is
              // deleted from the tablestatus file, the corresponding .segment file from
              // the metadata folder should also be deleted as it contains the
              // mergefilename which does not exist anymore as the segment is deleted.
              deleteStaleSegmentFileIfPresent(carbonLoadModel,
                indexTable,
                failedLoadMetadataDetails)
              CarbonIndexUtil
                .LoadToSITable(sparkSession,
                  carbonLoadModel,
                  indexTableName,
                  isLoadToFailedSISegments = true,
                  indexModel,
                  carbonTable, indexTable, false, failedLoadMetadataDetails)
            }

            // get updated main table segments and si table segments
            val mainTblLoadMetadataDetails: Array[LoadMetadataDetails] =
              SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath)
            val siTblLoadMetadataDetails: Array[LoadMetadataDetails] =
              SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath)

            // check if main table has load in progress and SI table has no load
            // in progress entry, then no need to enable the SI table
            // Only if the valid segments of maintable match the valid segments of SI
            // table then we can enable the SI for query
            if (CarbonInternalLoaderUtil
                  .checkMainTableSegEqualToSISeg(mainTblLoadMetadataDetails,
                    siTblLoadMetadataDetails)
                && CarbonInternalLoaderUtil.checkInProgLoadInMainTableAndSI(carbonTable,
              mainTblLoadMetadataDetails, siTblLoadMetadataDetails)) {
              // enable the SI table if it was disabled earlier due to failure during SI
              // creation time
              sparkSession.sql(
                s"""ALTER TABLE ${ carbonLoadModel.getDatabaseName }.$indexTableName SET
                   |SERDEPROPERTIES ('isSITableEnabled' = 'true')""".stripMargin).collect()
            }
          } catch {
            case ex: Exception =>
              // in case of SI load only for for failed segments, catch the exception, but
              // do not fail the main table load, as main table segments should be available
              // for query
              LOGGER.error(s"Load to SI table to $indexTableName is failed " +
                           s"or SI table ENABLE is failed. ", ex)
              Seq.empty
          } finally {
            segmentLocks.foreach {
              segmentLock => segmentLock.unlock()
            }
          }
        }
      } else {
        LOGGER.error(s"Didn't check failed segments for index [$indexTableName] as compaction " +
                     s"is progress on ${ carbonTable.getTableUniqueName }. " +
                     s"Please call SI repair again")
        if (!isLoadOrCompaction) {
          throw new ConcurrentOperationException(carbonTable.getDatabaseName,
            carbonTable.getTableName, "compaction", "reindex command")
        }
      }
    } finally {
      compactionLock.unlock()
    }
    Seq.empty
  }

  def checkIfMainTableLoadIsValid(mainTableDetails: Array[LoadMetadataDetails],
    loadName: String): Boolean = {
    // in concurrent scenarios there can be cases when loadName is not present in the
    // mainTableDetails array. Added a check to see if the loadName is even present in the
    // mainTableDetails.
    val mainTableLoadDetail = mainTableDetails
      .filter(mainTableDetail => mainTableDetail.getLoadName.equals(loadName))
    if (mainTableLoadDetail.length == 0) {
      false
    } else {
      if (mainTableLoadDetail.head.getSegmentStatus ==
        SegmentStatus.MARKED_FOR_DELETE ||
        mainTableLoadDetail.head.getSegmentStatus == SegmentStatus.COMPACTED) {
        false
      } else {
        true
      }
    }
  }

  def deleteStaleSegmentFileIfPresent(carbonLoadModel: CarbonLoadModel, indexTable: CarbonTable,
    failedLoadMetaDataDetails: java.util.List[LoadMetadataDetails]): Unit = {
    failedLoadMetaDataDetails.asScala.map(failedLoadMetaData => {
      carbonLoadModel.getLoadMetadataDetails.asScala.map(loadMetaData => {
        if (failedLoadMetaData.getLoadName == loadMetaData.getLoadName) {
          val segmentFilePath = CarbonTablePath.getSegmentFilesLocation(indexTable.getTablePath) +
            CarbonCommonConstants.FILE_SEPARATOR + loadMetaData.getSegmentFile
          if (FileFactory.isFileExist(segmentFilePath)) {
            // delete the file if it exists
            FileFactory.deleteFile(segmentFilePath)
          }
        }
      })
    })
  }

  def initializeSILoadModel(carbonLoadModel: CarbonLoadModel,
      header: Array[String]): Unit = {
    carbonLoadModel.setSerializationNullFormat(
      TableOptionConstant.SERIALIZATION_NULL_FORMAT.getName + ",\\N")
    carbonLoadModel.setBadRecordsLoggerEnable(
      TableOptionConstant.BAD_RECORDS_LOGGER_ENABLE.getName + ",false")
    carbonLoadModel.setBadRecordsAction(
      TableOptionConstant.BAD_RECORDS_ACTION.getName + ",force")
    carbonLoadModel.setIsEmptyDataBadRecord(
      DataLoadProcessorConstants.IS_EMPTY_DATA_BAD_RECORD + ",false")
    carbonLoadModel.setTimestampFormat(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT))
    carbonLoadModel.setDateFormat(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
        CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT))
    carbonLoadModel.setCsvHeaderColumns(header)
  }
}
