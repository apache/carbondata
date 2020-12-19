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

package org.apache.carbondata.view

import java._
import java.io.IOException

import scala.collection.JavaConverters._

import com.google.gson.Gson
import org.apache.log4j.Logger
import org.apache.spark.sql.{CarbonThreadUtil, SparkSession}
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand
import org.apache.spark.sql.parser.MVQueryParser

import org.apache.carbondata.common.exceptions.sql.NoSuchMVException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.ICarbonLock
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, RelationIdentifier}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.statusmanager.SegmentStatusManager.ValidAndInvalidSegmentsInfo
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.view.{MVSchema, MVStatus}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class MVRefresher{

}

object MVRefresher {

  private val LOGGER: Logger = LogServiceFactory.getLogService(
    classOf[MVRefresher].getCanonicalName)

  /**
   * Refresh the mv by loading all existing data from related table
   * This is called when refreshing the mv when
   * 1. after mv creation and no "WITH DEFERRED REBUILD" defined
   * 2. user manually trigger REFRESH MATERIALIZED VIEW command
   */
  @throws[IOException]
  @throws[NoSuchMVException]
  def refresh(viewSchema: MVSchema, session: SparkSession): Boolean = {
    var newLoadName: String = ""
    var segmentMap: String = ""
    val viewTable: CarbonTable = CarbonTable.buildFromTablePath(
      viewSchema.getIdentifier.getTableName,
      viewSchema.getIdentifier.getDatabaseName,
      viewSchema.getIdentifier.getTablePath,
      viewSchema.getIdentifier.getTableId)
    val viewIdentifier = viewSchema.getIdentifier
    val viewTableIdentifier = viewTable.getAbsoluteTableIdentifier
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(viewTableIdentifier)
    // Acquire table status lock to handle concurrent data loading
    val lock: ICarbonLock = segmentStatusManager.getTableStatusLock
    val segmentMapping: util.Map[String, util.List[String]] =
      new util.HashMap[String, util.List[String]]
    val viewManager = MVManagerInSpark.get(session)
    try if (lock.lockWithRetries) {
      LOGGER.info("Acquired lock for mv " + viewIdentifier + " for table status update")
      val viewTableMetadataPath: String =
        CarbonTablePath.getMetadataPath(viewIdentifier.getTablePath)
      val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(viewTableMetadataPath)
      val loadMetadataDetailList: util.List[LoadMetadataDetails] =
        new util.ArrayList[LoadMetadataDetails](CarbonCommonConstants.DEFAULT_COLLECTION_SIZE)
      // Mark for delete all stale loadMetataDetail
      for (loadMetadataDetail <- loadMetadataDetails) {
        if (((loadMetadataDetail.getSegmentStatus eq SegmentStatus.INSERT_IN_PROGRESS) ||
             (loadMetadataDetail.getSegmentStatus eq SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS)) &&
            loadMetadataDetail.getVisibility.equalsIgnoreCase("false")) {
          loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
        }
        loadMetadataDetailList.add(loadMetadataDetail)
      }
      if (viewSchema.isRefreshOnManual) {
        // check if rebuild to mv is already in progress and throw exception
        if (loadMetadataDetails.nonEmpty) {
          for (loadMetaDetail <- loadMetadataDetails) {
            if (((loadMetaDetail.getSegmentStatus eq SegmentStatus.INSERT_IN_PROGRESS) ||
                 (loadMetaDetail.getSegmentStatus eq SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS)) &&
                SegmentStatusManager.isLoadInProgress(viewTableIdentifier,
                  loadMetaDetail.getLoadName)) {
              throw new RuntimeException(
                "Rebuild to materialized view " + viewSchema.getIdentifier.getTableName +
                  " is already in progress")
            }
          }
        }
      }
      if (viewSchema.isRefreshIncremental) {
        if (!getSpecificSegmentsTobeLoaded(viewSchema, segmentMapping, loadMetadataDetailList)) {
          return false
        }
      } else {
          // set segment mapping only for carbondata table
          val relatedTableIds =
            viewSchema.getRelatedTables.asScala.filter(_.isCarbonDataTable)
          for (relatedTableId <- relatedTableIds) {
            val validAndInvalidSegmentsInfo =
              SegmentStatusManager.getValidAndInvalidSegmentsInfo(relatedTableId)
            val relatedTableSegmentList: util.List[String] = SegmentStatusManager
              .getValidSegmentList(validAndInvalidSegmentsInfo)
            if (relatedTableSegmentList.isEmpty) {
              return false
            }
            segmentMapping.put(relatedTableId.toString, relatedTableSegmentList)
          }
        }
      segmentMap = new Gson().toJson(segmentMapping)
      // To handle concurrent data loading to mv, create new loadMetaEntry and
      // set segmentMap to new loadMetaEntry and pass new segmentId with load command
      val loadMetadataDetail: LoadMetadataDetails = new LoadMetadataDetails
      val segmentId: String = String.valueOf(
        SegmentStatusManager.createNewSegmentId(loadMetadataDetails))
      loadMetadataDetail.setLoadName(segmentId)
      loadMetadataDetail.setSegmentStatus(SegmentStatus.INSERT_IN_PROGRESS)
      loadMetadataDetail.setExtraInfo(segmentMap)
      loadMetadataDetailList.add(loadMetadataDetail)
      newLoadName = segmentId
      SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
        viewSchema.getIdentifier.getTablePath),
        loadMetadataDetailList.toArray(new Array[LoadMetadataDetails](loadMetadataDetailList
          .size)))
    } else {
      LOGGER.error("Not able to acquire the lock for table status update for table " +
                   viewSchema.getIdentifier.getDatabaseName + "." +
                   viewSchema.getIdentifier.getTableName)
      viewManager.setStatus(viewSchema.getIdentifier, MVStatus.DISABLED)
      return false
    } finally {
      if (lock.unlock) {
        LOGGER.info("Table unlocked successfully after table status update" +
                    viewSchema.getIdentifier.getDatabaseName + "." +
                    viewSchema.getIdentifier.getTableName)
      } else {
        LOGGER.error("Unable to unlock Table lock for table" +
                     viewSchema.getIdentifier.getDatabaseName + "." +
                     viewSchema.getIdentifier.getTableName +
                     " during table status update")
      }
    }
    refreshInternal(viewManager, viewSchema, viewTable, newLoadName, segmentMapping, session)
  }

  @throws[IOException]
  private def refreshInternal(
                               viewManager: MVManagerInSpark,
                               viewSchema: MVSchema,
                               viewTable: CarbonTable,
                               newLoadName: String,
                               segmentMap: java.util.Map[String, java.util.List[String]],
                               session: SparkSession): Boolean = {
    val query = viewSchema.getQuery
    if (query != null) {
      val viewIdentifier = viewSchema.getIdentifier
      val updatedQuery = MVQueryParser.getQuery(query, session)
      val isFullRefresh = !viewSchema.isRefreshIncremental
      // Set specified segments for incremental load
      val segmentMapIterator = segmentMap.entrySet().iterator()
      while (segmentMapIterator.hasNext) {
        val entry = segmentMapIterator.next()
        setInputSegments(entry.getKey, entry.getValue)
      }
      val header = viewTable.getTableInfo.getFactTable.getListOfColumns.asScala
        .filter { column =>
          !column.getColumnName
            .equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE)
        }.sortBy(_.getSchemaOrdinal).map(_.getColumnName).mkString(",")
      val insertIntoCommand = CarbonInsertIntoCommand(
        databaseNameOp = Some(viewIdentifier.getDatabaseName),
        tableName = viewIdentifier.getTableName,
        options = scala.collection.immutable.Map("fileheader" -> header),
        isFullRefresh,
        logicalPlan = updatedQuery.queryExecution.analyzed,
        tableInfo = viewTable.getTableInfo,
        internalOptions = Map("mergedSegmentName" -> newLoadName,
          CarbonCommonConstants.IS_INTERNAL_LOAD_CALL -> "true"),
        partition = Map.empty)
      try {
        insertIntoCommand.run(session)
      } catch {
        case exception: Exception =>
          // If load to mv table fails, disable the mv and if newLoad is still
          // in INSERT_IN_PROGRESS state, mark for delete the newLoad and update table status file
          viewManager.setStatus(viewSchema.getIdentifier, MVStatus.DISABLED)
          LOGGER.error("Data Load failed for mv: ", exception)
          CarbonLoaderUtil.updateTableStatusInCaseOfFailure(
            newLoadName, viewTable, SegmentStatus.INSERT_IN_PROGRESS)
          throw exception
      } finally {
        unsetInputSegments(viewSchema)
      }
    }
    true
  }

  /**
   * This method will compare main table and mv table segment List and loads only newly added
   * segment from main table to mv table.
   * In case if mainTable is compacted, then based on mv to main tables segmentMapping, mv
   * will be loaded
   * Eg:
   * case 1: Consider mainTableSegmentList: {0, 1, 2}, mvToMainTable segmentMap:
   * { 0 -> 0, 1-> 1,2}. If (1, 2) segments of main table are compacted to 1.1 and new segment (3)
   * is loaded to main table, then mainTableSegmentList will be updated to{0, 1.1, 3}.
   * In this case, segment (1) of mv table will be marked for delete, and new segment
   * {2 -> 1.1, 3} will be loaded to mv table
   * case 2: Consider mainTableSegmentList: {0, 1, 2, 3}, mvToMainTable segmentMap:
   * { 0 -> 0,1,2, 1-> 3}. If (1, 2) segments of main table are compacted to 1.1 and new segment
   * (4) is loaded to main table, then mainTableSegmentList will be updated to {0, 1.1, 3, 4}.
   * In this case, segment (0) of mv table will be marked for delete and segment (0) of
   * main table will be added to validSegmentList which needs to be loaded again. Now, new mv
   * table segment (2) with main table segmentList{2 -> 1.1, 4, 0} will be loaded to mv table.
   * mvToMainTable segmentMap will be updated to {1 -> 3, 2 -> 1.1, 4, 0} after rebuild
   */
  @throws[IOException]
  private def getSpecificSegmentsTobeLoaded(schema: MVSchema,
      segmentMapping: util.Map[String, util.List[String]],
      listOfLoadFolderDetails: util.List[LoadMetadataDetails]): Boolean = {
    val relationIdentifiers: util.List[RelationIdentifier] = schema.getRelatedTables
    // invalidSegmentList holds segment list which needs to be marked for delete
    val invalidSegmentList: util.HashSet[String] = new util.HashSet[String]
    if (listOfLoadFolderDetails.isEmpty) {
      // If segment Map is empty, load all valid segments from main tables to mv
      for (relationIdentifier <- relationIdentifiers.asScala) {
        val validAndInvalidSegmentsInfo =
          SegmentStatusManager.getValidAndInvalidSegmentsInfo(relationIdentifier)
        val mainTableSegmentList: util.List[String] = SegmentStatusManager
          .getValidSegmentList(validAndInvalidSegmentsInfo)
        // If mainTableSegmentList is empty, no need to trigger load command
        // TODO: handle in case of multiple tables load to mv table
        if (mainTableSegmentList.isEmpty) return false
        segmentMapping.put(relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                           relationIdentifier.getTableName, mainTableSegmentList)
      }
    }
    else {
      for (relationIdentifier <- relationIdentifiers.asScala) {
        val segmentList: util.List[String] = new util.ArrayList[String]
        // Get all segments for parent relationIdentifier
        val validAndInvalidSegmentsInfo =
          SegmentStatusManager.getValidAndInvalidSegmentsInfo(relationIdentifier)
        val mainTableSegmentList: util.List[String] = SegmentStatusManager
          .getValidSegmentList(validAndInvalidSegmentsInfo)
        var ifTableStatusUpdateRequired: Boolean = false
        for (loadMetaDetail <- listOfLoadFolderDetails.asScala) {
          if ((loadMetaDetail.getSegmentStatus eq SegmentStatus.SUCCESS) ||
              (loadMetaDetail.getSegmentStatus eq SegmentStatus.INSERT_IN_PROGRESS)) {
            val segmentMaps: util.Map[String, util.List[String]] =
              new Gson().fromJson(loadMetaDetail.getExtraInfo, classOf[util.Map[_, _]])
            val table: String = relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                                relationIdentifier.getTableName
            for (segmentId <- mainTableSegmentList.asScala) {
              // In case if mv segment(0) is mapped
              // to mainTable segments{0,1,2} and if
              // {0,1,2} segments of mainTable are compacted to 0.1. Then,
              // on next rebuild/load to mv, no need to load segment(0.1) again. Update the
              // segmentMapping of mv segment from {0,1,2} to {0.1}
              if (!checkIfSegmentsToBeReloaded(validAndInvalidSegmentsInfo,
                segmentMaps.get(table),
                segmentId)) {
                ifTableStatusUpdateRequired = true
                // Update loadMetaDetail with updated segment info and clear old segmentMap
                val updatedSegmentMap: util.Map[String, util.List[String]] =
                  new util.HashMap[String, util.List[String]]
                val segmentIdList: util.List[String] = new util.ArrayList[String]
                segmentIdList.add(segmentId)
                updatedSegmentMap.put(table, segmentIdList)
                segmentList.add(segmentId)
                loadMetaDetail.setExtraInfo(new Gson().toJson(updatedSegmentMap))
                segmentMaps.get(table).clear()
              }
            }
            segmentList.addAll(segmentMaps.get(table))
          }
        }
        val originSegmentList: util.List[String] = new util.ArrayList[String](segmentList)
        segmentList.removeAll(mainTableSegmentList)
        mainTableSegmentList.removeAll(originSegmentList)
        if (ifTableStatusUpdateRequired && mainTableSegmentList.isEmpty) {
          SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
            schema.getIdentifier.getTablePath),
            listOfLoadFolderDetails.toArray(new Array[LoadMetadataDetails](listOfLoadFolderDetails
              .size)))
          return false
        } else if (mainTableSegmentList.isEmpty) {
          return false
        }
        if (!segmentList.isEmpty) {
          val invalidMainTableSegmentList: util.List[String] = new util.ArrayList[String]
          // validMainTableSegmentList holds segment list which needs to be loaded again
          val validMainTableSegmentList: util.HashSet[String] = new util.HashSet[String]
          // For mv segments which are not in main table segment list(if main table
          // is compacted), iterate over those segments and get mv segments which needs to
          // be marked for delete and main table segments which needs to be loaded again
          for (segmentId <- segmentList.asScala) {
            for (loadMetaDetail <- listOfLoadFolderDetails.asScala) {
              if ((loadMetaDetail.getSegmentStatus eq SegmentStatus.SUCCESS) ||
                  (loadMetaDetail.getSegmentStatus eq SegmentStatus.INSERT_IN_PROGRESS)) {
                val segmentMaps: util.Map[String, util.List[String]] =
                  new Gson().fromJson(loadMetaDetail.getExtraInfo, classOf[util.Map[_, _]])
                val segmentIds: util.List[String] = segmentMaps.get(
                  relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                  relationIdentifier.getTableName)
                if (segmentIds.contains(segmentId)) {
                  segmentIds.remove(segmentId)
                  validMainTableSegmentList.addAll(segmentIds)
                  invalidMainTableSegmentList.add(segmentId)
                  invalidSegmentList.add(loadMetaDetail.getLoadName)
                }
              }
            }
          }
          // remove invalid segment from validMainTableSegmentList if present
          validMainTableSegmentList.removeAll(invalidMainTableSegmentList)
          // Add all valid segments of main table which needs to be loaded again
          mainTableSegmentList.addAll(validMainTableSegmentList)
          segmentMapping.put(relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                             relationIdentifier.getTableName, mainTableSegmentList)
        }
        else segmentMapping.put(relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                                relationIdentifier.getTableName, mainTableSegmentList)
      }
    }
    // Remove invalid mv segments
    if (!invalidSegmentList.isEmpty) {
      for (loadMetadataDetail <- listOfLoadFolderDetails.asScala) {
        if (invalidSegmentList.contains(loadMetadataDetail.getLoadName)) {
          loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
        }
      }
    }
    true
  }

  /**
   * This method checks if mv table segment has to be reloaded again or not
   */
  private def checkIfSegmentsToBeReloaded(validAndInvalidSegmentsInfo: ValidAndInvalidSegmentsInfo,
      segmentIds: util.List[String],
      segmentId: String): Boolean = {
    var isToBeLoadedAgain: Boolean = true
    val mergedSegments: util.List[String] = new util.ArrayList[String]
    val mergedLoadMapping = validAndInvalidSegmentsInfo.getMergedLoadMapping
    if (!mergedLoadMapping.isEmpty && mergedLoadMapping.containsKey(segmentId)) {
      mergedSegments.addAll(mergedLoadMapping.get(segmentId))
    }
    if (!mergedSegments.isEmpty && segmentIds.containsAll(mergedSegments)) {
      isToBeLoadedAgain = false
    }
    isToBeLoadedAgain
  }

  /**
   * This method will set main table segments which needs to be loaded to mv
   */
  private def setInputSegments(tableUniqueName: String,
      mainTableSegmentList: java.util.List[String]): Unit = {
    CarbonThreadUtil
      .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                 tableUniqueName, mainTableSegmentList.asScala.mkString(","))
  }

  private def unsetInputSegments(schema: MVSchema): Unit = {
    val relatedTableIdentifiers = schema.getRelatedTables
    for (relationIdentifier <- relatedTableIdentifiers.asScala) {
      CarbonThreadUtil
        .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     relationIdentifier.getDatabaseName + "." +
                     relationIdentifier.getTableName)
    }
  }

}
