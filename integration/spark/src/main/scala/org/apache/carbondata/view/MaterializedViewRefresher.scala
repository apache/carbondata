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
import org.apache.spark.sql.{CarbonUtils, SparkSession}
import org.apache.spark.sql.execution.command.management.CarbonInsertIntoCommand
import org.apache.spark.sql.parser.MaterializedViewQueryParser

import org.apache.carbondata.common.exceptions.sql.NoSuchMaterializedViewException
import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.locks.ICarbonLock
import org.apache.carbondata.core.metadata.schema.table.{CarbonTable, RelationIdentifier}
import org.apache.carbondata.core.statusmanager.{LoadMetadataDetails, SegmentStatus, SegmentStatusManager}
import org.apache.carbondata.core.util.path.CarbonTablePath
import org.apache.carbondata.core.view.{MaterializedViewSchema, MaterializedViewStatus}
import org.apache.carbondata.processing.util.CarbonLoaderUtil

class MaterializedViewRefresher{

}

object MaterializedViewRefresher {

  private val LOGGER: Logger = LogServiceFactory.getLogService(
    classOf[MaterializedViewRefresher].getCanonicalName)

  /**
   * Refresh the mv by loading all existing data from associated table
   * This is called when refreshing the mv when
   * 1. after mv creation and no "WITH DEFERRED REBUILD" defined
   * 2. user manually trigger REFRESH MATERIALIZED VIEW command
   */
  @throws[IOException]
  @throws[NoSuchMaterializedViewException]
  def refresh(viewSchema: MaterializedViewSchema, session: SparkSession): Boolean = {
    var newLoadName: String = ""
    var segmentMap: String = ""
    val viewTable: CarbonTable = CarbonTable.buildFromTablePath(
      viewSchema.getIdentifier.getTableName,
      viewSchema.getIdentifier.getDatabaseName,
      viewSchema.getIdentifier.getTablePath,
      viewSchema.getIdentifier.getTableId)
    val viewIdentifier = viewSchema.getIdentifier
    val viewTableIdentifier = viewTable.getAbsoluteTableIdentifier
    // Clean up the old invalid segment data before creating a new entry for new load.
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(viewTable, false, null)
    val segmentStatusManager: SegmentStatusManager = new SegmentStatusManager(viewTableIdentifier)
    // Acquire table status lock to handle concurrent dataloading
    val lock: ICarbonLock = segmentStatusManager.getTableStatusLock
    val segmentMapping: util.Map[String, util.List[String]] =
      new util.HashMap[String, util.List[String]]
    val viewManager = MaterializedViewManagerInSpark.get(session)
    try if (lock.lockWithRetries) {
      LOGGER.info("Acquired lock for mv " + viewIdentifier + " for table status updation")
      val viewTableMetadataPath: String =
        CarbonTablePath.getMetadataPath(viewIdentifier.getTablePath)
      val loadMetadataDetails = SegmentStatusManager.readLoadMetadata(viewTableMetadataPath)
      val loadMetadataDetailList: util.List[LoadMetadataDetails] =
        new util.ArrayList[LoadMetadataDetails](CarbonCommonConstants.DEFAULT_COLLECTION_SIZE)
      // Mark for delete all stale loadMetadetail
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
          val associatedTableIds =
            viewSchema.getAssociatedTables.asScala.filter(_.isCarbonDataTable)
          for (associatedTableId <- associatedTableIds) {
            val associatedTableSegmentList: util.List[String] =
              SegmentStatusManager.getValidSegmentList(associatedTableId)
            if (associatedTableSegmentList.isEmpty) {
              return false
            }
            segmentMapping.put(associatedTableId.toString, associatedTableSegmentList)
          }
        }
      segmentMap = new Gson().toJson(segmentMapping)
      // To handle concurrent dataloading to mv, create new loadMetaEntry and
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
      LOGGER.error("Not able to acquire the lock for Table status updation for table " +
                   viewSchema.getIdentifier.getDatabaseName + "." +
                   viewSchema.getIdentifier.getTableName)
      viewManager.setStatus(viewSchema.getIdentifier, MaterializedViewStatus.DISABLED)
      return false
    } finally {
      if (lock.unlock) {
        LOGGER.info("Table unlocked successfully after table status updation" +
                    viewSchema.getIdentifier.getDatabaseName + "." +
                    viewSchema.getIdentifier.getTableName)
      } else {
        LOGGER.error("Unable to unlock Table lock for table" +
                     viewSchema.getIdentifier.getDatabaseName + "." +
                     viewSchema.getIdentifier.getTableName +
                     " during table status updation")
      }
    }
    refreshInternal(viewManager, viewSchema, viewTable, newLoadName, segmentMapping, session)
  }

  @throws[IOException]
  private def refreshInternal(
      viewManager: MaterializedViewManagerInSpark,
      viewSchema: MaterializedViewSchema,
      viewTable: CarbonTable,
      newLoadName: String,
      segmentMap: java.util.Map[String, java.util.List[String]],
      session: SparkSession): Boolean = {
    val query = viewSchema.getQuery
    if (query != null) {
      val viewIdentifier = viewSchema.getIdentifier
      val updatedQuery = MaterializedViewQueryParser.getQuery(query, session)
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
          // If load to dataMap table fails, disable the dataMap and if newLoad is still
          // in INSERT_IN_PROGRESS state, mark for delete the newLoad and update table status file
          viewManager.setStatus(viewSchema.getIdentifier, MaterializedViewStatus.DISABLED)
          LOGGER.error("Data Load failed for DataMap: ", exception)
          CarbonLoaderUtil.updateTableStatusInCaseOfFailure(
            newLoadName,
            viewTable.getAbsoluteTableIdentifier,
            viewTable.getTableName,
            viewTable.getDatabaseName,
            viewTable.getTablePath,
            viewTable.getMetadataPath)
          throw exception
      } finally {
        unsetInputSegments(viewSchema)
      }
    }
    true
  }

  /**
   * This method will compare mainTable and dataMapTable segment List and loads only newly added
   * segment from main table to dataMap table.
   * In case if mainTable is compacted, then based on dataMap to mainTables segmentMapping, dataMap
   * will be loaded
   * Eg:
   * case 1: Consider mainTableSegmentList: {0, 1, 2}, dataMapToMainTable segmentMap:
   * { 0 -> 0, 1-> 1,2}. If (1, 2) segments of main table are compacted to 1.1 and new segment (3)
   * is loaded to main table, then mainTableSegmentList will be updated to{0, 1.1, 3}.
   * In this case, segment (1) of dataMap table will be marked for delete, and new segment
   * {2 -> 1.1, 3} will be loaded to dataMap table
   * case 2: Consider mainTableSegmentList: {0, 1, 2, 3}, dataMapToMainTable segmentMap:
   * { 0 -> 0,1,2, 1-> 3}. If (1, 2) segments of main table are compacted to 1.1 and new segment
   * (4) is loaded to main table, then mainTableSegmentList will be updated to {0, 1.1, 3, 4}.
   * In this case, segment (0) of dataMap table will be marked for delete and segment (0) of
   * main table will be added to validSegmentList which needs to be loaded again. Now, new dataMap
   * table segment (2) with main table segmentList{2 -> 1.1, 4, 0} will be loaded to dataMap table.
   * dataMapToMainTable segmentMap will be updated to {1 -> 3, 2 -> 1.1, 4, 0} after rebuild
   */
  @throws[IOException]
  private def getSpecificSegmentsTobeLoaded(schema: MaterializedViewSchema,
      segmentMapping: util.Map[String, util.List[String]],
      listOfLoadFolderDetails: util.List[LoadMetadataDetails]): Boolean = {
    val relationIdentifiers: util.List[RelationIdentifier] = schema.getAssociatedTables
    // invalidDataMapSegmentList holds segment list which needs to be marked for delete
    val invalidDataMapSegmentList: util.HashSet[String] = new util.HashSet[String]
    if (listOfLoadFolderDetails.isEmpty) {
      // If segment Map is empty, load all valid segments from main tables to mv
      for (relationIdentifier <- relationIdentifiers.asScala) {
        val mainTableSegmentList: util.List[String] = SegmentStatusManager.getValidSegmentList(
          relationIdentifier)
        // If mainTableSegmentList is empty, no need to trigger load command
        // TODO: handle in case of multiple tables load to datamap table
        if (mainTableSegmentList.isEmpty) return false
        segmentMapping.put(relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                           relationIdentifier.getTableName, mainTableSegmentList)
      }
    }
    else {
      for (relationIdentifier <- relationIdentifiers.asScala) {
        val dataMapTableSegmentList: util.List[String] = new util.ArrayList[String]
        // Get all segments for parent relationIdentifier
        val mainTableSegmentList: util.List[String] = SegmentStatusManager.getValidSegmentList(
          relationIdentifier)
        var ifTableStatusUpdateRequired: Boolean = false
        for (loadMetaDetail <- listOfLoadFolderDetails.asScala) {
          if ((loadMetaDetail.getSegmentStatus eq SegmentStatus.SUCCESS) ||
              (loadMetaDetail.getSegmentStatus eq SegmentStatus.INSERT_IN_PROGRESS)) {
            val segmentMaps: util.Map[String, util.List[String]] =
              new Gson().fromJson(loadMetaDetail.getExtraInfo, classOf[util.Map[_, _]])
            val mainTableMetaDataPath: String = CarbonTablePath.getMetadataPath(relationIdentifier
              .getTablePath)
            val parentTableLoadMetaDataDetails: Array[LoadMetadataDetails] = SegmentStatusManager
              .readLoadMetadata(
                mainTableMetaDataPath)
            val table: String = relationIdentifier.getDatabaseName + CarbonCommonConstants.POINT +
                                relationIdentifier.getTableName
            for (segmentId <- mainTableSegmentList.asScala) {
              // In case if dataMap segment(0) is mapped
              // to mainTable segments{0,1,2} and if
              // {0,1,2} segments of mainTable are compacted to 0.1. Then,
              // on next rebuild/load to dataMap, no need to load segment(0.1) again. Update the
              // segmentMapping of dataMap segment from {0,1,2} to {0.1}
              if (!checkIfSegmentsToBeReloaded(parentTableLoadMetaDataDetails,
                segmentMaps.get(table),
                segmentId)) {
                ifTableStatusUpdateRequired = true
                // Update loadMetaDetail with updated segment info and clear old segmentMap
                val updatedSegmentMap: util.Map[String, util.List[String]] =
                  new util.HashMap[String, util.List[String]]
                val segmentList: util.List[String] = new util.ArrayList[String]
                segmentList.add(segmentId)
                updatedSegmentMap.put(table, segmentList)
                dataMapTableSegmentList.add(segmentId)
                loadMetaDetail.setExtraInfo(new Gson().toJson(updatedSegmentMap))
                segmentMaps.get(table).clear()
              }
            }
            dataMapTableSegmentList.addAll(segmentMaps.get(table))
          }
        }
        val dataMapSegmentList: util.List[String] =
          new util.ArrayList[String](dataMapTableSegmentList)
        dataMapTableSegmentList.removeAll(mainTableSegmentList)
        mainTableSegmentList.removeAll(dataMapSegmentList)
        if (ifTableStatusUpdateRequired && mainTableSegmentList.isEmpty) {
          SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(
            schema.getIdentifier.getTablePath),
            listOfLoadFolderDetails.toArray(new Array[LoadMetadataDetails](listOfLoadFolderDetails
              .size)))
          return false
        } else if (mainTableSegmentList.isEmpty) {
          return false
        }
        if (!dataMapTableSegmentList.isEmpty) {
          val invalidMainTableSegmentList: util.List[String] = new util.ArrayList[String]
          // validMainTableSegmentList holds segment list which needs to be loaded again
          val validMainTableSegmentList: util.HashSet[String] = new util.HashSet[String]
          // For dataMap segments which are not in main table segment list(if main table
          // is compacted), iterate over those segments and get dataMap segments which needs to
          // be marked for delete and main table segments which needs to be loaded again
          for (segmentId <- dataMapTableSegmentList.asScala) {
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
                  invalidDataMapSegmentList.add(loadMetaDetail.getLoadName)
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
    // Remove invalid datamap segments
    if (!invalidDataMapSegmentList.isEmpty) {
      for (loadMetadataDetail <- listOfLoadFolderDetails.asScala) {
        if (invalidDataMapSegmentList.contains(loadMetadataDetail.getLoadName)) {
          loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE)
        }
      }
    }
    true
  }

  /**
   * This method checks if dataMap table segment has to be reloaded again or not
   */
  private def checkIfSegmentsToBeReloaded(loadMetaDataDetails: Array[LoadMetadataDetails],
      segmentIds: util.List[String],
      segmentId: String): Boolean = {
    var isToBeLoadedAgain: Boolean = true
    val mergedSegments: util.List[String] = new util.ArrayList[String]
    for (loadMetadataDetail <- loadMetaDataDetails) {
      if (null != loadMetadataDetail.getMergedLoadName &&
          loadMetadataDetail.getMergedLoadName.equalsIgnoreCase(segmentId)) {
        mergedSegments.add(loadMetadataDetail.getLoadName)
      }
    }
    if (!mergedSegments.isEmpty && segmentIds.containsAll(mergedSegments)) {
      isToBeLoadedAgain = false
    }
    isToBeLoadedAgain
  }

  /**
   * This method will set main table segments which needs to be loaded to mv dataMap
   */
  private def setInputSegments(tableUniqueName: String,
      mainTableSegmentList: java.util.List[String]): Unit = {
    CarbonUtils
      .threadSet(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                 tableUniqueName, mainTableSegmentList.asScala.mkString(","))
  }

  private def unsetInputSegments(schema: MaterializedViewSchema): Unit = {
    val associatedTableIdentifiers = schema.getAssociatedTables
    for (relationIdentifier <- associatedTableIdentifiers.asScala) {
      CarbonUtils
        .threadUnset(CarbonCommonConstants.CARBON_INPUT_SEGMENTS +
                     relationIdentifier.getDatabaseName + "." +
                     relationIdentifier.getTableName)
    }
  }

}
