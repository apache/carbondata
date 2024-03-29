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

package org.apache.spark.sql.secondaryindex.load;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.log4j.Logger;
import org.apache.spark.sql.index.CarbonIndexUtil;
import org.apache.spark.sql.secondaryindex.command.ErrorMessage;

public class CarbonInternalLoaderUtil {

  private static final Logger LOGGER = LogServiceFactory.getLogService(
      CarbonInternalLoaderUtil.class.getName());

  public static List<String> getListOfValidSlices(LoadMetadataDetails[] details) {
    List<String> activeSlices = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails oneLoad : details) {
      // External added segments are not loaded to SI
      if (isaBoolean(oneLoad)) {
        activeSlices.add(oneLoad.getLoadName());
      }
    }
    return activeSlices;
  }

  private static boolean isaBoolean(LoadMetadataDetails oneLoad) {
    return oneLoad.getPath() == null && SegmentStatus.SUCCESS.equals(oneLoad.getSegmentStatus())
        || SegmentStatus.LOAD_PARTIAL_SUCCESS.equals(oneLoad.getSegmentStatus())
        || SegmentStatus.MARKED_FOR_UPDATE.equals(oneLoad.getSegmentStatus());
  }

  /**
   * This method will return the mapping of valid segments to segment load start time
   */
  public static Map<String, Long> getSegmentToLoadStartTimeMapping(LoadMetadataDetails[] details) {
    Map<String, Long> segmentToLoadStartTimeMap = new HashMap<>(details.length);
    for (LoadMetadataDetails oneLoad : details) {
      // valid segments will only have Success status
      if (SegmentStatus.SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.LOAD_PARTIAL_SUCCESS.equals(oneLoad.getSegmentStatus())) {
        segmentToLoadStartTimeMap.put(oneLoad.getLoadName(), oneLoad.getLoadStartTime());
      }
    }
    return segmentToLoadStartTimeMap;
  }

  /**
   * This API will write the load level metadata for the load management module inorder to
   * manage the load and query execution management smoothly.
   *
   * @return boolean which determines whether status update is done or not.
   */
  public static boolean recordLoadMetadata(List<LoadMetadataDetails> newLoadMetadataDetails,
      List<String> validSegments, CarbonTable carbonTable, List<CarbonTable> indexCarbonTables,
      CarbonLoadModel loadModel, String tableName, SegmentStatus segmentStatus,
      boolean isRebuiltSegments) {
    String databaseName = loadModel.getDatabaseName();
    boolean status = false;
    String metaDataFilepath = carbonTable.getMetadataPath();
    AbsoluteTableIdentifier absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier,
        loadModel.getLatestTableStatusWriteVersion());
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      int retryCount = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
      int maxTimeout = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info("Acquired lock for table" + databaseName + "." + tableName
            + " for table status update");

        if (isSegmentsAlreadyCompactedForNewMetaDataDetails(indexCarbonTables, tableName,
            newLoadMetadataDetails)) {
          return false;
        }

        loadModel.setLatestTableStatusWriteVersion(String.valueOf(System.currentTimeMillis()));
        String tblStatusVersion = loadModel.getLatestTableStatusWriteVersion();
        if (segmentStatus == SegmentStatus.INSERT_IN_PROGRESS
            || segmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS || isRebuiltSegments) {
          tblStatusVersion = carbonTable.getTableStatusVersion();
        }

        LoadMetadataDetails[] currentLoadMetadataDetails = SegmentStatusManager.readLoadMetadata(
            metaDataFilepath, tblStatusVersion);

        List<LoadMetadataDetails> updatedLoadMetadataDetails = new ArrayList<>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        // check which load needs to be overwritten which are in in progress state
        boolean found = false;
        for (int i = 0; i < currentLoadMetadataDetails.length; i++) {
          for (LoadMetadataDetails newLoadMetadataDetail : newLoadMetadataDetails) {
            if (currentLoadMetadataDetails[i].getLoadName().equals(
                newLoadMetadataDetail.getLoadName())) {
              currentLoadMetadataDetails[i] = newLoadMetadataDetail;
              found = true;
              break;
            }
          }
          updatedLoadMetadataDetails.add(currentLoadMetadataDetails[i]);
        }

        // check if newLoadMetadataDetail has segments which are not in  currentLoadMetaDetails
        // and add them to the updatedLoadMetadataDetails
        boolean foundNext = false;
        for (int i = 0; i < newLoadMetadataDetails.size(); i++) {
          foundNext = false;
          for (int j = 0; j < currentLoadMetadataDetails.length; j++) {
            if (newLoadMetadataDetails.get(i).getLoadName().equals(
                currentLoadMetadataDetails[j].getLoadName())) {
              foundNext = true;
              break;
            }
            if (j == currentLoadMetadataDetails.length - 1 && !foundNext) {
              // if not found in the list then add it
              updatedLoadMetadataDetails.add(newLoadMetadataDetails.get(i));
              found = true;
            }
          }
        }

        // when data load is done for first time, add all the details
        if (currentLoadMetadataDetails.length == 0 || !found) {
          updatedLoadMetadataDetails.addAll(newLoadMetadataDetails);
        }

        List<String> indexTables = CarbonIndexUtil.getSecondaryIndexes(carbonTable);
        if (!indexTables.isEmpty()) {
          List<LoadMetadataDetails> newSegmentDetailsListForIndexTable = new ArrayList<>(
              validSegments.size());
          for (String segmentId : validSegments) {
            LoadMetadataDetails newSegmentDetailsObject = new LoadMetadataDetails();
            newSegmentDetailsObject.setLoadName(segmentId);
            newSegmentDetailsListForIndexTable.add(newSegmentDetailsObject);
          }
          for (CarbonTable indexTable : indexCarbonTables) {
            List<LoadMetadataDetails> indexTableDetailsList =
                CarbonIndexUtil.getTableStatusDetailsForIndexTable(updatedLoadMetadataDetails,
                    indexTable, newSegmentDetailsListForIndexTable);

            SegmentStatusManager.writeLoadDetailsIntoFile(
                CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath(),
                    loadModel.getLatestTableStatusWriteVersion()),
                indexTableDetailsList.toArray(new LoadMetadataDetails[0]));
          }
        } else if (carbonTable.isIndexTable()) {
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath(),
                  loadModel.getLatestTableStatusWriteVersion()),
              updatedLoadMetadataDetails.toArray(new LoadMetadataDetails[0]));
        }
        status = true;
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status update for table " + databaseName + "."
                + tableName);
        throw new RuntimeException(
            "Not able to acquire the lock for Table status updation for table " + databaseName + "."
                + tableName);
      }
    } catch (IOException e) {
      LOGGER.error(
          "Not able to acquire the lock for Table status update for table " + databaseName + "."
              + tableName);
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status update" + databaseName + "."
            + tableName);
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + databaseName + "." + tableName
            + " during table status update");
      }
    }
    return status;
  }

  /**
   * This method read the details of SI table and check whether new metadata details are already
   * compacted, if it is, then already compaction for SI is completed and updating with new segment
   * status is useless, this can happen in case of updating the status of index while loading
   * segments for failed segments, so do not update anything, just exit gracefully
   */
  private static boolean isSegmentsAlreadyCompactedForNewMetaDataDetails(
      List<CarbonTable> indexTables, String indexTableName,
      List<LoadMetadataDetails> newLoadMetadataDetails) {
    CarbonTable currentIndexTable = null;
    for (CarbonTable indexTable : indexTables) {
      if (indexTable.getTableName().equalsIgnoreCase(indexTableName)) {
        currentIndexTable = indexTable;
        break;
      }
    }
    boolean isIndexTableSegmentsCompacted = false;
    if (null != currentIndexTable) {
      LoadMetadataDetails[] existingLoadMetaDataDetails = SegmentStatusManager.readLoadMetadata(
          currentIndexTable.getMetadataPath(), currentIndexTable.getTableStatusVersion());
      for (LoadMetadataDetails existingLoadMetaDataDetail : existingLoadMetaDataDetails) {
        for (LoadMetadataDetails newLoadMetadataDetail : newLoadMetadataDetails) {
          if (existingLoadMetaDataDetail.getLoadName().equalsIgnoreCase(
              newLoadMetadataDetail.getLoadName())
              && existingLoadMetaDataDetail.getSegmentStatus() == SegmentStatus.COMPACTED) {
            isIndexTableSegmentsCompacted = true;
            break;
          }
        }
        if (isIndexTableSegmentsCompacted) {
          break;
        }
      }
      return isIndexTableSegmentsCompacted;
    } else {
      return false;
    }
  }

  /**
   * method to update table status in case of IUD Update Delta Compaction.
   */
  public static boolean updateLoadMetadataWithMergeStatus(CarbonTable indexCarbonTable,
      String[] loadsToMerge, String mergedLoadNumber, Map<String, String> segmentToLoadStartTimeMap,
      long mergeLoadStartTime, SegmentStatus segmentStatus, long newLoadStartTime,
      List<String> rebuiltSegments) throws IOException {
    boolean tableStatusUpdateStatus = false;
    List<String> loadMergeList = new ArrayList<>(Arrays.asList(loadsToMerge));
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(
        indexCarbonTable.getAbsoluteTableIdentifier(), indexCarbonTable.getTableStatusVersion());

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for the table " + indexCarbonTable.getDatabaseName() + "."
            + indexCarbonTable.getTableName() + " for table status update ");
        LoadMetadataDetails[] loadDetails = SegmentStatusManager.readLoadMetadata(
            indexCarbonTable.getMetadataPath(), indexCarbonTable.getTableStatusVersion());

        long modificationOrDeletionTimeStamp = CarbonUpdateUtil.readCurrentTime();
        for (LoadMetadataDetails loadDetail : loadDetails) {
          // check if this segment is merged.
          if (loadMergeList.contains(loadDetail.getLoadName()) || loadMergeList.contains(
              loadDetail.getMergedLoadName())) {
            // if the compacted load is deleted after the start of the compaction process,
            // then need to discard the compaction process and treat it as failed compaction.
            if (loadDetail.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE) {
              LOGGER.error("Compaction is aborted as the segment " + loadDetail.getLoadName()
                  + " is deleted after the compaction is started.");
              return false;
            }
            loadDetail.setSegmentStatus(SegmentStatus.COMPACTED);
            loadDetail.setModificationOrDeletionTimestamp(modificationOrDeletionTimeStamp);
            loadDetail.setMergedLoadName(mergedLoadNumber);
          }
        }

        // create entry for merged one.
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setSegmentStatus(segmentStatus);
        long loadEndDate = CarbonUpdateUtil.readCurrentTime();
        loadMetadataDetails.setLoadEndTime(loadEndDate);
        loadMetadataDetails.setLoadName(mergedLoadNumber);
        loadMetadataDetails.setSegmentFile(SegmentFileStore.genSegmentFileName(mergedLoadNumber,
            String.valueOf(segmentToLoadStartTimeMap.get(mergedLoadNumber)))
            + CarbonTablePath.SEGMENT_EXT);
        CarbonLoaderUtil.addDataIndexSizeIntoMetaEntry(loadMetadataDetails, mergedLoadNumber,
            indexCarbonTable);
        if (rebuiltSegments.contains(loadMetadataDetails.getLoadName())) {
          loadMetadataDetails.setLoadStartTime(newLoadStartTime);
        } else {
          loadMetadataDetails.setLoadStartTime(mergeLoadStartTime);
        }

        // put the merged folder entry
        for (int i = 0; i < loadDetails.length; i++) {
          if (loadDetails[i].getLoadName().equals(loadMetadataDetails.getLoadName())) {
            loadDetails[i] = loadMetadataDetails;
          }
        }

        // if this is a major compaction then set the segment as major compaction.
        List<LoadMetadataDetails> updatedDetailsList = new ArrayList<>(Arrays.asList(loadDetails));

        SegmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(indexCarbonTable.getTablePath(),
                indexCarbonTable.getTableStatusVersion()),
            updatedDetailsList.toArray(new LoadMetadataDetails[0]));
        tableStatusUpdateStatus = true;
      } else {
        LOGGER.error(
            "Could not able to obtain lock for table" + indexCarbonTable.getDatabaseName() + "."
                + indexCarbonTable.getTableName() + "for table status update");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status update" + indexCarbonTable
            .getDatabaseName() + "." + indexCarbonTable.getTableName());
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + indexCarbonTable.getDatabaseName() + "."
                + indexCarbonTable.getTableName() + " during table status update");
      }
    }
    return tableStatusUpdateStatus;
  }

  public static boolean checkMainTableSegEqualToSiSeg(
      LoadMetadataDetails[] mainTableLoadMetadataDetails,
      LoadMetadataDetails[] siTableLoadMetadataDetails) throws ErrorMessage {
    return checkMainTableSegEqualToSiSeg(mainTableLoadMetadataDetails, siTableLoadMetadataDetails,
        false);
  }

  /**
   * Method to check if main table and SI have same number of valid segments or not
   */
  public static boolean checkMainTableSegEqualToSiSeg(
      LoadMetadataDetails[] mainTableLoadMetadataDetails,
      LoadMetadataDetails[] siTableLoadMetadataDetails, boolean isRegisterIndex)
      throws ErrorMessage {
    List<String> mainTableSegmentsList = getListOfValidSlices(mainTableLoadMetadataDetails);
    List<String> indexList = getListOfValidSlices(siTableLoadMetadataDetails);
    Collections.sort(mainTableSegmentsList);
    Collections.sort(indexList);
    // In the case when number of SI segments are more than the main table segments do nothing
    // and proceed to process the segments. Return False in case if main table segments are more
    // than SI Segments
    if (indexList.size() < mainTableSegmentsList.size()) {
      return false;
    } else if ((indexList.size() > mainTableSegmentsList.size()) && isRegisterIndex) {
      throw new ErrorMessage("Cannot register index, as the number of Secondary index table "
          + "segments are more than the main table segments. Try Drop and recreate SI.");
    }
    // There can be cases when the number of segments in the main table are less than the index
    // table. In this case mapping all the segments in main table to SI table.
    // Return False if a segment in main table is not in index table
    HashSet<String> indexTableSet = new HashSet<String>();
    for (int i = 0; i < indexList.size(); i++) {
      indexTableSet.add(indexList.get(i));
    }
    for (int i = 0; i < mainTableSegmentsList.size(); i++) {
      if (!indexTableSet.contains(mainTableSegmentsList.get(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Method to check if main table has in progress load and same segment not present in SI
   */
  public static boolean checkInProgLoadInMainTableAndSi(CarbonTable carbonTable,
      LoadMetadataDetails[] mainTableLoadMetadataDetails,
      LoadMetadataDetails[] siTableLoadMetadataDetails) {
    List<String> allSiSlices = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails oneLoad : siTableLoadMetadataDetails) {
      allSiSlices.add(oneLoad.getLoadName());
    }

    if (mainTableLoadMetadataDetails.length != 0) {
      for (LoadMetadataDetails loadDetail : mainTableLoadMetadataDetails) {
        // if load in progress and check if same load is present in SI.
        if (SegmentStatusManager.isLoadInProgress(carbonTable.getAbsoluteTableIdentifier(),
            loadDetail.getLoadName())) {
          if (!allSiSlices.contains(loadDetail.getLoadName())) {
            return false;
          }
        }
      }
    }
    return true;
  }
}
