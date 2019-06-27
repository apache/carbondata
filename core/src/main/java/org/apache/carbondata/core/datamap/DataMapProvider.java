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

package org.apache.carbondata.core.datamap;

import java.io.IOException;
import java.util.*;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.status.DataMapSegmentStatusUtil;
import org.apache.carbondata.core.datamap.status.DataMapStatusManager;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * DataMap is a accelerator for certain type of query. Developer can add new DataMap
 * implementation to improve query performance.
 *
 * Currently two types of DataMap are supported
 * <ol>
 *   <li> MVDataMap: materialized view type of DataMap to accelerate olap style query,
 * like SPJG query (select, predicate, join, groupby) </li>
 *   <li> DataMap: index type of DataMap to accelerate filter query </li>
 * </ol>
 *
 * <p>
 * In following command <br>
 * {@code CREATE DATAMAP dm ON TABLE main USING 'provider'}, <br>
 * the <b>provider</b> string can be a short name or class name of the DataMap implementation.
 *
 * <br>Currently CarbonData supports following provider:
 * <ol>
 *   <li> preaggregate: pre-aggregate table of single table </li>
 *   <li> timeseries: pre-aggregate table based on time dimension of the table </li>
 *   <li> lucene: index backed by Apache Lucene </li>
 *   <li> bloomfilter: index backed by Bloom Filter </li>
 * </ol>
 *
 * @since 1.4.0
 */
@InterfaceAudience.Internal
public abstract class DataMapProvider {

  private CarbonTable mainTable;
  private DataMapSchema dataMapSchema;

  private Logger LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  public DataMapProvider(CarbonTable mainTable, DataMapSchema dataMapSchema) {
    this.mainTable = mainTable;
    this.dataMapSchema = dataMapSchema;
  }

  protected final CarbonTable getMainTable() {
    return mainTable;
  }

  public final DataMapSchema getDataMapSchema() {
    return dataMapSchema;
  }

  /**
   * Initialize a datamap's metadata.
   * This is called when user creates datamap, for example "CREATE DATAMAP dm ON TABLE mainTable"
   * Implementation should initialize metadata for datamap, like creating table
   */
  public abstract void initMeta(String ctasSqlStatement) throws MalformedDataMapCommandException,
      IOException;

  /**
   * Initialize a datamap's data.
   * This is called when user creates datamap, for example "CREATE DATAMAP dm ON TABLE mainTable"
   * Implementation should initialize data for datamap, like creating data folders
   */
  public void initData() { }

  /**
   * Opposite operation of {@link #initMeta(String)}.
   * This is called when user drops datamap, for example "DROP DATAMAP dm ON TABLE mainTable"
   * Implementation should clean all meta for the datamap
   */
  public abstract void cleanMeta() throws IOException;

  /**
   * Opposite operation of {@link #initData()}.
   * This is called when user drops datamap, for example "DROP DATAMAP dm ON TABLE mainTable"
   * Implementation should clean all data for the datamap
   */
  public abstract void cleanData();

  /**
   * Rebuild the datamap by loading all existing data from mainTable
   * This is called when refreshing the datamap when
   * 1. after datamap creation and if `autoRefreshDataMap` is set to true
   * 2. user manually trigger REBUILD DATAMAP command
   */
  public boolean rebuild() throws IOException, NoSuchDataMapException {
    if (null == dataMapSchema.getRelationIdentifier()) {
      return false;
    }
    String newLoadName = "";
    String segmentMap = "";
    CarbonTable dataMapTable = CarbonTable
        .buildFromTablePath(dataMapSchema.getRelationIdentifier().getTableName(),
            dataMapSchema.getRelationIdentifier().getDatabaseName(),
            dataMapSchema.getRelationIdentifier().getTablePath(),
            dataMapSchema.getRelationIdentifier().getTableId());
    AbsoluteTableIdentifier dataMapTableAbsoluteTableIdentifier =
        dataMapTable.getAbsoluteTableIdentifier();
    // Clean up the old invalid segment data before creating a new entry for new load.
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(dataMapTable, false, null);
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(dataMapTableAbsoluteTableIdentifier);
    Map<String, List<String>> segmentMapping = new HashMap<>();
    // Acquire table status lock to handle concurrent dataloading
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + dataMapSchema.getRelationIdentifier().getDatabaseName()
                + "." + dataMapSchema.getRelationIdentifier().getTableName()
                + " for table status updation");
        String dataMapTableMetadataPath =
            CarbonTablePath.getMetadataPath(dataMapSchema.getRelationIdentifier().getTablePath());
        LoadMetadataDetails[] loadMetaDataDetails =
            SegmentStatusManager.readLoadMetadata(dataMapTableMetadataPath);
        // Mark for delete all stale loadMetadetail
        for (LoadMetadataDetails loadMetadataDetail : loadMetaDataDetails) {
          if ((loadMetadataDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS
              || loadMetadataDetail.getSegmentStatus()
              == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) && loadMetadataDetail.getVisibility()
              .equalsIgnoreCase("false")) {
            loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          }
        }
        List<LoadMetadataDetails> listOfLoadFolderDetails =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        Collections.addAll(listOfLoadFolderDetails, loadMetaDataDetails);
        if (dataMapSchema.isLazy()) {
          // check if rebuild to datamap is already in progress and throw exception
          if (!listOfLoadFolderDetails.isEmpty()) {
            for (LoadMetadataDetails loadMetaDetail : loadMetaDataDetails) {
              if ((loadMetaDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS
                  || loadMetaDetail.getSegmentStatus()
                  == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) && SegmentStatusManager
                  .isLoadInProgress(dataMapTableAbsoluteTableIdentifier,
                      loadMetaDetail.getLoadName())) {
                throw new RuntimeException("Rebuild to datamap " + dataMapSchema.getDataMapName()
                    + " is already in progress");
              }
            }
          }
        }
        boolean isFullRefresh = false;
        if (null != dataMapSchema.getProperties().get("full_refresh")) {
          isFullRefresh = Boolean.valueOf(dataMapSchema.getProperties().get("full_refresh"));
        }
        if (!isFullRefresh) {
          if (!getSpecificSegmentsTobeLoaded(segmentMapping, listOfLoadFolderDetails)) {
            return false;
          }
          segmentMap = new Gson().toJson(segmentMapping);
        } else {
          List<RelationIdentifier> relationIdentifiers = dataMapSchema.getParentTables();
          for (RelationIdentifier relationIdentifier : relationIdentifiers) {
            List<String> mainTableSegmentList =
                DataMapUtil.getMainTableValidSegmentList(relationIdentifier);
            if (mainTableSegmentList.isEmpty()) {
              return false;
            }
            segmentMapping.put(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                + relationIdentifier.getTableName(), mainTableSegmentList);
          }
          segmentMap = new Gson().toJson(segmentMapping);
        }

        // To handle concurrent dataloading to datamap, create new loadMetaEntry and
        // set segmentMap to new loadMetaEntry and pass new segmentId with load command
        LoadMetadataDetails loadMetadataDetail = new LoadMetadataDetails();
        String segmentId =
            String.valueOf(SegmentStatusManager.createNewSegmentId(loadMetaDataDetails));
        loadMetadataDetail.setLoadName(segmentId);
        loadMetadataDetail.setSegmentStatus(SegmentStatus.INSERT_IN_PROGRESS);
        loadMetadataDetail.setExtraInfo(segmentMap);
        listOfLoadFolderDetails.add(loadMetadataDetail);
        newLoadName = segmentId;

        SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
                .getTableStatusFilePath(dataMapSchema.getRelationIdentifier().getTablePath()),
            listOfLoadFolderDetails
                .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table " + dataMapSchema
                .getRelationIdentifier().getDatabaseName() + "." + dataMapSchema
                .getRelationIdentifier().getTableName());
        DataMapStatusManager.disableDataMap(dataMapSchema.getDataMapName());
        return false;
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + dataMapSchema
            .getRelationIdentifier().getDatabaseName() + "." + dataMapSchema.getRelationIdentifier()
            .getTableName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + dataMapSchema.getRelationIdentifier()
            .getDatabaseName() + "." + dataMapSchema.getRelationIdentifier().getTableName()
            + " during table status updation");
      }
    }
    return rebuildInternal(newLoadName, segmentMapping, dataMapTable);
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
  private boolean getSpecificSegmentsTobeLoaded(Map<String, List<String>> segmentMapping,
      List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    List<RelationIdentifier> relationIdentifiers = dataMapSchema.getParentTables();
    // invalidDataMapSegmentList holds segment list which needs to be marked for delete
    HashSet<String> invalidDataMapSegmentList = new HashSet<>();
    if (listOfLoadFolderDetails.isEmpty()) {
      // If segment Map is empty, load all valid segments from main tables to dataMap
      for (RelationIdentifier relationIdentifier : relationIdentifiers) {
        List<String> mainTableSegmentList =
            DataMapUtil.getMainTableValidSegmentList(relationIdentifier);
        // If mainTableSegmentList is empty, no need to trigger load command
        // TODO: handle in case of multiple tables load to datamap table
        if (mainTableSegmentList.isEmpty()) {
          return false;
        }
        segmentMapping.put(
            relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT + relationIdentifier
                .getTableName(), mainTableSegmentList);
      }
    } else {
      for (RelationIdentifier relationIdentifier : relationIdentifiers) {
        List<String> dataMapTableSegmentList = new ArrayList<>();
        // Get all segments for parent relationIdentifier
        List<String> mainTableSegmentList =
            DataMapUtil.getMainTableValidSegmentList(relationIdentifier);
        boolean ifTableStatusUpdateRequired = false;
        for (LoadMetadataDetails loadMetaDetail : listOfLoadFolderDetails) {
          if (loadMetaDetail.getSegmentStatus() == SegmentStatus.SUCCESS
              || loadMetaDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS) {
            Map<String, List<String>> segmentMaps =
                DataMapSegmentStatusUtil.getSegmentMap(loadMetaDetail.getExtraInfo());
            String mainTableMetaDataPath =
                CarbonTablePath.getMetadataPath(relationIdentifier.getTablePath());
            LoadMetadataDetails[] parentTableLoadMetaDataDetails =
                SegmentStatusManager.readLoadMetadata(mainTableMetaDataPath);
            String table = relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                + relationIdentifier.getTableName();
            for (String segmentId : mainTableSegmentList) {
              // In case if dataMap segment(0) is mapped to mainTable segments{0,1,2} and if
              // {0,1,2} segments of mainTable are compacted to 0.1. Then,
              // on next rebuild/load to dataMap, no need to load segment(0.1) again. Update the
              // segmentMapping of dataMap segment from {0,1,2} to {0.1}
              if (!checkIfSegmentsToBeReloaded(parentTableLoadMetaDataDetails,
                  segmentMaps.get(table), segmentId)) {
                ifTableStatusUpdateRequired = true;
                // Update loadMetaDetail with updated segment info and clear old segmentMap
                Map<String, List<String>> updatedSegmentMap = new HashMap<>();
                List<String> segmentList = new ArrayList<>();
                segmentList.add(segmentId);
                updatedSegmentMap.put(table, segmentList);
                dataMapTableSegmentList.add(segmentId);
                loadMetaDetail.setExtraInfo(new Gson().toJson(updatedSegmentMap));
                segmentMaps.get(table).clear();
              }
            }
            dataMapTableSegmentList.addAll(segmentMaps.get(table));
          }
        }
        List<String> dataMapSegmentList = new ArrayList<>(dataMapTableSegmentList);
        dataMapTableSegmentList.removeAll(mainTableSegmentList);
        mainTableSegmentList.removeAll(dataMapSegmentList);
        if (ifTableStatusUpdateRequired && mainTableSegmentList.isEmpty()) {
          SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
                  .getTableStatusFilePath(dataMapSchema.getRelationIdentifier().getTablePath()),
              listOfLoadFolderDetails
                  .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
          return false;
        } else if (mainTableSegmentList.isEmpty()) {
          return false;
        }
        if (!dataMapTableSegmentList.isEmpty()) {
          List<String> invalidMainTableSegmentList = new ArrayList<>();
          // validMainTableSegmentList holds segment list which needs to be loaded again
          HashSet<String> validMainTableSegmentList = new HashSet<>();

          // For dataMap segments which are not in main table segment list(if main table
          // is compacted), iterate over those segments and get dataMap segments which needs to
          // be marked for delete and main table segments which needs to be loaded again
          for (String segmentId : dataMapTableSegmentList) {
            for (LoadMetadataDetails loadMetaDetail : listOfLoadFolderDetails) {
              if (loadMetaDetail.getSegmentStatus() == SegmentStatus.SUCCESS
                  || loadMetaDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS) {
                Map<String, List<String>> segmentMaps =
                    DataMapSegmentStatusUtil.getSegmentMap(loadMetaDetail.getExtraInfo());
                List<String> segmentIds = segmentMaps.get(
                    relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                        + relationIdentifier.getTableName());
                if (segmentIds.contains(segmentId)) {
                  segmentIds.remove(segmentId);
                  validMainTableSegmentList.addAll(segmentIds);
                  invalidMainTableSegmentList.add(segmentId);
                  invalidDataMapSegmentList.add(loadMetaDetail.getLoadName());
                }
              }
            }
          }
          // remove invalid segment from validMainTableSegmentList if present
          validMainTableSegmentList.removeAll(invalidMainTableSegmentList);
          // Add all valid segments of main table which needs to be loaded again
          mainTableSegmentList.addAll(validMainTableSegmentList);
          segmentMapping.put(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
              + relationIdentifier.getTableName(), mainTableSegmentList);
        } else {
          segmentMapping.put(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
              + relationIdentifier.getTableName(), mainTableSegmentList);
        }
      }
    }
    // Remove invalid datamap segments
    if (!invalidDataMapSegmentList.isEmpty()) {
      for (LoadMetadataDetails loadMetadataDetail : listOfLoadFolderDetails) {
        if (invalidDataMapSegmentList.contains(loadMetadataDetail.getLoadName())) {
          loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
        }
      }
    }
    return true;
  }

  /**
   * This method checks if dataMap table segment has to be reloaded again or not
   */
  private boolean checkIfSegmentsToBeReloaded(LoadMetadataDetails[] loadMetaDataDetails,
      List<String> segmentIds, String segmentId) {
    boolean isToBeLoadedAgain = true;
    List<String> mergedSegments = new ArrayList<>();
    for (LoadMetadataDetails loadMetadataDetail : loadMetaDataDetails) {
      if (null != loadMetadataDetail.getMergedLoadName() && loadMetadataDetail.getMergedLoadName()
          .equalsIgnoreCase(segmentId)) {
        mergedSegments.add(loadMetadataDetail.getLoadName());
      }
    }
    if (!mergedSegments.isEmpty() && segmentIds.containsAll(mergedSegments)) {
      isToBeLoadedAgain = false;
    }
    return isToBeLoadedAgain;
  }

  /**
   * Provide the datamap catalog instance or null if this datamap not required to rewrite
   * the query.
   */
  public DataMapCatalog createDataMapCatalog() {
    return null;
  }

  public abstract DataMapFactory getDataMapFactory();

  public abstract boolean supportRebuild();

  public abstract boolean rebuildInternal(String newLoadName, Map<String, List<String>> segmentMap,
      CarbonTable carbonTable);
}
