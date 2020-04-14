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

package org.apache.carbondata.core.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchIndexException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.index.status.IndexStatusManager;
import org.apache.carbondata.core.index.status.MVSegmentStatusUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * Index is a accelerator for certain type of query. Developer can add new Index
 * implementation to improve query performance.
 *
 * Currently two types of Index are supported
 * <ol>
 *   <li> MV: materialized view type of Index to accelerate olap style query,
 * like SPJG query (select, predicate, join, groupby) </li>
 *   <li> Index: index type of Index to accelerate filter query </li>
 * </ol>
 *
 * <p>
 * In following command <br>
 * {@code CREATE INDEX dm ON TABLE main AS 'provider'}, <br>
 * the <b>provider</b> string can be a short name or class name of the Index implementation.
 *
 * <br>Currently CarbonData supports following provider:
 * <ol>
 *   <li> lucene: index backed by Apache Lucene </li>
 *   <li> bloomfilter: index backed by Bloom Filter </li>
 * </ol>
 *
 * @since 1.4.0
 */
@InterfaceAudience.Internal
public abstract class CarbonIndexProvider {

  public void setMainTable(CarbonTable mainTable) {
    this.mainTable = mainTable;
  }

  private CarbonTable mainTable;
  private IndexSchema indexSchema;

  private Logger LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  public CarbonIndexProvider(CarbonTable mainTable, IndexSchema indexSchema) {
    this.mainTable = mainTable;
    this.indexSchema = indexSchema;
  }

  protected final CarbonTable getMainTable() {
    return mainTable;
  }

  public final IndexSchema getIndexSchema() {
    return indexSchema;
  }

  /**
   * Initialize a index's metadata.
   * This is called when user creates index "CREATE INDEX index ON mainTable"
   * Implementation should initialize metadata for index, like creating table
   */
  public abstract void initMeta(String ctasSqlStatement) throws MalformedIndexCommandException,
      IOException;

  /**
   * Initialize a index's data.
   * This is called when user creates index, for example "CREATE INDEX dm ON TABLE mainTable"
   * Implementation should initialize data for index, like creating data folders
   */
  public void initData() { }

  /**
   * Opposite operation of {@link #initMeta(String)}.
   * This is called when user drops index, for example "DROP INDEX dm ON TABLE mainTable"
   * Implementation should clean all meta for the index
   */
  public abstract void cleanMeta() throws IOException;

  /**
   * Opposite operation of {@link #initData()}.
   * This is called when user drops index, for example "DROP INDEX dm ON TABLE mainTable"
   * Implementation should clean all data for the index
   */
  public abstract void cleanData();

  /**
   * Rebuild the index by loading all existing data from mainTable
   * This is called when refreshing the index when
   * 1. after index creation and no "WITH DEFERRED REFRESH" defined
   * 2. user manually trigger REFRESH index command
   */
  public boolean rebuild() throws IOException, NoSuchIndexException {
    if (null == indexSchema.getRelationIdentifier()) {
      return false;
    }
    String newLoadName = "";
    String segmentMap = "";
    CarbonTable table = CarbonTable
        .buildFromTablePath(indexSchema.getRelationIdentifier().getTableName(),
            indexSchema.getRelationIdentifier().getDatabaseName(),
            indexSchema.getRelationIdentifier().getTablePath(),
            indexSchema.getRelationIdentifier().getTableId());
    AbsoluteTableIdentifier childTableAbsoluteTableIdentifier =
        table.getAbsoluteTableIdentifier();
    // Clean up the old invalid segment data before creating a new entry for new load.
    SegmentStatusManager.deleteLoadsAndUpdateMetadata(table, false, null);
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(childTableAbsoluteTableIdentifier);
    Map<String, List<String>> segmentMapping = new HashMap<>();
    // Acquire table status lock to handle concurrent dataloading
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + indexSchema.getRelationIdentifier().getDatabaseName()
                + "." + indexSchema.getRelationIdentifier().getTableName()
                + " for table status updation");
        String childTableMetadataPath =
            CarbonTablePath.getMetadataPath(indexSchema.getRelationIdentifier().getTablePath());
        LoadMetadataDetails[] loadMetaDataDetails =
            SegmentStatusManager.readLoadMetadata(childTableMetadataPath);
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
        if (indexSchema.isLazy()) {
          // check if rebuild to index is already in progress and throw exception
          if (!listOfLoadFolderDetails.isEmpty()) {
            for (LoadMetadataDetails loadMetaDetail : loadMetaDataDetails) {
              if ((loadMetaDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS
                  || loadMetaDetail.getSegmentStatus()
                  == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) && SegmentStatusManager
                  .isLoadInProgress(childTableAbsoluteTableIdentifier,
                      loadMetaDetail.getLoadName())) {
                throw new RuntimeException("Rebuild to index " + indexSchema.getIndexName()
                    + " is already in progress");
              }
            }
          }
        }
        boolean incrementalBuild = indexSchema.canBeIncrementalBuild();
        if (incrementalBuild) {
          if (!getSpecificSegmentsTobeLoaded(segmentMapping, listOfLoadFolderDetails)) {
            return false;
          }
        } else {
          // set segment mapping only for carbondata table
          List<RelationIdentifier> relationIdentifiers = indexSchema.getParentTables()
              .stream()
              .filter(RelationIdentifier::isCarbonDataTable)
              .collect(Collectors.toList());
          for (RelationIdentifier relationIdentifier : relationIdentifiers) {
            List<String> mainTableSegmentList =
                IndexUtil.getMainTableValidSegmentList(relationIdentifier);
            if (mainTableSegmentList.isEmpty()) {
              return false;
            }
            segmentMapping.put(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                + relationIdentifier.getTableName(), mainTableSegmentList);
          }
        }
        segmentMap = new Gson().toJson(segmentMapping);

        // To handle concurrent data loading to index, create new loadMetaEntry and
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
                .getTableStatusFilePath(indexSchema.getRelationIdentifier().getTablePath()),
            listOfLoadFolderDetails
                .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table " + indexSchema
                .getRelationIdentifier().getDatabaseName() + "." + indexSchema
                .getRelationIdentifier().getTableName());
        IndexStatusManager.disableIndex(indexSchema.getIndexName());
        return false;
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + indexSchema
            .getRelationIdentifier().getDatabaseName() + "." + indexSchema.getRelationIdentifier()
            .getTableName());
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + indexSchema.getRelationIdentifier()
            .getDatabaseName() + "." + indexSchema.getRelationIdentifier().getTableName()
            + " during table status updation");
      }
    }
    return rebuildInternal(newLoadName, segmentMapping, table);
  }

  /**
   * This method will compare mainTable and index segment List and loads only newly added
   * segment from main table to index.
   * In case if mainTable is compacted, then based on index to mainTables segmentMapping, index
   * will be loaded
   * Eg:
   * case 1: Consider mainTableSegmentList: {0, 1, 2}, indexToMainTable segmentMap:
   * { 0 -> 0, 1-> 1,2}. If (1, 2) segments of main table are compacted to 1.1 and new segment (3)
   * is loaded to main table, then mainTableSegmentList will be updated to{0, 1.1, 3}.
   * In this case, segment (1) of index will be marked for delete, and new segment
   * {2 -> 1.1, 3} will be loaded to index
   * case 2: Consider mainTableSegmentList: {0, 1, 2, 3}, indexToMainTable segmentMap:
   * { 0 -> 0,1,2, 1-> 3}. If (1, 2) segments of main table are compacted to 1.1 and new segment
   * (4) is loaded to main table, then mainTableSegmentList will be updated to {0, 1.1, 3, 4}.
   * In this case, segment (0) of index will be marked for delete and segment (0) of
   * main table will be added to validSegmentList which needs to be loaded again. Now, new index
   * segment (2) with main table segmentList{2 -> 1.1, 4, 0} will be loaded to index.
   * indexToMainTable segmentMap will be updated to {1 -> 3, 2 -> 1.1, 4, 0} after rebuild
   */
  private boolean getSpecificSegmentsTobeLoaded(Map<String, List<String>> segmentMapping,
      List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    List<RelationIdentifier> relationIdentifiers = indexSchema.getParentTables();
    // invalidIndexSegmentList holds segment list which needs to be marked for delete
    HashSet<String> invalidIndexSegmentList = new HashSet<>();
    if (listOfLoadFolderDetails.isEmpty()) {
      // If segment Map is empty, load all valid segments from main tables to index
      for (RelationIdentifier relationIdentifier : relationIdentifiers) {
        List<String> mainTableSegmentList =
            IndexUtil.getMainTableValidSegmentList(relationIdentifier);
        // If mainTableSegmentList is empty, no need to trigger load command
        if (mainTableSegmentList.isEmpty()) {
          return false;
        }
        segmentMapping.put(
            relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT +
                relationIdentifier.getTableName(), mainTableSegmentList);
      }
    } else {
      for (RelationIdentifier relationIdentifier : relationIdentifiers) {
        List<String> indexSegmentList = new ArrayList<>();
        // Get all segments for parent relationIdentifier
        List<String> mainTableSegmentList =
            IndexUtil.getMainTableValidSegmentList(relationIdentifier);
        boolean ifTableStatusUpdateRequired = false;
        for (LoadMetadataDetails loadMetaDetail : listOfLoadFolderDetails) {
          if (loadMetaDetail.getSegmentStatus() == SegmentStatus.SUCCESS
              || loadMetaDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS) {
            Map<String, List<String>> segmentMaps =
                MVSegmentStatusUtil.getSegmentMap(loadMetaDetail.getExtraInfo());
            String mainTableMetaDataPath =
                CarbonTablePath.getMetadataPath(relationIdentifier.getTablePath());
            LoadMetadataDetails[] parentTableLoadMetaDataDetails =
                SegmentStatusManager.readLoadMetadata(mainTableMetaDataPath);
            String table = relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                + relationIdentifier.getTableName();
            for (String segmentId : mainTableSegmentList) {
              // In case if index segment(0) is mapped to mainTable segments{0,1,2} and if
              // {0,1,2} segments of mainTable are compacted to 0.1. Then,
              // on next rebuild/load to index, no need to load segment(0.1) again. Update the
              // segmentMapping of index segment from {0,1,2} to {0.1}
              if (!checkIfSegmentsToBeReloaded(parentTableLoadMetaDataDetails,
                  segmentMaps.get(table), segmentId)) {
                ifTableStatusUpdateRequired = true;
                // Update loadMetaDetail with updated segment info and clear old segmentMap
                Map<String, List<String>> updatedSegmentMap = new HashMap<>();
                List<String> segmentList = new ArrayList<>();
                segmentList.add(segmentId);
                updatedSegmentMap.put(table, segmentList);
                indexSegmentList.add(segmentId);
                loadMetaDetail.setExtraInfo(new Gson().toJson(updatedSegmentMap));
                segmentMaps.get(table).clear();
              }
            }
            indexSegmentList.addAll(segmentMaps.get(table));
          }
        }
        List<String> childTableSegmentList = new ArrayList<>(indexSegmentList);
        indexSegmentList.removeAll(mainTableSegmentList);
        mainTableSegmentList.removeAll(childTableSegmentList);
        if (ifTableStatusUpdateRequired && mainTableSegmentList.isEmpty()) {
          SegmentStatusManager.writeLoadDetailsIntoFile(CarbonTablePath
                  .getTableStatusFilePath(indexSchema.getRelationIdentifier().getTablePath()),
              listOfLoadFolderDetails.toArray(new LoadMetadataDetails[0]));
          return false;
        } else if (mainTableSegmentList.isEmpty()) {
          return false;
        }
        if (!indexSegmentList.isEmpty()) {
          List<String> invalidMainTableSegmentList = new ArrayList<>();
          // validMainTableSegmentList holds segment list which needs to be loaded again
          HashSet<String> validMainTableSegmentList = new HashSet<>();

          // For index segments which are not in main table segment list(if main table
          // is compacted), iterate over those segments and get index segments which needs to
          // be marked for delete and main table segments which needs to be loaded again
          for (String segmentId : indexSegmentList) {
            for (LoadMetadataDetails loadMetaDetail : listOfLoadFolderDetails) {
              if (loadMetaDetail.getSegmentStatus() == SegmentStatus.SUCCESS
                  || loadMetaDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS) {
                Map<String, List<String>> segmentMaps =
                    MVSegmentStatusUtil.getSegmentMap(loadMetaDetail.getExtraInfo());
                List<String> segmentIds = segmentMaps.get(
                    relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
                        + relationIdentifier.getTableName());
                if (segmentIds.contains(segmentId)) {
                  segmentIds.remove(segmentId);
                  validMainTableSegmentList.addAll(segmentIds);
                  invalidMainTableSegmentList.add(segmentId);
                  invalidIndexSegmentList.add(loadMetaDetail.getLoadName());
                }
              }
            }
          }
          // remove invalid segment from validMainTableSegmentList if present
          validMainTableSegmentList.removeAll(invalidMainTableSegmentList);
          // Add all valid segments of main table which needs to be loaded again
          mainTableSegmentList.addAll(validMainTableSegmentList);
        }
        segmentMapping.put(relationIdentifier.getDatabaseName() + CarbonCommonConstants.POINT
            + relationIdentifier.getTableName(), mainTableSegmentList);
      }
    }
    // Remove invalid index segments
    if (!invalidIndexSegmentList.isEmpty()) {
      for (LoadMetadataDetails loadMetadataDetail : listOfLoadFolderDetails) {
        if (invalidIndexSegmentList.contains(loadMetadataDetail.getLoadName())) {
          loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
        }
      }
    }
    return true;
  }

  /**
   * This method checks if index segment has to be reloaded again or not
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
   * Provide the MV catalog instance
   */
  public MVCatalog createMVCatalog() {
    return null;
  }

  public abstract IndexFactory getIndexFactory();

  public abstract boolean supportRebuild();

  public abstract boolean rebuildInternal(String newLoadName, Map<String, List<String>> segmentMap,
      CarbonTable carbonTable);
}
