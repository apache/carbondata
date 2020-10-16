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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.index.dev.IndexFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * It maintains all the Indexes in it.
 */
@InterfaceAudience.Internal
public final class IndexStoreManager {

  private static IndexStoreManager instance = new IndexStoreManager();

  public Map<String, List<TableIndex>> getTableIndexForAllTables() {
    return allIndexes;
  }

  /**
   * Contains the list of indexes for each table.
   */
  private Map<String, List<TableIndex>> allIndexes = new ConcurrentHashMap<>();

  /**
   * Contains the table name to the table path mapping.
   */
  private Map<String, String> tablePathMap = new ConcurrentHashMap<>();

  private Map<String, TableSegmentRefresher> segmentRefreshMap = new ConcurrentHashMap<>();

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(IndexStoreManager.class.getName());

  private IndexStoreManager() {

  }

  /**
   * Collect's Coarse grain and Fine grain indexes on a table
   *
   * @return
   */
  public List<TableIndex> getAllCGAndFGIndexes(CarbonTable carbonTable) throws IOException {
    List<TableIndex> indexes = new ArrayList<>();
    // get bloom indexes and lucene indexes
    for (Map.Entry<String, Map<String, Map<String, String>>> providerEntry : carbonTable
        .getIndexesMap().entrySet()) {
      for (Map.Entry<String, Map<String, String>> indexEntry : providerEntry.getValue()
          .entrySet()) {
        IndexSchema indexSchema = new IndexSchema(indexEntry.getKey(),
            indexEntry.getValue().get(CarbonCommonConstants.INDEX_PROVIDER));
        indexSchema.setProperties(indexEntry.getValue());
        indexes.add(getIndex(carbonTable, indexSchema));
      }
    }
    return indexes;
  }

  /**
   * It gives the default index of the table. Default index of any table is BlockletIndex
   *
   * @param table
   * @return
   */
  public TableIndex getDefaultIndex(CarbonTable table) {
    return getIndex(table, BlockletIndexFactory.INDEX_SCHEMA);
  }

  /**
   * Get the index for reading data.
   */
  public TableIndex getIndex(CarbonTable table, IndexSchema indexSchema) {
    String tableId =
        table.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableId();
    List<TableIndex> tableIndices = allIndexes.get(table.getTableId());
    if (tableIndices == null && !table.isTransactionalTable()) {
      String keyUsingTablePath = getKeyUsingTablePath(table.getTablePath());
      if (keyUsingTablePath != null) {
        tableId = keyUsingTablePath;
        tableIndices = allIndexes.get(tableId);
      }
    }
    // in case of file format or sdk, when table is dropped or schema is changed the indexes are
    // not cleared, they need to be cleared by using API, so compare the columns, if not same, clear
    // the indexes on that table
    if (allIndexes.size() > 0 && !CollectionUtils.isEmpty(allIndexes.get(tableId))
        && !allIndexes.get(tableId).get(0).getTable().getTableInfo().getFactTable()
        .getListOfColumns().equals(table.getTableInfo().getFactTable().getListOfColumns())) {
      clearIndex(tableId);
      tableIndices = null;
    }
    TableIndex index = null;
    if (tableIndices != null) {
      index = getTableIndex(indexSchema.getIndexName(), tableIndices);
    }
    if (index == null) {
      synchronized (tableId.intern()) {
        tableIndices = allIndexes.get(tableId);
        if (tableIndices != null) {
          index = getTableIndex(indexSchema.getIndexName(), tableIndices);
        }
        if (index == null) {
          try {
            index = createAndRegisterIndex(table, indexSchema);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    if (index == null) {
      throw new RuntimeException("Index does not exist");
    }
    // This is done to handle the scenario of stale cache because of which schema mismatch
    // exception can be thrown. Scenario: In case of carbondata used through FileFormat API,
    // once a table is dropped and recreated with the same name again then because the Index
    // contains the stale carbon table schema mismatch exception is thrown. To avoid such scenarios
    // it is always better to update the carbon table object retrieved
    index.getIndexFactory().setCarbonTable(table);
    return index;
  }

  private String getKeyUsingTablePath(String tablePath) {
    if (tablePath != null) {
      // Try get using table path
      for (Map.Entry<String, String> entry : tablePathMap.entrySet()) {
        if (new Path(entry.getValue()).equals(new Path(tablePath))) {
          return entry.getKey();
        }
      }
    }
    return null;
  }

  /**
   * Return a new Index instance and registered in the store manager.
   * The Index is created using Index name, Index factory class and table identifier.
   */
  public IndexFactory getIndexFactoryClass(CarbonTable table, IndexSchema indexSchema)
      throws MalformedIndexCommandException {
    try {
      // try to create Index by reflection to test whether it is a valid IndexFactory class
      return (IndexFactory)
          Class.forName(indexSchema.getProviderName()).getConstructors()[0]
              .newInstance(table, indexSchema);
    } catch (ClassNotFoundException e) {
      // try to create IndexClassProvider instance by taking providerName as short name
      return IndexRegistry.getIndexFactoryByShortName(table, indexSchema);
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to get Index factory for'" + indexSchema.getProviderName() + "'", e);
    }
  }

  /**
   * registered in the store manager.
   * The Index is created using Index name, Index factory class and table identifier.
   */
  private TableIndex createAndRegisterIndex(CarbonTable table,
      IndexSchema indexSchema) throws MalformedIndexCommandException {
    IndexFactory indexFactory = getIndexFactoryClass(table, indexSchema);
    return registerIndex(table, indexSchema, indexFactory);
  }

  public TableIndex registerIndex(CarbonTable table,
      IndexSchema indexSchema,  IndexFactory indexFactory) {
    String tableUniqueName = table.getCarbonTableIdentifier().getTableUniqueName();
    // Just update the segmentRefreshMap with the table if not added.
    getTableSegmentRefresher(table);
    List<TableIndex> tableIndices = allIndexes.get(table.getTableId());
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(table.getTablePath());
      if (keyUsingTablePath != null) {
        tableUniqueName = keyUsingTablePath;
        tableIndices = allIndexes.get(table.getTableId());
      }
    }
    if (tableIndices == null) {
      tableIndices = new ArrayList<>();
    }

    BlockletDetailsFetcher blockletDetailsFetcher;
    SegmentPropertiesFetcher segmentPropertiesFetcher = null;
    if (indexFactory instanceof BlockletDetailsFetcher) {
      blockletDetailsFetcher = (BlockletDetailsFetcher) indexFactory;
    } else {
      blockletDetailsFetcher = getBlockletDetailsFetcher(table);
    }
    segmentPropertiesFetcher = (SegmentPropertiesFetcher) blockletDetailsFetcher;
    TableIndex index = new TableIndex(table, indexSchema, indexFactory, blockletDetailsFetcher,
        segmentPropertiesFetcher);

    tableIndices.add(index);
    allIndexes.put(table.getTableId(), tableIndices);
    tablePathMap.put(table.getTableId(), table.getTablePath());
    return index;
  }

  private TableIndex getTableIndex(String indexName, List<TableIndex> tableIndices) {
    TableIndex index = null;
    for (TableIndex tableIndex : tableIndices) {
      if (tableIndex.getIndexSchema().getIndexName().equals(indexName)) {
        index = tableIndex;
        break;
      }
    }
    return index;
  }

  /**
   * Clear the invalid segments from all the indexes of the table
   *
   * @param carbonTable table for which the operation has to be performed.
   * @param segments segments which have to be cleared from cache.
   */
  public void clearInvalidSegments(CarbonTable carbonTable, List<String> segments)
      throws IOException {
    getDefaultIndex(carbonTable).clear(segments);
    List<TableIndex> indexes = getAllCGAndFGIndexes(carbonTable);
    for (TableIndex index: indexes) {
      index.clear(segments);
    }

  }

  public List<String> getSegmentsToBeRefreshed(CarbonTable carbonTable,
      List<Segment> filteredSegmentToAccess) {
    List<String> toBeCleanedSegments = new ArrayList<>();
    for (Segment filteredSegment : filteredSegmentToAccess) {
      boolean refreshNeeded = getTableSegmentRefresher(carbonTable).isRefreshNeeded(filteredSegment,
          SegmentUpdateStatusManager
              .getInvalidTimestampRange(filteredSegment.getLoadMetadataDetails()));
      if (refreshNeeded) {
        toBeCleanedSegments.add(filteredSegment.getSegmentNo());
      }
    }
    return toBeCleanedSegments;
  }

  public void refreshSegmentCacheIfRequired(CarbonTable carbonTable,
      SegmentUpdateStatusManager updateStatusManager, List<Segment> filteredSegmentToAccess)
      throws IOException {
    List<String> toBeCleanedSegments =
        getSegmentsToBeRefreshed(carbonTable, filteredSegmentToAccess);
    if (toBeCleanedSegments.size() > 0) {
      clearInvalidSegments(carbonTable, toBeCleanedSegments);
    }
  }

  /**
   * Clear the index/indexes of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearIndex(AbsoluteTableIdentifier identifier) {
    CarbonTable carbonTable = getCarbonTable(identifier);
    boolean launchJob = false;
    try {
      // launchJob will be true if either the table has a CGIndex or index server is enabled for
      // the specified table.
      launchJob = hasCGIndex(carbonTable) ||
          CarbonProperties.getInstance().isDistributedPruningEnabled(identifier.getDatabaseName(),
              identifier.getTableName());
    } catch (IOException e) {
      LOGGER.warn("Unable to launch job to clear indexes.", e);
    }
    clearIndexCache(identifier, launchJob);
  }

  /**
   * Clear the index/indexes of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearIndexCache(AbsoluteTableIdentifier identifier, boolean clearInAllWorkers) {
    String tableId = identifier.getCarbonTableIdentifier().getTableId();
    if (clearInAllWorkers) {
      // carbon table need to lookup only if launch job is set.
      CarbonTable carbonTable = getCarbonTable(identifier);
      if (null != carbonTable) {
        String jobClassName;
        if (CarbonProperties.getInstance()
            .isDistributedPruningEnabled(identifier.getDatabaseName(), identifier.getTableName())) {
          jobClassName = IndexUtil.DISTRIBUTED_JOB_NAME;
        } else {
          jobClassName = IndexUtil.EMBEDDED_JOB_NAME;
        }
        try {
          IndexUtil.executeClearIndexJob(carbonTable, jobClassName);
        } catch (IOException e) {
          LOGGER.error("clear index job failed", e);
          // ignoring the exception
        }
      }
    } else {
      // remove carbon table from meta cache if launchJob is false as this would be called in
      // executor side.
      CarbonMetadata.getInstance()
          .removeTable(identifier.getDatabaseName(), identifier.getTableName());
    }
    List<TableIndex> tableIndices =
        allIndexes.get(identifier.getCarbonTableIdentifier().getTableId());
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(identifier.getTablePath());
      if (keyUsingTablePath != null) {
        tableId = keyUsingTablePath;
      }
    }
    segmentRefreshMap.remove(tableId);
    clearIndex(tableId);
    allIndexes.remove(tableId);
    tablePathMap.remove(tableId);
  }

  /**
   * This method returns the carbonTable from identifier
   * @param identifier
   * @return
   */
  public CarbonTable getCarbonTable(AbsoluteTableIdentifier identifier) {
    CarbonTable carbonTable = null;
    carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(identifier.getDatabaseName(), identifier.getTableName());
    if (carbonTable == null) {
      try {
        carbonTable = CarbonTable
            .buildFromTablePath(identifier.getTableName(), identifier.getDatabaseName(),
                identifier.getTablePath(), identifier.getCarbonTableIdentifier().getTableId());
      } catch (IOException e) {
        LOGGER.warn("failed to get carbon table from table Path" + e.getMessage(), e);
        // ignoring exception
      }
    }
    return carbonTable;
  }

  /**
   * this methods clears the index of table from memory
   */
  public void clearIndex(String tableId) {
    List<TableIndex> tableIndices = allIndexes.get(tableId);
    if (tableIndices != null) {
      for (TableIndex tableIndex : tableIndices) {
        if (tableIndex != null) {
          // clear the segmentMap in BlockletDetailsFetcher,else the Segment will remain in executor
          // and the query fails as we will check whether the blocklet contains in the index or not
          tableIndex.getBlockletDetailsFetcher().clear();
          tableIndex.clear();
        }
      }
    }
    allIndexes.remove(tableId);
    tablePathMap.remove(tableId);
  }




  /**
   * Clear the index/indexes of a table from memory and disk
   */
  public void deleteIndex(CarbonTable carbonTable, String indexName) {
    if (carbonTable == null) {
      // If carbon table is null then it means table is already deleted, therefore return without
      // doing any further changes.
      return;
    }
    String tableId = carbonTable.getTableId();
    if (CarbonProperties.getInstance()
        .isDistributedPruningEnabled(carbonTable.getDatabaseName(), carbonTable.getTableName())) {
      try {
        IndexUtil
            .executeClearIndexJob(carbonTable, IndexUtil.DISTRIBUTED_JOB_NAME, indexName);
      } catch (IOException e) {
        LOGGER.error("clear index job failed", e);
        // ignoring the exception
      }
    } else {
      List<TableIndex> tableIndices = allIndexes.get(tableId);
      if (tableIndices != null) {
        int i = 0;
        for (TableIndex tableIndex : tableIndices) {
          if (tableIndex != null && indexName
              .equalsIgnoreCase(tableIndex.getIndexSchema().getIndexName())) {
            try {
              IndexUtil
                  .executeClearIndexJob(carbonTable, IndexUtil.EMBEDDED_JOB_NAME, indexName);
              tableIndex.clear();
            } catch (IOException e) {
              LOGGER.error("clear index job failed", e);
              // ignoring the exception
            }
            tableIndex.deleteIndexData();
            tableIndices.remove(i);
            break;
          }
          i++;
        }
        allIndexes.put(tableId, tableIndices);
      }
    }
  }

  /**
   * Get the blocklet index factory to get the detail information of blocklets
   *
   * @param table
   * @return
   */
  private BlockletDetailsFetcher getBlockletDetailsFetcher(CarbonTable table) {
    TableIndex index = getIndex(table, BlockletIndexFactory.INDEX_SCHEMA);
    return (BlockletDetailsFetcher) index.getIndexFactory();
  }

  /**
   * Returns the singleton instance
   *
   * @return
   */
  public static IndexStoreManager getInstance() {
    return instance;
  }

  /**
   * Get the TableSegmentRefresher for the table. If not existed then add one and return.
   */
  public TableSegmentRefresher getTableSegmentRefresher(CarbonTable table) {
    String tableId = table.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableId();
    if (segmentRefreshMap.get(tableId) == null) {
      segmentRefreshMap.put(tableId, new TableSegmentRefresher(table));
    }
    return segmentRefreshMap.get(tableId);
  }

  /**
   * Keep track of the segment refresh time.
   */
  public static class TableSegmentRefresher {

    // This map stores the latest segment refresh time.So in case of update/delete we check the
    // time against this map.
    private Map<String, SegmentRefreshInfo> segmentRefreshTime = new HashMap<>();

    // This map keeps the manual refresh entries from users. It is mainly used for partition
    // altering.
    private Map<String, Boolean> manualSegmentRefresh = new HashMap<>();

    TableSegmentRefresher(CarbonTable table) {
      SegmentStatusManager segmentStatusManager =
          new SegmentStatusManager(table.getAbsoluteTableIdentifier());
      List<Segment> validSegments;
      try {
        validSegments = segmentStatusManager.getValidAndInvalidSegments().getValidSegments();
      } catch (IOException e) {
        LOGGER.error("Error while getting the valid segments.", e);
        throw new RuntimeException(e);
      }
      for (Segment segment : validSegments) {
        UpdateVO updateVO =
            SegmentUpdateStatusManager.getInvalidTimestampRange(segment.getLoadMetadataDetails());
        SegmentRefreshInfo segmentRefreshInfo;
        String segmentFileName = segment.getSegmentFileName();
        if ((updateVO != null && updateVO.getLatestUpdateTimestamp() != null)
            || segmentFileName != null) {
          // Do not use getLastModifiedTime API on segment file carbon file object as it will
          // slow down operation in Object stores like S3. Now the segment file is always written
          // for operations which was overwriting earlier, so this timestamp can be checked always
          // to check whether to refresh the cache or not.
          long segmentFileTimeStamp = Long.parseLong(segmentFileName
              .substring(segmentFileName.indexOf(CarbonCommonConstants.UNDERSCORE) + 1,
                  segmentFileName.lastIndexOf(CarbonCommonConstants.POINT)));
          segmentRefreshInfo =
              new SegmentRefreshInfo(updateVO.getLatestUpdateTimestamp(), 0, segmentFileTimeStamp);
        } else {
          segmentRefreshInfo = new SegmentRefreshInfo(0L, 0, 0L);
        }
        segmentRefreshTime.put(segment.getSegmentNo(), segmentRefreshInfo);
      }
    }

    public boolean isRefreshNeeded(Segment seg, UpdateVO updateVo) {
      SegmentRefreshInfo segmentRefreshInfo =
          seg.getSegmentRefreshInfo(updateVo);
      String segmentId = seg.getSegmentNo();
      if (segmentRefreshInfo.getSegmentUpdatedTimestamp() == null
          && segmentRefreshInfo.getSegmentFileTimestamp() == 0) {
        return false;
      }

      if (segmentRefreshTime.get(segmentId) == null) {
        if (segmentRefreshInfo.getSegmentUpdatedTimestamp() != null
            && segmentRefreshInfo.getSegmentUpdatedTimestamp() != 0) {
          segmentRefreshTime.put(segmentId, segmentRefreshInfo);
          return true;
        }
        if (segmentRefreshInfo.getSegmentFileTimestamp() != 0) {
          segmentRefreshTime.put(segmentId, segmentRefreshInfo);
          return true;
        }
      }

      if (manualSegmentRefresh.get(segmentId) != null && manualSegmentRefresh.get(segmentId)) {
        manualSegmentRefresh.put(segmentId, false);
        return true;
      }

      boolean isRefresh = segmentRefreshInfo.compare(segmentRefreshTime.get(segmentId));
      if (isRefresh) {
        segmentRefreshTime.remove(segmentId);
      }
      return isRefresh;
    }
  }

  public synchronized void clearInvalidIndex(CarbonTable carbonTable, List<String> segmentNos,
      String indexToClear) throws IOException {
    List<TableIndex> indexes = getAllCGAndFGIndexes(carbonTable);
    List<TableIndex> remainingIndexes = new ArrayList<>();
    if (StringUtils.isNotEmpty(indexToClear)) {
      for (TableIndex tableIndex : indexes) {
        if (indexToClear.equalsIgnoreCase(tableIndex.getIndexSchema().getIndexName())) {
          for (String segment : segmentNos) {
            tableIndex.deleteSegmentIndexData(segment);
          }
          tableIndex.clear();
        } else {
          remainingIndexes.add(tableIndex);
        }
      }
      allIndexes.put(carbonTable.getTableId(), remainingIndexes);
    } else {
      clearIndex(carbonTable.getTableId());
      // clear the segment properties cache from executor
      SegmentPropertiesAndSchemaHolder.getInstance()
          .invalidate(carbonTable.getAbsoluteTableIdentifier());
    }
  }

  private boolean hasCGIndex(CarbonTable carbonTable) throws IOException {
    // In case of spark file format flow, carbon table will be null
    if (null == carbonTable) {
      return false;
    }
    for (TableIndex tableIndex : carbonTable.getAllVisibleIndexes()) {
      if (tableIndex.getIndexFactory().getIndexLevel().equals(IndexLevel.CG)) {
        return true;
      }
    }
    return false;
  }

}
