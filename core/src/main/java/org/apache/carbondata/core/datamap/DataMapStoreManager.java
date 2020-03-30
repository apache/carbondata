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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedIndexCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.IndexFactory;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.index.CarbonIndexProvider;
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaFactory;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import static org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.MV;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * It maintains all the DataMaps in it.
 */
@InterfaceAudience.Internal
public final class DataMapStoreManager {

  private static DataMapStoreManager instance = new DataMapStoreManager();

  public Map<String, List<TableIndex>> getTableIndexForAllTables() {
    return allIndexes;
  }

  /**
   * Contains the list of indexes for each table.
   */
  private Map<String, List<TableIndex>> allIndexes = new ConcurrentHashMap<>();

  /**
   * Contains the table name to the tablepath mapping.
   */
  private Map<String, String> tablePathMap = new ConcurrentHashMap<>();

  /**
   * Contains the mv catalog for each mv provider.
   */
  private Map<String, MVCatalog> mvCatalogMap = null;

  private Map<String, TableSegmentRefresher> segmentRefreshMap = new ConcurrentHashMap<>();

  private DataMapSchemaStorageProvider provider =
      DataMapSchemaFactory.getDataMapSchemaStorageProvider();

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataMapStoreManager.class.getName());

  private final Object lockObject = new Object();

  private DataMapStoreManager() {

  }

  /**
   * It gives all indexes except the default index and secondary index.
   * Collect's Coarse grain and Fine grain indexes on a table
   *
   * @return
   */
  public List<TableIndex> getAllIndexes(CarbonTable carbonTable) throws IOException {
    String indexMeta = carbonTable.getTableInfo().getFactTable().getTableProperties()
        .get(carbonTable.getCarbonTableIdentifier().getTableId());
    IndexMetadata indexMetadata = IndexMetadata.deserialize(indexMeta);
    List<TableIndex> indexes = new ArrayList<>();
    if (null != indexMetadata) {
      // get bloom indexes and lucene indexes
      for (Map.Entry<String, Map<String, Map<String, String>>> providerEntry : indexMetadata
          .getIndexesMap().entrySet()) {
        for (Map.Entry<String, Map<String, String>> indexEntry : providerEntry.getValue()
            .entrySet()) {
          if (!indexEntry.getValue().get(CarbonCommonConstants.INDEX_PROVIDER)
              .equalsIgnoreCase(CarbonIndexProvider.SI.getIndexProviderName())) {
            DataMapSchema indexSchema = new DataMapSchema(indexEntry.getKey(),
                indexEntry.getValue().get(CarbonCommonConstants.INDEX_PROVIDER));
            indexSchema.setProperties(indexEntry.getValue());
            indexes.add(getIndex(carbonTable, indexSchema));
          }
        }
      }
    }
    return indexes;
  }

  /**
   * It gives all datamap schemas of a given table.
   *
   */
  public List<DataMapSchema> getDataMapSchemasOfTable(CarbonTable carbonTable) throws IOException {
    return provider.retrieveSchemas(carbonTable);
  }

  /**
   * It gives all datamap schemas from store.
   */
  public List<DataMapSchema> getAllDataMapSchemas() throws IOException {
    return provider.retrieveAllSchemas();
  }

  public DataMapSchema getDataMapSchema(String dataMapName)
      throws NoSuchDataMapException, IOException {
    return provider.retrieveSchema(dataMapName);
  }

  /**
   * Saves the datamap schema to storage
   * @param dataMapSchema
   */
  public void saveDataMapSchema(DataMapSchema dataMapSchema) throws IOException {
    provider.saveSchema(dataMapSchema);
  }

  /**
   * Drops the datamap schema from storage
   * @param dataMapName
   */
  public void dropDataMapSchema(String dataMapName) throws IOException {
    provider.dropSchema(dataMapName);
  }

  /**
   * Update the datamap schema after table rename
   * This should be invoked after changing table name
   * @param dataMapSchemaList
   * @param newTableName
   */
  public void updateDataMapSchema(List<DataMapSchema> dataMapSchemaList,
      String newTableName) throws IOException {
    List<DataMapSchema> newDataMapSchemas = new ArrayList<>();
    for (DataMapSchema dataMapSchema : dataMapSchemaList) {
      RelationIdentifier relationIdentifier = dataMapSchema.getRelationIdentifier();
      String dataBaseName =  relationIdentifier.getDatabaseName();
      String tableId = relationIdentifier.getTableId();
      String providerName = dataMapSchema.getProviderName();
      // if the mv datamap,not be modified the relationIdentifier
      if (!providerName.equalsIgnoreCase(MV.toString())) {
        RelationIdentifier newRelationIdentifier = new RelationIdentifier(dataBaseName,
            newTableName, tableId);
        dataMapSchema.setRelationIdentifier(newRelationIdentifier);
      }
      List<RelationIdentifier> newParentTables = new ArrayList<>();
      List<RelationIdentifier> parentTables = dataMapSchema.getParentTables();
      for (RelationIdentifier identifier : parentTables) {
        RelationIdentifier newParentTableIdentifier = new RelationIdentifier(
            identifier.getDatabaseName(), newTableName, identifier.getTableId());
        newParentTables.add(newParentTableIdentifier);
      }
      dataMapSchema.setParentTables(newParentTables);
      newDataMapSchemas.add(dataMapSchema);
      // frist drop old schema
      String dataMapName = dataMapSchema.getDataMapName();
      dropDataMapSchema(dataMapName);
    }
    // save new datamap schema to storage
    for (DataMapSchema newDataMapSchema : newDataMapSchemas) {
      saveDataMapSchema(newDataMapSchema);
    }
  }

  /**
   * Register datamap catalog for the datamap provider
   * @param dataMapProvider
   * @param dataMapSchema
   */
  public synchronized void registerMVCatalog(DataMapProvider dataMapProvider,
      DataMapSchema dataMapSchema, boolean clearCatalogs) throws IOException {
    // this check is added to check if when registering the datamapCatalog, if the catalog map has
    // datasets with old session, then need to clear and reload the map, else error can be thrown
    // if the databases are different in both the sessions
    if (clearCatalogs) {
      mvCatalogMap = null;
    }
    initializeMVCatalogs(dataMapProvider);
    String name = dataMapSchema.getProviderName().toLowerCase();
    MVCatalog mvCatalog = mvCatalogMap.get(name);
    if (mvCatalog == null) {
      mvCatalog = dataMapProvider.createDataMapCatalog();
      // If MVDataMapProvider, then createDataMapCatalog will return summaryDatasetCatalog
      // instance, which needs to be added to dataMapCatalogs.
      // For other datamaps, createDataMapCatalog will return null, so no need to register
      if (mvCatalog != null) {
        mvCatalogMap.put(name, mvCatalog);
        mvCatalog.registerSchema(dataMapSchema);
      }
    } else {
      if (!mvCatalog.isMVExists(dataMapSchema.getDataMapName())) {
        mvCatalog.registerSchema(dataMapSchema);
      }
    }
  }

  /**
   * Unregister datamap catalog.
   * @param dataMapSchema
   */
  public synchronized void unRegisterDataMapCatalog(DataMapSchema dataMapSchema) {
    if (mvCatalogMap == null) {
      return;
    }
    String name = dataMapSchema.getProviderName().toLowerCase();
    MVCatalog MVCatalog = mvCatalogMap.get(name);
    if (MVCatalog != null) {
      MVCatalog.unregisterSchema(dataMapSchema.getDataMapName());
    }
  }

  /**
   * Get the datamap catalog for provider.
   * @param providerName
   * @return
   */
  public synchronized MVCatalog getMVCatalog(
      DataMapProvider dataMapProvider,
      String providerName,
      boolean clearCatalogs) throws IOException {
    // This method removes the datamapCatalog for the corresponding provider if the session gets
    // refreshed or updated
    if (clearCatalogs) {
      mvCatalogMap = null;
    }
    initializeMVCatalogs(dataMapProvider);
    return mvCatalogMap.get(providerName.toLowerCase());
  }

  /**
   * Initialize by reading all datamaps from store and re register it
   * @param dataMapProvider
   */
  private synchronized void initializeMVCatalogs(DataMapProvider dataMapProvider)
      throws IOException {
    if (mvCatalogMap == null) {
      mvCatalogMap = new ConcurrentHashMap<>();
      List<DataMapSchema> dataMapSchemas = getAllDataMapSchemas();
      for (DataMapSchema schema : dataMapSchemas) {
        if (schema.getProviderName()
            .equalsIgnoreCase(dataMapProvider.getDataMapSchema().getProviderName())) {
          MVCatalog MVCatalog =
              mvCatalogMap.get(schema.getProviderName().toLowerCase());
          if (MVCatalog == null) {
            MVCatalog = dataMapProvider.createDataMapCatalog();
            if (null == MVCatalog) {
              throw new RuntimeException("Internal Error.");
            }
            mvCatalogMap.put(schema.getProviderName().toLowerCase(), MVCatalog);
          }
          try {
            MVCatalog.registerSchema(schema);
          } catch (Exception e) {
            // Ignore the schema
            LOGGER.error("Error while registering schema", e);
          }
        }
      }
    }
  }

  /**
   * It gives the default datamap of the table. Default datamap of any table is BlockletIndex
   *
   * @param table
   * @return
   */
  public TableIndex getDefaultIndex(CarbonTable table) {
    return getIndex(table, BlockletIndexFactory.DATA_MAP_SCHEMA);
  }

  /**
   * Get the datamap for reading data.
   */
  public TableIndex getIndex(CarbonTable table, DataMapSchema dataMapSchema) {
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
    // in case of fileformat or sdk, when table is dropped or schema is changed the datamaps are
    // not cleared, they need to be cleared by using API, so compare the columns, if not same, clear
    // the datamaps on that table
    if (allIndexes.size() > 0 && !CollectionUtils.isEmpty(allIndexes.get(tableId))
        && !allIndexes.get(tableId).get(0).getTable().getTableInfo().getFactTable()
        .getListOfColumns().equals(table.getTableInfo().getFactTable().getListOfColumns())) {
      clearIndex(tableId);
      tableIndices = null;
    }
    TableIndex dataMap = null;
    if (tableIndices != null) {
      dataMap = getTableIndex(dataMapSchema.getDataMapName(), tableIndices);
    }
    if (dataMap == null) {
      synchronized (tableId.intern()) {
        tableIndices = allIndexes.get(tableId);
        if (tableIndices != null) {
          dataMap = getTableIndex(dataMapSchema.getDataMapName(), tableIndices);
        }
        if (dataMap == null) {
          try {
            dataMap = createAndRegisterIndex(table, dataMapSchema);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    if (dataMap == null) {
      throw new RuntimeException("Datamap does not exist");
    }
    // This is done to handle the scenario of stale cache because of which schema mismatch
    // exception can be thrown. Scenario: In case of carbondata used through FileFormat API,
    // once a table is dropped and recreated with the same name again then because the dataMap
    // contains the stale carbon table schema mismatch exception is thrown. To avoid such scenarios
    // it is always better to update the carbon table object retrieved
    dataMap.getIndexFactory().setCarbonTable(table);
    return dataMap;
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
   * Return a new datamap instance and registered in the store manager.
   * The datamap is created using datamap name, datamap factory class and table identifier.
   */
  public IndexFactory getDataMapFactoryClass(CarbonTable table, DataMapSchema dataMapSchema)
      throws MalformedIndexCommandException {
    try {
      // try to create datamap by reflection to test whether it is a valid IndexFactory class
      return (IndexFactory)
          Class.forName(dataMapSchema.getProviderName()).getConstructors()[0]
              .newInstance(table, dataMapSchema);
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      return IndexRegistry.getDataMapFactoryByShortName(table, dataMapSchema);
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to get Index factory for'" + dataMapSchema.getProviderName() + "'", e);
    }
  }

  /**
   * registered in the store manager.
   * The datamap is created using datamap name, datamap factory class and table identifier.
   */
  private TableIndex createAndRegisterIndex(CarbonTable table,
      DataMapSchema dataMapSchema) throws MalformedIndexCommandException {
    IndexFactory indexFactory = getDataMapFactoryClass(table, dataMapSchema);
    return registerIndex(table, dataMapSchema, indexFactory);
  }

  public TableIndex registerIndex(CarbonTable table,
      DataMapSchema dataMapSchema,  IndexFactory indexFactory) {
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
    TableIndex dataMap = new TableIndex(table,
        dataMapSchema, indexFactory, blockletDetailsFetcher, segmentPropertiesFetcher);

    tableIndices.add(dataMap);
    allIndexes.put(table.getTableId(), tableIndices);
    tablePathMap.put(table.getTableId(), table.getTablePath());
    return dataMap;
  }

  private TableIndex getTableIndex(String dataMapName, List<TableIndex> tableIndices) {
    TableIndex dataMap = null;
    for (TableIndex tableIndex : tableIndices) {
      if (tableIndex.getDataMapSchema().getDataMapName().equals(dataMapName)) {
        dataMap = tableIndex;
        break;
      }
    }
    return dataMap;
  }

  /**
   * Clear the invalid segments from all the datamaps of the table
   *
   * @param carbonTable table for which the operation has to be performed.
   * @param segments segments which have to be cleared from cache.
   */
  public void clearInvalidSegments(CarbonTable carbonTable, List<String> segments)
      throws IOException {
    getDefaultIndex(carbonTable).clear(segments);
    List<TableIndex> indexes = getAllIndexes(carbonTable);
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
   * Clear the datamap/datamaps of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearIndex(AbsoluteTableIdentifier identifier) {
    CarbonTable carbonTable = getCarbonTable(identifier);
    boolean launchJob = false;
    try {
      // launchJob will be true if either the table has a CGDatamap or index server is enabled for
      // the specified table.
      launchJob = hasCGIndex(carbonTable) ||
          CarbonProperties.getInstance().isDistributedPruningEnabled(identifier.getDatabaseName(),
              identifier.getTableName());
    } catch (IOException e) {
      LOGGER.warn("Unable to launch job to clear datamaps.", e);
    }
    clearIndexCache(identifier, launchJob);
  }

  /**
   * Clear the datamap/datamaps of a table from memory
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
          LOGGER.error("clear dataMap job failed", e);
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
   * this methods clears the datamap of table from memory
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
   * Clear the datamap/datamaps of a table from memory and disk
   */
  public void deleteIndex(CarbonTable carbonTable, String dataMapName) {
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
            .executeClearIndexJob(carbonTable, IndexUtil.DISTRIBUTED_JOB_NAME, dataMapName);
      } catch (IOException e) {
        LOGGER.error("clear dataMap job failed", e);
        // ignoring the exception
      }
    } else {
      List<TableIndex> tableIndices = allIndexes.get(tableId);
      if (tableIndices != null) {
        int i = 0;
        for (TableIndex tableIndex : tableIndices) {
          if (tableIndex != null && dataMapName
              .equalsIgnoreCase(tableIndex.getDataMapSchema().getDataMapName())) {
            try {
              IndexUtil
                  .executeClearIndexJob(carbonTable, IndexUtil.EMBEDDED_JOB_NAME, dataMapName);
              tableIndex.clear();
            } catch (IOException e) {
              LOGGER.error("clear dataMap job failed", e);
              // ignoring the exception
            }
            tableIndex.deleteDatamapData();
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
   * is datamap exist
   * @return true if exist, else return false
   */
  public boolean isDataMapExist(String tableId, String dmName) {
    List<TableIndex> tableIndices = allIndexes.get(tableId);
    if (tableIndices != null) {
      for (TableIndex dm : tableIndices) {
        if (dm != null && dmName.equalsIgnoreCase(dm.getDataMapSchema().getDataMapName())) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Get the blocklet datamap factory to get the detail information of blocklets
   *
   * @param table
   * @return
   */
  private BlockletDetailsFetcher getBlockletDetailsFetcher(CarbonTable table) {
    TableIndex index = getIndex(table, BlockletIndexFactory.DATA_MAP_SCHEMA);
    return (BlockletDetailsFetcher) index.getIndexFactory();
  }

  /**
   * Returns the singleton instance
   *
   * @return
   */
  public static DataMapStoreManager getInstance() {
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
        if (updateVO != null && updateVO.getLatestUpdateTimestamp() != null
            || segment.getSegmentFileName() != null) {
          long segmentFileTimeStamp = FileFactory.getCarbonFile(CarbonTablePath
              .getSegmentFilePath(table.getTablePath(), segment.getSegmentFileName()))
              .getLastModifiedTime();
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

    public void refreshSegments(List<String> segmentIds) {
      for (String segmentId : segmentIds) {
        manualSegmentRefresh.put(segmentId, true);
      }
    }

    public boolean isRefreshNeeded(String segmentId) {
      if (manualSegmentRefresh.get(segmentId) != null && manualSegmentRefresh.get(segmentId)) {
        manualSegmentRefresh.put(segmentId, false);
        return true;
      } else {
        return false;
      }
    }
  }

  public synchronized void clearInvalidIndex(CarbonTable carbonTable, List<String> segmentNos,
      String dataMapToClear) throws IOException {
    List<TableIndex> indexes = getAllIndexes(carbonTable);
    List<TableIndex> remainingIndexes = new ArrayList<>();
    if (StringUtils.isNotEmpty(dataMapToClear)) {
      Iterator<TableIndex> dataMapIterator = indexes.iterator();
      while (dataMapIterator.hasNext()) {
        TableIndex tableIndex = dataMapIterator.next();
        if (dataMapToClear.equalsIgnoreCase(tableIndex.getDataMapSchema().getDataMapName())) {
          for (String segment: segmentNos) {
            tableIndex.deleteSegmentDatamapData(segment);
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
    if (null == carbonTable) {
      return false;
    }
    for (TableIndex tableIndex : carbonTable.getAllVisibleIndexes()) {
      if (tableIndex.getIndexFactory().getDataMapLevel().equals(IndexLevel.CG)) {
        return true;
      }
    }
    return false;
  }

}
