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
import java.util.stream.Collectors;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.exceptions.sql.NoSuchIndexException;
import org.apache.carbondata.common.exceptions.sql.NoSuchMaterializedViewException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.IndexFactory;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.index.IndexLevel;
import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletIndexFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaFactory;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

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

  public Map<String, List<TableIndex>> getIndexes() {
    return indexes;
  }

  /**
   * Contains the list of datamaps for each table.
   */
  private Map<String, List<TableIndex>> indexes = new ConcurrentHashMap<>();

  /**
   * Contains the table name to the tablepath mapping.
   */
  private Map<String, String> tablePathMap = new ConcurrentHashMap<>();

  /**
   * Contains the MV catalog
   */
  private Map<String, MVCatalog> mvCatalogs = null;

  private Map<String, TableSegmentRefresher> segmentRefreshMap = new ConcurrentHashMap<>();

  private DataMapSchemaStorageProvider provider =
      DataMapSchemaFactory.getDataMapSchemaStorageProvider();

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataMapStoreManager.class.getName());

  private final Object lockObject = new Object();

  private DataMapStoreManager() {

  }

  /**
   * It only gives the visible indexes
   */
  public List<TableIndex> getAllVisibleIndexes(CarbonTable carbonTable) throws IOException {
    CarbonSessionInfo sessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    List<TableIndex> allIndexes = getAllIndex(carbonTable);
    Iterator<TableIndex> dataMapIterator = allIndexes.iterator();
    while (dataMapIterator.hasNext()) {
      TableIndex dataMap = dataMapIterator.next();
      String dbName = carbonTable.getDatabaseName();
      String tableName = carbonTable.getTableName();
      String dmName = dataMap.getDataMapSchema().getDataMapName();
      // TODO: need support get the visible status of datamap without sessionInfo in the future
      if (sessionInfo != null) {
        boolean isDmVisible = sessionInfo.getSessionParams().getProperty(
            String.format("%s%s.%s.%s", CarbonCommonConstants.CARBON_DATAMAP_VISIBLE,
                dbName, tableName, dmName), "true").trim().equalsIgnoreCase("true");
        if (!isDmVisible) {
          LOGGER.warn(String.format("Ignore invisible datamap %s on table %s.%s",
              dmName, dbName, tableName));
          dataMapIterator.remove();
        }
      } else {
        String message = "Carbon session info is null";
        LOGGER.info(message);
      }
    }
    return allIndexes;
  }

  /**
   * It gives all indexes except the default index.
   *
   * @return
   */
  public List<TableIndex> getAllIndex(CarbonTable carbonTable) throws IOException {
    List<DataMapSchema> dataMapSchemas = getDataMapSchemasOfTable(carbonTable);
    List<TableIndex> indexes = new ArrayList<>();
    if (dataMapSchemas != null) {
      for (DataMapSchema dataMapSchema : dataMapSchemas) {
        RelationIdentifier identifier = dataMapSchema.getParentTables().get(0);
        if (dataMapSchema.isIndexDataMap() && identifier.getTableId()
            .equals(carbonTable.getTableId())) {
          indexes.add(getIndex(carbonTable, dataMapSchema));
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
   * It gives all index schemas from store.
   */
  public List<DataMapSchema> getAllIndexSchemas() throws IOException {
    return provider.retrieveAllSchemas().stream()
        .filter(schema -> schema.isIndexDataMap())
        .collect(Collectors.toList());
  }

  public List<DataMapSchema> getAllMVSchemas() throws IOException {
    return provider.retrieveAllSchemas().stream()
        .filter(schema -> !schema.isIndexDataMap())
        .collect(Collectors.toList());
  }

  public DataMapSchema getIndexSchema(String indexName)
      throws NoSuchIndexException, IOException {
    try {
      DataMapSchema schema = provider.retrieveSchema(indexName);
      if (!schema.isIndexDataMap()) {
        throw new NoSuchIndexException(indexName);
      }
      return schema;
    } catch (NoSuchDataMapException e) {
      throw new NoSuchIndexException(indexName);
    }
  }

  public DataMapSchema getMVSchema(String mvName)
      throws NoSuchMaterializedViewException, IOException {
    try {
      DataMapSchema schema = provider.retrieveSchema(mvName);
      if (schema == null || schema.isIndexDataMap()) {
        throw new NoSuchMaterializedViewException(mvName);
      }
      return schema;
    } catch (NoSuchDataMapException e) {
      throw new NoSuchMaterializedViewException(mvName);
    }
  }

  public List<DataMapSchema> getAllMVSchemasOfTable(CarbonTable table) throws IOException {
    return provider.retrieveAllSchemas().stream()
        .filter(schema -> !schema.isIndexDataMap() &&
            schema.getRelationIdentifier().getTableId().equals(table.getTableId()))
        .collect(Collectors.toList());
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
      // if the mvvnot be modified the relationIdentifier
      if (!providerName.equalsIgnoreCase(DataMapClassProvider.MV.toString())) {
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
   * Register MV catalog for the provider
   * @param dataMapProvider
   * @param dataMapSchema
   */
  public void registerMVCatalog(
      DataMapProvider dataMapProvider,
      DataMapSchema dataMapSchema) throws IOException {
    synchronized (lockObject) {
      initializeMVCatalogs(dataMapProvider);
      String name = dataMapSchema.getProviderName().toLowerCase();
      MVCatalog MVCatalog = mvCatalogs.get(name);
      if (MVCatalog == null) {
        MVCatalog = dataMapProvider.createMVCatalog();
        if (MVCatalog != null) {
          mvCatalogs.put(name, MVCatalog);
          MVCatalog.registerSchema(dataMapSchema);
        }
      } else {
        MVCatalog.registerSchema(dataMapSchema);
      }
    }
  }

  /**
   * Unregister MV catalog.
   * @param dataMapSchema
   */
  public synchronized void unRegisterMVCatalog(DataMapSchema dataMapSchema) {
    if (mvCatalogs == null) {
      return;
    }
    String name = dataMapSchema.getProviderName().toLowerCase();
    MVCatalog MVCatalog = mvCatalogs.get(name);
    if (MVCatalog != null) {
      MVCatalog.unregisterSchema(dataMapSchema.getDataMapName());
    }
  }

  /**
   * Get the MV catalog for provider.
   */
  public synchronized MVCatalog getMVCatalog(DataMapProvider dataMapProvider) throws IOException {
    initializeMVCatalogs(dataMapProvider);
    return mvCatalogs.get(DataMapClassProvider.MV.toString().toLowerCase());
  }

  /**
   * This method removes the MVCatalog for the corresponding provider if the session gets
   * refreshed or updated
   */
  public void clearMVCatalog() {
    mvCatalogs = null;
  }

  /**
   * Initialize by reading all MV from store and re register it
   * @param dataMapProvider
   */
  private void initializeMVCatalogs(DataMapProvider dataMapProvider) throws IOException {
    if (mvCatalogs == null) {
      mvCatalogs = new ConcurrentHashMap<>();
      List<DataMapSchema> dataMapSchemas = getAllMVSchemas();
      for (DataMapSchema schema : dataMapSchemas) {
        if (schema.getProviderName()
            .equalsIgnoreCase(dataMapProvider.getDataMapSchema().getProviderName())) {
          MVCatalog MVCatalog =
              mvCatalogs.get(schema.getProviderName().toLowerCase());
          if (MVCatalog == null) {
            MVCatalog = dataMapProvider.createMVCatalog();
            if (null == MVCatalog) {
              throw new RuntimeException("Internal Error.");
            }
            mvCatalogs.put(schema.getProviderName().toLowerCase(), MVCatalog);
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
   * It gives the default index of the table. Default index of any table is BlockletIndex
   *
   * @param table
   * @return
   */
  public TableIndex getDefaultIndex(CarbonTable table) {
    return getIndex(table, BlockletIndexFactory.DATA_MAP_SCHEMA);
  }

  /**
   * Get the index for reading data.
   */
  public TableIndex getIndex(CarbonTable table, DataMapSchema dataMapSchema) {
    String tableId =
        table.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableId();
    List<TableIndex> tableIndices = indexes.get(table.getTableId());
    if (tableIndices == null && !table.isTransactionalTable()) {
      String keyUsingTablePath = getKeyUsingTablePath(table.getTablePath());
      if (keyUsingTablePath != null) {
        tableId = keyUsingTablePath;
        tableIndices = indexes.get(tableId);
      }
    }
    // in case of fileformat or sdk, when table is dropped or schema is changed the datamaps are
    // not cleared, they need to be cleared by using API, so compare the columns, if not same, clear
    // the datamaps on that table
    if (indexes.size() > 0 && !CollectionUtils.isEmpty(indexes.get(tableId))
        && !indexes.get(tableId).get(0).getTable().getTableInfo().getFactTable()
        .getListOfColumns().equals(table.getTableInfo().getFactTable().getListOfColumns())) {
      clearIndexes(tableId);
      tableIndices = null;
    }
    TableIndex tableIndex = null;
    if (tableIndices != null) {
      tableIndex = getTableIndex(dataMapSchema.getDataMapName(), tableIndices);
    }
    if (tableIndex == null) {
      synchronized (tableId.intern()) {
        tableIndices = indexes.get(tableId);
        if (tableIndices != null) {
          tableIndex = getTableIndex(dataMapSchema.getDataMapName(), tableIndices);
        }
        if (tableIndex == null) {
          try {
            tableIndex = createAndRegisterIndex(table, dataMapSchema);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      }
    }

    if (tableIndex == null) {
      throw new RuntimeException("Index does not exist");
    }
    // This is done to handle the scenario of stale cache because of which schema mismatch
    // exception can be thrown. Scenario: In case of carbondata used through FileFormat API,
    // once a table is dropped and recreated with the same name again then because the index
    // contains the stale carbon table schema mismatch exception is thrown. To avoid such scenarios
    // it is always better to update the carbon table object retrieved
    tableIndex.getIndexFactory().setCarbonTable(table);
    return tableIndex;
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
   * Return a new index instance and registered in the store manager.
   * The index is created using index name, index factory class and table identifier.
   */
  public IndexFactory getIndexFactoryClass(CarbonTable table, DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    try {
      // try to create datamap by reflection to test whether it is a valid IndexFactory class
      return (IndexFactory)
          Class.forName(dataMapSchema.getProviderName()).getConstructors()[0]
              .newInstance(table, dataMapSchema);
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      return DataMapRegistry.getIndexFactoryByShortName(table, dataMapSchema);
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to get Index factory for'" + dataMapSchema.getProviderName() + "'", e);
    }
  }

  /**
   * registered in the store manager.
   * The index is created using index name, index factory class and table identifier.
   */
  // TODO: make it private
  public TableIndex createAndRegisterIndex(CarbonTable table,
      DataMapSchema dataMapSchema) throws MalformedDataMapCommandException {
    IndexFactory indexFactory = getIndexFactoryClass(table, dataMapSchema);
    return registerDataMap(table, dataMapSchema, indexFactory);
  }

  public TableIndex registerDataMap(CarbonTable table,
      DataMapSchema dataMapSchema,  IndexFactory indexFactory) {
    // Just update the segmentRefreshMap with the table if not added.
    getTableSegmentRefresher(table);
    List<TableIndex> tableIndices = indexes.get(table.getTableId());
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(table.getTablePath());
      if (keyUsingTablePath != null) {
        tableIndices = indexes.get(table.getTableId());
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
    TableIndex index = new TableIndex(table,
        dataMapSchema, indexFactory, blockletDetailsFetcher, segmentPropertiesFetcher);

    tableIndices.add(index);
    indexes.put(table.getTableId(), tableIndices);
    tablePathMap.put(table.getTableId(), table.getTablePath());
    return index;
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
    List<TableIndex> allIndexes = getAllIndex(carbonTable);
    for (TableIndex index: allIndexes) {
      index.clear(segments);
    }

  }

  public List<String> getSegmentsToBeRefreshed(CarbonTable carbonTable,
      SegmentUpdateStatusManager updateStatusManager, List<Segment> filteredSegmentToAccess) {
    List<String> toBeCleanedSegments = new ArrayList<>();
    for (Segment filteredSegment : filteredSegmentToAccess) {
      boolean refreshNeeded = getTableSegmentRefresher(carbonTable).isRefreshNeeded(filteredSegment,
          updateStatusManager.getInvalidTimestampRange(filteredSegment.getSegmentNo()));
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
        getSegmentsToBeRefreshed(carbonTable, updateStatusManager, filteredSegmentToAccess);
    if (toBeCleanedSegments.size() > 0) {
      clearInvalidSegments(carbonTable, toBeCleanedSegments);
    }
  }

  /**
   * Clear the indexes of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearIndexes(AbsoluteTableIdentifier identifier) {
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
   * Clear the indexes of a table from memory
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
        indexes.get(identifier.getCarbonTableIdentifier().getTableId());
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(identifier.getTablePath());
      if (keyUsingTablePath != null) {
        tableId = keyUsingTablePath;
      }
    }
    segmentRefreshMap.remove(tableId);
    clearIndexes(tableId);
    indexes.remove(tableId);
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
   * this methods clears the indexes of table from memory
   */
  public void clearIndexes(String tableId) {
    List<TableIndex> tableIndices = indexes.get(tableId);
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
    indexes.remove(tableId);
    tablePathMap.remove(tableId);
  }

  /**
   * Clear the index of a table from memory and disk
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
      List<TableIndex> tableIndices = indexes.get(tableId);
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
            tableIndex.deleteIndexData();
            tableIndices.remove(i);
            break;
          }
          i++;
        }
        indexes.put(tableId, tableIndices);
      }
    }
  }

  /**
   * is index exist
   * @return true if exist, else return false
   */
  public boolean isIndexExist(String tableId, String dmName) {
    List<TableIndex> tableIndices = indexes.get(tableId);
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
    TableIndex blockletMap = getIndex(table, BlockletIndexFactory.DATA_MAP_SCHEMA);
    return (BlockletDetailsFetcher) blockletMap.getIndexFactory();
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
      SegmentUpdateStatusManager statusManager = new SegmentUpdateStatusManager(table);
      SegmentUpdateDetails[] updateStatusDetails = statusManager.getUpdateStatusDetails();
      for (SegmentUpdateDetails updateDetails : updateStatusDetails) {
        UpdateVO updateVO = statusManager.getInvalidTimestampRange(updateDetails.getSegmentName());
        SegmentRefreshInfo segmentRefreshInfo;
        if (updateVO != null && updateVO.getLatestUpdateTimestamp() != null) {
          segmentRefreshInfo = new SegmentRefreshInfo(updateVO.getLatestUpdateTimestamp(), 0);
        } else {
          segmentRefreshInfo = new SegmentRefreshInfo(0L, 0);
        }
        segmentRefreshTime.put(updateVO.getSegmentId(), segmentRefreshInfo);
      }
    }

    public boolean isRefreshNeeded(Segment seg, UpdateVO updateVo) {
      SegmentRefreshInfo segmentRefreshInfo =
          seg.getSegmentRefreshInfo(updateVo);
      String segmentId = seg.getSegmentNo();
      if (segmentRefreshInfo.getSegmentUpdatedTimestamp() == null) {
        return false;
      }
      if (segmentRefreshTime.get(segmentId) == null
          && segmentRefreshInfo.getSegmentUpdatedTimestamp() != 0) {
        segmentRefreshTime.put(segmentId, segmentRefreshInfo);
        return true;
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
    List<TableIndex> indexes = getAllIndex(carbonTable);
    List<TableIndex> remainingIndexes = new ArrayList<>();
    if (StringUtils.isNotEmpty(dataMapToClear)) {
      Iterator<TableIndex> dataMapIterator = indexes.iterator();
      while (dataMapIterator.hasNext()) {
        TableIndex tableIndex = dataMapIterator.next();
        if (dataMapToClear.equalsIgnoreCase(tableIndex.getDataMapSchema().getDataMapName())) {
          for (String segment: segmentNos) {
            tableIndex.deleteSegmentIndexData(segment);
          }
          tableIndex.clear();
        } else {
          remainingIndexes.add(tableIndex);
        }
      }
      this.indexes.put(carbonTable.getTableId(), remainingIndexes);
    } else {
      clearIndexes(carbonTable.getTableId());
      // clear the segment properties cache from executor
      SegmentPropertiesAndSchemaHolder.getInstance()
          .invalidate(carbonTable.getAbsoluteTableIdentifier());
    }
  }

  private boolean hasCGIndex(CarbonTable carbonTable) throws IOException {
    for (TableIndex tableIndex : getAllVisibleIndexes(carbonTable)) {
      if (tableIndex.getIndexFactory().getIndexLevel().equals(IndexLevel.CG)) {
        return true;
      }
    }
    return false;
  }

}
