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
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.DiskBasedDMSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentRefreshInfo;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonSessionInfo;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;

import static org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.MV;
import static org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider.PREAGGREGATE;

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

  public Map<String, List<TableDataMap>> getAllDataMaps() {
    return allDataMaps;
  }

  /**
   * Contains the list of datamaps for each table.
   */
  private Map<String, List<TableDataMap>> allDataMaps = new ConcurrentHashMap<>();

  /**
   * Contains the table name to the tablepath mapping.
   */
  private Map<String, String> tablePathMap = new ConcurrentHashMap<>();

  /**
   * Contains the datamap catalog for each datamap provider.
   */
  private Map<String, DataMapCatalog> dataMapCatalogs = null;

  private Map<String, TableSegmentRefresher> segmentRefreshMap = new ConcurrentHashMap<>();

  private DataMapSchemaStorageProvider provider = new DiskBasedDMSchemaStorageProvider(
      CarbonProperties.getInstance().getSystemFolderLocation());

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataMapStoreManager.class.getName());

  private DataMapStoreManager() {

  }

  /**
   * It only gives the visible datamaps
   */
  List<TableDataMap> getAllVisibleDataMap(CarbonTable carbonTable) throws IOException {
    CarbonSessionInfo sessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo();
    List<TableDataMap> allDataMaps = getAllDataMap(carbonTable);
    Iterator<TableDataMap> dataMapIterator = allDataMaps.iterator();
    while (dataMapIterator.hasNext()) {
      TableDataMap dataMap = dataMapIterator.next();
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
    return allDataMaps;
  }

  /**
   * It gives all datamaps except the default datamap.
   *
   * @return
   */
  public List<TableDataMap> getAllDataMap(CarbonTable carbonTable) throws IOException {
    List<DataMapSchema> dataMapSchemas = getDataMapSchemasOfTable(carbonTable);
    List<TableDataMap> dataMaps = new ArrayList<>();
    if (dataMapSchemas != null) {
      for (DataMapSchema dataMapSchema : dataMapSchemas) {
        RelationIdentifier identifier = dataMapSchema.getParentTables().get(0);
        if (dataMapSchema.isIndexDataMap() && identifier.getTableId()
            .equals(carbonTable.getTableId())) {
          dataMaps.add(getDataMap(carbonTable, dataMapSchema));
        }
      }
    }
    return dataMaps;
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
      // if the preaggregate datamap,not be modified the schema
      if (providerName.equalsIgnoreCase(PREAGGREGATE.toString())) {
        continue;
      }
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
  public synchronized void registerDataMapCatalog(DataMapProvider dataMapProvider,
      DataMapSchema dataMapSchema) throws IOException {
    initializeDataMapCatalogs(dataMapProvider);
    String name = dataMapSchema.getProviderName();
    DataMapCatalog dataMapCatalog = dataMapCatalogs.get(name);
    if (dataMapCatalog == null) {
      dataMapCatalog = dataMapProvider.createDataMapCatalog();
      if (dataMapCatalog != null) {
        dataMapCatalogs.put(name.toLowerCase(), dataMapCatalog);
        dataMapCatalog.registerSchema(dataMapSchema);
      }
    } else {
      dataMapCatalog.registerSchema(dataMapSchema);
    }
  }

  /**
   * Unregister datamap catalog.
   * @param dataMapSchema
   */
  public synchronized void unRegisterDataMapCatalog(DataMapSchema dataMapSchema) {
    if (dataMapCatalogs == null) {
      return;
    }
    String name = dataMapSchema.getProviderName();
    DataMapCatalog dataMapCatalog = dataMapCatalogs.get(name);
    if (dataMapCatalog != null) {
      dataMapCatalog.unregisterSchema(dataMapSchema.getDataMapName());
    }
  }

  /**
   * Get the datamap catalog for provider.
   * @param providerName
   * @return
   */
  public synchronized DataMapCatalog getDataMapCatalog(DataMapProvider dataMapProvider,
      String providerName) throws IOException {
    initializeDataMapCatalogs(dataMapProvider);
    return dataMapCatalogs.get(providerName);
  }

  /**
   * Initialize by reading all datamaps from store and re register it
   * @param dataMapProvider
   */
  private void initializeDataMapCatalogs(DataMapProvider dataMapProvider) throws IOException {
    if (dataMapCatalogs == null) {
      dataMapCatalogs = new ConcurrentHashMap<>();
      List<DataMapSchema> dataMapSchemas = getAllDataMapSchemas();
      for (DataMapSchema schema : dataMapSchemas) {
        if (schema.getProviderName()
            .equalsIgnoreCase(dataMapProvider.getDataMapSchema().getProviderName())) {
          DataMapCatalog dataMapCatalog = dataMapCatalogs.get(schema.getProviderName());
          if (dataMapCatalog == null) {
            dataMapCatalog = dataMapProvider.createDataMapCatalog();
            if (null == dataMapCatalog) {
              throw new RuntimeException("Internal Error.");
            }
            dataMapCatalogs.put(schema.getProviderName().toLowerCase(), dataMapCatalog);
          }
          try {
            dataMapCatalog.registerSchema(schema);
          } catch (Exception e) {
            // Ignore the schema
            LOGGER.error("Error while registering schema", e);
          }
        }
      }
    }
  }

  /**
   * It gives the default datamap of the table. Default datamap of any table is BlockletDataMap
   *
   * @param table
   * @return
   */
  public TableDataMap getDefaultDataMap(CarbonTable table) {
    return getDataMap(table, BlockletDataMapFactory.DATA_MAP_SCHEMA);
  }

  /**
   * Get the datamap for reading data.
   */
  public TableDataMap getDataMap(CarbonTable table, DataMapSchema dataMapSchema) {
    String tableUniqueName =
        table.getAbsoluteTableIdentifier().getCarbonTableIdentifier().getTableUniqueName();
    List<TableDataMap> tableIndices = allDataMaps.get(tableUniqueName);
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(table.getTablePath());
      if (keyUsingTablePath != null) {
        tableUniqueName = keyUsingTablePath;
        tableIndices = allDataMaps.get(tableUniqueName);
      }
    }
    // in case of fileformat or sdk, when table is dropped or schema is changed the datamaps are
    // not cleared, they need to be cleared by using API, so compare the columns, if not same, clear
    // the datamaps on that table
    if (allDataMaps.size() > 0 && !CollectionUtils.isEmpty(allDataMaps.get(tableUniqueName))
        && !allDataMaps.get(tableUniqueName).get(0).getTable().getTableInfo().getFactTable()
        .getListOfColumns().equals(table.getTableInfo().getFactTable().getListOfColumns())) {
      clearDataMaps(tableUniqueName);
      tableIndices = null;
    }
    TableDataMap dataMap = null;
    if (tableIndices != null) {
      dataMap = getTableDataMap(dataMapSchema.getDataMapName(), tableIndices);
    }
    if (dataMap == null) {
      synchronized (tableUniqueName.intern()) {
        tableIndices = allDataMaps.get(tableUniqueName);
        if (tableIndices != null) {
          dataMap = getTableDataMap(dataMapSchema.getDataMapName(), tableIndices);
        }
        if (dataMap == null) {
          try {
            dataMap = createAndRegisterDataMap(table, dataMapSchema);
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
    dataMap.getDataMapFactory().setCarbonTable(table);
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
  public DataMapFactory getDataMapFactoryClass(CarbonTable table, DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    try {
      // try to create datamap by reflection to test whether it is a valid DataMapFactory class
      return (DataMapFactory)
          Class.forName(dataMapSchema.getProviderName()).getConstructors()[0]
              .newInstance(table, dataMapSchema);
    } catch (ClassNotFoundException e) {
      // try to create DataMapClassProvider instance by taking providerName as short name
      return DataMapRegistry.getDataMapFactoryByShortName(table, dataMapSchema);
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to get DataMap factory for'" + dataMapSchema.getProviderName() + "'", e);
    }
  }

  /**
   * registered in the store manager.
   * The datamap is created using datamap name, datamap factory class and table identifier.
   */
  // TODO: make it private
  public TableDataMap createAndRegisterDataMap(CarbonTable table,
      DataMapSchema dataMapSchema) throws MalformedDataMapCommandException {
    DataMapFactory dataMapFactory  = getDataMapFactoryClass(table, dataMapSchema);
    return registerDataMap(table, dataMapSchema, dataMapFactory);
  }

  public TableDataMap registerDataMap(CarbonTable table,
      DataMapSchema dataMapSchema,  DataMapFactory dataMapFactory) {
    String tableUniqueName = table.getCarbonTableIdentifier().getTableUniqueName();
    // Just update the segmentRefreshMap with the table if not added.
    getTableSegmentRefresher(table);
    List<TableDataMap> tableIndices = allDataMaps.get(tableUniqueName);
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(table.getTablePath());
      if (keyUsingTablePath != null) {
        tableUniqueName = keyUsingTablePath;
        tableIndices = allDataMaps.get(tableUniqueName);
      }
    }
    if (tableIndices == null) {
      tableIndices = new ArrayList<>();
    }

    BlockletDetailsFetcher blockletDetailsFetcher;
    SegmentPropertiesFetcher segmentPropertiesFetcher = null;
    if (dataMapFactory instanceof BlockletDetailsFetcher) {
      blockletDetailsFetcher = (BlockletDetailsFetcher) dataMapFactory;
    } else {
      blockletDetailsFetcher = getBlockletDetailsFetcher(table);
    }
    segmentPropertiesFetcher = (SegmentPropertiesFetcher) blockletDetailsFetcher;
    TableDataMap dataMap = new TableDataMap(table,
        dataMapSchema, dataMapFactory, blockletDetailsFetcher, segmentPropertiesFetcher);

    tableIndices.add(dataMap);
    allDataMaps.put(tableUniqueName, tableIndices);
    tablePathMap.put(tableUniqueName, table.getTablePath());
    return dataMap;
  }

  private TableDataMap getTableDataMap(String dataMapName, List<TableDataMap> tableIndices) {
    TableDataMap dataMap = null;
    for (TableDataMap tableDataMap : tableIndices) {
      if (tableDataMap.getDataMapSchema().getDataMapName().equals(dataMapName)) {
        dataMap = tableDataMap;
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
    getDefaultDataMap(carbonTable).clear(segments);
    List<TableDataMap> allDataMap = getAllDataMap(carbonTable);
    for (TableDataMap dataMap: allDataMap) {
      dataMap.clear(segments);
    }

  }

  public List<String> getSegmentsToBeRefreshed(CarbonTable carbonTable,
      SegmentUpdateStatusManager updateStatusManager, List<Segment> filteredSegmentToAccess)
      throws IOException {
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
   * Clear the datamap/datamaps of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearDataMaps(AbsoluteTableIdentifier identifier) {
    clearDataMaps(identifier, true);
  }

  /**
   * Clear the datamap/datamaps of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearDataMaps(AbsoluteTableIdentifier identifier, boolean launchJob) {
    String tableUniqueName = identifier.getCarbonTableIdentifier().getTableUniqueName();
    if (launchJob) {
      // carbon table need to lookup only if launch job is set.
      CarbonTable carbonTable = getCarbonTable(identifier);
      if (null != carbonTable) {
        String jobClassName;
        if (CarbonProperties.getInstance()
            .isDistributedPruningEnabled(identifier.getDatabaseName(), identifier.getTableName())) {
          jobClassName = DataMapUtil.DISTRIBUTED_JOB_NAME;
        } else {
          jobClassName = DataMapUtil.EMBEDDED_JOB_NAME;
        }
        try {
          DataMapUtil.executeClearDataMapJob(carbonTable, jobClassName);
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
    List<TableDataMap> tableIndices = allDataMaps.get(tableUniqueName);
    if (tableIndices == null) {
      String keyUsingTablePath = getKeyUsingTablePath(identifier.getTablePath());
      if (keyUsingTablePath != null) {
        tableUniqueName = keyUsingTablePath;
      }
    }
    segmentRefreshMap.remove(identifier.uniqueName());
    clearDataMaps(tableUniqueName);
    allDataMaps.remove(tableUniqueName);
    tablePathMap.remove(tableUniqueName);
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
  public void clearDataMaps(String tableUniqName) {
    List<TableDataMap> tableIndices = allDataMaps.get(tableUniqName);
    if (tableIndices != null) {
      for (TableDataMap tableDataMap : tableIndices) {
        if (tableDataMap != null) {
          // clear the segmentMap in BlockletDetailsFetcher,else the Segment will remain in executor
          // and the query fails as we will check whether the blocklet contains in the index or not
          tableDataMap.getBlockletDetailsFetcher().clear();
          tableDataMap.clear();
        }
      }
    }
    allDataMaps.remove(tableUniqName);
    tablePathMap.remove(tableUniqName);
  }

  /**
   * Clear the datamap/datamaps of a table from memory and disk
   *
   * @param identifier Table identifier
   */
  public void deleteDataMap(AbsoluteTableIdentifier identifier, String dataMapName) {
    CarbonTable carbonTable = getCarbonTable(identifier);
    String tableUniqueName = identifier.getCarbonTableIdentifier().getTableUniqueName();
    if (CarbonProperties.getInstance()
        .isDistributedPruningEnabled(identifier.getDatabaseName(), identifier.getTableName())) {
      try {
        DataMapUtil
            .executeClearDataMapJob(carbonTable, DataMapUtil.DISTRIBUTED_JOB_NAME, dataMapName);
      } catch (IOException e) {
        LOGGER.error("clear dataMap job failed", e);
        // ignoring the exception
      }
    } else {
      List<TableDataMap> tableIndices = allDataMaps.get(tableUniqueName);
      if (tableIndices != null) {
        int i = 0;
        for (TableDataMap tableDataMap : tableIndices) {
          if (carbonTable != null && tableDataMap != null && dataMapName
              .equalsIgnoreCase(tableDataMap.getDataMapSchema().getDataMapName())) {
            try {
              DataMapUtil
                  .executeClearDataMapJob(carbonTable, DataMapUtil.EMBEDDED_JOB_NAME, dataMapName);
              tableDataMap.clear();
            } catch (IOException e) {
              LOGGER.error("clear dataMap job failed", e);
              // ignoring the exception
            }
            tableDataMap.deleteDatamapData();
            tableIndices.remove(i);
            break;
          }
          i++;
        }
        allDataMaps.put(tableUniqueName, tableIndices);
      }
    }
  }

  /**
   * is datamap exist
   * @return true if exist, else return false
   */
  public boolean isDataMapExist(String dbName, String tableName, String dmName) {
    List<TableDataMap> tableDataMaps = allDataMaps.get(dbName + '_' + tableName);
    if (tableDataMaps != null) {
      for (TableDataMap dm : tableDataMaps) {
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
    TableDataMap blockletMap = getDataMap(table, BlockletDataMapFactory.DATA_MAP_SCHEMA);
    return (BlockletDetailsFetcher) blockletMap.getDataMapFactory();
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
    String uniqueName = table.getAbsoluteTableIdentifier().uniqueName();
    if (segmentRefreshMap.get(uniqueName) == null) {
      segmentRefreshMap.put(uniqueName, new TableSegmentRefresher(table));
    }
    return segmentRefreshMap.get(uniqueName);
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
        segmentRefreshTime.put(updateVO.getSegmentId(),
            new SegmentRefreshInfo(updateVO.getCreatedOrUpdatedTimeStamp(), 0));
      }
    }

    public boolean isRefreshNeeded(Segment seg, UpdateVO updateVo) throws IOException {
      SegmentRefreshInfo segmentRefreshInfo =
          seg.getSegmentRefreshInfo(updateVo);
      String segmentId = seg.getSegmentNo();
      if (segmentRefreshTime.get(segmentId) == null
          && segmentRefreshInfo.getSegmentUpdatedTimestamp() != null) {
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

  public synchronized void clearInvalidDataMaps(CarbonTable carbonTable, List<String> segmentNos,
      String dataMapToClear) throws IOException {
    List<TableDataMap> dataMaps = getAllDataMap(carbonTable);
    List<TableDataMap> remainingDataMaps = new ArrayList<>();
    if (StringUtils.isNotEmpty(dataMapToClear)) {
      Iterator<TableDataMap> dataMapIterator = dataMaps.iterator();
      while (dataMapIterator.hasNext()) {
        TableDataMap tableDataMap = dataMapIterator.next();
        if (dataMapToClear.equalsIgnoreCase(tableDataMap.getDataMapSchema().getDataMapName())) {
          for (String segment: segmentNos) {
            tableDataMap.deleteSegmentDatamapData(segment);
          }
          tableDataMap.clear();
        } else {
          remainingDataMaps.add(tableDataMap);
        }
      }
      getAllDataMaps().put(carbonTable.getTableUniqueName(), remainingDataMaps);
    } else {
      clearDataMaps(carbonTable.getTableUniqueName());
      // clear the segment properties cache from executor
      SegmentPropertiesAndSchemaHolder.getInstance()
          .invalidate(carbonTable.getAbsoluteTableIdentifier());
    }
  }

}
