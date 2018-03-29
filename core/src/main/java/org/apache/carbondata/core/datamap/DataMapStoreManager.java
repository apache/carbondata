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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapClassProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.DiskBasedDMSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * It maintains all the DataMaps in it.
 */
@InterfaceAudience.Internal
public final class DataMapStoreManager {

  private static DataMapStoreManager instance = new DataMapStoreManager();

  /**
   * Contains the list of datamaps for each table.
   */
  private Map<String, List<TableDataMap>> allDataMaps = new ConcurrentHashMap<>();

  /**
   * Contains the datamap catalog for each datamap provider.
   */
  private Map<String, DataMapCatalog> dataMapCatalogs = new ConcurrentHashMap<>();

  private Map<String, TableSegmentRefresher> segmentRefreshMap = new ConcurrentHashMap<>();

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataMapStoreManager.class.getName());

  private DataMapStoreManager() {

  }

  /**
   * It gives all datamaps of type @mapType except the default datamap.
   *
   */
  public List<TableDataMap> getAllDataMap(CarbonTable carbonTable, DataMapLevel mapType) {
    List<TableDataMap> dataMaps = new ArrayList<>();
    List<TableDataMap> tableIndices = getAllDataMap(carbonTable);
    if (tableIndices != null) {
      for (TableDataMap dataMap : tableIndices) {
        if (mapType == dataMap.getDataMapFactory().getDataMapType()) {
          dataMaps.add(dataMap);
        }
      }
    }
    return dataMaps;
  }

  /**
   * It gives all datamaps except the default datamap.
   *
   * @return
   */
  public List<TableDataMap> getAllDataMap(CarbonTable carbonTable) {
    // TODO cache all schemas and update only when datamap status file updates
    List<DataMapSchema> dataMapSchemas = getAllDataMapSchemas();
    List<TableDataMap> dataMaps = new ArrayList<>();
    if (dataMapSchemas != null) {
      for (DataMapSchema dataMapSchema : dataMapSchemas) {
        RelationIdentifier identifier = dataMapSchema.getParentTables().get(0);
        if (dataMapSchema.isIndexDataMap() && identifier.getTableName()
            .equals(carbonTable.getTableName()) && identifier.getDatabaseName()
            .equals(carbonTable.getDatabaseName())) {
          dataMaps.add(getDataMap(carbonTable, dataMapSchema));
        }
      }
    }
    return dataMaps;
  }

  /**
   * It gives all datamap schemas.
   *
   * @return
   */
  public List<DataMapSchema> getAllDataMapSchemas(CarbonTable carbonTable) {
    // TODO cache all schemas and update only when datamap status file updates
    List<DataMapSchema> dataMapSchemas = getAllDataMapSchemas();
    List<DataMapSchema> dataMaps = new ArrayList<>();
    if (dataMapSchemas != null) {
      for (DataMapSchema dataMapSchema : dataMapSchemas) {
        RelationIdentifier identifier = dataMapSchema.getParentTables().get(0);
        if (dataMapSchema.isIndexDataMap() && identifier.getTableName()
            .equals(carbonTable.getTableName()) && identifier.getDatabaseName()
            .equals(carbonTable.getDatabaseName())) {
          dataMaps.add(dataMapSchema);
        }
      }
    }
    return dataMaps;
  }

  public List<DataMapSchema> getAllDataMapSchemas() {
    DataMapSchemaStorageProvider provider = new DiskBasedDMSchemaStorageProvider(
        CarbonProperties.getInstance().getSystemFolderLocation());
    List<DataMapSchema> dataMapSchemas;
    try {
      dataMapSchemas = provider.retrieveAllSchemas();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return dataMapSchemas;
  }

  public DataMapSchema getDataMapSchema(String dataMapName) throws NoSuchDataMapException {
    List<DataMapSchema> allDataMapSchemas = getAllDataMapSchemas();
    for (DataMapSchema dataMapSchema : allDataMapSchemas) {
      if (dataMapSchema.getDataMapName().equalsIgnoreCase(dataMapName)) {
        return dataMapSchema;
      }
    }
    throw new NoSuchDataMapException(dataMapName);
  }

  /**
   * Register datamap catalog for the datamap provider
   * @param dataMapProvider
   * @param dataMapSchema
   */
  public synchronized void registerDataMapCatalog(DataMapProvider dataMapProvider,
      DataMapSchema dataMapSchema) {
    String name = dataMapSchema.getProviderName();
    DataMapCatalog dataMapCatalog = dataMapCatalogs.get(name);
    if (dataMapCatalog == null) {
      dataMapCatalog = dataMapProvider.createDataMapCatalog();
      if (dataMapCatalog != null) {
        dataMapCatalogs.put(name, dataMapCatalog);
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
  public DataMapCatalog getDataMapCatalog(String providerName) {
    return dataMapCatalogs.get(providerName);
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
    return dataMap;
  }

  /**
   * Return a new datamap instance and registered in the store manager.
   * The datamap is created using datamap name, datamap factory class and table identifier.
   */
  // TODO: make it private
  public TableDataMap createAndRegisterDataMap(CarbonTable table,
      DataMapSchema dataMapSchema) throws MalformedDataMapCommandException, IOException {
    DataMapFactory dataMapFactory;
    if (dataMapSchema.getProviderName()
        .equalsIgnoreCase(DataMapClassProvider.LUCENEFG.getShortName()) || dataMapSchema
        .getProviderName().equalsIgnoreCase(DataMapClassProvider.LUCENECG.getShortName())) {
      dataMapFactory =
          IndexDataMapProvider.getDataMapFactoryByShortName(dataMapSchema.getProviderName());
    } else {
      try {
        // try to create datamap by reflection to test whether it is a valid DataMapFactory class
        Class<? extends DataMapFactory> factoryClass =
            (Class<? extends DataMapFactory>) Class.forName(dataMapSchema.getProviderName());
        dataMapFactory = factoryClass.newInstance();
      } catch (ClassNotFoundException e) {
        throw new MalformedDataMapCommandException(
            "DataMap '" + dataMapSchema.getProviderName() + "' not found");
      } catch (Throwable e) {
        throw new MetadataProcessException(
            "failed to create DataMap '" + dataMapSchema.getProviderName() + "'", e);
      }
    }
    return registerDataMap(table, dataMapSchema, dataMapFactory);
  }

  public TableDataMap registerDataMap(CarbonTable table,
      DataMapSchema dataMapSchema,  DataMapFactory dataMapFactory)
      throws IOException, MalformedDataMapCommandException {
    String tableUniqueName = table.getCarbonTableIdentifier().getTableUniqueName();
    // Just update the segmentRefreshMap with the table if not added.
    getTableSegmentRefresher(table);
    List<TableDataMap> tableIndices = allDataMaps.get(tableUniqueName);
    if (tableIndices == null) {
      tableIndices = new ArrayList<>();
    }

    dataMapFactory.init(table, dataMapSchema);
    BlockletDetailsFetcher blockletDetailsFetcher;
    SegmentPropertiesFetcher segmentPropertiesFetcher = null;
    if (dataMapFactory instanceof BlockletDetailsFetcher) {
      blockletDetailsFetcher = (BlockletDetailsFetcher) dataMapFactory;
    } else {
      blockletDetailsFetcher = getBlockletDetailsFetcher(table);
    }
    segmentPropertiesFetcher = (SegmentPropertiesFetcher) blockletDetailsFetcher;
    TableDataMap dataMap = new TableDataMap(table.getAbsoluteTableIdentifier(),
        dataMapSchema, dataMapFactory, blockletDetailsFetcher, segmentPropertiesFetcher);

    tableIndices.add(dataMap);
    allDataMaps.put(tableUniqueName, tableIndices);
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
   * @param carbonTable
   * @param segments
   */
  public void clearInvalidSegments(CarbonTable carbonTable, List<Segment> segments) {
    getDefaultDataMap(carbonTable).clear(segments);
    List<TableDataMap> allDataMap = getAllDataMap(carbonTable);
    for (TableDataMap dataMap: allDataMap) {
      dataMap.clear(segments);
    }

  }

  /**
   * Clear the datamap/datamaps of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearDataMaps(AbsoluteTableIdentifier identifier) {
    String tableUniqueName = identifier.getCarbonTableIdentifier().getTableUniqueName();
    List<TableDataMap> tableIndices = allDataMaps.get(tableUniqueName);
    segmentRefreshMap.remove(identifier.uniqueName());
    if (tableIndices != null) {
      for (TableDataMap tableDataMap : tableIndices) {
        if (tableDataMap != null) {
          tableDataMap.clear();
          break;
        }
      }
      allDataMaps.remove(tableUniqueName);
    }
  }

  /**
   * Clear the datamap/datamaps of a table from memory
   *
   * @param identifier Table identifier
   */
  public void clearDataMap(AbsoluteTableIdentifier identifier, String dataMapName) {
    List<TableDataMap> tableIndices =
        allDataMaps.get(identifier.getCarbonTableIdentifier().getTableUniqueName());
    if (tableIndices != null) {
      int i = 0;
      for (TableDataMap tableDataMap : tableIndices) {
        if (tableDataMap != null && dataMapName
            .equalsIgnoreCase(tableDataMap.getDataMapSchema().getDataMapName())) {
          tableDataMap.clear();
          tableIndices.remove(i);
          break;
        }
        i++;
      }
    }
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
    private Map<String, Long> segmentRefreshTime = new HashMap<>();

    // This map keeps the manual refresh entries from users. It is mainly used for partition
    // altering.
    private Map<String, Boolean> manualSegmentRefresh = new HashMap<>();

    TableSegmentRefresher(CarbonTable table) {
      SegmentUpdateStatusManager statusManager = new SegmentUpdateStatusManager(table);
      SegmentUpdateDetails[] updateStatusDetails = statusManager.getUpdateStatusDetails();
      for (SegmentUpdateDetails updateDetails : updateStatusDetails) {
        UpdateVO updateVO = statusManager.getInvalidTimestampRange(updateDetails.getSegmentName());
        segmentRefreshTime.put(updateVO.getSegmentId(), updateVO.getCreatedOrUpdatedTimeStamp());
      }
    }

    public boolean isRefreshNeeded(String segmentId, SegmentUpdateStatusManager statusManager) {
      UpdateVO updateVO = statusManager.getInvalidTimestampRange(segmentId);
      if (segmentRefreshTime.get(segmentId) == null) {
        segmentRefreshTime.put(segmentId, updateVO.getCreatedOrUpdatedTimeStamp());
        return true;
      }
      if (manualSegmentRefresh.get(segmentId) != null && manualSegmentRefresh.get(segmentId)) {
        manualSegmentRefresh.put(segmentId, false);
        return true;
      }
      Long updateTimestamp = updateVO.getLatestUpdateTimestamp();
      boolean isRefresh =
          updateTimestamp != null && (updateTimestamp > segmentRefreshTime.get(segmentId));
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

}
