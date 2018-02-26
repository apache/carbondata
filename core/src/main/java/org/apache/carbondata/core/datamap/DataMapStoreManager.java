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
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.datamap.DataMapProvider;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;

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
    List<DataMapSchema> dataMapSchemaList = carbonTable.getTableInfo().getDataMapSchemaList();
    List<TableDataMap> dataMaps = new ArrayList<>();
    if (dataMapSchemaList != null) {
      for (DataMapSchema dataMapSchema : dataMapSchemaList) {
        if (!dataMapSchema.getClassName().equalsIgnoreCase(
            DataMapProvider.PREAGGREGATE.toString())) {
          dataMaps.add(getDataMap(carbonTable.getAbsoluteTableIdentifier(), dataMapSchema));
        }
      }
    }
    return dataMaps;
  }

  /**
   * It gives the default datamap of the table. Default datamap of any table is BlockletDataMap
   *
   * @param identifier
   * @return
   */
  public TableDataMap getDefaultDataMap(AbsoluteTableIdentifier identifier) {
    return getDataMap(identifier, BlockletDataMapFactory.DATA_MAP_SCHEMA);
  }

  /**
   * Get the datamap for reading data.
   */
  public TableDataMap getDataMap(AbsoluteTableIdentifier identifier, DataMapSchema dataMapSchema) {
    String table = identifier.getCarbonTableIdentifier().getTableUniqueName();
    List<TableDataMap> tableIndices = allDataMaps.get(table);
    TableDataMap dataMap = null;
    if (tableIndices != null) {
      dataMap = getTableDataMap(dataMapSchema.getDataMapName(), tableIndices);
    }
    if (dataMap == null) {
      synchronized (table.intern()) {
        tableIndices = allDataMaps.get(table);
        if (tableIndices != null) {
          dataMap = getTableDataMap(dataMapSchema.getDataMapName(), tableIndices);
        }
        if (dataMap == null) {
          try {
            dataMap = createAndRegisterDataMap(identifier, dataMapSchema);
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
  private TableDataMap createAndRegisterDataMap(AbsoluteTableIdentifier identifier,
      DataMapSchema dataMapSchema) throws MalformedDataMapCommandException, IOException {
    DataMapFactory dataMapFactory;
    try {
      // try to create datamap by reflection to test whether it is a valid DataMapFactory class
      Class<? extends DataMapFactory> factoryClass =
          (Class<? extends DataMapFactory>) Class.forName(dataMapSchema.getClassName());
      dataMapFactory = factoryClass.newInstance();
    } catch (ClassNotFoundException e) {
      throw new MalformedDataMapCommandException(
          "DataMap '" + dataMapSchema.getClassName() + "' not found");
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create DataMap '" + dataMapSchema.getClassName() + "'", e);
    }
    return registerDataMap(identifier, dataMapSchema, dataMapFactory);
  }

  public TableDataMap registerDataMap(AbsoluteTableIdentifier identifier,
      DataMapSchema dataMapSchema,  DataMapFactory dataMapFactory) throws IOException {
    String table = identifier.getCarbonTableIdentifier().getTableUniqueName();
    // Just update the segmentRefreshMap with the table if not added.
    getTableSegmentRefresher(identifier);
    List<TableDataMap> tableIndices = allDataMaps.get(table);
    if (tableIndices == null) {
      tableIndices = new ArrayList<>();
    }

    dataMapFactory.init(identifier, dataMapSchema);
    BlockletDetailsFetcher blockletDetailsFetcher;
    SegmentPropertiesFetcher segmentPropertiesFetcher = null;
    if (dataMapFactory instanceof BlockletDetailsFetcher) {
      blockletDetailsFetcher = (BlockletDetailsFetcher) dataMapFactory;
    } else {
      blockletDetailsFetcher = getBlockletDetailsFetcher(identifier);
    }
    segmentPropertiesFetcher = (SegmentPropertiesFetcher) blockletDetailsFetcher;
    TableDataMap dataMap = new TableDataMap(identifier, dataMapSchema, dataMapFactory,
        blockletDetailsFetcher, segmentPropertiesFetcher);

    tableIndices.add(dataMap);
    allDataMaps.put(table, tableIndices);
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
  public void clearInvalidSegments(CarbonTable carbonTable, List<String> segments) {
    getDefaultDataMap(carbonTable.getAbsoluteTableIdentifier()).clear(segments);
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
   * @param identifier
   * @return
   */
  private BlockletDetailsFetcher getBlockletDetailsFetcher(AbsoluteTableIdentifier identifier) {
    TableDataMap blockletMap = getDataMap(identifier, BlockletDataMapFactory.DATA_MAP_SCHEMA);
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
  public TableSegmentRefresher getTableSegmentRefresher(AbsoluteTableIdentifier identifier) {
    String uniqueName = identifier.uniqueName();
    if (segmentRefreshMap.get(uniqueName) == null) {
      segmentRefreshMap.put(uniqueName, new TableSegmentRefresher(identifier));
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

    public TableSegmentRefresher(AbsoluteTableIdentifier identifier) {
      SegmentUpdateStatusManager statusManager = new SegmentUpdateStatusManager(identifier);
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
