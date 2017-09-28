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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.indexstore.BlockletDetailsFetcher;
import org.apache.carbondata.core.indexstore.SegmentPropertiesFetcher;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;

/**
 * It maintains all the DataMaps in it.
 */
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

  public List<TableDataMap> getAllDataMap(AbsoluteTableIdentifier identifier) {
    return allDataMaps.get(identifier.uniqueName());
  }

  /**
   * Get the datamap for reading data.
   *
   * @param dataMapName
   * @param factoryClass
   * @return
   */
  public TableDataMap getDataMap(AbsoluteTableIdentifier identifier,
      String dataMapName, String factoryClass) {
    String table = identifier.uniqueName();
    List<TableDataMap> tableDataMaps = allDataMaps.get(table);
    TableDataMap dataMap;
    if (tableDataMaps == null) {
      synchronized (table.intern()) {
        tableDataMaps = allDataMaps.get(table);
        if (tableDataMaps == null) {
          dataMap = createAndRegisterDataMap(identifier, factoryClass, dataMapName);
        } else {
          dataMap = getTableDataMap(dataMapName, tableDataMaps);
        }
      }
    } else {
      dataMap = getTableDataMap(dataMapName, tableDataMaps);
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
  public TableDataMap createAndRegisterDataMap(AbsoluteTableIdentifier identifier,
      String factoryClassName, String dataMapName) {
    String table = identifier.uniqueName();
    // Just update the segmentRefreshMap with the table if not added.
    getTableSegmentRefresher(identifier);
    List<TableDataMap> tableDataMaps = allDataMaps.get(table);
    if (tableDataMaps == null) {
      tableDataMaps = new ArrayList<>();
    }
    TableDataMap dataMap = getTableDataMap(dataMapName, tableDataMaps);
    if (dataMap != null && dataMap.getDataMapName().equalsIgnoreCase(dataMapName)) {
      throw new RuntimeException("Already datamap exists in that path with type " + dataMapName);
    }

    try {
      Class<? extends DataMapFactory> factoryClass =
          (Class<? extends DataMapFactory>) Class.forName(factoryClassName);
      DataMapFactory dataMapFactory = factoryClass.newInstance();
      dataMapFactory.init(identifier, dataMapName);
      BlockletDetailsFetcher blockletDetailsFetcher;
      SegmentPropertiesFetcher segmentPropertiesFetcher = null;
      if (dataMapFactory instanceof BlockletDetailsFetcher) {
        blockletDetailsFetcher = (BlockletDetailsFetcher) dataMapFactory;
      } else {
        blockletDetailsFetcher = getBlockletDetailsFetcher(identifier);
      }
      segmentPropertiesFetcher = (SegmentPropertiesFetcher) blockletDetailsFetcher;
      dataMap = new TableDataMap(identifier, dataMapName, dataMapFactory, blockletDetailsFetcher,
          segmentPropertiesFetcher);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
    tableDataMaps.add(dataMap);
    allDataMaps.put(table, tableDataMaps);
    return dataMap;
  }

  private TableDataMap getTableDataMap(String dataMapName, List<TableDataMap> tableDataMaps) {
    TableDataMap dataMap = null;
    for (TableDataMap tableDataMap : tableDataMaps) {
      if (tableDataMap.getDataMapName().equals(dataMapName) || (!tableDataMap.getDataMapName()
          .equals(""))) {
        dataMap = tableDataMap;
        break;
      }
    }
    return dataMap;
  }

  /**
   * Clear the datamap/datamaps of a table from memory
   * @param identifier Table identifier
   */
  public void clearDataMaps(AbsoluteTableIdentifier identifier) {
    List<TableDataMap> tableDataMaps = allDataMaps.get(identifier.uniqueName());
    segmentRefreshMap.remove(identifier.uniqueName());
    if (tableDataMaps != null) {
      for (TableDataMap tableDataMap: tableDataMaps) {
        if (tableDataMap != null) {
          tableDataMap.clear();
          break;
        }
      }
      allDataMaps.remove(identifier.uniqueName());
    }
  }

  /**
   * Clear the datamap/datamaps of a table from memory
   * @param identifier Table identifier
   */
  public void clearDataMap(AbsoluteTableIdentifier identifier, String dataMapName) {
    List<TableDataMap> tableDataMaps = allDataMaps.get(identifier.uniqueName());
    if (tableDataMaps != null) {
      int i = 0;
      for (TableDataMap tableDataMap: tableDataMaps) {
        if (tableDataMap != null && dataMapName.equalsIgnoreCase(tableDataMap.getDataMapName())) {
          tableDataMap.clear();
          tableDataMaps.remove(i);
          break;
        }
        i++;
      }
    }
  }

  /**
   * Get the blocklet datamap factory to get the detail information of blocklets
   * @param identifier
   * @return
   */
  private BlockletDetailsFetcher getBlockletDetailsFetcher(AbsoluteTableIdentifier identifier) {
    TableDataMap blockletMap =
        getDataMap(identifier, BlockletDataMap.NAME, BlockletDataMapFactory.class.getName());
    return (BlockletDetailsFetcher) blockletMap.getDataMapFactory();
  }

  /**
   * Returns the singleton instance
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
      for (String segmentId: segmentIds) {
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
