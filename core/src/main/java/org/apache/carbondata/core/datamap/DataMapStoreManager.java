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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

/**
 * It maintains all the DataMaps in it.
 */
public final class DataMapStoreManager {

  private static DataMapStoreManager instance = new DataMapStoreManager();

  /**
   * Contains the list of datamaps for each table.
   */
  private Map<String, List<TableDataMap>> allDataMaps = new ConcurrentHashMap<>();

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
  public TableDataMap getDataMap(AbsoluteTableIdentifier identifier, String dataMapName,
      Class<? extends DataMapFactory> factoryClass) {
    String table = identifier.uniqueName();
    List<TableDataMap> tableDataMaps = allDataMaps.get(table);
    TableDataMap dataMap;
    if (tableDataMaps == null) {
      dataMap = createAndRegisterDataMap(identifier, factoryClass, dataMapName);
    } else {
      dataMap = getAbstractTableDataMap(dataMapName, tableDataMaps);
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
      Class<? extends DataMapFactory> factoryClass, String dataMapName) {
    String table = identifier.uniqueName();
    List<TableDataMap> tableDataMaps = allDataMaps.get(table);
    if (tableDataMaps == null) {
      tableDataMaps = new ArrayList<>();
      allDataMaps.put(table, tableDataMaps);
    }
    TableDataMap dataMap = getAbstractTableDataMap(dataMapName, tableDataMaps);
    if (dataMap != null) {
      throw new RuntimeException("Already datamap exists in that path with type " + dataMapName);
    }

    try {
      DataMapFactory dataMapFactory = factoryClass.newInstance();
      dataMapFactory.init(identifier, dataMapName);
      dataMap = new TableDataMap(identifier, dataMapName, dataMapFactory);
    } catch (Exception e) {
      LOGGER.error(e);
      throw new RuntimeException(e);
    }
    tableDataMaps.add(dataMap);
    return dataMap;
  }

  private TableDataMap getAbstractTableDataMap(String dataMapName,
      List<TableDataMap> tableDataMaps) {
    TableDataMap dataMap = null;
    for (TableDataMap tableDataMap: tableDataMaps) {
      if (tableDataMap.getDataMapName().equals(dataMapName)) {
        dataMap = tableDataMap;
        break;
      }
    }
    return dataMap;
  }

  /**
   * Clear the datamap/datamaps of a mentioned datamap name and table from memory
   * @param identifier
   * @param dataMapName
   */
  public void clearDataMap(AbsoluteTableIdentifier identifier, String dataMapName) {
    List<TableDataMap> tableDataMaps = allDataMaps.get(identifier);
    if (tableDataMaps != null) {
      int i = 0;
      for (TableDataMap tableDataMap: tableDataMaps) {
        if (tableDataMap != null && dataMapName.equals(tableDataMap.getDataMapName())) {
          tableDataMap.clear();
          tableDataMaps.remove(i);
          break;
        }
        i++;
      }
    }
  }

  /**
   * Returns the singleton instance
   * @return
   */
  public static DataMapStoreManager getInstance() {
    return instance;
  }

}
