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
package org.apache.carbondata.core.indexstore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

/**
 * It maintains all the DataMaps in it.
 */
public class DataMapStoreManager {

  private static DataMapStoreManager instance = new DataMapStoreManager();

  /**
   * Contains the list of datamaps for each table.
   */
  private Map<AbsoluteTableIdentifier, List<TableDataMap>> dataMapMappping = new HashMap<>();

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataMapStoreManager.class.getName());

  private DataMapStoreManager() {

  }

  /**
   * Get the datamap for reading data.
   *
   * @param dataMapName
   * @param mapType
   * @return
   */
  public TableDataMap getDataMap(AbsoluteTableIdentifier identifier, String dataMapName,
      DataMapType mapType) {
    List<TableDataMap> tableDataMaps = dataMapMappping.get(identifier);
    TableDataMap dataMap = null;
    if (tableDataMaps == null) {
      createTableDataMap(identifier, mapType, dataMapName);
      tableDataMaps = dataMapMappping.get(identifier);
    }
    dataMap = getAbstractTableDataMap(dataMapName, tableDataMaps);
    if (dataMap == null) {
      throw new RuntimeException("Datamap does not exist");
    }
    return dataMap;
  }

  /**
   * Create new datamap instance using datamap type and path
   *
   * @param mapType
   * @return
   */
  public TableDataMap createTableDataMap(AbsoluteTableIdentifier identifier,
      DataMapType mapType, String dataMapName) {
    List<TableDataMap> tableDataMaps = dataMapMappping.get(identifier);
    if (tableDataMaps == null) {
      tableDataMaps = new ArrayList<>();
      dataMapMappping.put(identifier, tableDataMaps);
    }
    TableDataMap dataMap = getAbstractTableDataMap(dataMapName, tableDataMaps);
    if (dataMap != null) {
      throw new RuntimeException("Already datamap exists in that path with type " + mapType);
    }

    try {
      DataMapFactory dataMapFactory = mapType.getClassObject().newInstance();
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

  public void clearDataMap(AbsoluteTableIdentifier identifier, String dataMapName) {
    List<TableDataMap> tableDataMaps = dataMapMappping.get(identifier);
    if (tableDataMaps != null) {
      int i = 0;
      for (TableDataMap tableDataMap: tableDataMaps) {
        if (tableDataMap != null && dataMapName.equals(tableDataMap.getDataMapName())) {
          tableDataMap.clear(new ArrayList<String>());
          tableDataMaps.remove(i);
          break;
        }
        i++;
      }
    }
  }

  public static DataMapStoreManager getInstance() {
    return instance;
  }

}
