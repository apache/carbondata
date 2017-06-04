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

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

/**
 * It maintains all the index tables in it.
 */
public class DataMapStore {

  private static DataMapStore instance = new DataMapStore();

  private Map<DataMapType, Map<String, TableDataMap>> dataMapMappping = new HashMap<>();

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DataMapStore.class.getName());

  private DataMapStore() {

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
    Map<String, TableDataMap> map = dataMapMappping.get(mapType);
    TableDataMap dataMap = null;
    if (map == null) {
      throw new RuntimeException("Datamap does not exist");
    } else {
      dataMap = map.get(dataMapName);
      if (dataMap == null) {
        throw new RuntimeException("Datamap does not exist");
      }
    }
    // Initialize datamap
    dataMap.init(identifier, dataMapName);
    return dataMap;
  }

  /**
   * Create new datamap instance using datamap type and path
   *
   * @param mapType
   * @return
   */
  public TableDataMap createTableDataMap(AbsoluteTableIdentifier identifier, DataMapType mapType,
      String dataMapName) {
    Map<String, TableDataMap> map = dataMapMappping.get(mapType);
    if (map == null) {
      map = new HashMap<>();
      dataMapMappping.put(mapType, map);
    }
    TableDataMap dataMap = map.get(dataMapName);
    if (dataMap != null) {
      throw new RuntimeException("Already datamap exists in that path with type " + mapType);
    }

    try {
      //TODO create datamap using @mapType.getClassName())
    } catch (Exception e) {
      LOGGER.error(e);
    }
    dataMap.init(identifier, dataMapName);
    map.put(dataMapName, dataMap);
    return dataMap;
  }

  public void clearDataMap(String dataMapName, DataMapType mapType) {
    Map<String, TableDataMap> map = dataMapMappping.get(mapType);
    if (map != null && map.get(dataMapName) != null) {
      map.remove(dataMapName).clear();
    }
  }

  public static DataMapStore getInstance() {
    return instance;
  }

}
