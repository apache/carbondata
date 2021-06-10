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

package org.apache.carbondata.core.metadata.schema.indextable;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

/**
 * Carbon Index properties holder
 */
public class IndexMetadata implements Serializable {

  private static final long serialVersionUID = -8076464279248926823L;
  /**
   * index provider and map of index table name and its index properties
   */
  private Map<String, Map<String, Map<String, String>>> indexProviderMap;
  /**
   * parent table name of this index table
   */
  private String parentTableName;
  /**
   * location of parent table, path till table folder
   */
  private String parentTablePath;
  /**
   * table ID of parent table
   */
  private String parentTableId;
  /**
   * flag to check for index table
   */
  private boolean isIndexTable;

  public IndexMetadata(boolean isIndexTable) {
    this.isIndexTable = isIndexTable;
  }

  public IndexMetadata(String parentTableName, boolean isIndexTable, String parentTablePath) {
    this(isIndexTable);
    this.parentTableName = parentTableName;
    this.parentTablePath = parentTablePath;
  }

  public IndexMetadata(Map<String, Map<String, Map<String, String>>> indexProviderMap,
      String parentTableName, boolean isIndexTable, String parentTablePath, String parentTableId) {
    this(parentTableName, isIndexTable, parentTablePath);
    this.indexProviderMap = indexProviderMap;
    this.parentTableId = parentTableId;
  }

  public void addIndexTableInfo(String providerName, String tableName,
      Map<String, String> indexProperties) {
    if (null == indexProviderMap) {
      indexProviderMap = new ConcurrentHashMap<>();
    }
    if (null == indexProviderMap.get(providerName) || indexProviderMap.isEmpty()) {
      Map<String, Map<String, String>> indexMap = new ConcurrentHashMap<>();
      indexMap.put(tableName, indexProperties);
      indexProviderMap.put(providerName, indexMap);
    } else {
      indexProviderMap.get(providerName).put(tableName, indexProperties);
    }
  }

  public void removeIndexTableInfo(String indexName) {
    if (null != indexProviderMap) {
      for (Map.Entry<String, Map<String, Map<String, String>>> indexProviderEntry : indexProviderMap
          .entrySet()) {
        Map<String, Map<String, String>> indexMap = indexProviderEntry.getValue();
        if (indexMap.containsKey(indexName)) {
          indexProviderMap.get(indexProviderEntry.getKey()).remove(indexName);
        }
      }
    }
  }

  public void renameIndexWithStatus(String indexProvider, String oldIndexName,
      String newIndexName, String indexStatus) {
    if (null != indexProviderMap) {
      Map<String, String> properties = indexProviderMap.get(indexProvider).remove(oldIndexName);
      if (properties != null) {
        properties.put(CarbonCommonConstants.INDEX_STATUS, indexStatus);
        indexProviderMap.get(indexProvider).put(newIndexName, properties);
      }
    }
  }

  public void updateIndexStatus(String indexProvider, String indexName, String indexStatus) {
    if (null != indexProviderMap) {
      indexProviderMap.get(indexProvider).get(indexName)
          .put(CarbonCommonConstants.INDEX_STATUS, indexStatus);
    }
  }

  public List<String> getIndexTables() {
    if (null != indexProviderMap) {
      List<String> indexTables = new ArrayList<>();
      for (Map.Entry<String, Map<String, Map<String, String>>> mapEntry : indexProviderMap
          .entrySet()) {
        indexTables.addAll(mapEntry.getValue().keySet());
      }
      return indexTables;
    } else {
      return new ArrayList<String>();
    }
  }

  public List<String> getIndexTables(String indexProvider) {
    if (null != indexProviderMap && null != indexProviderMap.get(indexProvider)) {
      return new ArrayList<>(indexProviderMap.get(indexProvider).keySet());
    } else {
      return new ArrayList<>();
    }
  }

  /**
   * indexTableMap will be null if index table info is not loaded.
   */
  public Map<String, Map<String, Map<String, String>>> getIndexesMap() {
    return indexProviderMap;
  }

  public String getParentTableName() {
    return parentTableName;
  }

  public boolean isIndexTable() {
    return isIndexTable;
  }

  public String getParentTablePath() {
    return parentTablePath;
  }

  public String getParentTableId() {
    return parentTableId;
  }

  public String serialize() throws IOException {
    return ObjectSerializationUtil.convertObjectToString(this);
  }

  public static IndexMetadata deserialize(String serializedIndexMeta) throws IOException {
    if (null == serializedIndexMeta) {
      return null;
    }
    return (IndexMetadata) ObjectSerializationUtil.convertStringToObject(serializedIndexMeta);
  }

  public String getIndexColumns(String provider, String indexName) {
    return indexProviderMap.get(provider).get(indexName).get(CarbonCommonConstants.INDEX_COLUMNS);
  }

  public String getIndexStatus(String provider, String indexName) {
    return indexProviderMap.get(provider).get(indexName).get(CarbonCommonConstants.INDEX_STATUS);
  }
}
