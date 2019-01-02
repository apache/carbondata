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
package org.apache.carbondata.core.datamap.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;

/**
 * Maintains the status of each datamap. As per the status query will decide whether to hit datamap
 * or not.
 */
public class DataMapStatusManager {

  // Create private constructor to not allow create instance of it
  private DataMapStatusManager() {

  }

  /**
   * TODO Use factory when we have more storage providers
   */
  private static DataMapStatusStorageProvider storageProvider =
      new DiskBasedDataMapStatusProvider();

  /**
   * Reads all datamap status file
   * @return
   * @throws IOException
   */
  public static DataMapStatusDetail[] readDataMapStatusDetails() throws IOException {
    return storageProvider.getDataMapStatusDetails();
  }

  /**
   * Get enabled datamap status details
   * @return
   * @throws IOException
   */
  public static DataMapStatusDetail[] getEnabledDataMapStatusDetails() throws IOException {
    DataMapStatusDetail[] dataMapStatusDetails = storageProvider.getDataMapStatusDetails();
    List<DataMapStatusDetail> statusDetailList = new ArrayList<>();
    for (DataMapStatusDetail statusDetail : dataMapStatusDetails) {
      if (statusDetail.getStatus() == DataMapStatus.ENABLED) {
        statusDetailList.add(statusDetail);
      }
    }
    return statusDetailList.toArray(new DataMapStatusDetail[statusDetailList.size()]);
  }

  public static Map<String, DataMapStatusDetail> readDataMapStatusMap() throws IOException {
    DataMapStatusDetail[] details = storageProvider.getDataMapStatusDetails();
    Map<String, DataMapStatusDetail> map = new HashMap<>(details.length);
    for (DataMapStatusDetail detail : details) {
      map.put(detail.getDataMapName(), detail);
    }
    return map;
  }

  public static void disableDataMap(String dataMapName) throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = getDataMapSchema(dataMapName);
    if (dataMapSchema != null) {
      List<DataMapSchema> list = new ArrayList<>();
      list.add(dataMapSchema);
      storageProvider.updateDataMapStatus(list, DataMapStatus.DISABLED);
    }
  }

  /**
   * This method will disable all lazy (DEFERRED REBUILD) datamap in the given table
   */
  public static void disableAllLazyDataMaps(CarbonTable table) throws IOException {
    List<DataMapSchema> allDataMapSchemas =
        DataMapStoreManager.getInstance().getDataMapSchemasOfTable(table);
    List<DataMapSchema> dataMapToBeDisabled = new ArrayList<>(allDataMapSchemas.size());
    for (DataMapSchema dataMap : allDataMapSchemas) {
      // TODO all non datamaps like MV is now supports only lazy. Once the support is made the
      // following check can be removed.
      if (dataMap.isLazy() || !dataMap.isIndexDataMap()) {
        dataMapToBeDisabled.add(dataMap);
      }
    }
    storageProvider.updateDataMapStatus(dataMapToBeDisabled, DataMapStatus.DISABLED);
  }

  public static void enableDataMap(String dataMapName) throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = getDataMapSchema(dataMapName);
    if (dataMapSchema != null) {
      List<DataMapSchema> list = new ArrayList<>();
      list.add(dataMapSchema);
      storageProvider.updateDataMapStatus(list, DataMapStatus.ENABLED);
    }
  }

  public static void dropDataMap(String dataMapName) throws IOException, NoSuchDataMapException {
    DataMapSchema dataMapSchema = getDataMapSchema(dataMapName);
    if (dataMapSchema != null) {
      List<DataMapSchema> list = new ArrayList<>();
      list.add(dataMapSchema);
      storageProvider.updateDataMapStatus(list, DataMapStatus.DROPPED);
    }
  }

  private static DataMapSchema getDataMapSchema(String dataMapName)
      throws IOException, NoSuchDataMapException {
    return DataMapStoreManager.getInstance().getDataMapSchema(dataMapName);
  }

}
