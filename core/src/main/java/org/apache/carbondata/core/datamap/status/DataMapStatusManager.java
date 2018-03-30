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
import java.util.List;

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

  public static void disableDataMap(String dataMapName) throws Exception {
    DataMapSchema dataMapSchema = validateDataMap(dataMapName, false);
    List<DataMapSchema> list = new ArrayList<>();
    if (dataMapSchema != null) {
      list.add(dataMapSchema);
    }
    storageProvider.updateDataMapStatus(list, DataMapStatus.DISABLED);
  }

  public static void disableDataMapsOfTable(CarbonTable table) throws IOException {
    List<DataMapSchema> allDataMapSchemas =
        DataMapStoreManager.getInstance().getAllDataMapSchemas(table);
    storageProvider.updateDataMapStatus(allDataMapSchemas, DataMapStatus.DISABLED);
  }

  public static void enableDataMap(String dataMapName) throws IOException {
    DataMapSchema dataMapSchema = validateDataMap(dataMapName, false);
    List<DataMapSchema> list = new ArrayList<>();
    if (dataMapSchema != null) {
      list.add(dataMapSchema);
    }
    storageProvider.updateDataMapStatus(list, DataMapStatus.ENABLED);
  }

  public static void enableDataMapsOfTable(CarbonTable table) throws IOException {
    List<DataMapSchema> allDataMapSchemas =
        DataMapStoreManager.getInstance().getAllDataMapSchemas(table);
    storageProvider.updateDataMapStatus(allDataMapSchemas, DataMapStatus.ENABLED);
  }

  public static void dropDataMap(String dataMapName) throws IOException {
    DataMapSchema dataMapSchema = validateDataMap(dataMapName, false);
    List<DataMapSchema> list = new ArrayList<>();
    if (dataMapSchema != null) {
      list.add(dataMapSchema);
    }
    storageProvider.updateDataMapStatus(list, DataMapStatus.DROPPED);
  }

  private static DataMapSchema validateDataMap(String dataMapName, boolean valdate) {
    List<DataMapSchema> allDataMapSchemas =
        DataMapStoreManager.getInstance().getAllDataMapSchemas();
    DataMapSchema dataMapSchema = null;
    for (DataMapSchema schema : allDataMapSchemas) {
      if (schema.getDataMapName().equalsIgnoreCase(dataMapName)) {
        dataMapSchema = schema;
      }
    }
    if (dataMapSchema == null && valdate) {
      throw new UnsupportedOperationException("Cannot be disabled non exist datamap");
    } else {
      return dataMapSchema;
    }
  }

}
