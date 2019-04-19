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
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;

/**
 * Maintains the status of each datamap. As per the status query will decide whether to hit datamap
 * or not.
 */
public class DataMapStatusManager {

  // Create private constructor to not allow create instance of it
  private DataMapStatusManager() {

  }

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DataMapStatusManager.class.getName());

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
      if (dataMap.isLazy()) {
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

  public static DataMapSchema getDataMapSchema(String dataMapName)
      throws IOException, NoSuchDataMapException {
    return DataMapStoreManager.getInstance().getDataMapSchema(dataMapName);
  }

  /**
   * This method will remove all segments of dataMap table in case of Insert-Overwrite/Update/Delete
   * operations on main table
   *
   * @param allDataMapSchemas of main carbon table
   * @throws IOException
   */
  public static void truncateDataMap(List<DataMapSchema> allDataMapSchemas)
      throws IOException, NoSuchDataMapException {
    for (DataMapSchema datamapschema : allDataMapSchemas) {
      if (!datamapschema.isLazy()) {
        disableDataMap(datamapschema.getDataMapName());
      }
      RelationIdentifier dataMapRelationIdentifier = datamapschema.getRelationIdentifier();
      SegmentStatusManager segmentStatusManager = new SegmentStatusManager(AbsoluteTableIdentifier
          .from(dataMapRelationIdentifier.getTablePath(),
              dataMapRelationIdentifier.getDatabaseName(),
              dataMapRelationIdentifier.getTableName()));
      ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
      try {
        if (carbonLock.lockWithRetries()) {
          LOGGER.info("Acquired lock for table" + dataMapRelationIdentifier.getDatabaseName() + "."
              + dataMapRelationIdentifier.getTableName() + " for table status updation");
          String metaDataPath =
              CarbonTablePath.getMetadataPath(dataMapRelationIdentifier.getTablePath());
          LoadMetadataDetails[] loadMetadataDetails =
              SegmentStatusManager.readLoadMetadata(metaDataPath);
          for (LoadMetadataDetails entry : loadMetadataDetails) {
            entry.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          }
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(dataMapRelationIdentifier.getTablePath()),
              loadMetadataDetails);
        } else {
          LOGGER.error("Not able to acquire the lock for Table status updation for table "
              + dataMapRelationIdentifier.getDatabaseName() + "." + dataMapRelationIdentifier
              .getTableName());
        }
      } finally {
        if (carbonLock.unlock()) {
          LOGGER.info(
              "Table unlocked successfully after table status updation" + dataMapRelationIdentifier
                  .getDatabaseName() + "." + dataMapRelationIdentifier.getTableName());
        } else {
          LOGGER.error(
              "Unable to unlock Table lock for table" + dataMapRelationIdentifier.getDatabaseName()
                  + "." + dataMapRelationIdentifier.getTableName()
                  + " during table status updation");
        }
      }
    }
  }
}
