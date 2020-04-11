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

package org.apache.carbondata.core.index.status;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.NoSuchIndexException;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;

/**
 * Maintains the status of each index. As per the status query will decide whether to hit index
 * or not.
 */
public class IndexStatusManager {

  // Create private constructor to not allow create instance of it
  private IndexStatusManager() {

  }

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(IndexStatusManager.class.getName());

  /**
   * TODO Use factory when we have more storage providers
   */
  private static IndexStatusStorageProvider storageProvider =
      getDataMapStatusStorageProvider();

  /**
   * Reads all datamap status file
   * @return
   * @throws IOException
   */
  public static IndexStatusDetail[] readDataMapStatusDetails() throws IOException {
    return storageProvider.getIndexStatusDetails();
  }

  /**
   * Get enabled datamap status details
   * @return
   * @throws IOException
   */
  public static IndexStatusDetail[] getEnabledDataMapStatusDetails() throws IOException {
    IndexStatusDetail[] indexStatusDetails = storageProvider.getIndexStatusDetails();
    List<IndexStatusDetail> statusDetailList = new ArrayList<>();
    for (IndexStatusDetail statusDetail : indexStatusDetails) {
      if (statusDetail.getStatus() == IndexStatus.ENABLED) {
        statusDetailList.add(statusDetail);
      }
    }
    return statusDetailList.toArray(new IndexStatusDetail[statusDetailList.size()]);
  }

  public static Map<String, IndexStatusDetail> readDataMapStatusMap() throws IOException {
    IndexStatusDetail[] details = storageProvider.getIndexStatusDetails();
    Map<String, IndexStatusDetail> map = new HashMap<>(details.length);
    for (IndexStatusDetail detail : details) {
      map.put(detail.getDataMapName(), detail);
    }
    return map;
  }

  public static void disableIndex(String indexName) throws IOException, NoSuchIndexException {
    IndexSchema indexSchema = getIndexSchema(indexName);
    if (indexSchema != null) {
      List<IndexSchema> list = new ArrayList<>();
      list.add(indexSchema);
      storageProvider.updateIndexStatus(list, IndexStatus.DISABLED);
    }
  }

  /**
   * This method will disable all lazy (DEFERRED REFRESH) index in the given table
   */
  public static void disableAllLazyIndexes(CarbonTable table) throws IOException {
    List<IndexSchema> allIndexSchemas =
        IndexStoreManager.getInstance().getIndexSchemasOfTable(table);
    List<IndexSchema> dataMapToBeDisabled = new ArrayList<>(allIndexSchemas.size());
    for (IndexSchema indexSchema : allIndexSchemas) {
      if (indexSchema.isLazy()) {
        dataMapToBeDisabled.add(indexSchema);
      }
    }
    storageProvider.updateIndexStatus(dataMapToBeDisabled, IndexStatus.DISABLED);
  }

  public static void enableIndex(String indexName) throws IOException, NoSuchIndexException {
    IndexSchema indexSchema = getIndexSchema(indexName);
    if (indexSchema != null) {
      List<IndexSchema> list = new ArrayList<>();
      list.add(indexSchema);
      storageProvider.updateIndexStatus(list, IndexStatus.ENABLED);
    }
  }

  public static void dropIndex(String indexName) throws IOException, NoSuchIndexException {
    IndexSchema indexSchema = getIndexSchema(indexName);
    if (indexSchema != null) {
      List<IndexSchema> list = new ArrayList<>();
      list.add(indexSchema);
      storageProvider.updateIndexStatus(list, IndexStatus.DROPPED);
    }
  }

  public static IndexSchema getIndexSchema(String indexName)
      throws IOException, NoSuchIndexException {
    return IndexStoreManager.getInstance().getIndexSchema(indexName);
  }

  /**
   * This method will remove all segments of dataMap table in case of Insert-Overwrite/Update/Delete
   * operations on main table
   *
   * @param allIndexSchemas of main carbon table
   * @throws IOException
   */
  public static void truncateIndex(List<IndexSchema> allIndexSchemas)
      throws IOException, NoSuchIndexException {
    for (IndexSchema indexSchema : allIndexSchemas) {
      if (!indexSchema.isLazy()) {
        disableIndex(indexSchema.getIndexName());
      }
      RelationIdentifier dataMapRelationIdentifier = indexSchema.getRelationIdentifier();
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

  public static IndexStatusStorageProvider getDataMapStatusStorageProvider() {
    String providerProperties = CarbonProperties.getDataMapStorageProvider();
    switch (providerProperties) {
      case CarbonCommonConstants.CARBON_DATAMAP_SCHEMA_STORAGE_DATABASE:
        return new DatabaseIndexStatusProvider();
      case CarbonCommonConstants.CARBON_DATAMAP_SCHEMA_STORAGE_DISK:
      default:
        return new DiskBasedIndexStatusProvider();
    }
  }
}
