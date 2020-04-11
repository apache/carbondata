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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.index.IndexUtil;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.schema.table.IndexSchema;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * It saves/serializes the array of {{@link IndexStatusDetail}} to disk in json format.
 * It ensures the data consistance while concurrent write through write lock. It saves the status
 * to the index status under the system folder.
 */
public class DiskBasedIndexStatusProvider implements IndexStatusStorageProvider {

  private static final Logger LOG =
      LogServiceFactory.getLogService(DiskBasedIndexStatusProvider.class.getName());

  private static final String DATAMAP_STATUS_FILE = "datamapstatus";

  @Override
  public IndexStatusDetail[] getIndexStatusDetails() throws IOException {
    String statusPath = CarbonProperties.getInstance().getSystemFolderLocation()
        + CarbonCommonConstants.FILE_SEPARATOR + DATAMAP_STATUS_FILE;
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    IndexStatusDetail[] indexStatusDetails;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return new IndexStatusDetail[0];
      }
      dataInputStream = FileFactory.getDataInputStream(statusPath);
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      indexStatusDetails = gsonObjectToRead.fromJson(buffReader, IndexStatusDetail[].class);
    } catch (IOException e) {
      LOG.error("Failed to read datamap status", e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    // if indexStatusDetails is null, return empty array
    if (null == indexStatusDetails) {
      return new IndexStatusDetail[0];
    }

    return indexStatusDetails;
  }

  /**
   * Update or add the status of passed datamaps with the given datamapstatus. If the datamapstatus
   * given is enabled/disabled then updates/adds the datamap, in case of drop it just removes it
   * from the file.
   * This method always overwrites the old file.
   * @param indexSchemas schemas of which are need to be updated in index status
   * @param indexStatus  status to be updated for the index schemas
   * @throws IOException
   */
  @Override
  public void updateIndexStatus(List<IndexSchema> indexSchemas, IndexStatus indexStatus)
      throws IOException {
    if (indexSchemas == null || indexSchemas.size() == 0) {
      // There is nothing to update
      return;
    }
    ICarbonLock carbonTableStatusLock = getDataMapStatusLock();
    boolean locked = false;
    try {
      locked = carbonTableStatusLock.lockWithRetries();
      if (locked) {
        LOG.info("Datamap status lock has been successfully acquired.");
        if (indexStatus == IndexStatus.ENABLED && !indexSchemas.get(0).isIndex()) {
          // Enable datamap only if datamap tables and main table are in sync
          if (!canIndexBeEnabled(indexSchemas.get(0))) {
            return;
          }
        }
        IndexStatusDetail[] indexStatusDetails = getIndexStatusDetails();
        List<IndexStatusDetail> dataMapStatusList = Arrays.asList(indexStatusDetails);
        dataMapStatusList = new ArrayList<>(dataMapStatusList);
        List<IndexStatusDetail> changedStatusDetails = new ArrayList<>();
        List<IndexStatusDetail> newStatusDetails = new ArrayList<>();
        for (IndexSchema indexSchema : indexSchemas) {
          boolean exists = false;
          for (IndexStatusDetail statusDetail : dataMapStatusList) {
            if (statusDetail.getDataMapName().equals(indexSchema.getIndexName())) {
              statusDetail.setStatus(indexStatus);
              changedStatusDetails.add(statusDetail);
              exists = true;
            }
          }
          if (!exists) {
            newStatusDetails
                .add(new IndexStatusDetail(indexSchema.getIndexName(), indexStatus));
          }
        }
        // Add the newly added index to the list.
        if (newStatusDetails.size() > 0 && indexStatus != IndexStatus.DROPPED) {
          dataMapStatusList.addAll(newStatusDetails);
        }
        // In case of dropped index, just remove from the list.
        if (indexStatus == IndexStatus.DROPPED) {
          dataMapStatusList.removeAll(changedStatusDetails);
        }
        writeLoadDetailsIntoFile(CarbonProperties.getInstance().getSystemFolderLocation()
                + CarbonCommonConstants.FILE_SEPARATOR + DATAMAP_STATUS_FILE,
            dataMapStatusList.toArray(new IndexStatusDetail[dataMapStatusList.size()]));
      } else {
        String errorMsg = "Upadating index status is failed due to another process taken the lock"
            + " for updating it";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.DATAMAP_STATUS_LOCK);
      }
    }
  }

  /**
   * This method checks if main table and index table are synchronised or not. If synchronised
   * return true to enable the index
   *
   * @param indexSchema of datamap to be disabled or enabled
   * @return flag to enable or disable index
   * @throws IOException
   */
  private boolean canIndexBeEnabled(IndexSchema indexSchema) throws IOException {
    boolean isIndexInSync = true;
    String metaDataPath =
        CarbonTablePath.getMetadataPath(indexSchema.getRelationIdentifier().getTablePath());
    LoadMetadataDetails[] indexLoadMetadataDetails =
        SegmentStatusManager.readLoadMetadata(metaDataPath);
    Map<String, List<String>> indexSegmentMap = new HashMap<>();
    for (LoadMetadataDetails loadMetadataDetail : indexLoadMetadataDetails) {
      if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.SUCCESS) {
        Map<String, List<String>> segmentMap =
            MVSegmentStatusUtil.getSegmentMap(loadMetadataDetail.getExtraInfo());
        if (indexSegmentMap.isEmpty()) {
          indexSegmentMap.putAll(segmentMap);
        } else {
          for (Map.Entry<String, List<String>> entry : segmentMap.entrySet()) {
            if (null != indexSegmentMap.get(entry.getKey())) {
              indexSegmentMap.get(entry.getKey()).addAll(entry.getValue());
            }
          }
        }
      }
    }
    List<RelationIdentifier> parentTables = indexSchema.getParentTables();
    for (RelationIdentifier parentTable : parentTables) {
      List<String> mainTableValidSegmentList =
          IndexUtil.getMainTableValidSegmentList(parentTable);
      if (!mainTableValidSegmentList.isEmpty() && !indexSegmentMap.isEmpty()) {
        isIndexInSync = indexSegmentMap.get(
            parentTable.getDatabaseName() + CarbonCommonConstants.POINT + parentTable
                .getTableName()).containsAll(mainTableValidSegmentList);
      } else if (indexSegmentMap.isEmpty() && !mainTableValidSegmentList.isEmpty()) {
        isIndexInSync = false;
      }
    }
    return isIndexInSync;
  }

  /**
   * writes index status details
   *
   * @param indexStatusDetails
   * @throws IOException
   */
  private static void writeLoadDetailsIntoFile(String location,
      IndexStatusDetail[] indexStatusDetails) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(location);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the datamap status file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(indexStatusDetails);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

  private static ICarbonLock getDataMapStatusLock() {
    return CarbonLockFactory
        .getSystemLevelCarbonLockObj(CarbonProperties.getInstance().getSystemFolderLocation(),
            LockUsage.DATAMAP_STATUS_LOCK);
  }
}
