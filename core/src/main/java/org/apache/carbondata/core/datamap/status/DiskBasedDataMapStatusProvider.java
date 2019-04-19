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
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMapSyncStatus;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * It saves/serializes the array of {{@link DataMapStatusDetail}} to disk in json format.
 * It ensures the data consistance while concurrent write through write lock. It saves the status
 * to the datamapstatus under the system folder.
 */
public class DiskBasedDataMapStatusProvider implements DataMapStatusStorageProvider {

  private static final Logger LOG =
      LogServiceFactory.getLogService(DiskBasedDataMapStatusProvider.class.getName());

  private static final String DATAMAP_STATUS_FILE = "datamapstatus";

  @Override
  public DataMapStatusDetail[] getDataMapStatusDetails() throws IOException {
    String statusPath = CarbonProperties.getInstance().getSystemFolderLocation()
        + CarbonCommonConstants.FILE_SEPARATOR + DATAMAP_STATUS_FILE;
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    DataMapStatusDetail[] dataMapStatusDetails;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return new DataMapStatusDetail[0];
      }
      dataInputStream =
          FileFactory.getDataInputStream(statusPath, FileFactory.getFileType(statusPath));
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      dataMapStatusDetails = gsonObjectToRead.fromJson(buffReader, DataMapStatusDetail[].class);
    } catch (IOException e) {
      LOG.error("Failed to read datamap status", e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    // if dataMapStatusDetails is null, return empty array
    if (null == dataMapStatusDetails) {
      return new DataMapStatusDetail[0];
    }

    return dataMapStatusDetails;
  }

  /**
   * Update or add the status of passed datamaps with the given datamapstatus. If the datamapstatus
   * given is enabled/disabled then updates/adds the datamap, in case of drop it just removes it
   * from the file.
   * This method always overwrites the old file.
   * @param dataMapSchemas schemas of which are need to be updated in datamap status
   * @param dataMapStatus  status to be updated for the datamap schemas
   * @throws IOException
   */
  @Override
  public void updateDataMapStatus(List<DataMapSchema> dataMapSchemas, DataMapStatus dataMapStatus)
      throws IOException {
    if (dataMapSchemas == null || dataMapSchemas.size() == 0) {
      // There is nothing to update
      return;
    }
    ICarbonLock carbonTableStatusLock = getDataMapStatusLock();
    boolean locked = false;
    try {
      locked = carbonTableStatusLock.lockWithRetries();
      if (locked) {
        LOG.info("Datamap status lock has been successfully acquired.");
        if (dataMapStatus == DataMapStatus.ENABLED && !dataMapSchemas.get(0).isIndexDataMap()) {
          // Enable datamap only if datamap tables and main table are in sync
          if (!DataMapSyncStatus.canDataMapBeEnabled(dataMapSchemas.get(0))) {
            return;
          }
        }
        DataMapStatusDetail[] dataMapStatusDetails = getDataMapStatusDetails();
        List<DataMapStatusDetail> dataMapStatusList = Arrays.asList(dataMapStatusDetails);
        dataMapStatusList = new ArrayList<>(dataMapStatusList);
        List<DataMapStatusDetail> changedStatusDetails = new ArrayList<>();
        List<DataMapStatusDetail> newStatusDetails = new ArrayList<>();
        for (DataMapSchema dataMapSchema : dataMapSchemas) {
          boolean exists = false;
          for (DataMapStatusDetail statusDetail : dataMapStatusList) {
            if (statusDetail.getDataMapName().equals(dataMapSchema.getDataMapName())) {
              statusDetail.setStatus(dataMapStatus);
              changedStatusDetails.add(statusDetail);
              exists = true;
            }
          }
          if (!exists) {
            newStatusDetails
                .add(new DataMapStatusDetail(dataMapSchema.getDataMapName(), dataMapStatus));
          }
        }
        // Add the newly added datamaps to the list.
        if (newStatusDetails.size() > 0 && dataMapStatus != DataMapStatus.DROPPED) {
          dataMapStatusList.addAll(newStatusDetails);
        }
        // In case of dropped datamap, just remove from the list.
        if (dataMapStatus == DataMapStatus.DROPPED) {
          dataMapStatusList.removeAll(changedStatusDetails);
        }
        writeLoadDetailsIntoFile(CarbonProperties.getInstance().getSystemFolderLocation()
                + CarbonCommonConstants.FILE_SEPARATOR + DATAMAP_STATUS_FILE,
            dataMapStatusList.toArray(new DataMapStatusDetail[dataMapStatusList.size()]));
      } else {
        String errorMsg = "Upadating datamapstatus is failed due to another process taken the lock"
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
   * writes datamap status details
   *
   * @param dataMapStatusDetails
   * @throws IOException
   */
  private static void writeLoadDetailsIntoFile(String location,
      DataMapStatusDetail[] dataMapStatusDetails) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(location);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the datamap status file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(dataMapStatusDetails);
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
