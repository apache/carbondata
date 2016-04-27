/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/**
 * Project Name  : Carbon
 * Module Name   : CARBON Data Processor
 * Author    : R00903928
 * Created Date  : 21-Sep-2015
 * FileName   : DeleteLoadFromMetadata.java
 * Description   : Kettle step to generate MD Key
 * Class Version  : 1.0
 */
package org.carbondata.integration.spark.load;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.fileperations.FileWriteOperation;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.locks.CarbonLockFactory;
import org.carbondata.core.locks.ICarbonLock;
import org.carbondata.core.locks.LockUsage;
import org.carbondata.core.util.CarbonCoreLogEvent;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.integration.spark.util.CarbonSparkInterFaceLogEvent;

import com.google.gson.Gson;

public final class DeleteLoadFromMetadata {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteLoadFromMetadata.class.getName());

  private DeleteLoadFromMetadata() {

  }

  public static List<String> updateDeletionStatus(List<String> loadIds, String cubeFolderPath) {
    ICarbonLock carbonLock =
        CarbonLockFactory.getCarbonLockObj(cubeFolderPath, LockUsage.METADATA_LOCK);
    BufferedWriter brWriter = null;
    List<String> invalidLoadIds = new ArrayList<String>(0);
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
            "Metadata lock has been successfully acquired");

        String dataLoadLocation = cubeFolderPath + CarbonCommonConstants.FILE_SEPARATOR
            + CarbonCommonConstants.LOADMETADATA_FILENAME;

        DataOutputStream dataOutputStream = null;
        Gson gsonObjectToWrite = new Gson();
        LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;

        if (!FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation))) {
          // log error.
          LOGGER
              .error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Load metadata file is not present.");
          return loadIds;
        }
        // read existing metadata details in load metadata.
        listOfLoadFolderDetailsArray = CarbonUtil.readLoadMetadata(cubeFolderPath);
        if (listOfLoadFolderDetailsArray != null && listOfLoadFolderDetailsArray.length != 0) {
          updateDeletionStatusInDetails(loadIds, listOfLoadFolderDetailsArray, invalidLoadIds);
          if (!invalidLoadIds.isEmpty()) {
            LOGGER.warn(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
                "Load doesnt exist or it is already deleted , LoadSeqId-" + invalidLoadIds);
          }

          AtomicFileOperations fileWrite = new AtomicFileOperationsImpl(dataLoadLocation,
              FileFactory.getFileType(dataLoadLocation));

          // write the updated data into the metadata file.

          try {
            dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
            brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
                CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT));

            String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetailsArray);
            brWriter.write(metadataInstance);
          } finally {
            if (null != brWriter) {
              brWriter.flush();
            }
            CarbonUtil.closeStreams(brWriter);
          }

          fileWrite.close();

        } else {
          LOGGER.warn(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
              "Load doesnt exist or it is already deleted , LoadSeqId-" + loadIds);
          return loadIds;
        }

      } else {
        LOGGER
            .error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Unable to acquire the metadata lock");
      }
    } catch (IOException e) {
      LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "IOException" + e.getMessage());
    } finally {
      fileUnlock(carbonLock);
    }

    return invalidLoadIds;
  }

  public static void fileUnlock(ICarbonLock carbonLock) {
    if (carbonLock.unlock()) {
      LOGGER.info(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG,
          "Metadata lock has been successfully released");
    } else {
      LOGGER
          .error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Not able to release the metadata lock");
    }
  }

  public static void updateDeletionStatusInDetails(List<String> loadIds,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray, List<String> invalidLoadIds) {
    for (String loadId : loadIds) {
      boolean loadFound = false;
      // For each load id loop through data and if the
      // load id is found then mark
      // the metadata as deleted.
      for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {

        if (loadId.equalsIgnoreCase(loadMetadata.getLoadName())) {
          loadFound = true;
          if (!CarbonCommonConstants.MARKED_FOR_DELETE.equals(loadMetadata.getLoadStatus())) {
            loadMetadata.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
            loadMetadata.setModificationOrdeletionTimesStamp(CarbonLoaderUtil.readCurrentTime());
            LOGGER.info(CarbonSparkInterFaceLogEvent.UNIBI_CARBON_SPARK_INTERFACE_MSG,
                "LoadId " + loadId + " Marked for Delete");
          } else {
            // it is already deleted . can not delete it again.
            invalidLoadIds.add(loadId);
          }

          break;
        }
      }

      if (!loadFound) {
        invalidLoadIds.add(loadId);
      }

    }

  }

}
