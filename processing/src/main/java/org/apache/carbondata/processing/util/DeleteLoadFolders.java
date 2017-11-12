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

package org.apache.carbondata.processing.util;

import java.io.IOException;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

public final class DeleteLoadFolders {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteLoadFolders.class.getName());

  private DeleteLoadFolders() {

  }

  /**
   * returns segment path
   *
   * @param dbName
   * @param tableName
   * @param storeLocation
   * @param partitionId
   * @param oneLoad
   * @return
   */
  private static String getSegmentPath(String dbName, String tableName, String storeLocation,
      int partitionId, LoadMetadataDetails oneLoad) {
    CarbonTablePath carbon = new CarbonStorePath(storeLocation).getCarbonTablePath(
        new CarbonTableIdentifier(dbName, tableName, ""));
    String segmentId = oneLoad.getLoadName();
    return carbon.getCarbonDataDirectoryPath("" + partitionId, segmentId);
  }

  private static boolean physicalFactAndMeasureMetadataDeletion(String path) {

    boolean status = false;
    try {
      if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
        CarbonFile file = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));
        CarbonFile[] filesToBeDeleted = file.listFiles(new CarbonFileFilter() {

          @Override public boolean accept(CarbonFile file) {
            return (CarbonTablePath.isCarbonDataFile(file.getName())
                || CarbonTablePath.isCarbonIndexFile(file.getName()));
          }
        });

        //if there are no fact and msr metadata files present then no need to keep
        //entry in metadata.
        if (filesToBeDeleted.length == 0) {
          status = true;
        } else {

          for (CarbonFile eachFile : filesToBeDeleted) {
            if (!eachFile.delete()) {
              LOGGER.warn("Unable to delete the file as per delete command "
                  + eachFile.getAbsolutePath());
              status = false;
            } else {
              status = true;
            }
          }
        }
        // need to delete the complete folder.
        if (status) {
          if (!file.delete()) {
            LOGGER.warn("Unable to delete the folder as per delete command "
                + file.getAbsolutePath());
            status = false;
          }
        }

      } else {
        status = false;
      }
    } catch (IOException e) {
      LOGGER.warn("Unable to delete the file as per delete command " + path);
    }

    return status;

  }

  private static boolean checkIfLoadCanBeDeleted(LoadMetadataDetails oneLoad,
      boolean isForceDelete) {
    if ((SegmentStatus.MARKED_FOR_DELETE == oneLoad.getSegmentStatus() ||
        SegmentStatus.COMPACTED == oneLoad.getSegmentStatus())
        && oneLoad.getVisibility().equalsIgnoreCase("true")) {
      if (isForceDelete) {
        return true;
      }
      long deletionTime = oneLoad.getModificationOrdeletionTimesStamp();

      return CarbonUpdateUtil.isMaxQueryTimeoutExceeded(deletionTime);

    }

    return false;
  }

  public static boolean deleteLoadFoldersFromFileSystem(String dbName, String tableName,
      String storeLocation, boolean isForceDelete, LoadMetadataDetails[] details) {

    boolean isDeleted = false;

    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, isForceDelete)) {
          String path = getSegmentPath(dbName, tableName, storeLocation, 0, oneLoad);
          boolean deletionStatus = physicalFactAndMeasureMetadataDeletion(path);
          if (deletionStatus) {
            isDeleted = true;
            oneLoad.setVisibility("false");
            LOGGER.info("Info: Deleted the load " + oneLoad.getLoadName());
          }
        }
      }
    }

    return isDeleted;
  }


}
