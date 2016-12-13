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
 * Module Name   : CARBON spark interface
 * Author    : R00903928
 * Created Date  : 22-Sep-2015
 * FileName   : DeleteLoadFolders.java
 * Description   : for physical deletion of load folders.
 * Class Version  : 1.0
 */
package org.apache.carbondata.spark.load;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.processing.model.CarbonLoadModel;

public final class DeleteLoadFolders {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DeleteLoadFolders.class.getName());

  private DeleteLoadFolders() {

  }

  /**
   * returns segment path
   *
   * @param loadModel
   * @param storeLocation
   * @param partitionId
   * @param oneLoad
   * @return
   */
  private static String getSegmentPath(CarbonLoadModel loadModel, String storeLocation,
      int partitionId, LoadMetadataDetails oneLoad) {

    String path = null;
    String segmentId = oneLoad.getLoadName();

    path = new CarbonStorePath(storeLocation).getCarbonTablePath(
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getCarbonTableIdentifier())
        .getCarbonDataDirectoryPath("" + partitionId, segmentId);
    return path;
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
        if(status){
          if(!file.delete()){
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
    if ((CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oneLoad.getLoadStatus())
        || CarbonCommonConstants.SEGMENT_COMPACTED.equalsIgnoreCase(oneLoad.getLoadStatus()))
        && oneLoad.getVisibility().equalsIgnoreCase("true")) {
      if (isForceDelete) {
        return true;
      }
      String deletionTime = oneLoad.getModificationOrdeletionTimesStamp();
      SimpleDateFormat parser = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
      Date deletionDate = null;
      String date = null;
      Date currentTimeStamp = null;
      try {
        deletionDate = parser.parse(deletionTime);
        date = CarbonLoaderUtil.readCurrentTime();
        currentTimeStamp = parser.parse(date);
      } catch (ParseException e) {
        return false;
      }

      long difference = currentTimeStamp.getTime() - deletionDate.getTime();

      long minutesElapsed = (difference / (1000 * 60));

      int maxTime;
      try {
        maxTime = Integer.parseInt(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME));
      } catch (NumberFormatException e) {
        maxTime = CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
      }
      if (minutesElapsed > maxTime) {
        return true;
      }

    }

    return false;
  }

  private static void factFileRenaming(String loadFolderPath) {

    FileFactory.FileType fileType = FileFactory.getFileType(loadFolderPath);
    try {
      if (FileFactory.isFileExist(loadFolderPath, fileType)) {
        CarbonFile loadFolder = FileFactory.getCarbonFile(loadFolderPath, fileType);

        CarbonFile[] listFiles = loadFolder.listFiles(new CarbonFileFilter() {

          @Override public boolean accept(CarbonFile file) {
            return (file.getName().endsWith('_' + CarbonCommonConstants.FACT_FILE_UPDATED));
          }
        });

        for (CarbonFile file : listFiles) {
          if (!file.renameTo(file.getName().substring(0,
              file.getName().length() - CarbonCommonConstants.FACT_FILE_UPDATED.length()))) {
            LOGGER.warn("could not rename the updated fact file.");
          }
        }

      }
    } catch (IOException e) {
      LOGGER.error("exception" + e.getMessage());
    }

  }

  private static void cleanDeletedFactFile(String loadFolderPath) {
    FileFactory.FileType fileType = FileFactory.getFileType(loadFolderPath);
    try {
      if (FileFactory.isFileExist(loadFolderPath, fileType)) {
        CarbonFile loadFolder = FileFactory.getCarbonFile(loadFolderPath, fileType);

        CarbonFile[] listFiles = loadFolder.listFiles(new CarbonFileFilter() {

          @Override public boolean accept(CarbonFile file) {
            return (file.getName().endsWith(CarbonCommonConstants.FACT_DELETE_EXTENSION));
          }
        });

        for (CarbonFile file : listFiles) {
          if (!file.delete()) {
            LOGGER.warn("could not delete the marked fact file.");
          }
        }

      }
    } catch (IOException e) {
      LOGGER.error("exception" + e.getMessage());
    }
  }

  /**
   * @param loadModel
   * @param storeLocation
   * @param isForceDelete
   * @param details
   * @return
   *
   */
  public static boolean deleteLoadFoldersFromFileSystem(CarbonLoadModel loadModel,
      String storeLocation, boolean isForceDelete, LoadMetadataDetails[] details) {
    List<LoadMetadataDetails> deletedLoads =
        new ArrayList<LoadMetadataDetails>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    boolean isDeleted = false;

    if (details != null && details.length != 0) {
      for (LoadMetadataDetails oneLoad : details) {
        if (checkIfLoadCanBeDeleted(oneLoad, isForceDelete)) {
          String path = getSegmentPath(loadModel, storeLocation, 0, oneLoad);
          boolean deletionStatus = physicalFactAndMeasureMetadataDeletion(path);
          if (deletionStatus) {
            isDeleted = true;
            oneLoad.setVisibility("false");
            deletedLoads.add(oneLoad);
            LOGGER.info("Info: " +
                " Deleted the load " + oneLoad.getLoadName());
          }
        }
      }
    }

    return isDeleted;
  }


}
