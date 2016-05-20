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

package org.carbondata.spark.merger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.spark.load.CarbonLoadModel;
import org.carbondata.spark.load.CarbonLoaderUtil;
import org.carbondata.spark.load.DeleteLoadFolders;
import org.carbondata.spark.util.LoadMetadataUtil;

/**
 * utility class for load merging.
 */
public final class CarbonDataMergerUtil {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonDataMergerUtil.class.getName());

  private CarbonDataMergerUtil() {

  }

  public static boolean executeMerging(CarbonLoadModel carbonLoadModel, String storeLocation,
      String hdfsStoreLocation, int currentRestructNumber, String metadataFilePath,
      List<String> loadsToMerge, String mergedLoadName) throws Exception {
    // TODO: Implement it
    return false;

  }

  public static List<String> getLoadsToMergeFromHDFS(String storeLocation, FileType fileType,
      String metadataPath, CarbonLoadModel carbonLoadModel, int currentRestructNumber,
      int partitionCount) {
    List<String> loadNames = new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    try {
      if (!FileFactory.isFileExist(storeLocation, fileType)) {
        return null;
      }
    } catch (IOException e) {
      LOGGER.error("Error occurred :: " + e.getMessage());
    }

    int toLoadMergeMaxSize;
    try {
      toLoadMergeMaxSize = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.TO_LOAD_MERGE_MAX_SIZE,
              CarbonCommonConstants.TO_LOAD_MERGE_MAX_SIZE_DEFAULT));
    } catch (NumberFormatException e) {
      toLoadMergeMaxSize = Integer.parseInt(CarbonCommonConstants.TO_LOAD_MERGE_MAX_SIZE_DEFAULT);
    }

    LoadMetadataDetails[] loadDetails = CarbonUtil.readLoadMetadata(metadataPath);

    for (LoadMetadataDetails loadDetail : loadDetails) {
      if (loadNames.size() < 2) {
        // check if load is not deleted.
        if (checkIfLoadIsNotDeleted(loadDetail)) {
          // check if load is merged
          if (checkIfLoadIsMergedAlready(loadDetail)) {
            if (checkSizeOfloadToMerge(loadDetail, toLoadMergeMaxSize, carbonLoadModel,
                partitionCount, storeLocation, currentRestructNumber,
                loadDetail.getMergedLoadName())) {
              if (!loadNames.contains(loadDetail.getMergedLoadName())) {
                loadNames.add(loadDetail.getMergedLoadName());
              }
            }
          } else
          // take this load as To Load.
          {
            if (checkSizeOfloadToMerge(loadDetail, toLoadMergeMaxSize, carbonLoadModel,
                partitionCount, storeLocation, currentRestructNumber, loadDetail.getLoadName())) {
              loadNames.add(loadDetail.getLoadName());
            }
          }
        }
      } else {
        break;
      }
    }

    return loadNames;

  }

  private static boolean checkSizeOfloadToMerge(final LoadMetadataDetails loadDetail,
      int toLoadMergeMaxSize, CarbonLoadModel carbonLoadModel, int partitionCount,
      String storeLocation, int currentRestructNumber, final String loadNameToMatch) {

    long factSizeAcrossPartition = 0;

    for (int partition = 0; partition < partitionCount; partition++) {
      String loadPath = LoadMetadataUtil
          .createLoadFolderPath(carbonLoadModel, storeLocation, partition, currentRestructNumber);

      CarbonFile parentLoadFolder =
          FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));
      CarbonFile[] loadFiles = parentLoadFolder.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          if (file.getName().substring(file.getName().indexOf('_') + 1, file.getName().length())
              .equalsIgnoreCase(loadNameToMatch)) {
            return true;
          }
          return false;
        }
      });

      // no found load folder in current RS
      if (loadFiles.length == 0) {
        return false;
      }

      // check if fact file is present or not. this is in case of Restructure folder.
      if (!isFactFilePresent(loadFiles[0])) {
        return false;
      }
      factSizeAcrossPartition += getSizeOfFactFileInLoad(loadFiles[0]);
    }
    // check avg fact size if less than configured max size of to load.
    if (factSizeAcrossPartition < toLoadMergeMaxSize * 1024 * 1024 * 1024) {
      return true;
    }
    return false;
  }

  private static long getSizeOfFactFileInLoad(CarbonFile carbonFile) {
    long factSize = 0;

    // check if update fact is present.

    CarbonFile[] factFileUpdated = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
          return true;
        }
        return false;
      }
    });

    if (factFileUpdated.length != 0) {
      for (CarbonFile fact : factFileUpdated) {
        factSize += fact.getSize();
      }
      return factSize;
    }

    // normal fact case.
    CarbonFile[] factFile = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });

    for (CarbonFile fact : factFile) {
      factSize += fact.getSize();
    }

    return factSize;
  }

  private static boolean checkIfLoadIsMergedAlready(LoadMetadataDetails loadDetail) {
    if (null != loadDetail.getMergedLoadName()) {
      return true;
    }
    return false;
  }

  private static boolean checkIfLoadIsNotDeleted(LoadMetadataDetails loadDetail) {
    if (!loadDetail.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_DELETE)) {
      return true;
    } else {
      return false;
    }
  }

  public static boolean checkIfLoadMergingRequired(String metadataFilePath,
      CarbonLoadModel carbonLoadModel, String storeLocation, int partition,
      int currentRestructNumber) {

    String loadPath = LoadMetadataUtil
        .createLoadFolderPath(carbonLoadModel, storeLocation, 0, currentRestructNumber);

    CarbonFile parentLoadFolder =
        FileFactory.getCarbonFile(loadPath, FileFactory.getFileType(loadPath));

    //get all the load files in the current RS
    CarbonFile[] loadFiles = parentLoadFolder.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        if (file.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER)) {
          return true;
        }
        return false;
      }
    });

    String isLoadMergeEnabled = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_LOAD_MERGE,
            CarbonCommonConstants.DEFAULT_ENABLE_LOAD_MERGE);

    if (isLoadMergeEnabled.equalsIgnoreCase("false")) {
      return false;
    }

    int mergeThreshold;
    try {
      mergeThreshold = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.MERGE_THRESHOLD_VALUE,
              CarbonCommonConstants.MERGE_THRESHOLD_DEFAULT_VAL));
    } catch (NumberFormatException e) {
      mergeThreshold = Integer.parseInt(CarbonCommonConstants.MERGE_THRESHOLD_DEFAULT_VAL);
    }

    LoadMetadataDetails[] details = CarbonUtil.readLoadMetadata(metadataFilePath);

    int validLoadsNumber = getNumberOfValidLoads(details, loadFiles);

    if (validLoadsNumber > mergeThreshold + 1) {
      return true;
    } else {
      return false;
    }
  }

  private static int getNumberOfValidLoads(LoadMetadataDetails[] details, CarbonFile[] loadFiles) {
    int validLoads = 0;

    for (LoadMetadataDetails load : details) {
      if (load.getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS)
          || load.getLoadStatus()
          .equalsIgnoreCase(CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS) || load
          .getLoadStatus().equalsIgnoreCase(CarbonCommonConstants.MARKED_FOR_UPDATE)) {

        if (isLoadMetadataPresentInRsFolders(loadFiles, load.getLoadName())) {
          validLoads++;
        }
      }
    }

    return validLoads;

  }

  private static boolean isLoadMetadataPresentInRsFolders(CarbonFile[] loadFiles, String loadName) {
    for (CarbonFile load : loadFiles) {
      String nameOfLoad = load.getName()
          .substring(load.getName().indexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              load.getName().length());
      if (nameOfLoad.equalsIgnoreCase(loadName)) {
        //check if it is a RS load or not.
        CarbonFile[] factFiles = load.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile file) {
            if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT) || file.getName()
                .endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
              return true;
            }
            return false;
          }
        });

        if (factFiles.length > 0) {
          return true;
        } else {
          return false;
        }
      }

    }
    return false;
  }

  public static String getMergedLoadName(List<String> loadName) {
    String mergeLoadName = loadName.get(0);
    String timeStamp = new Date().getTime() + "";

    if (mergeLoadName.contains(CarbonCommonConstants.MERGERD_EXTENSION)) {
      String loadNum = mergeLoadName
          .substring(0, mergeLoadName.indexOf(CarbonCommonConstants.MERGERD_EXTENSION));
      return loadNum + CarbonCommonConstants.MERGERD_EXTENSION + CarbonCommonConstants.UNDERSCORE
          + timeStamp;
    } else {
      return mergeLoadName + CarbonCommonConstants.MERGERD_EXTENSION
          + CarbonCommonConstants.UNDERSCORE + timeStamp;
    }

  }

  public static void updateLoadMetadataWithMergeStatus(List<String> loadsToMerge,
      String metaDataFilepath, String MergedLoadName, CarbonLoadModel carbonLoadModel) {
    LoadMetadataDetails[] loadDetails = CarbonUtil.readLoadMetadata(metaDataFilepath);

    boolean first = true;

    for (LoadMetadataDetails loadDetail : loadDetails) {

      if (null != loadDetail.getMergedLoadName()) {
        if (loadsToMerge.contains(loadDetail.getMergedLoadName()) && first) {
          loadDetail.setMergedLoadName(MergedLoadName);
          first = false;
        } else {
          continue;
        }
      } else if (loadsToMerge.contains(loadDetail.getLoadName())) {
        if (first) {
          loadDetail.setMergedLoadName(MergedLoadName);
          first = false;
        } else {
          loadDetail.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
          loadDetail.setModificationOrdeletionTimesStamp(CarbonLoaderUtil.readCurrentTime());
        }

      }

    }

    try {
      CarbonLoaderUtil.writeLoadMetadata(carbonLoadModel.getCarbonDataLoadSchema(),
          carbonLoadModel.getDatabaseName(), carbonLoadModel.getTableName(),
          Arrays.asList(loadDetails));
    } catch (IOException e) {
      LOGGER.error("Error while writing metadata");
    }

  }

  public static void cleanUnwantedMergeLoadFolder(CarbonLoadModel loadModel, int partitionCount,
      String storeLocation, boolean isForceDelete, int currentRestructNumber) {

    CarbonTable cube = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
        .getCarbonTable(loadModel.getDatabaseName() + '_' + loadModel.getTableName());

    String loadMetadataFilePath = cube.getMetaDataFilepath();
    //String loadMetadataFilePath = CarbonLoaderUtil.extractLoadMetadataFileLocation(loadModel);

    LoadMetadataDetails[] details = CarbonUtil.readLoadMetadata(loadMetadataFilePath);

    // for first time before any load , this will be null
    if (null == details || details.length == 0) {
      return;
    }

    for (int partitionId = 0; partitionId < partitionCount; partitionId++) {

      String path = LoadMetadataUtil
          .createLoadFolderPath(loadModel, storeLocation, partitionId, currentRestructNumber);

      CarbonFile loadFolder = FileFactory.getCarbonFile(path, FileFactory.getFileType(path));

      CarbonFile[] loads = loadFolder.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          if (file.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER) && file.getName()
              .contains(CarbonCommonConstants.MERGER_FOLDER_EXT)) {
            return true;
          } else {
            return false;
          }
        }
      });

      for (int i = 0; i < loads.length; i++) {
        if (checkIfOldMergeLoadCanBeDeleted(loads[i], details)) {
          // delete merged load folder
          CarbonFile[] files = loads[i].listFiles();
          // deleting individual files
          if (files != null) {
            for (CarbonFile eachFile : files) {
              if (!eachFile.delete()) {
                LOGGER.warn("Unable to delete the file." + loadFolder.getAbsolutePath());
              }
            }

            loads[i].delete();

          }

          // delete corresponding aggregate table.

          CarbonFile[] aggFiles = LoadMetadataUtil
              .getAggregateTableList(loadModel, storeLocation, partitionId, currentRestructNumber);
          DeleteLoadFolders.deleteAggLoadFolders(aggFiles, loads[i].getName());

        }
      }
    }
  }

  private static boolean checkIfOldMergeLoadCanBeDeleted(CarbonFile eachMergeLoadFolder,
      LoadMetadataDetails[] details) {
    boolean found = false;
    for (LoadMetadataDetails loadDetail : details) {
      if (null != loadDetail.getMergedLoadName() && (CarbonCommonConstants.LOAD_FOLDER + loadDetail
          .getMergedLoadName()).equalsIgnoreCase(eachMergeLoadFolder.getName())) {
        found = true;
        break;
      }
    }

    if (!found) {
      // check the query execution time out and check the time stamp on load and delete.

      String loadName = eachMergeLoadFolder.getName();
      long loadTime = Long.parseLong(loadName
          .substring(loadName.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              loadName.length()));
      long currentTime = new Date().getTime();

      long millis = getMaxQueryTimeOut();

      if ((currentTime - loadTime) > millis) {
        // delete that merge load folder
        return true;
      }
    }

    return false;
  }

  private static long getMaxQueryTimeOut() {
    int maxTime;
    try {
      maxTime = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME));
    } catch (NumberFormatException e) {
      maxTime = CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
    }

    return maxTime * 60000;

  }

  private static boolean isFactFilePresent(CarbonFile carbonFile) {

    CarbonFile[] factFileUpdated = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_UPDATE_EXTENSION)) {
          return true;
        }
        return false;
      }
    });

    if (factFileUpdated.length != 0) {
      return true;
    }

    CarbonFile[] factFile = carbonFile.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile file) {
        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });

    if (factFile.length != 0) {
      return true;
    }

    return false;
  }

}
