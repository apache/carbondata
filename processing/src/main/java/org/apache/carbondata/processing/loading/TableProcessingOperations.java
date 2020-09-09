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

package org.apache.carbondata.processing.loading;

import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.util.path.TrashUtil;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public class TableProcessingOperations {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonLoaderUtil.class.getName());

  private static List<CarbonFile> filesInTrashFolder = new ArrayList<CarbonFile>();

  /**
   * delete folder which metadata no exist in tablestatus
   * this method don't check tablestatus history.
   */
  public static void deletePartialLoadDataIfExist(CarbonTable carbonTable,
      final boolean isCompactionFlow) throws IOException {
    String metaDataLocation = carbonTable.getMetadataPath();
    String partitionPath = CarbonTablePath.getPartitionDir(carbonTable.getTablePath());
    String timeStampForTrashFolder = String.valueOf(new Timestamp(System.currentTimeMillis())
        .getTime());
    if (FileFactory.isFileExist(partitionPath)) {
      // list all segments before reading tablestatus file.
      CarbonFile[] allSegments = FileFactory.getCarbonFile(partitionPath).listFiles();
      // there is no segment
      if (allSegments == null || allSegments.length == 0) {
        return;
      }
      int retryCount = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
      int maxTimeout = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
              CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
      ICarbonLock carbonTableStatusLock = CarbonLockFactory
          .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier(), LockUsage.TABLE_STATUS_LOCK);
      try {
        if (carbonTableStatusLock.lockWithRetries(retryCount, maxTimeout)) {
          LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(metaDataLocation);
          // there is no segment or failed to read tablestatus file.
          // so it should stop immediately.
          // Do not stop if the tablestatus file does not exist, it can have stale segments.
          if (FileFactory.getCarbonFile(carbonTable.getMetadataPath() + CarbonCommonConstants
              .FILE_SEPARATOR + "tablestatus").exists() && (details == null || details
              .length == 0)) {
            return;
          }
          HashMap<CarbonFile, String> staleSegments = getStaleSegments(details, allSegments,
                  isCompactionFlow);
          // move these segments one by one to the trash folder
          for (Map.Entry<CarbonFile, String> entry : staleSegments.entrySet()) {
            TrashUtil.copyDataToTrashBySegment(entry.getKey(), carbonTable.getTablePath(),
                    timeStampForTrashFolder + CarbonCommonConstants.FILE_SEPARATOR +
                      CarbonCommonConstants.LOAD_FOLDER + entry.getValue());
            LOGGER.info("Deleting Segment: " + entry.getKey().getAbsolutePath());
            try {
              CarbonUtil.deleteFoldersAndFiles(entry.getKey());
              LOGGER.info("Deleting Segment: "  + entry.getKey());
            } catch (IOException | InterruptedException e) {
              LOGGER.error("Unable to delete the given path :: " + e.getMessage(), e);
            }
          }
          if (staleSegments.size() > 0) {
            // get the segment metadata path
            String segmentFilesLocation =
                CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath());
            // delete the segment metadata files also
            CarbonFile[] staleSegmentMetadataFiles = FileFactory.getCarbonFile(segmentFilesLocation)
                .listFiles(file -> (staleSegments.containsValue(file.getName()
                        .split(CarbonCommonConstants.UNDERSCORE)[0])));
            for (CarbonFile staleSegmentMetadataFile : staleSegmentMetadataFiles) {
              staleSegmentMetadataFile.delete();
            }
          }
        } else {
          String errorMessage =
              "Not able to acquire the Table status lock for partial load deletion for table "
                  + carbonTable.getDatabaseName() + "." + carbonTable.getTableName();
          if (isCompactionFlow) {
            LOGGER.error(errorMessage + ", retry compaction");
            throw new RuntimeException(errorMessage + ", retry compaction");
          } else {
            LOGGER.error(errorMessage);
          }
        }
      } finally {
        carbonTableStatusLock.unlock();
      }
    } else {
      ICarbonLock carbonTableStatusLock = CarbonLockFactory
          .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier(), LockUsage.TABLE_STATUS_LOCK);
      try {
        if (carbonTableStatusLock.lockWithRetries()) {
          // detect stale segments for partition table
          // logic: read table status and compare with metadata folder
          LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(metaDataLocation);
          Map<String, String> detailName = new HashMap<>();
          for (LoadMetadataDetails detail : details) {
            detailName.put(detail.getLoadName(), detail.getSegmentFile());
          }
          String segmentListPath = carbonTable.getMetadataPath() + CarbonCommonConstants
              .FILE_SEPARATOR + CarbonTablePath.SEGMENTS_METADATA_FOLDER;
          CarbonFile[] allSegment = FileFactory.getCarbonFile(segmentListPath).listFiles();
          List<String> staleSegments = new ArrayList<>();
          for (CarbonFile file : allSegment) {
            staleSegments.add(file.getName());
          }
          Iterator detailNameIterator = detailName.entrySet().iterator();
          while (detailNameIterator.hasNext()) {
            Map.Entry mapElement = (Map.Entry) detailNameIterator.next();
            staleSegments.remove(mapElement.getValue());
          }

          // handle staleSegments for alter scenario. Ignoring the segment file with the load start
          // time with loadstart time as it was written during load.
          for (LoadMetadataDetails detail : details) {
            staleSegments.remove(detail.getLoadName() + CarbonCommonConstants.UNDERSCORE +
                detail.getLoadStartTime() + CarbonTablePath.SEGMENT_EXT);
          }

          // handle scenario where updateStatusFileName is defined, no need to consider that file
          // as a stale segment.
          for (LoadMetadataDetails detail : details) {
            if (detail.getUpdateStatusFileName() != null && !detail.getUpdateStatusFileName()
                .isEmpty()) {
              if (detail.getUpdateStatusFileName().contains(CarbonCommonConstants
                  .TABLEUPDATESTATUS_FILENAME)) {
                staleSegments.remove(detail.getLoadName() +
                    CarbonCommonConstants.UNDERSCORE + detail.getUpdateStatusFileName().substring(
                    CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME.length() +
                      CarbonCommonConstants.HYPHEN.length()) + CarbonTablePath.SEGMENT_EXT);
              }
            }
          }

          if (staleSegments.size() != 0) {
            filesInTrashFolder.clear();
            listAllPartitionFile(carbonTable.getTablePath());
            List<String> filesToDelete = new ArrayList<>();
            for (String segmentFile : staleSegments) {
              SegmentFileStore.SegmentFile segmentFileName = SegmentFileStore.readSegmentFile(
                  segmentListPath + CarbonCommonConstants.FILE_SEPARATOR + segmentFile);
              Map<String, SegmentFileStore.FolderDetails> locationMap = segmentFileName
                  .getLocationMap();
              Iterator<Map.Entry<String, SegmentFileStore.FolderDetails>> it = locationMap
                  .entrySet().iterator();
              while (it.hasNext()) {
                Map.Entry<String, SegmentFileStore.FolderDetails> nextValue = it.next();
                String indexMergeFile = nextValue.getValue().getMergeFileName();
                String segmentNo = "";
                if (indexMergeFile != null) {
                  segmentNo = indexMergeFile.substring(0, indexMergeFile.indexOf(
                    CarbonCommonConstants.UNDERSCORE));
                } else {
                  segmentNo = segmentFile.substring(0, segmentFile.indexOf(CarbonCommonConstants
                    .UNDERSCORE));
                }
                // search for indexMergeFile everywhere and segmentFile in the carbondata file
                for (CarbonFile fileList : filesInTrashFolder) {
                  String nameofFile = fileList.getName().replace(CarbonCommonConstants.HYPHEN,
                      CarbonCommonConstants.UNDERSCORE);
                  String nameOfSegment = segmentFile.substring(0, segmentFile.indexOf(
                      CarbonCommonConstants.POINT));
                  if (fileList.getName().equals(indexMergeFile)) {
                    String partitionValue = fileList.getAbsolutePath().split(CarbonCommonConstants
                        .FILE_SEPARATOR)[fileList.getAbsolutePath().split(CarbonCommonConstants
                        .FILE_SEPARATOR).length - 2];
                    TrashUtil.copyDataToTrashFolderByFile(carbonTable.getTablePath(),
                        fileList.getAbsolutePath(), timeStampForTrashFolder + CarbonCommonConstants
                        .FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + segmentNo +
                        CarbonCommonConstants.FILE_SEPARATOR + partitionValue);
                    filesToDelete.add(fileList.getAbsolutePath());
                  } else if (nameofFile.contains(nameOfSegment) && fileList.getName().endsWith(
                      CarbonTablePath.INDEX_FILE_EXT)) {
                    String partitionValue = fileList.getAbsolutePath().split(CarbonCommonConstants
                        .FILE_SEPARATOR)[fileList.getAbsolutePath().split(CarbonCommonConstants
                        .FILE_SEPARATOR).length - 2];
                    TrashUtil.copyDataToTrashFolderByFile(carbonTable.getTablePath(),
                        fileList.getAbsolutePath(), timeStampForTrashFolder + CarbonCommonConstants
                        .FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + segmentNo +
                        CarbonCommonConstants.FILE_SEPARATOR + partitionValue);
                    filesToDelete.add(fileList.getAbsolutePath());
                  } else if (nameofFile.contains(nameOfSegment) && fileList.getName().endsWith(
                      CarbonCommonConstants.FACT_FILE_EXT)) {
                    // carbondata file found
                    String partitionValue = fileList.getAbsolutePath().split(CarbonCommonConstants
                        .FILE_SEPARATOR)[fileList.getAbsolutePath().split(CarbonCommonConstants
                        .FILE_SEPARATOR).length - 2];
                    TrashUtil.copyDataToTrashFolderByFile(carbonTable.getTablePath(), fileList
                        .getAbsolutePath(), timeStampForTrashFolder + CarbonCommonConstants
                        .FILE_SEPARATOR + CarbonCommonConstants.LOAD_FOLDER + segmentNo +
                        CarbonCommonConstants.FILE_SEPARATOR + partitionValue);
                    filesToDelete.add(fileList.getAbsolutePath());
                  }
                }
              }
            }
            for (String fileName : filesToDelete) {
              try {
                CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(fileName));
              } catch (InterruptedException e) {
                LOGGER.error("Unable to delete the given path :: " + e.getMessage(), e);
              }
            }
            filesInTrashFolder.clear();
          }
        }
      } finally {
        carbonTableStatusLock.unlock();
      }
    }
  }

  public static void listAllPartitionFile(String tablePath) {
    if (!FileFactory.getCarbonFile(tablePath).isDirectory()) {
      filesInTrashFolder.add(FileFactory.getCarbonFile(tablePath));
    }
    CarbonFile[] fileList = FileFactory.getCarbonFile(tablePath).listFiles();
    for (CarbonFile fileName : fileList) {
      if (!fileName.isDirectory() || fileName.getName().contains("=")) {
        listAllPartitionFile(fileName.getAbsolutePath());
      }
    }
  }

  public static HashMap<CarbonFile, String> getStaleSegments(LoadMetadataDetails[] details,
      CarbonFile[] allSegments, boolean isCompactionFlow) {
    Set<String> metadataSet = new HashSet<>(details.length);
    for (LoadMetadataDetails detail : details) {
      metadataSet.add(detail.getLoadName());
    }
    HashMap<CarbonFile, String> staleSegments = new HashMap<>(allSegments.length);
    for (CarbonFile segment : allSegments) {
      String segmentName = segment.getName();
      // check segment folder pattern
      if (segmentName.startsWith(CarbonTablePath.SEGMENT_PREFIX)) {
        String[] parts = segmentName.split(CarbonCommonConstants.UNDERSCORE);
        if (parts.length == 2) {
          boolean isOriginal = !parts[1].contains(".");
          if (isCompactionFlow) {
            // in compaction flow,
            // it should be merged segment and segment metadata doesn't exists
            if (!isOriginal && !metadataSet.contains(parts[1])) {
              staleSegments.put(segment, parts[1]);
            }
          } else {
            // in loading flow,
            // it should be original segment and segment metadata doesn't exists
            if (isOriginal && !metadataSet.contains(parts[1])) {
              staleSegments.put(segment, parts[1]);
            }
          }
        }
      }
    }
    return staleSegments;
  }


  /**
   *
   * This method will delete the local data load folder location after data load is complete
   *
   * @param loadModel
   * @param isCompactionFlow COMPACTION keyword will be added to path to make path unique if true
   * @param isAltPartitionFlow Alter_Partition keyword will be added to path to make path unique if
   *                           true
   */
  public static void deleteLocalDataLoadFolderLocation(CarbonLoadModel loadModel,
      boolean isCompactionFlow, boolean isAltPartitionFlow) {
    String tableName = loadModel.getTableName();
    String databaseName = loadModel.getDatabaseName();
    String tempLocationKey = CarbonDataProcessorUtil
        .getTempStoreLocationKey(databaseName, tableName, loadModel.getSegmentId(),
            loadModel.getTaskNo(), isCompactionFlow, isAltPartitionFlow);
    deleteLocalDataLoadFolderLocation(tempLocationKey, tableName);
  }

  /**
   *
   * This method will delete the local data load folder location after data load is complete
   *
   * @param tempLocationKey temporary location set in carbon properties
   * @param tableName
   */
  public static void deleteLocalDataLoadFolderLocation(String tempLocationKey, String tableName) {

    // form local store location
    final String localStoreLocations = CarbonProperties.getInstance().getProperty(tempLocationKey);
    if (localStoreLocations == null) {
      throw new RuntimeException("Store location not set for the key " + tempLocationKey);
    }
    // submit local folder clean up in another thread so that main thread execution is not blocked
    ExecutorService localFolderDeletionService = Executors
        .newFixedThreadPool(1, new CarbonThreadFactory("LocalFolderDeletionPool:" + tableName,
                true));
    try {
      localFolderDeletionService.submit(new Callable<Void>() {
        @Override
        public Void call() {
          long startTime = System.currentTimeMillis();
          String[] locArray = StringUtils.split(localStoreLocations, File.pathSeparator);
          for (String loc : locArray) {
            try {
              CarbonUtil.deleteFoldersAndFiles(new File(loc));
            } catch (IOException | InterruptedException e) {
              LOGGER.error("Failed to delete local data load folder location: " + loc, e);
            }
          }
          LOGGER.info(
              "Deleted the local store location: " + localStoreLocations + " : Time taken: " + (
                  System.currentTimeMillis() - startTime));
          return null;
        }
      });
    } finally {
      CarbonProperties.getInstance().removeProperty(tempLocationKey);
      if (null != localFolderDeletionService) {
        localFolderDeletionService.shutdown();
      }
    }

  }
}
