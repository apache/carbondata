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

package org.apache.carbondata.core.util;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil;

import org.apache.log4j.Logger;

/**
 *This util provide clean stale data methods for clean files command
 */
public class CleanFilesUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CleanFilesUtil.class.getName());

  /**
   * This method will clean all the stale segments for a table, delete the source folder after
   * copying the data to the trash and also remove the .segment files of the stale segments
   */
  public static void cleanStaleSegments(CarbonTable carbonTable)
      throws IOException {
    long timeStampForTrashFolder = CarbonUpdateUtil.readCurrentTime();
    List<String> staleSegmentFiles = new ArrayList<>();
    List<String> redundantSegmentFile = new ArrayList<>();
    getStaleSegmentFiles(carbonTable, staleSegmentFiles, redundantSegmentFile);
    for (String staleSegmentFile : staleSegmentFiles) {
      SegmentFileStore fileStore = new SegmentFileStore(carbonTable.getTablePath(),
          staleSegmentFile);
      SegmentFileStore.SegmentFile segmentFile = fileStore.getSegmentFile();
      if (segmentFile != null) {
        Map<String, SegmentFileStore.FolderDetails> locationMap = segmentFile.getLocationMap();
        if (locationMap != null) {
          if (locationMap.entrySet().iterator().next().getValue().isRelative()) {
            String segmentNumber = DataFileUtil.getSegmentNoFromSegmentFile(staleSegmentFile);
            CarbonFile segmentPath = FileFactory.getCarbonFile(CarbonTablePath.getSegmentPath(
                carbonTable.getTablePath(), segmentNumber));
            // copy the complete segment to the trash folder
            TrashUtil.copySegmentToTrash(segmentPath, TrashUtil.getCompleteTrashFolderPath(
                carbonTable.getTablePath(), timeStampForTrashFolder, segmentNumber));
            // Deleting the stale Segment folders and the segment file.
            try {
              CarbonUtil.deleteFoldersAndFiles(segmentPath);
              // delete the segment file as well
              FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath(),
                  staleSegmentFile));
              List<String> deletedFiles = new ArrayList<>();
              deletedFiles.add(staleSegmentFile);
              for (String duplicateStaleSegmentFile : redundantSegmentFile) {
                if (DataFileUtil.getSegmentNoFromSegmentFile(duplicateStaleSegmentFile)
                    .equals(segmentNumber)) {
                  FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable
                      .getTablePath(), duplicateStaleSegmentFile));
                  deletedFiles.add(duplicateStaleSegmentFile);
                }
              }
              LOGGER.info("Deleted the Segment :" + segmentPath.getName() + " after"
                  + " moving it to the trash folder");
              LOGGER.info("Deleted stale segment files: " + String.join(",", deletedFiles));
            } catch (IOException | InterruptedException e) {
              LOGGER.error("Unable to delete the segment: " + segmentPath + " from after moving" +
                  " it to the trash folder. Please delete them manually : " + e.getMessage(), e);
            }
          }
        }
      }
    }
  }

  /**
   * This method will clean all the stale segments for partition table, delete the source folders
   * after copying the data to the trash and also remove the .segment files of the stale segments
   */
  public static void cleanStaleSegmentsForPartitionTable(CarbonTable carbonTable)
      throws IOException {
    long timeStampForTrashFolder = CarbonUpdateUtil.readCurrentTime();
    List<String> staleSegmentFiles = new ArrayList<>();
    List<String> redundantSegmentFile = new ArrayList<>();
    getStaleSegmentFiles(carbonTable, staleSegmentFiles, redundantSegmentFile);
    for (String staleSegmentFile : staleSegmentFiles) {
      String segmentNumber = DataFileUtil.getSegmentNoFromSegmentFile(staleSegmentFile);
      // for each segment we get the indexfile first, then we get the carbondata file. Move both
      // of those to trash folder
      SegmentFileStore fileStore = new SegmentFileStore(carbonTable.getTablePath(),
          staleSegmentFile);
      List<String> filesToProcess = fileStore.readIndexFiles(SegmentStatus.SUCCESS, true,
          FileFactory.getConfiguration());

      // get carbondata files from here
      Map<String, List<String>> indexFilesMap = fileStore.getIndexFilesMap();
      for (Map.Entry<String, List<String>> entry : indexFilesMap.entrySet()) {
        filesToProcess.addAll(entry.getValue());
      }
      // After all the files have been added to list, move them to the trash folder
      TrashUtil.copyFilesToTrash(filesToProcess, TrashUtil.getCompleteTrashFolderPath(
          carbonTable.getTablePath(), timeStampForTrashFolder, segmentNumber), segmentNumber);
      // After every file of that segment has been copied, need to delete those files.
      try {
        for (String file : filesToProcess) {
          FileFactory.deleteFile(file);
          LOGGER.info("Deleted file :" + file + " after moving it to the trash folder");
        }
        // Delete the segment file too
        FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath(),
            staleSegmentFile));
        LOGGER.info("Deleted stale segment file after moving it to the trash folder :"
            + staleSegmentFile);
        // remove duplicate segment files if any
        for (String duplicateStaleSegmentFile : redundantSegmentFile) {
          if (DataFileUtil.getSegmentNoFromSegmentFile(duplicateStaleSegmentFile)
              .equals(segmentNumber)) {
            FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath(),
                duplicateStaleSegmentFile));
            LOGGER.info("Deleted redundant segment file :" + duplicateStaleSegmentFile);
          }
        }
      } catch (IOException e) {
        LOGGER.error("Error while deleting the source data files. Please delete the files of" +
            " segment: " + segmentNumber + " manually.", e);
      }
    }
    // Delete the empty partition folders
    deleteEmptyPartitionFoldersRecursively(FileFactory.getCarbonFile(carbonTable.getTablePath()));
  }

  /**
   * This method will find all the stale segments by comparing the segment files in the
   * metadata directory with the segments in the table status file. Any segment which has entry
   * in the metadata folder and is not present in the table status file is considered as a
   * stale segment. Only comparing from tablestatus file, not checking tablestatus.history file
   */
  private static void getStaleSegmentFiles(CarbonTable carbonTable, List<String> staleSegmentFiles,
      List<String> redundantSegmentFile) {
    String segmentFilesLocation =
        CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath());
    List<String> segmentFiles = Arrays.stream(FileFactory.getCarbonFile(segmentFilesLocation)
        .listFiles()).map(CarbonFile::getName).filter(segmentFileName -> segmentFileName
        .endsWith(CarbonTablePath.SEGMENT_EXT)).collect(Collectors.toList());
    // there are no segments present in the Metadata folder. Can return here
    if (segmentFiles.size() == 0) {
      return;
    }
    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(carbonTable
        .getMetadataPath());
    if (details == null || details.length == 0) {
      return;
    }
    Set<String> loadNameSet = Arrays.stream(details).map(LoadMetadataDetails::getLoadName)
        .collect(Collectors.toSet());
    // get all stale segment files, not include compaction segments.
    // during compaction, we don't add in-progress metadata entry into tablestatus file,
    // so here we don't known whether compaction segment is stale or not.
    List<String> staleSegments = segmentFiles.stream().filter(segmentFile -> {
      String segmentNo = DataFileUtil.getSegmentNoFromSegmentFile(segmentFile);
      return !loadNameSet.contains(segmentNo) && !segmentNo.contains(CarbonCommonConstants.POINT);
    }).collect(Collectors.toList());
    if (staleSegments.size() == 0) {
      return;
    }
    // sort the stale segment List
    Collections.sort(staleSegments);
    // in case of multiple segment files for a segment, add the segment with the largest
    // timestamp to staleSegmentFiles list and add the others to redundantsegmentfile list.
    for (int i = 0; i < staleSegments.size() - 1; i++) {
      if (!DataFileUtil.getSegmentNoFromSegmentFile(staleSegments.get(i)).equals(
          DataFileUtil.getSegmentNoFromSegmentFile(staleSegments.get(i + 1)))) {
        staleSegmentFiles.add(staleSegments.get(i));
      } else {
        redundantSegmentFile.add(staleSegments.get(i));
      }
    }
    // adding the last occurrence always
    staleSegmentFiles.add(staleSegments.get(staleSegments.size() - 1));
  }

  /**
   * This method will delete all the empty partition folders starting from the table path
   */
  private static void deleteEmptyPartitionFoldersRecursively(CarbonFile tablePath) {
    CarbonFile[] listOfFiles = tablePath.listFiles();
    if (listOfFiles.length == 0) {
      tablePath.delete();
    } else {
      for (CarbonFile file: listOfFiles) {
        if (file.isDirectory() && file.getName().contains("=")) {
          deleteEmptyPartitionFoldersRecursively(file);
        }
      }
    }
  }
}
