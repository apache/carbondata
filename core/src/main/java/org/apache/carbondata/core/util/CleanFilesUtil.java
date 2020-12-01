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

import javafx.util.Pair;

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
    Pair<List<String>, List<String>> staleSegmentFiles = getStaleSegmentFiles(carbonTable);
    for (String staleSegmentFile : staleSegmentFiles.getKey()) {
      String segmentNumber = DataFileUtil.getSegmentNoFromSegmentFile(staleSegmentFile);
      SegmentFileStore fileStore = new SegmentFileStore(carbonTable.getTablePath(),
          staleSegmentFile);
      Map<String, SegmentFileStore.FolderDetails> locationMap = fileStore.getSegmentFile()
          .getLocationMap();
      if (locationMap != null) {
        if (locationMap.entrySet().iterator().next().getValue().isRelative()) {
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
            for (String duplicateStaleSegmentFile : staleSegmentFiles.getValue()) {
              if (DataFileUtil.getSegmentNoFromSegmentFile(duplicateStaleSegmentFile)
                  .equals(segmentNumber)) {
                FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable
                    .getTablePath(), duplicateStaleSegmentFile));
              }
            }
          } catch (IOException | InterruptedException e) {
            LOGGER.error("Unable to delete the segment: " + segmentPath + " from after moving" +
                " it to the trash folder. Please delete them manually : " + e.getMessage(), e);
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
    Pair<List<String>, List<String>> staleSegmentFiles = getStaleSegmentFiles(carbonTable);
    for (String staleSegmentFile : staleSegmentFiles.getKey()) {
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
        }
        // Delete the segment file too
        FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath(),
            staleSegmentFile));
        // remove duplicate segment files if any
        for (String duplicateStaleSegmentFile : staleSegmentFiles.getValue()) {
          if (DataFileUtil.getSegmentNoFromSegmentFile(duplicateStaleSegmentFile)
              .equals(segmentNumber)) {
            FileFactory.deleteFile(CarbonTablePath.getSegmentFilePath(carbonTable.getTablePath(),
                duplicateStaleSegmentFile));
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
  private static Pair<List<String>, List<String>> getStaleSegmentFiles(CarbonTable carbonTable) {
    // details contains segments in the tablestatus file, and all segments contains segments files.
    // Segment number from those segment files is extracted and Stale segement file name is
    // returned.
    String segmentFilesLocation =
        CarbonTablePath.getSegmentFilesLocation(carbonTable.getTablePath());
    Pair<List<String>, List<String>> segmentFileNameList = getSegmentFiles(segmentFilesLocation);
    ArrayList<String> staleSegmentList = new ArrayList<>(segmentFileNameList.getKey().size());
    // there are no segments present in the Metadata folder. Can return here
    if (segmentFileNameList.getKey().size() == 0) {
      return segmentFileNameList;
    }
    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(carbonTable
        .getMetadataPath());
    Set<String> loadNameSet = Arrays.stream(details).map(loadMetadataDetails -> loadMetadataDetails
        .getLoadName()).collect(Collectors.toSet());
    for (String segmentFileName : segmentFileNameList.getKey()) {
      if (!loadNameSet.contains(DataFileUtil.getSegmentNoFromSegmentFile(
          segmentFileName))) {
        staleSegmentList.add(segmentFileName);
      }
    }
    List<String> redundantSegmentFile = new ArrayList<>(segmentFileNameList.getValue().size());
    for (String segmentFileName : segmentFileNameList.getValue()) {
      if (!loadNameSet.contains(DataFileUtil.getSegmentNoFromSegmentFile(
          segmentFileName))) {
        redundantSegmentFile.add(segmentFileName);
      }
    }
    return new Pair<>(staleSegmentList, redundantSegmentFile);
  }

  /**
   * This method will delete all the empty partition folders starting from the table path
   */
  public static void deleteEmptyPartitionFoldersRecursively(CarbonFile tablePath) {
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

  /**
   * This method will give the segment file names in the metadata folder after removing duplicates
   * and will also return the duplicate segment files which are not required(the one with less
   * than maximum timestamp)
   */
  public static Pair<List<String>, List<String>> getSegmentFiles(String segmentFileLocation) {
    List<String> loadNameSet = Arrays.stream(FileFactory.getCarbonFile(segmentFileLocation)
        .listFiles()).map(segmentFile -> segmentFile.getName()).sorted().collect(Collectors
        .toList());
    if (loadNameSet.size() == 0 || loadNameSet.size() == 1) {
      return new Pair<>(loadNameSet, new ArrayList<>(0));
    }
    List<String> segmentFileNameList = new ArrayList<>(loadNameSet.size());
    List<String> redundantSegmentFile = new ArrayList<>();
    for (int i = 0; i < loadNameSet.size() - 1; i++) {
      if (!DataFileUtil.getSegmentNoFromSegmentFile(loadNameSet.get(i)).equals(
          DataFileUtil.getSegmentNoFromSegmentFile(loadNameSet.get(i + 1)))) {
        segmentFileNameList.add(loadNameSet.get(i));
      } else {
        redundantSegmentFile.add(loadNameSet.get(i));
      }
    }
    // adding the last occurence always
    segmentFileNameList.add(loadNameSet.get(loadNameSet.size() - 1));
    // the key of the pair contains the unique segment files which are to be processed
    // the value of the pair contains the redundant segment files which are not required
    return new Pair<>(segmentFileNameList, redundantSegmentFile);
  }
}
