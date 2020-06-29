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

package org.apache.carbondata.core.mutate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonCommonConstantsInternal;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.mutate.data.RowCountDetailsVO;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * This class contains all update utility methods
 */
public class CarbonUpdateUtil {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(CarbonUpdateUtil.class.getName());

  /**
   * returns required filed from tuple id
   *
   */
  public static String getRequiredFieldFromTID(String Tid, int index) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3724
    return Tid.split(CarbonCommonConstants.FILE_SEPARATOR)[index];
  }

  /**
   * returns required filed from tuple id
   *
   * @param Tid
   * @param tid
   * @return
   */
  public static String getRequiredFieldFromTID(String Tid, TupleIdEnum tid) {
    return Tid.split("/")[tid.getTupleIdIndex()];
  }

  /**
   * returns segment along with block id
   * @param Tid
   * @return
   */
  public static String getSegmentWithBlockFromTID(String Tid, boolean isPartitionTable) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3724
    if (isPartitionTable) {
      return getRequiredFieldFromTID(Tid, TupleIdEnum.SEGMENT_ID);
    }
    return getRequiredFieldFromTID(Tid, TupleIdEnum.SEGMENT_ID)
        + CarbonCommonConstants.FILE_SEPARATOR + getRequiredFieldFromTID(Tid, TupleIdEnum.BLOCK_ID);
  }

  /**
   * Returns block path from tuple id
   */
  public static String getTableBlockPath(String tid, String tablePath, boolean isStandardTable) {
    String partField = getRequiredFieldFromTID(tid, TupleIdEnum.PART_ID);
    // If it has segment file then partfield can be appended directly to table path
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2734
    if (!isStandardTable) {
      return tablePath + CarbonCommonConstants.FILE_SEPARATOR + partField.replace("#", "/");
    }
    String part = CarbonTablePath.addPartPrefix(partField);
    String segment =
            CarbonTablePath.addSegmentPrefix(getRequiredFieldFromTID(tid, TupleIdEnum.SEGMENT_ID));
    return CarbonTablePath.getFactDir(tablePath) + CarbonCommonConstants.FILE_SEPARATOR + part
            + CarbonCommonConstants.FILE_SEPARATOR + segment;
  }

  /**
   * returns delete delta file path
   *
   * @param blockPath
   * @param blockPath
   * @param timestamp
   * @return
   */
  public static String getDeleteDeltaFilePath(String blockPath, String blockName,
                                              String timestamp) {
    return blockPath + CarbonCommonConstants.FILE_SEPARATOR + blockName
        + CarbonCommonConstants.HYPHEN + timestamp + CarbonCommonConstants.DELETE_DELTA_FILE_EXT;

  }

  /**
   * @param updateDetailsList
   * @param table
   * @param updateStatusFileIdentifier
   * @return
   */
  public static boolean updateSegmentStatus(List<SegmentUpdateDetails> updateDetailsList,
      CarbonTable table, String updateStatusFileIdentifier, boolean isCompaction) {
    boolean status = false;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270
    SegmentUpdateStatusManager segmentUpdateStatusManager = new SegmentUpdateStatusManager(table);
    ICarbonLock updateLock = segmentUpdateStatusManager.getTableUpdateStatusLock();
    boolean lockStatus = false;

    try {
      lockStatus = updateLock.lockWithRetries();
      if (lockStatus) {

        // read the existing file if present and update the same.
        SegmentUpdateDetails[] oldDetails = segmentUpdateStatusManager
                .getUpdateStatusDetails();

        List<SegmentUpdateDetails> oldList = new ArrayList(Arrays.asList(oldDetails));

        for (SegmentUpdateDetails newBlockEntry : updateDetailsList) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3597
          mergeSegmentUpdate(isCompaction, oldList, newBlockEntry);
        }

        segmentUpdateStatusManager.writeLoadDetailsIntoFile(oldList, updateStatusFileIdentifier);
        status = true;
      } else {
        LOGGER.error("Not able to acquire the segment update lock.");
        status = false;
      }
    } catch (IOException e) {
      status = false;
    } finally {
      if (lockStatus) {
        if (updateLock.unlock()) {
          LOGGER.info("Unlock the segment update lock successfull.");
        } else {
          LOGGER.error("Not able to unlock the segment update lock.");
        }
      }
    }
    return status;
  }

  public static void mergeSegmentUpdate(boolean isCompaction, List<SegmentUpdateDetails> oldList,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3597
      SegmentUpdateDetails newBlockEntry) {
    int index = oldList.indexOf(newBlockEntry);
    if (index != -1) {
      // update the element in existing list.
      SegmentUpdateDetails blockDetail = oldList.get(index);
      if (blockDetail.getDeleteDeltaStartTimestamp().isEmpty() || isCompaction) {
        blockDetail
            .setDeleteDeltaStartTimestamp(newBlockEntry.getDeleteDeltaStartTimestamp());
      }
      blockDetail.setDeleteDeltaEndTimestamp(newBlockEntry.getDeleteDeltaEndTimestamp());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1693
      blockDetail.setSegmentStatus(newBlockEntry.getSegmentStatus());
      blockDetail.setDeletedRowsInBlock(newBlockEntry.getDeletedRowsInBlock());
      // If the start and end time is different then the delta is there in multiple files so
      // add them to the list to get the delta files easily with out listing.
      if (!blockDetail.getDeleteDeltaStartTimestamp()
          .equals(blockDetail.getDeleteDeltaEndTimestamp())) {
        blockDetail.addDeltaFileStamp(blockDetail.getDeleteDeltaStartTimestamp());
        blockDetail.addDeltaFileStamp(blockDetail.getDeleteDeltaEndTimestamp());
      } else {
        blockDetail.setDeltaFileStamps(null);
      }
    } else {
      // add the new details to the list.
      oldList.add(newBlockEntry);
    }
  }

  /**
   * Update table status
   * @param updatedSegmentsList
   * @param table
   * @param updatedTimeStamp
   * @param isTimestampUpdationRequired
   * @param segmentsToBeDeleted
   * @return
   */
  public static boolean updateTableMetadataStatus(Set<Segment> updatedSegmentsList,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2187
      CarbonTable table, String updatedTimeStamp, boolean isTimestampUpdationRequired,
      List<Segment> segmentsToBeDeleted) {
    return updateTableMetadataStatus(updatedSegmentsList, table, updatedTimeStamp,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2294
        isTimestampUpdationRequired, segmentsToBeDeleted, new ArrayList<Segment>(), "");
  }

  /**
   *
   * @param updatedSegmentsList
   * @param table
   * @param updatedTimeStamp
   * @param isTimestampUpdationRequired
   * @param segmentsToBeDeleted
   * @return
   */
  public static boolean updateTableMetadataStatus(Set<Segment> updatedSegmentsList,
      CarbonTable table, String updatedTimeStamp, boolean isTimestampUpdationRequired,
      List<Segment> segmentsToBeDeleted, List<Segment> segmentFilesTobeUpdated, String uuid) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2294

    boolean status = false;
    String metaDataFilepath = table.getMetadataPath();
    AbsoluteTableIdentifier identifier = table.getAbsoluteTableIdentifier();
    String tableStatusPath =
        CarbonTablePath.getTableStatusFilePathWithUUID(identifier.getTablePath(), uuid);
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    boolean lockStatus = false;
    try {
      lockStatus = carbonLock.lockWithRetries();
      if (lockStatus) {
        LOGGER.info(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1739
                "Acquired lock for table" + table.getDatabaseName() + "." + table.getTableName()
                        + " for table status updation");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
                SegmentStatusManager.readLoadMetadata(metaDataFilepath);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025

        for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {

          if (isTimestampUpdationRequired) {
            // we are storing the link between the 2 status files in the segment 0 only.
            if (loadMetadata.getLoadName().equalsIgnoreCase("0")) {
              loadMetadata.setUpdateStatusFileName(
                      CarbonUpdateUtil.getUpdateStatusFileName(updatedTimeStamp));
            }

            // if the segments is in the list of marked for delete then update the status.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2909
            if (segmentsToBeDeleted.contains(new Segment(loadMetadata.getLoadName()))) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1693
              loadMetadata.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
              loadMetadata.setModificationOrdeletionTimesStamp(Long.parseLong(updatedTimeStamp));
            }
          }
          for (Segment segName : updatedSegmentsList) {
            if (loadMetadata.getLoadName().equalsIgnoreCase(segName.getSegmentNo())) {
              // if this call is coming from the delete delta flow then the time stamp
              // String will come empty then no need to write into table status file.
              if (isTimestampUpdationRequired) {
                // if in case of update flow.
                if (loadMetadata.getUpdateDeltaStartTimestamp().isEmpty()) {
                  // this means for first time it is getting updated .
                  loadMetadata.setUpdateDeltaStartTimestamp(updatedTimeStamp);
                }
                // update end timestamp for each time.
                loadMetadata.setUpdateDeltaEndTimestamp(updatedTimeStamp);
              }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2361
              if (segmentFilesTobeUpdated
                  .contains(Segment.toSegment(loadMetadata.getLoadName(), null))) {
                loadMetadata.setSegmentFile(loadMetadata.getLoadName() + "_" + updatedTimeStamp
                    + CarbonTablePath.SEGMENT_EXT);
              }
            }
          }
        }

        try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
          SegmentStatusManager
                  .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
        } catch (IOException e) {
          return false;
        }

        status = true;
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + table
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1739
                .getDatabaseName() + "." + table.getTableName());
      }
    } finally {
      if (lockStatus) {
        if (carbonLock.unlock()) {
          LOGGER.info(
                 "Table unlocked successfully after table status updation" + table.getDatabaseName()
                          + "." + table.getTableName());
        } else {
          LOGGER.error(
                  "Unable to unlock Table lock for table" + table.getDatabaseName() + "." + table
                          .getTableName() + " during table status updation");
        }
      }
    }
    return status;

  }

  /**
   * gets the file name of the update status file. by appending the latest timestamp to it.
   *
   * @param updatedTimeStamp
   * @return
   */
  public static String getUpdateStatusFileName(String updatedTimeStamp) {
    return CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME + CarbonCommonConstants.HYPHEN
            + updatedTimeStamp;
  }

  /**
   * This will handle the clean up cases if the update fails.
   *
   * @param table
   * @param timeStamp
   */
  public static void cleanStaleDeltaFiles(CarbonTable table, final String timeStamp) {

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
    AbsoluteTableIdentifier identifier = table.getAbsoluteTableIdentifier();
    String partitionDir = CarbonTablePath.getPartitionDir(identifier.getTablePath());
    CarbonFile file =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
            FileFactory.getCarbonFile(partitionDir);
    if (!file.exists()) {
      return;
    }
    for (CarbonFile eachDir : file.listFiles()) {
      // for each dir check if the file with the delta timestamp is present or not.
      CarbonFile[] toBeDeleted = eachDir.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          String fileName = file.getName();
          return (fileName.endsWith(timeStamp + CarbonCommonConstants.UPDATE_DELTA_FILE_EXT)
                  || fileName.endsWith(timeStamp + CarbonCommonConstants.UPDATE_INDEX_FILE_EXT)
                  || fileName.endsWith(timeStamp + CarbonCommonConstants.DELETE_DELTA_FILE_EXT));
        }
      });
      // deleting the files of a segment.
      try {
        CarbonUtil.deleteFoldersAndFilesSilent(toBeDeleted);
      } catch (IOException e) {
        LOGGER.error("Exception in deleting the delta files." + e);
      } catch (InterruptedException e) {
        LOGGER.error("Exception in deleting the delta files." + e);
      }
    }
  }

  /**
   * returns timestamp as long value
   *
   * @param timtstamp
   * @return
   */
  public static Long getTimeStampAsLong(String timtstamp) {
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-886
      return Long.parseLong(timtstamp);
    } catch (NumberFormatException nfe) {
      String errorMsg = "Invalid timestamp : " + timtstamp;
      LOGGER.error(errorMsg);
      return null;
    }
  }

  /**
   * returns integer value from given string
   *
   * @param value
   * @return
   * @throws Exception
   */
  public static Integer getIntegerValue(String value) throws Exception {
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-886
      return Integer.parseInt(value);
    } catch (NumberFormatException nfe) {
      LOGGER.error("Invalid row : " + value + nfe.getLocalizedMessage());
      throw new Exception("Invalid row : " + nfe.getLocalizedMessage());
    }
  }

  /**
   * return only block name from completeBlockName
   *
   * @param completeBlockName
   * @return
   */
  public static String getBlockName(String completeBlockName) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-886
    return completeBlockName
        .substring(0, completeBlockName.lastIndexOf(CarbonCommonConstants.HYPHEN));
  }

  /**
   * returns segment id from segment name
   *
   * @param segmentName
   * @return
   */
  public static String getSegmentId(String segmentName) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-886
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
    return segmentName.split(CarbonCommonConstants.UNDERSCORE)[1];
  }

  public static long getLatestTaskIdForSegment(Segment segment, String tablePath)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2428
      throws IOException {
    long max = 0;
    List<String> dataFiles = new ArrayList<>();
    if (segment.getSegmentFileName() != null) {
      SegmentFileStore fileStore = new SegmentFileStore(tablePath, segment.getSegmentFileName());
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2909
      fileStore.readIndexFiles(FileFactory.getConfiguration());
      Map<String, List<String>> indexFilesMap = fileStore.getIndexFilesMap();
      List<String> dataFilePaths = new ArrayList<>();
      for (List<String> paths : indexFilesMap.values()) {
        dataFilePaths.addAll(paths);
      }
      for (String dataFilePath : dataFilePaths) {
        dataFiles.add(new Path(dataFilePath).getName());
      }

    } else {
      String segmentDirPath = CarbonTablePath.getSegmentPath(tablePath, segment.getSegmentNo());
      // scan all the carbondata files and get the latest task ID.
      CarbonFile segmentDir =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
          FileFactory.getCarbonFile(segmentDirPath);
      CarbonFile[] carbonDataFiles = segmentDir.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {

          if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
            return true;
          }
          return false;
        }
      });
      for (CarbonFile carbonDataFile : carbonDataFiles) {
        dataFiles.add(carbonDataFile.getName());
      }
    }
    for (String name : dataFiles) {
      long taskNumber =
          Long.parseLong(CarbonTablePath.DataFileUtil.getTaskNo(name).split("_")[0]);
      if (taskNumber > max) {
        max = taskNumber;
      }
    }
    // return max task No
    return max;

  }

  /**
   * Handling of the clean up of old carbondata files, index files , delte delta,
   * update status files.
   * @param table clean up will be handled on this table.
   * @param forceDelete if true then max query execution timeout will not be considered.
   */
  public static void cleanUpDeltaFiles(CarbonTable table, boolean forceDelete) throws IOException {

    SegmentStatusManager ssm = new SegmentStatusManager(table.getAbsoluteTableIdentifier());

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
    LoadMetadataDetails[] details =
        SegmentStatusManager.readLoadMetadata(table.getMetadataPath());

    String validUpdateStatusFile = "";

    boolean isAbortedFile = true;

    boolean isInvalidFile = false;

    List<Segment> segmentFilesToBeUpdated = new ArrayList<>();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270

    // scan through each segment.

    for (LoadMetadataDetails segment : details) {

      // take the update status file name from 0th segment.
      validUpdateStatusFile = ssm.getUpdateStatusFileName(details);

      // if this segment is valid then only we will go for delta file deletion.
      // if the segment is mark for delete or compacted then any way it will get deleted.

//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1693
      if (segment.getSegmentStatus() == SegmentStatus.SUCCESS
              || segment.getSegmentStatus() == SegmentStatus.LOAD_PARTIAL_SUCCESS) {

        // when there is no update operations done on table, then no need to go ahead. So
        // just check the update delta start timestamp and proceed if not empty
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3394
        if (!segment.getUpdateDeltaStartTimestamp().isEmpty()) {
          // take the list of files from this segment.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2025
          String segmentPath = CarbonTablePath.getSegmentPath(
              table.getAbsoluteTableIdentifier().getTablePath(), segment.getLoadName());
          CarbonFile segDir =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863
              FileFactory.getCarbonFile(segmentPath);
          CarbonFile[] allSegmentFiles = segDir.listFiles();

          // scan through the segment and find the carbondatafiles and index files.
          SegmentUpdateStatusManager updateStatusManager = new SegmentUpdateStatusManager(table);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270

          boolean updateSegmentFile = false;
          // deleting of the aborted file scenario.
          if (deleteStaleCarbonDataFiles(segment, allSegmentFiles, updateStatusManager)) {
            updateSegmentFile = true;
          }

          // get Invalid update  delta files.
          CarbonFile[] invalidUpdateDeltaFiles = updateStatusManager
              .getUpdateDeltaFilesList(segment, false,
                  CarbonCommonConstants.UPDATE_DELTA_FILE_EXT, true, allSegmentFiles,
                  isInvalidFile);

          // now for each invalid delta file need to check the query execution time out
          // and then delete.
          for (CarbonFile invalidFile : invalidUpdateDeltaFiles) {
            compareTimestampsAndDelete(invalidFile, forceDelete, false);
          }
          // do the same for the index files.
          CarbonFile[] invalidIndexFiles = updateStatusManager
              .getUpdateDeltaFilesList(segment, false,
                  CarbonCommonConstants.UPDATE_INDEX_FILE_EXT, true, allSegmentFiles,
                  isInvalidFile);

          // now for each invalid index file need to check the query execution time out
          // and then delete.

          for (CarbonFile invalidFile : invalidIndexFiles) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270
            if (compareTimestampsAndDelete(invalidFile, forceDelete, false)) {
              updateSegmentFile = true;
            }
          }
          // now handle all the delete delta files which needs to be deleted.
          // there are 2 cases here .
          // 1. if the block is marked as compacted then the corresponding delta files
          //    can be deleted if query exec timeout is done.
          // 2. if the block is in success state then also there can be delete
          //    delta compaction happened and old files can be deleted.

          SegmentUpdateDetails[] updateDetails = updateStatusManager.readLoadMetadata();
          for (SegmentUpdateDetails block : updateDetails) {
            CarbonFile[] completeListOfDeleteDeltaFiles;
            CarbonFile[] invalidDeleteDeltaFiles;

            if (!block.getSegmentName().equalsIgnoreCase(segment.getLoadName())) {
              continue;
            }

            // aborted scenario.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2021
            invalidDeleteDeltaFiles = updateStatusManager
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
                .getDeleteDeltaInvalidFilesList(block, false,
                    allSegmentFiles, isAbortedFile);
            for (CarbonFile invalidFile : invalidDeleteDeltaFiles) {
              boolean doForceDelete = true;
              compareTimestampsAndDelete(invalidFile, doForceDelete, false);
            }

            // case 1
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1693
            if (CarbonUpdateUtil.isBlockInvalid(block.getSegmentStatus())) {
              completeListOfDeleteDeltaFiles = updateStatusManager
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
                  .getDeleteDeltaInvalidFilesList(block, true,
                      allSegmentFiles, isInvalidFile);
              for (CarbonFile invalidFile : completeListOfDeleteDeltaFiles) {
                compareTimestampsAndDelete(invalidFile, forceDelete, false);
              }

            } else {
              invalidDeleteDeltaFiles = updateStatusManager
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2099
                  .getDeleteDeltaInvalidFilesList(block, false,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2021
                      allSegmentFiles, isInvalidFile);
              for (CarbonFile invalidFile : invalidDeleteDeltaFiles) {
                compareTimestampsAndDelete(invalidFile, forceDelete, false);
              }
            }
          }
          if (updateSegmentFile) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3755
            segmentFilesToBeUpdated.add(
                new Segment(segment.getLoadName(), segment.getSegmentFile(), null));
          }
        }
        // handle cleanup of merge index files and data files after small files merge happened for
        // SI table
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
        cleanUpDataFilesAfterSmallFilesMergeForSI(table, segment);
      }
    }
    String UUID = String.valueOf(System.currentTimeMillis());
    List<Segment> segmentFilesToBeUpdatedLatest = new ArrayList<>();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3755
    CarbonFile segmentFilesLocation =
        FileFactory.getCarbonFile(CarbonTablePath.getSegmentFilesLocation(table.getTablePath()));
    for (Segment segment : segmentFilesToBeUpdated) {
      SegmentFileStore fileStore =
          new SegmentFileStore(table.getTablePath(), segment.getSegmentFileName());
      segment.setSegmentMetaDataInfo(fileStore.getSegmentFile().getSegmentMetaDataInfo());
      String updatedSegmentFile = SegmentFileStore
          .writeSegmentFile(table, segment.getSegmentNo(), UUID,
              CarbonTablePath.getSegmentPath(table.getTablePath(), segment.getSegmentNo()),
              segment.getSegmentMetaDataInfo());
      segmentFilesToBeUpdatedLatest.add(new Segment(segment.getSegmentNo(), updatedSegmentFile));

      // delete the old segment files
      CarbonFile[] invalidSegmentFiles = segmentFilesLocation.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          return !file.getName().equalsIgnoreCase(updatedSegmentFile);
        }
      });
      for (CarbonFile invalidSegmentFile : invalidSegmentFiles) {
        invalidSegmentFile.delete();
      }
    }
    if (segmentFilesToBeUpdated.size() > 0) {
      updateTableMetadataStatus(
          new HashSet<Segment>(segmentFilesToBeUpdated),
          table,
          UUID,
          false,
          new ArrayList<Segment>(),
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2294
          segmentFilesToBeUpdatedLatest, "");
    }

    // delete the update table status files which are old.
    if (null != validUpdateStatusFile && !validUpdateStatusFile.isEmpty()) {

      final String updateStatusTimestamp = validUpdateStatusFile
          .substring(validUpdateStatusFile.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720

      String tablePath = table.getAbsoluteTableIdentifier().getTablePath();
      CarbonFile metaFolder = FileFactory.getCarbonFile(
          CarbonTablePath.getMetadataPath(tablePath));
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2863

      CarbonFile[] invalidUpdateStatusFiles = metaFolder.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          if (file.getName().startsWith(CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME)) {

            // CHECK if this is valid or not.
            // we only send invalid ones to delete.
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2720
            return !file.getName().endsWith(updateStatusTimestamp);
          }
          return false;
        }
      });

      for (CarbonFile invalidFile : invalidUpdateStatusFiles) {

        compareTimestampsAndDelete(invalidFile, forceDelete, true);
      }
    }
  }

  /**
   * this is the clean up added specifically for SI table, because after we merge the data files
   * inside the secondary index table, we need to delete the stale carbondata files.
   * refer {@link org.apache.spark.sql.secondaryindex.rdd.CarbonSIRebuildRDD}
   */
  private static void cleanUpDataFilesAfterSmallFilesMergeForSI(CarbonTable table,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3680
      LoadMetadataDetails segment) throws IOException {
    if (table.isIndexTable()) {
      String segmentPath = CarbonTablePath
          .getSegmentPath(table.getAbsoluteTableIdentifier().getTablePath(),
              segment.getLoadName());
      CarbonFile segmentDirPath =
          FileFactory.getCarbonFile(segmentPath);
      CarbonFile[] allFilesOfSegment = segmentDirPath.listFiles();
      long startTimeStampFinal = segment.getLoadStartTime();
      long endTimeStampFinal = segment.getLoadEndTime();
      boolean deleteFile;
      for (CarbonFile file : allFilesOfSegment) {
        deleteFile = false;
        String fileTimestamp =
            CarbonTablePath.DataFileUtil.getTimeStampFromFileName(file.getName());
        // check for old files before load start time and the aborted files after end time
        if ((file.getName().endsWith(CarbonTablePath.CARBON_DATA_EXT) || file.getName()
            .endsWith(CarbonTablePath.INDEX_FILE_EXT)) && (
            Long.parseLong(fileTimestamp) < startTimeStampFinal
                || Long.parseLong(fileTimestamp) > endTimeStampFinal)) {
          deleteFile = true;
        } else if (file.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
            && Long.parseLong(fileTimestamp) < startTimeStampFinal) {
          deleteFile = true;
        }
        if (deleteFile) {
          // delete the files and folders.
          try {
            LOGGER.info("Deleting the invalid file : " + file.getName());
            CarbonUtil.deleteFoldersAndFiles(file);
          } catch (IOException e) {
            LOGGER.error("Error in clean up of merged files." + e.getMessage(), e);
          } catch (InterruptedException e) {
            LOGGER.error("Error in clean up of merged files." + e.getMessage(), e);
          }
        }
      }
    }
  }

  /**
   * This function deletes all the stale carbondata files during clean up before update operation
   * one scenario is if update operation is ubruptly stopped before updation of table status then
   * the carbondata file created during update operation is stale file and it will be deleted in
   * this function in next update operation
   * @param segment
   * @param allSegmentFiles
   * @param updateStatusManager
   */
  private static boolean deleteStaleCarbonDataFiles(LoadMetadataDetails segment,
      CarbonFile[] allSegmentFiles, SegmentUpdateStatusManager updateStatusManager) {
    CarbonFile[] invalidUpdateDeltaFiles = updateStatusManager
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3394
        .getUpdateDeltaFilesList(segment, false,
            CarbonCommonConstants.UPDATE_DELTA_FILE_EXT, true, allSegmentFiles,
            true);
    // now for each invalid delta file need to check the query execution time out
    // and then delete.
    for (CarbonFile invalidFile : invalidUpdateDeltaFiles) {
      compareTimestampsAndDelete(invalidFile, true, false);
    }
    // do the same for the index files.
    CarbonFile[] invalidIndexFiles = updateStatusManager
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3394
        .getUpdateDeltaFilesList(segment, false,
            CarbonCommonConstants.UPDATE_INDEX_FILE_EXT, true, allSegmentFiles,
            true);
    // now for each invalid index file need to check the query execution time out
    // and then delete.
    boolean updateSegmentFile = false;
    for (CarbonFile invalidFile : invalidIndexFiles) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270
      if (compareTimestampsAndDelete(invalidFile, true, false)) {
        updateSegmentFile = true;
      }
    }
    return updateSegmentFile;
  }

  /**
   * This will tell whether the max query timeout has been expired or not.
   * @param fileTimestamp
   * @return
   */
  public static boolean isMaxQueryTimeoutExceeded(long fileTimestamp) {
    // record current time.
    long currentTime = CarbonUpdateUtil.readCurrentTime();
    int maxTime;
    try {
      maxTime = Integer.parseInt(CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.MAX_QUERY_EXECUTION_TIME));
    } catch (NumberFormatException e) {
      maxTime = CarbonCommonConstants.DEFAULT_MAX_QUERY_EXECUTION_TIME;
    }

    long difference = currentTime - fileTimestamp;

    long minutesElapsed = (difference / (1000 * 60));

    return minutesElapsed > maxTime;

  }

  /**
   *
   * @param invalidFile
   * @param forceDelete
   * @param isUpdateStatusFile if true then the parsing of file name logic changes.
   */
  private static boolean compareTimestampsAndDelete(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270
      CarbonFile invalidFile,
      boolean forceDelete, boolean isUpdateStatusFile) {
    boolean isDeleted = false;
    Long fileTimestamp;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3360

    if (isUpdateStatusFile) {
      fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(invalidFile.getName()
              .substring(invalidFile.getName().lastIndexOf(CarbonCommonConstants.HYPHEN) + 1));
    } else {
      fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(
              CarbonTablePath.DataFileUtil.getTimeStampFromFileName(invalidFile.getName()));
    }

    // This check is because, when there are some invalid files like tableStatusUpdate.write files
    // present in store [[which can happen during delete or update if the disk is full or hdfs quota
    // is finished]] then fileTimestamp will be null, in that case check for max query out and
    // delete the .write file after timeout
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3360
    if (fileTimestamp == null) {
      String tableUpdateStatusFilename = invalidFile.getName();
      if (tableUpdateStatusFilename.endsWith(".write")) {
        long tableUpdateStatusFileTimeStamp = Long.parseLong(
            CarbonTablePath.DataFileUtil.getTimeStampFromFileName(tableUpdateStatusFilename));
        if (isMaxQueryTimeoutExceeded(tableUpdateStatusFileTimeStamp)) {
          isDeleted = deleteInvalidFiles(invalidFile);
        }
      }
    } else {
      // if the timestamp of the file is more than the current time by query execution timeout.
      // then delete that file.
      if (CarbonUpdateUtil.isMaxQueryTimeoutExceeded(fileTimestamp) || forceDelete) {
        isDeleted = deleteInvalidFiles(invalidFile);
      }
    }
    return isDeleted;
  }

  private static boolean deleteInvalidFiles(CarbonFile invalidFile) {
    boolean isDeleted;
    try {
      LOGGER.info("deleting the invalid file : " + invalidFile.getName());
      CarbonUtil.deleteFoldersAndFiles(invalidFile);
      isDeleted = true;
    } catch (IOException | InterruptedException e) {
      LOGGER.error("error in clean up of invalid files." + e.getMessage(), e);
      isDeleted = false;
    }
    return isDeleted;
  }

  public static boolean isBlockInvalid(SegmentStatus blockStatus) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1693
    return blockStatus == SegmentStatus.COMPACTED || blockStatus == SegmentStatus.MARKED_FOR_DELETE;
  }

  /**
   * This will return the current time in millis.
   * @return
   */
  public static long readCurrentTime() {
    return System.currentTimeMillis();
  }

  /**
   *
   * @param details
   * @param segmentBlockCount
   */
  public static void decrementDeletedBlockCount(SegmentUpdateDetails details,
                                                Map<String, Long> segmentBlockCount) {

    String segId = details.getSegmentName();

    segmentBlockCount.put(details.getSegmentName(), segmentBlockCount.get(segId) - 1);

  }

  /**
   *
   * @param segmentBlockCount
   * @return
   */
  public static List<Segment> getListOfSegmentsToMarkDeleted(Map<String, Long> segmentBlockCount) {
    List<Segment> segmentsToBeDeleted =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (Map.Entry<String, Long> eachSeg : segmentBlockCount.entrySet()) {

      if (eachSeg.getValue() == 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2909
        segmentsToBeDeleted.add(new Segment(eachSeg.getKey(), ""));
      }

    }
    return segmentsToBeDeleted;
  }

  /**
   * Return row count of input block
   */
  public static long getRowCount(BlockMappingVO blockMappingVO, CarbonTable carbonTable) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3293
    if (blockMappingVO.getBlockRowCountMapping().size() == 1
        && blockMappingVO.getBlockRowCountMapping().get(CarbonCommonConstantsInternal.ROW_COUNT)
        != null) {
      return blockMappingVO.getBlockRowCountMapping().get(CarbonCommonConstantsInternal.ROW_COUNT);
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1746
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(carbonTable);
    long rowCount = 0;
    Map<String, Long> blockRowCountMap = blockMappingVO.getBlockRowCountMapping();
    for (Map.Entry<String, Long> blockRowEntry : blockRowCountMap.entrySet()) {
      String key = blockRowEntry.getKey();
      long alreadyDeletedCount = 0;
      SegmentUpdateDetails detail = updateStatusManager.getDetailsForABlock(key);
      if (detail != null) {
        alreadyDeletedCount = Long.parseLong(detail.getDeletedRowsInBlock());
      }
      rowCount += (blockRowEntry.getValue() - alreadyDeletedCount);
    }
    return rowCount;
  }

  /**
   *
   * @param blockMappingVO
   * @param segmentUpdateStatusManager
   */
  public static void createBlockDetailsMap(BlockMappingVO blockMappingVO,
                                           SegmentUpdateStatusManager segmentUpdateStatusManager) {

    Map<String, Long> blockRowCountMap = blockMappingVO.getBlockRowCountMapping();

    Map<String, RowCountDetailsVO> outputMap =
            new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (Map.Entry<String, Long> blockRowEntry : blockRowCountMap.entrySet()) {
      String key = blockRowEntry.getKey();
      long alreadyDeletedCount = 0;

      SegmentUpdateDetails detail = segmentUpdateStatusManager.getDetailsForABlock(key);

      if (null != detail) {

        alreadyDeletedCount = Long.parseLong(detail.getDeletedRowsInBlock());

      }

      RowCountDetailsVO rowCountDetailsVO =
              new RowCountDetailsVO(blockRowEntry.getValue(), alreadyDeletedCount);
      outputMap.put(key, rowCountDetailsVO);

    }

    blockMappingVO.setCompleteBlockRowDetailVO(outputMap);

  }

  /**
   *
   * @param segID
   * @param blockName
   * @return
   */
  public static String getSegmentBlockNameKey(String segID, String blockName,
      boolean isPartitionTable) {
    String blockNameWithOutPart = blockName
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3724
        .substring(blockName.indexOf(CarbonCommonConstants.HYPHEN) + 1,
            blockName.lastIndexOf(CarbonTablePath.getCarbonDataExtension()));
    if (isPartitionTable) {
      return blockNameWithOutPart;
    }
    return segID + CarbonCommonConstants.FILE_SEPARATOR + blockNameWithOutPart;
  }

  /**
   * Below method will be used to get the latest delete delta file timestamp
   * @param deleteDeltaFiles
   * @return latest delete delta file time stamp
   */
  public static long getLatestDeleteDeltaTimestamp(String[] deleteDeltaFiles) {
    long latestTimestamp = 0;
    for (int i = 0; i < deleteDeltaFiles.length; i++) {
      long convertTimeStampToLong = Long.parseLong(
          CarbonTablePath.DataFileUtil.getTimeStampFromDeleteDeltaFile(deleteDeltaFiles[i]));
      if (latestTimestamp < convertTimeStampToLong) {
        latestTimestamp = convertTimeStampToLong;
      }
    }
    return latestTimestamp;
  }
}
