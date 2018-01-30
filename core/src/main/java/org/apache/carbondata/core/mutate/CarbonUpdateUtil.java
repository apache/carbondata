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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.data.BlockMappingVO;
import org.apache.carbondata.core.mutate.data.RowCountDetailsVO;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;


/**
 * This class contains all update utility methods
 */
public class CarbonUpdateUtil {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(CarbonUpdateUtil.class.getName());

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
  public static String getSegmentWithBlockFromTID(String Tid) {
    return getRequiredFieldFromTID(Tid, TupleIdEnum.SEGMENT_ID)
        + CarbonCommonConstants.FILE_SEPARATOR + getRequiredFieldFromTID(Tid, TupleIdEnum.BLOCK_ID);
  }

  /**
   * Returns block path from tuple id
   *
   * @param tid
   * @param factPath
   * @return
   */
  public static String getTableBlockPath(String tid, String factPath, boolean isPartitionTable) {
    String partField = getRequiredFieldFromTID(tid, TupleIdEnum.PART_ID);
    if (isPartitionTable) {
      return factPath + CarbonCommonConstants.FILE_SEPARATOR + partField;
    }
    String part = CarbonTablePath.addPartPrefix(partField);
    String segment =
            CarbonTablePath.addSegmentPrefix(getRequiredFieldFromTID(tid, TupleIdEnum.SEGMENT_ID));
    return factPath + CarbonCommonConstants.FILE_SEPARATOR + part
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
    SegmentUpdateStatusManager segmentUpdateStatusManager =
            new SegmentUpdateStatusManager(table.getAbsoluteTableIdentifier());
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
          int index = oldList.indexOf(newBlockEntry);
          if (index != -1) {
            // update the element in existing list.
            SegmentUpdateDetails blockDetail = oldList.get(index);
            if (blockDetail.getDeleteDeltaStartTimestamp().isEmpty() || (isCompaction == true)) {
              blockDetail
                  .setDeleteDeltaStartTimestamp(newBlockEntry.getDeleteDeltaStartTimestamp());
            }
            blockDetail.setDeleteDeltaEndTimestamp(newBlockEntry.getDeleteDeltaEndTimestamp());
            blockDetail.setSegmentStatus(newBlockEntry.getSegmentStatus());
            blockDetail.setDeletedRowsInBlock(newBlockEntry.getDeletedRowsInBlock());
          } else {
            // add the new details to the list.
            oldList.add(newBlockEntry);
          }
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
      CarbonTable table, String updatedTimeStamp, boolean isTimestampUpdationRequired,
      List<Segment> segmentsToBeDeleted) {
    return updateTableMetadataStatus(updatedSegmentsList, table, updatedTimeStamp,
        isTimestampUpdationRequired, segmentsToBeDeleted, new ArrayList<Segment>());
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
      List<Segment> segmentsToBeDeleted, List<Segment> segmentFilesTobeUpdated) {

    boolean status = false;

    String metaDataFilepath = table.getMetaDataFilepath();

    AbsoluteTableIdentifier absoluteTableIdentifier = table.getAbsoluteTableIdentifier();

    CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(absoluteTableIdentifier.getTablePath(),
                    absoluteTableIdentifier.getCarbonTableIdentifier());

    String tableStatusPath = carbonTablePath.getTableStatusFilePath();

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    boolean lockStatus = false;
    try {
      lockStatus = carbonLock.lockWithRetries();
      if (lockStatus) {
        LOGGER.info(
                "Acquired lock for table" + table.getDatabaseName() + "." + table.getTableName()
                        + " for table status updation");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
                segmentStatusManager.readLoadMetadata(metaDataFilepath);

        for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {

          if (isTimestampUpdationRequired) {
            // we are storing the link between the 2 status files in the segment 0 only.
            if (loadMetadata.getLoadName().equalsIgnoreCase("0")) {
              loadMetadata.setUpdateStatusFileName(
                      CarbonUpdateUtil.getUpdateStatusFileName(updatedTimeStamp));
            }

            // if the segments is in the list of marked for delete then update the status.
            if (segmentsToBeDeleted.contains(new Segment(loadMetadata.getLoadName(), null))) {
              loadMetadata.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
              loadMetadata.setModificationOrdeletionTimesStamp(Long.parseLong(updatedTimeStamp));
            }
          }
          for (Segment segName : updatedSegmentsList) {
            if (loadMetadata.getLoadName().equalsIgnoreCase(segName.getSegmentNo())) {
              // if this call is coming from the delete delta flow then the time stamp
              // String will come empty then no need to write into table status file.
              if (isTimestampUpdationRequired) {
                loadMetadata.setIsDeleted(CarbonCommonConstants.KEYWORD_TRUE);
                // if in case of update flow.
                if (loadMetadata.getUpdateDeltaStartTimestamp().isEmpty()) {
                  // this means for first time it is getting updated .
                  loadMetadata.setUpdateDeltaStartTimestamp(updatedTimeStamp);
                }
                // update end timestamp for each time.
                loadMetadata.setUpdateDeltaEndTimestamp(updatedTimeStamp);
              }
              if (segmentFilesTobeUpdated.contains(Segment.toSegment(loadMetadata.getLoadName()))) {
                loadMetadata.setSegmentFile(loadMetadata.getLoadName() + "_" + updatedTimeStamp
                    + CarbonTablePath.SEGMENT_EXT);
              }
            }
          }
        }

        try {
          segmentStatusManager
                  .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
        } catch (IOException e) {
          return false;
        }

        status = true;
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + table
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

    AbsoluteTableIdentifier absoluteTableIdentifier = table.getAbsoluteTableIdentifier();

    CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(absoluteTableIdentifier.getTablePath(),
                    absoluteTableIdentifier.getCarbonTableIdentifier());
    String partitionDir = carbonTablePath.getPartitionDir();
    CarbonFile file =
            FileFactory.getCarbonFile(partitionDir, FileFactory.getFileType(partitionDir));
    if (!file.exists()) {
      return;
    }
    for (CarbonFile eachDir : file.listFiles()) {
      // for each dir check if the file with the delta timestamp is present or not.
      CarbonFile[] toBeDeleted = eachDir.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
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
    return completeBlockName
        .substring(0, completeBlockName.lastIndexOf(CarbonCommonConstants.HYPHEN));
  }

  public static long getLatestTaskIdForSegment(String segmentId, CarbonTablePath tablePath) {
    String segmentDirPath = tablePath.getCarbonDataDirectoryPath(segmentId);

    // scan all the carbondata files and get the latest task ID.
    CarbonFile segment =
            FileFactory.getCarbonFile(segmentDirPath, FileFactory.getFileType(segmentDirPath));
    CarbonFile[] dataFiles = segment.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {

        if (file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT)) {
          return true;
        }
        return false;
      }
    });
    long max = 0;
    if (null != dataFiles) {
      for (CarbonFile file : dataFiles) {
        long taskNumber =
            Long.parseLong(CarbonTablePath.DataFileUtil.getTaskNo(file.getName()).split("_")[0]);
        if (taskNumber > max) {
          max = taskNumber;
        }
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
  public static void cleanUpDeltaFiles(CarbonTable table, boolean forceDelete) {

    SegmentStatusManager ssm = new SegmentStatusManager(table.getAbsoluteTableIdentifier());

    CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(table.getAbsoluteTableIdentifier().getTablePath(),
                    table.getAbsoluteTableIdentifier().getCarbonTableIdentifier());

    LoadMetadataDetails[] details = ssm.readLoadMetadata(table.getMetaDataFilepath());

    String validUpdateStatusFile = "";

    boolean isAbortedFile = true;

    boolean isInvalidFile = false;

    // scan through each segment.

    for (LoadMetadataDetails segment : details) {

      // take the update status file name from 0th segment.
      validUpdateStatusFile = ssm.getUpdateStatusFileName(details);

      // if this segment is valid then only we will go for delta file deletion.
      // if the segment is mark for delete or compacted then any way it will get deleted.

      if (segment.getSegmentStatus() == SegmentStatus.SUCCESS
              || segment.getSegmentStatus() == SegmentStatus.LOAD_PARTIAL_SUCCESS) {

        // take the list of files from this segment.
        String segmentPath = carbonTablePath.getCarbonDataDirectoryPath(segment.getLoadName());
        CarbonFile segDir =
                FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
        CarbonFile[] allSegmentFiles = segDir.listFiles();

        // scan through the segment and find the carbondatafiles and index files.
        SegmentUpdateStatusManager updateStatusManager =
                new SegmentUpdateStatusManager(table.getAbsoluteTableIdentifier());

        // deleting of the aborted file scenario.
        deleteStaleCarbonDataFiles(segment, allSegmentFiles, updateStatusManager);

        // get Invalid update  delta files.
        CarbonFile[] invalidUpdateDeltaFiles = updateStatusManager
            .getUpdateDeltaFilesList(segment.getLoadName(), false,
                CarbonCommonConstants.UPDATE_DELTA_FILE_EXT, true, allSegmentFiles,
                isInvalidFile);

        // now for each invalid delta file need to check the query execution time out
        // and then delete.

        for (CarbonFile invalidFile : invalidUpdateDeltaFiles) {

          compareTimestampsAndDelete(invalidFile, forceDelete, false);
        }

        // do the same for the index files.
        CarbonFile[] invalidIndexFiles = updateStatusManager
            .getUpdateDeltaFilesList(segment.getLoadName(), false,
                CarbonCommonConstants.UPDATE_INDEX_FILE_EXT, true, allSegmentFiles,
                isInvalidFile);

        // now for each invalid index file need to check the query execution time out
        // and then delete.

        for (CarbonFile invalidFile : invalidIndexFiles) {

          compareTimestampsAndDelete(invalidFile, forceDelete, false);
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
          invalidDeleteDeltaFiles = updateStatusManager
              .getDeleteDeltaInvalidFilesList(block, false,
                  allSegmentFiles, isAbortedFile);
          for (CarbonFile invalidFile : invalidDeleteDeltaFiles) {
            boolean doForceDelete = true;
            compareTimestampsAndDelete(invalidFile, doForceDelete, false);
          }

          // case 1
          if (CarbonUpdateUtil.isBlockInvalid(block.getSegmentStatus())) {
            completeListOfDeleteDeltaFiles = updateStatusManager
                    .getDeleteDeltaInvalidFilesList(block, true,
                            allSegmentFiles, isInvalidFile);
            for (CarbonFile invalidFile : completeListOfDeleteDeltaFiles) {

              compareTimestampsAndDelete(invalidFile, forceDelete, false);
            }

            CarbonFile[] blockRelatedFiles = updateStatusManager
                    .getAllBlockRelatedFiles(allSegmentFiles,
                            block.getActualBlockName());

            // now for each invalid index file need to check the query execution time out
            // and then delete.

            for (CarbonFile invalidFile : blockRelatedFiles) {

              compareTimestampsAndDelete(invalidFile, forceDelete, false);
            }


          } else {
            invalidDeleteDeltaFiles = updateStatusManager
                    .getDeleteDeltaInvalidFilesList(block, false,
                            allSegmentFiles, isInvalidFile);
            for (CarbonFile invalidFile : invalidDeleteDeltaFiles) {

              compareTimestampsAndDelete(invalidFile, forceDelete, false);
            }
          }
        }
      }
    }

    // delete the update table status files which are old.
    if (null != validUpdateStatusFile && !validUpdateStatusFile.isEmpty()) {

      final String updateStatusTimestamp = validUpdateStatusFile
              .substring(validUpdateStatusFile.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1);

      CarbonFile metaFolder = FileFactory.getCarbonFile(carbonTablePath.getMetadataDirectoryPath(),
              FileFactory.getFileType(carbonTablePath.getMetadataDirectoryPath()));

      CarbonFile[] invalidUpdateStatusFiles = metaFolder.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          if (file.getName().startsWith(CarbonCommonConstants.TABLEUPDATESTATUS_FILENAME)) {

            // CHECK if this is valid or not.
            // we only send invalid ones to delete.
            if (!file.getName().endsWith(updateStatusTimestamp)) {
              return true;
            }
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
   * This function deletes all the stale carbondata files during clean up before update operation
   * one scenario is if update operation is ubruptly stopped before updation of table status then
   * the carbondata file created during update operation is stale file and it will be deleted in
   * this function in next update operation
   * @param segment
   * @param allSegmentFiles
   * @param updateStatusManager
   */
  private static void deleteStaleCarbonDataFiles(LoadMetadataDetails segment,
      CarbonFile[] allSegmentFiles, SegmentUpdateStatusManager updateStatusManager) {
    CarbonFile[] invalidUpdateDeltaFiles = updateStatusManager
        .getUpdateDeltaFilesList(segment.getLoadName(), false,
            CarbonCommonConstants.UPDATE_DELTA_FILE_EXT, true, allSegmentFiles,
            true);
    // now for each invalid delta file need to check the query execution time out
    // and then delete.
    for (CarbonFile invalidFile : invalidUpdateDeltaFiles) {
      compareTimestampsAndDelete(invalidFile, true, false);
    }
    // do the same for the index files.
    CarbonFile[] invalidIndexFiles = updateStatusManager
        .getUpdateDeltaFilesList(segment.getLoadName(), false,
            CarbonCommonConstants.UPDATE_INDEX_FILE_EXT, true, allSegmentFiles,
            true);
    // now for each invalid index file need to check the query execution time out
    // and then delete.
    for (CarbonFile invalidFile : invalidIndexFiles) {
      compareTimestampsAndDelete(invalidFile, true, false);
    }
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
  private static void compareTimestampsAndDelete(CarbonFile invalidFile,
                                                 boolean forceDelete, boolean isUpdateStatusFile) {
    long fileTimestamp = 0L;

    if (isUpdateStatusFile) {
      fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(invalidFile.getName()
              .substring(invalidFile.getName().lastIndexOf(CarbonCommonConstants.HYPHEN) + 1));
    } else {
      fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(
              CarbonTablePath.DataFileUtil.getTimeStampFromFileName(invalidFile.getName()));
    }

    // if the timestamp of the file is more than the current time by query execution timeout.
    // then delete that file.
    if (CarbonUpdateUtil.isMaxQueryTimeoutExceeded(fileTimestamp) || forceDelete) {
      // delete the files.
      try {
        LOGGER.info("deleting the invalid file : " + invalidFile.getName());
        CarbonUtil.deleteFoldersAndFiles(invalidFile);
      } catch (IOException e) {
        LOGGER.error("error in clean up of compacted files." + e.getMessage());
      } catch (InterruptedException e) {
        LOGGER.error("error in clean up of compacted files." + e.getMessage());
      }
    }
  }

  public static boolean isBlockInvalid(SegmentStatus blockStatus) {
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
        segmentsToBeDeleted.add(new Segment(eachSeg.getKey(), null));
      }

    }
    return segmentsToBeDeleted;
  }

  /**
   * Return row count of input block
   */
  public static long getRowCount(
      BlockMappingVO blockMappingVO,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(absoluteTableIdentifier);
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
  public static String getSegmentBlockNameKey(String segID, String blockName) {

    String blockNameWithOutPart = blockName
            .substring(blockName.indexOf(CarbonCommonConstants.HYPHEN) + 1,
                    blockName.lastIndexOf(CarbonTablePath.getCarbonDataExtension()));

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
