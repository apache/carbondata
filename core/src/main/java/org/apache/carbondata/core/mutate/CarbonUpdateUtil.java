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

import org.apache.commons.collections.CollectionUtils;
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
    if (isPartitionTable) {
      return getRequiredFieldFromTID(Tid, TupleIdEnum.PARTITION_SEGMENT_ID);
    }
    // this case is to check for add segment case, as now the segment id is present at first index,
    // in add segment case, it will be in second index as the blockletID is generated by adding the
    // complete external path
    // this is in case of the external segment, where the tuple id has external path with #
    // here no need to check for any path present in metadta details, as # can come in tuple id in
    // case of multiple partitions, so partition check is already present above.
    if (Tid.contains("#/")) {
      return getRequiredFieldFromTID(Tid, TupleIdEnum.EXTERNAL_SEGMENT_ID)
          + CarbonCommonConstants.FILE_SEPARATOR + getRequiredFieldFromTID(Tid,
          TupleIdEnum.EXTERNAL_BLOCK_ID);
    }
    return getRequiredFieldFromTID(Tid, TupleIdEnum.SEGMENT_ID)
        + CarbonCommonConstants.FILE_SEPARATOR + getRequiredFieldFromTID(Tid, TupleIdEnum.BLOCK_ID);
  }

  /**
   * Returns block path from tuple id
   */
  public static String getTableBlockPath(String tid, String tablePath, boolean isStandardTable,
      boolean isPartitionTable) {
    String partField = "0";
    // If it has segment file then part field can be appended directly to table path
    if (!isStandardTable) {
      if (isPartitionTable) {
        partField = getRequiredFieldFromTID(tid, TupleIdEnum.PARTITION_PART_ID);
        return tablePath + CarbonCommonConstants.FILE_SEPARATOR + partField.replace("#", "/");
      } else {
        return tablePath;
      }
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
      CarbonTable table, String updateStatusFileIdentifier, boolean isCompaction,
      boolean isForceWrite) {
    boolean status = false;
    SegmentUpdateStatusManager segmentUpdateStatusManager = new SegmentUpdateStatusManager(table);
    if (isForceWrite && !CollectionUtils.isEmpty(updateDetailsList)) {
      String segId = String.valueOf(SegmentStatusManager
          .createNewSegmentId(segmentUpdateStatusManager.getLoadMetadataDetails()));
      for (SegmentUpdateDetails detail : updateDetailsList) {
        detail.setSegmentName(segId);
      }
    }
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
          mergeSegmentUpdate(isCompaction, oldList, newBlockEntry);
        }

        List<SegmentUpdateDetails> updateDetailsValidSeg = new ArrayList<>();
        Set<String> loadDetailsSet = new HashSet<>();
        for (LoadMetadataDetails details : segmentUpdateStatusManager.getLoadMetadataDetails()) {
          loadDetailsSet.add(details.getLoadName());
        }
        for (SegmentUpdateDetails updateDetails : oldList) {
          if (loadDetailsSet.contains(updateDetails.getSegmentName())) {
            // we should only keep the update info of segments in table status, especially after
            // compaction and clean files some compacted segments will be removed. It can keep
            // tableupdatestatus file in small size which is good for performance.
            updateDetailsValidSeg.add(updateDetails);
          }
        }
        // In case of ForceWrite, write the segmentUpdateDetails to the tableUpdateStatus file
        // without any validation of segments.
        if (isForceWrite) {
          segmentUpdateStatusManager.writeLoadDetailsIntoFile(oldList, updateStatusFileIdentifier);
        } else {
          segmentUpdateStatusManager
              .writeLoadDetailsIntoFile(updateDetailsValidSeg, updateStatusFileIdentifier);
        }
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
          LOGGER.info("Unlock the segment update lock successful.");
        } else {
          LOGGER.error("Not able to unlock the segment update lock.");
        }
      }
    }
    return status;
  }

  public static void mergeSegmentUpdate(boolean isCompaction, List<SegmentUpdateDetails> oldList,
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
   * @param isTimestampUpdateRequired
   * @param segmentsToBeDeleted
   * @return
   */
  public static boolean updateTableMetadataStatus(Set<Segment> updatedSegmentsList,
      CarbonTable table, String updatedTimeStamp, boolean isTimestampUpdateRequired,
      boolean isUpdateStatusFileUpdateRequired, List<Segment> segmentsToBeDeleted) {
    return updateTableMetadataStatus(updatedSegmentsList, table, updatedTimeStamp,
        isTimestampUpdateRequired, isUpdateStatusFileUpdateRequired,
        segmentsToBeDeleted, new ArrayList<Segment>(), "");
  }

  /**
   *
   * @param updatedSegmentsList
   * @param table
   * @param updatedTimeStamp
   * @param isTimestampUpdateRequired
   * @param segmentsToBeDeleted
   * @return
   */
  public static boolean updateTableMetadataStatus(Set<Segment> updatedSegmentsList,
      CarbonTable table, String updatedTimeStamp, boolean isTimestampUpdateRequired,
      boolean isUpdateStatusFileUpdateRequired, List<Segment> segmentsToBeDeleted,
      List<Segment> segmentFilesTobeUpdated, String uuid) {

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
        LOGGER.info("Acquired lock for table" + table.getDatabaseName() + "." + table.getTableName()
             + " for table status update");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
          // we are storing the link between the 2 status files in the segment 0 only.
          if (isUpdateStatusFileUpdateRequired &&
              loadMetadata.getLoadName().equalsIgnoreCase("0")) {
            loadMetadata.setUpdateStatusFileName(
                CarbonUpdateUtil.getUpdateStatusFileName(updatedTimeStamp));
          }

          if (isTimestampUpdateRequired) {
            // if the segments is in the list of marked for delete then update the status.
            if (segmentsToBeDeleted.contains(new Segment(loadMetadata.getLoadName()))) {
              loadMetadata.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
              loadMetadata.setModificationOrDeletionTimestamp(Long.parseLong(updatedTimeStamp));
            }
          }
          for (Segment segName : updatedSegmentsList) {
            if (loadMetadata.getLoadName().equalsIgnoreCase(segName.getSegmentNo())) {
              // if this call is coming from the delete delta flow then the time stamp
              // String will come empty then no need to write into table status file.
              if (isTimestampUpdateRequired) {
                // if in case of update flow.
                if (loadMetadata.getUpdateDeltaStartTimestamp().isEmpty()) {
                  // this means for first time it is getting updated .
                  loadMetadata.setUpdateDeltaStartTimestamp(updatedTimeStamp);
                }
                // update end timestamp for each time.
                loadMetadata.setUpdateDeltaEndTimestamp(updatedTimeStamp);
              }
              if (segmentFilesTobeUpdated
                  .contains(Segment.toSegment(loadMetadata.getLoadName(), null))) {
                loadMetadata.setSegmentFile(loadMetadata.getLoadName() + "_" + updatedTimeStamp
                    + CarbonTablePath.SEGMENT_EXT);
              }
            }
          }
        }

        try {
          SegmentStatusManager
                  .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
        } catch (IOException e) {
          return false;
        }

        status = true;
      } else {
        LOGGER.error("Not able to acquire the lock for Table status update for table " + table
                .getDatabaseName() + "." + table.getTableName());
      }
    } finally {
      if (lockStatus) {
        if (carbonLock.unlock()) {
          LOGGER.info(
                 "Table unlocked successfully after table status update" + table.getDatabaseName()
                          + "." + table.getTableName());
        } else {
          LOGGER.error(
                  "Unable to unlock Table lock for table" + table.getDatabaseName() + "." + table
                          .getTableName() + " during table status update");
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

    AbsoluteTableIdentifier identifier = table.getAbsoluteTableIdentifier();
    String partitionDir = CarbonTablePath.getPartitionDir(identifier.getTablePath());
    CarbonFile file =
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
          return fileName.endsWith(timeStamp + CarbonCommonConstants.DELETE_DELTA_FILE_EXT);
        }
      });
      // deleting the files of a segment.
      try {
        CarbonUtil.deleteFoldersAndFilesSilent(toBeDeleted);
      } catch (IOException | InterruptedException e) {
        LOGGER.error("Exception in deleting the delta files." + e);
      }
    }
  }

  /**
   * returns timestamp as long value
   *
   * @param timestamp
   * @return
   */
  public static Long getTimeStampAsLong(String timestamp) {
    try {
      return Long.parseLong(timestamp);
    } catch (NumberFormatException nfe) {
      String errorMsg = "Invalid timestamp : " + timestamp;
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

  /**
   * returns segment id from segment name
   *
   * @param segmentName
   * @return
   */
  public static String getSegmentId(String segmentName) {
    return segmentName.split(CarbonCommonConstants.UNDERSCORE)[1];
  }

  public static long getLatestTaskIdForSegment(Segment segment, String tablePath)
      throws IOException {
    long max = 0;
    List<String> dataFiles = new ArrayList<>();
    if (segment.getSegmentFileName() != null) {
      SegmentFileStore fileStore = new SegmentFileStore(tablePath, segment.getSegmentFileName());
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
          FileFactory.getCarbonFile(segmentDirPath);
      CarbonFile[] carbonDataFiles = segmentDir.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          return file.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT);
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
        segmentsToBeDeleted.add(new Segment(eachSeg.getKey(), ""));
      }

    }
    return segmentsToBeDeleted;
  }

  /**
   * Return row count of input block
   */
  public static long getRowCount(BlockMappingVO blockMappingVO, CarbonTable carbonTable) {
    if (blockMappingVO.getBlockRowCountMapping().size() == 1
        && blockMappingVO.getBlockRowCountMapping().get(CarbonCommonConstantsInternal.ROW_COUNT)
        != null) {
      return blockMappingVO.getBlockRowCountMapping().get(CarbonCommonConstantsInternal.ROW_COUNT);
    }
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
    String blockNameWithOutPartAndBatchNo = blockName
        .substring(blockName.indexOf(CarbonCommonConstants.HYPHEN) + 1,
            blockName.lastIndexOf(CarbonTablePath.getCarbonDataExtension()))
        .replace(CarbonTablePath.BATCH_PREFIX, CarbonCommonConstants.UNDERSCORE);
    // to remove compressor name
    int index = blockNameWithOutPartAndBatchNo.lastIndexOf(CarbonCommonConstants.POINT);
    if (index != -1) {
      blockNameWithOutPartAndBatchNo = blockNameWithOutPartAndBatchNo
          .replace(blockNameWithOutPartAndBatchNo.substring(index), "");
    }
    if (isPartitionTable) {
      return blockNameWithOutPartAndBatchNo;
    } else {
      return segID + CarbonCommonConstants.FILE_SEPARATOR + blockNameWithOutPartAndBatchNo;
    }
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
