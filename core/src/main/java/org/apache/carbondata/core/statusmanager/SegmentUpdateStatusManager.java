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

package org.apache.carbondata.core.statusmanager;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Manages Segment & block status of carbon table for Delete operation
 */
public class SegmentUpdateStatusManager {

  /**
   * logger
   */
  private static final Logger LOG =
      LogServiceFactory.getLogService(SegmentUpdateStatusManager.class.getName());

  private final AbsoluteTableIdentifier identifier;
  private LoadMetadataDetails[] segmentDetails;
  private SegmentUpdateDetails[] updateDetails;
  private Map<String, SegmentUpdateDetails> blockAndDetailsMap;
  private boolean isPartitionTable;
  /**
   * It contains the mapping of segment path and corresponding delete delta file paths,
   * avoiding listing these files for every query
   */
  private Map<String, List<String>> segmentDeleteDeltaListMap = new HashMap<>();

  public SegmentUpdateStatusManager(CarbonTable table,
      LoadMetadataDetails[] segmentDetails) {
    this(table, segmentDetails, null);
  }

  /**
   * It takes the updateVersion as one of the parameter. Basically user can give on which
   * updateVersion user can retrieve the data.It is useful to get the history changed data
   * of a particular version.
   */
  public SegmentUpdateStatusManager(CarbonTable table,
      LoadMetadataDetails[] segmentDetails, String updateVersion) {
    this.identifier = table.getAbsoluteTableIdentifier();
    this.isPartitionTable = table.isHivePartitionTable();
    // current it is used only for read function scenarios, as file update always requires to work
    // on latest file status.
    this.segmentDetails = segmentDetails;
    updateDetails = readLoadMetadata();
    updateUpdateDetails(updateVersion);
    populateMap();
  }

  public SegmentUpdateStatusManager(CarbonTable table) {
    this(table, (String) null);
  }

  public SegmentUpdateStatusManager(CarbonTable table, String updateVersion) {
    this.identifier = table.getAbsoluteTableIdentifier();
    // current it is used only for read function scenarios, as file update always requires to work
    // on latest file status.
    if (!table.getTableInfo().isTransactionalTable()) {
      // fileExist is costly operation, so check based on table Type
      segmentDetails = new LoadMetadataDetails[0];
    } else {
      segmentDetails = SegmentStatusManager.readLoadMetadata(
          CarbonTablePath.getMetadataPath(identifier.getTablePath()));
    }
    this.isPartitionTable = table.isHivePartitionTable();
    if (segmentDetails.length != 0) {
      updateDetails = readLoadMetadata();
    } else {
      updateDetails = new SegmentUpdateDetails[0];
    }
    updateUpdateDetails(updateVersion);
    populateMap();
  }

  /**
   * It adds only the SegmentUpdateDetails of given updateVersion, it is used to get the history
   * data of updated/deleted data.
   */
  private void updateUpdateDetails(String updateVersion) {
    if (updateVersion != null) {
      List<SegmentUpdateDetails> newUpdateDetails = new ArrayList<>();
      for (SegmentUpdateDetails updateDetail : updateDetails) {
        if (updateDetail.getDeltaFileStamps() != null) {
          if (updateDetail.getDeltaFileStamps().contains(updateVersion)) {
            HashSet<String> set = new HashSet<>();
            set.add(updateVersion);
            updateDetail.setDeltaFileStamps(set);
            updateDetail.setSegmentStatus(SegmentStatus.SUCCESS);
            newUpdateDetails.add(updateDetail);
          }
        } else if (updateDetail.getDeleteDeltaStartTimestamp().equalsIgnoreCase(updateVersion)) {
          updateDetail.setSegmentStatus(SegmentStatus.SUCCESS);
          newUpdateDetails.add(updateDetail);
        }
      }
      updateDetails = newUpdateDetails.toArray(new SegmentUpdateDetails[0]);
    }
  }

  /**
   * populate the block and its details in a map.
   */
  private void populateMap() {
    blockAndDetailsMap = new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (SegmentUpdateDetails blockDetails : updateDetails) {
      String blockIdentifier = CarbonUpdateUtil
          .getSegmentBlockNameKey(blockDetails.getSegmentName(), blockDetails.getActualBlockName(),
              isPartitionTable);
      blockAndDetailsMap.put(blockIdentifier, blockDetails);
    }
  }

  /**
   *
   * @param segID
   * @param actualBlockName
   * @return null if block is not present in segment update status.
   */
  private SegmentUpdateDetails getDetailsForABlock(String segID, String actualBlockName) {

    String blockIdentifier = CarbonUpdateUtil
        .getSegmentBlockNameKey(segID, actualBlockName, isPartitionTable);

    return blockAndDetailsMap.get(blockIdentifier);

  }

  /**
   *
   * @param key will be like (segmentId/blockName)  0/0-0-5464654654654
   * @return
   */
  public SegmentUpdateDetails getDetailsForABlock(String key) {

    return blockAndDetailsMap.get(key);

  }

  /**
   * Returns the LoadMetadata Details
   * @return
   */
  public LoadMetadataDetails[] getLoadMetadataDetails() {
    return segmentDetails;
  }

  /**
   * Returns the UpdateStatus Details.
   * @return
   */
  public SegmentUpdateDetails[] getUpdateStatusDetails() {
    return updateDetails;
  }

  /**
   *
   * @param segmentUpdateDetails
   */
  public void setUpdateStatusDetails(SegmentUpdateDetails[] segmentUpdateDetails) {
    this.updateDetails = segmentUpdateDetails;
  }

  /**
   * This will return the lock object used to lock the table update status file before updating.
   *
   * @return
   */
  public ICarbonLock getTableUpdateStatusLock() {
    return CarbonLockFactory.getCarbonLockObj(identifier,
        LockUsage.TABLE_UPDATE_STATUS_LOCK);
  }

  /**
   * Below method will be used to get all the delete delta files based on block name
   *
   * @param blockFilePath actual block filePath
   * @return all delete delta files
   */
  public String[] getDeleteDeltaFilePath(String blockFilePath, String segmentId) {
    return getDeltaFiles(blockFilePath, segmentId, CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
        .toArray(new String[0]);
  }

  /**
   * Returns all delta file paths of specified block
   */
  private List<String> getDeltaFiles(String blockPath, String segment, String extension) {
    Path path = new Path(blockPath);
    String completeBlockName = path.getName();
    String blockNameWithoutExtension =
        completeBlockName.substring(0, completeBlockName.lastIndexOf('.'));
    //blockName without timestamp
    final String blockNameFromTuple =
        blockNameWithoutExtension.substring(0, blockNameWithoutExtension.lastIndexOf("-"));
    return getDeltaFiles(path.getParent().toString(), blockNameFromTuple, extension, segment);
  }

  /**
   * This method returns the list of Blocks associated with the segment
   * from the SegmentUpdateDetails List.
   * @param segmentName
   * @return
   */
  public List<String> getBlockNameFromSegment(String segmentName) {
    List<String> blockNames = new ArrayList<String>();
    for (SegmentUpdateDetails block : updateDetails) {
      if (block.getSegmentName().equalsIgnoreCase(segmentName) && !CarbonUpdateUtil
          .isBlockInvalid(block.getSegmentStatus())) {
        blockNames.add(block.getBlockName());
      }
    }
    return blockNames;
  }

  /**
   * check the block whether is valid
   *
   * @param segName segment name
   * @param blockName  block name
   * @return the status of block whether is valid
   */
  public boolean isBlockValid(String segName, String blockName) {

    SegmentUpdateDetails details = getDetailsForABlock(segName, blockName);

    return details == null || !CarbonUpdateUtil.isBlockInvalid(details.getSegmentStatus());

  }

  /**
   * Returns all delta file paths of specified block
   *
   * @param blockDir block directory with CarbonFile format
   * @param blockNameFromTuple block name from tuple
   * @param extension the file extension name
   * @param segment the segment name
   * @return the list of delete file
   */
  private List<String> getDeltaFiles(String blockDir, final String blockNameFromTuple,
      final String extension, String segment) {
    List<String> deleteFileList = new ArrayList<>();
    for (SegmentUpdateDetails block : updateDetails) {
      if (block.getBlockName().equalsIgnoreCase(blockNameFromTuple) && block.getSegmentName()
          .equalsIgnoreCase(segment) && !CarbonUpdateUtil
          .isBlockInvalid(block.getSegmentStatus())) {
        final long deltaStartTimestamp = getStartTimeOfDeltaFile(extension, block);
        // If there is no delete delete file , then return null
        if (deltaStartTimestamp == 0) {
          return deleteFileList;
        }
        final long deltaEndTimeStamp = getEndTimeOfDeltaFile(extension, block);
        // If start and end time is same then it has only one delta file so construct the file
        // directly with available information with out listing
        if (block.getDeleteDeltaStartTimestamp().equals(block.getDeleteDeltaEndTimestamp())) {
          deleteFileList.add(
              new StringBuilder(blockDir).append(CarbonCommonConstants.FILE_SEPARATOR)
                  .append(block.getBlockName()).append("-")
                  .append(block.getDeleteDeltaStartTimestamp()).append(extension).toString());
          // If delta timestamp list has data then it has multiple delta file so construct the file
          // directly with list of deltas with out listing
        } else if (block.getDeltaFileStamps() != null && block.getDeltaFileStamps().size() > 0) {
          for (String delta : block.getDeltaFileStamps()) {
            deleteFileList.add(
                new StringBuilder(blockDir).append(CarbonCommonConstants.FILE_SEPARATOR)
                    .append(block.getBlockName()).append("-").append(delta).append(extension)
                    .toString());
          }
        } else {
          // It is for backward compatibility.It lists the files.
          return getFilePaths(blockDir, blockNameFromTuple, extension, deleteFileList,
              deltaStartTimestamp, deltaEndTimeStamp);
        }
      }
    }
    return deleteFileList;
  }

  private List<String> getFilePaths(String blockDir, final String blockNameFromTuple,
      final String extension, List<String> deleteFileList, final long deltaStartTimestamp,
      final long deltaEndTimeStamp) {
    List<String> deltaList = segmentDeleteDeltaListMap.get(blockDir);
    if (deltaList == null) {
      CarbonFile[] files = FileFactory.getCarbonFile(blockDir).listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile pathName) {
          String fileName = pathName.getName();
          return fileName.endsWith(extension) && pathName.getSize() > 0;
        }
      });
      deltaList = new ArrayList<>(files.length);
      for (CarbonFile file : files) {
        deltaList.add(file.getCanonicalPath());
      }
      segmentDeleteDeltaListMap.put(blockDir, deltaList);
    }
    for (String deltaFile : deltaList) {
      String deltaFilePathName = new Path(deltaFile).getName();
      String firstPart = deltaFilePathName.substring(0, deltaFilePathName.lastIndexOf('.'));
      String blockName =
          firstPart.substring(0, firstPart.lastIndexOf(CarbonCommonConstants.HYPHEN));
      long timestamp =
          Long.parseLong(CarbonTablePath.DataFileUtil.getTimeStampFromFileName(deltaFilePathName));
      // It compares whether this delta file belongs to this block or not. And also checks that
      // corresponding delta file is valid or not by considering its load start and end time with
      // the file timestamp.
      if (blockNameFromTuple.equals(blockName) && ((timestamp <= deltaEndTimeStamp)
          && (timestamp >= deltaStartTimestamp))) {
        if (null == deleteFileList) {
          deleteFileList = new ArrayList<String>();
        }
        deleteFileList.add(deltaFile);
      }
    }
    return deleteFileList;
  }

  /**
   * Get all delete delta files of the block of specified segment.
   * Actually, delete delta file name is generated from each SegmentUpdateDetails.
   *
   * @param segment the segment which is to find block and its delete delta files
   * @param blockName the specified block of the segment
   * @return delete delta file list of the block
   */
  public List<String> getDeleteDeltaFilesList(final Segment segment, final String blockName)
      throws IOException {
    List<String> deleteDeltaFileList = new ArrayList<>();
    String segmentPath = null;
    if (segment.isExternalSegment()) {
      for (LoadMetadataDetails details : segmentDetails) {
        if (details.getLoadName().equals(segment.getSegmentNo())) {
          segmentPath = details.getPath();
          break;
        }
      }
    } else if (isPartitionTable) {
      String segmentFileName = Arrays.stream(segmentDetails).filter(
          loadMetaDataDetail -> loadMetaDataDetail.getLoadName()
              .equalsIgnoreCase(segment.getSegmentNo())).collect(Collectors.toList()).get(0)
          .getSegmentFile();
      SegmentFileStore segmentFileStore =
          new SegmentFileStore(identifier.getTablePath(), segmentFileName);
      segmentFileStore.readIndexFiles(SegmentStatus.SUCCESS, false, FileFactory.getConfiguration());
      for (Map.Entry<String, List<String>> entry : segmentFileStore.getIndexFilesMap().entrySet()) {
        List<String> matchedBlocksInPartition = entry.getValue().stream().filter(blockFile -> {
          String blockFileName = blockFile.substring(blockFile.lastIndexOf(File.separator) + 1);
          return blockName.equalsIgnoreCase(CarbonUpdateUtil.getBlockName(blockFileName));
        }).collect(Collectors.toList());
        if (matchedBlocksInPartition.size() > 0) {
          segmentPath = matchedBlocksInPartition.get(0)
              .substring(0, matchedBlocksInPartition.get(0).lastIndexOf(File.separator));
          break;
        }
      }
    } else {
      segmentPath = CarbonTablePath.getSegmentPath(
              identifier.getTablePath(), segment.getSegmentNo());
    }

    for (SegmentUpdateDetails block : updateDetails) {
      if ((block.getBlockName().equalsIgnoreCase(blockName)) &&
          (block.getSegmentName().equalsIgnoreCase(segment.getSegmentNo())) &&
          !CarbonUpdateUtil.isBlockInvalid(block.getSegmentStatus())) {
        Set<String> deltaFileTimestamps = block.getDeltaFileStamps();
        if (deltaFileTimestamps != null && deltaFileTimestamps.size() > 0) {
          String finalSegmentPath = segmentPath;
          deltaFileTimestamps.forEach(timestamp -> deleteDeltaFileList.add(
              CarbonUpdateUtil.getDeleteDeltaFilePath(finalSegmentPath, blockName, timestamp)));
        } else {
          // when the deltaFileTimestamps is null, then there is only one delta file
          // and the SegmentUpdateDetails will have same start and end timestamp,
          // just take one to form the delete delta file name
          final long deltaEndTimeStamp =
              getEndTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);
          deleteDeltaFileList.add(CarbonUpdateUtil.getDeleteDeltaFilePath(
              segmentPath, blockName, String.valueOf(deltaEndTimeStamp)));
        }
        return deleteDeltaFileList;
      }
    }
    return deleteDeltaFileList;
  }

  /**
   *
   * @param extension
   * @param block
   * @return
   */
  private long getStartTimeOfDeltaFile(String extension, SegmentUpdateDetails block) {
    long startTimestamp;
    switch (extension) {
      case CarbonCommonConstants.DELETE_DELTA_FILE_EXT:
        startTimestamp = block.getDeleteDeltaStartTimeAsLong();
        break;
      default:
        startTimestamp = 0;
    }
    return startTimestamp;
  }

  /**
   *
   * @param extension
   * @param block
   * @return
   */
  private long getEndTimeOfDeltaFile(String extension, SegmentUpdateDetails block) {
    long endTimestamp;
    switch (extension) {
      case CarbonCommonConstants.DELETE_DELTA_FILE_EXT:
        endTimestamp = block.getDeleteDeltaEndTimeAsLong();
        break;
      default: endTimestamp = 0;
    }
    return endTimestamp;
  }

  /**
   * This method loads segment update details
   *
   * @return
   */
  public SegmentUpdateDetails[] readLoadMetadata() {
    // get the updated status file identifier from the table status.
    String tableUpdateStatusIdentifier = getUpdatedStatusIdentifier();
    return readLoadMetadata(tableUpdateStatusIdentifier, identifier.getTablePath());
  }

  /**
   * This method loads segment update details
   *
   * @return
   */
  public static SegmentUpdateDetails[] readLoadMetadata(String tableUpdateStatusIdentifier,
      String tablePath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    SegmentUpdateDetails[] listOfSegmentUpdateDetailsArray;

    if (StringUtils.isEmpty(tableUpdateStatusIdentifier)) {
      return new SegmentUpdateDetails[0];
    }

    String tableUpdateStatusPath =
        CarbonTablePath.getMetadataPath(tablePath) +
            CarbonCommonConstants.FILE_SEPARATOR + tableUpdateStatusIdentifier;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(tableUpdateStatusPath);

    try {
      if (!FileFactory
          .isFileExist(tableUpdateStatusPath)) {
        return new SegmentUpdateDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          CarbonCommonConstants.DEFAULT_CHARSET);
      buffReader = new BufferedReader(inStream);
      listOfSegmentUpdateDetailsArray =
          gsonObjectToRead.fromJson(buffReader, SegmentUpdateDetails[].class);
    } catch (IOException e) {
      return new SegmentUpdateDetails[0];
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }

    return listOfSegmentUpdateDetailsArray;
  }

  /**
   * @return updateStatusFileName
   */
  private String getUpdatedStatusIdentifier() {
    if (segmentDetails.length == 0) {
      return null;
    }
    return segmentDetails[0].getUpdateStatusFileName();
  }

  /**
   * writes segment update details into a given file at @param dataLoadLocation
   *
   * @param listOfSegmentUpdateDetailsArray
   * @throws IOException
   */
  public void writeLoadDetailsIntoFile(List<SegmentUpdateDetails> listOfSegmentUpdateDetailsArray,
      String updateStatusFileIdentifier) throws IOException {
    String fileLocation =
        CarbonTablePath.getMetadataPath(identifier.getTablePath())
            + CarbonCommonConstants.FILE_SEPARATOR
            + CarbonUpdateUtil.getUpdateStatusFileName(updateStatusFileIdentifier);

    AtomicFileOperations fileWrite =
        AtomicFileOperationFactory.getAtomicFileOperations(fileLocation);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the metadata file.

    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          CarbonCommonConstants.DEFAULT_CHARSET));

      String metadataInstance = gsonObjectToWrite.toJson(listOfSegmentUpdateDetailsArray);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

  /**
   * This method closes the streams
   *
   * @param streams - streams to close.
   */
  private static void closeStreams(Closeable... streams) {
    // Added if to avoid NullPointerException in case one stream is being passed as null
    if (null != streams) {
      for (Closeable stream : streams) {
        if (null != stream) {
          try {
            stream.close();
          } catch (IOException e) {
            LOG.error("Error while closing stream" + stream);
          }
        }
      }
    }
  }

  /**
   * Returns the invalid timestamp range of a segment.
   */
  public static UpdateVO getInvalidTimestampRange(LoadMetadataDetails loadMetadataDetails) {
    UpdateVO range = new UpdateVO();
    if (loadMetadataDetails != null) {
      range.setSegmentId(loadMetadataDetails.getLoadName());
      range.setFactTimestamp(loadMetadataDetails.getLoadStartTime());
      if (!loadMetadataDetails.getUpdateDeltaStartTimestamp().isEmpty() && !loadMetadataDetails
          .getUpdateDeltaEndTimestamp().isEmpty()) {
        range.setUpdateDeltaStartTimestamp(CarbonUpdateUtil
            .getTimeStampAsLong(loadMetadataDetails.getUpdateDeltaStartTimestamp()));
        range.setLatestUpdateTimestamp(
            CarbonUpdateUtil.getTimeStampAsLong(loadMetadataDetails.getUpdateDeltaEndTimestamp()));
      }
    }
    return range;
  }

  /**
   *
   * @param block
   * @param needCompleteList
   * @return
   */
  public CarbonFile[] getDeleteDeltaInvalidFilesList(
      final SegmentUpdateDetails block, final boolean needCompleteList,
      CarbonFile[] allSegmentFiles, boolean isAbortedFile) {

    final long deltaStartTimestamp =
        getStartTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);

    final long deltaEndTimestamp =
        getEndTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);

    Set<CarbonFile> files =
        new HashSet<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (CarbonFile eachFile : allSegmentFiles) {
      String fileName = eachFile.getName();
      if (fileName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)) {
        String blkName = CarbonTablePath.DataFileUtil.getBlockNameFromDeleteDeltaFile(fileName);

        // complete list of delta files of that block is returned.
        if (needCompleteList && block.getBlockName().equalsIgnoreCase(blkName)) {
          files.add(eachFile);
        }

        // invalid delete delta files only will be returned.
        long timestamp = CarbonUpdateUtil.getTimeStampAsLong(
            CarbonTablePath.DataFileUtil.getTimeStampFromDeleteDeltaFile(fileName));

        if (block.getBlockName().equalsIgnoreCase(blkName)) {

          if (isAbortedFile) {
            if (timestamp > deltaEndTimestamp) {
              files.add(eachFile);
            }
          } else if (timestamp < deltaStartTimestamp
              || timestamp > deltaEndTimestamp) {
            files.add(eachFile);
          }
        }
      }
    }

    return files.toArray(new CarbonFile[files.size()]);
  }
}
