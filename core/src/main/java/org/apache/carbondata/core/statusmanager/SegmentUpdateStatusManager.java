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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.TupleIdEnum;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.commons.lang3.StringUtils;
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
  private boolean isStandardTable;

  public SegmentUpdateStatusManager(CarbonTable table,
      LoadMetadataDetails[] segmentDetails) {
    this.identifier = table.getAbsoluteTableIdentifier();
    this.isStandardTable = CarbonUtil.isStandardCarbonTable(table);
    // current it is used only for read function scenarios, as file update always requires to work
    // on latest file status.
    this.segmentDetails = segmentDetails;
    updateDetails = readLoadMetadata();
    populateMap();
  }

  public SegmentUpdateStatusManager(CarbonTable table) {
    this.identifier = table.getAbsoluteTableIdentifier();
    this.isStandardTable = CarbonUtil.isStandardCarbonTable(table);
    // current it is used only for read function scenarios, as file update always requires to work
    // on latest file status.
    if (!table.getTableInfo().isTransactionalTable()) {
      // fileExist is costly operation, so check based on table Type
      segmentDetails = new LoadMetadataDetails[0];
    } else {
      segmentDetails = SegmentStatusManager.readLoadMetadata(
          CarbonTablePath.getMetadataPath(identifier.getTablePath()));
    }
    if (segmentDetails.length != 0) {
      updateDetails = readLoadMetadata();
    } else {
      updateDetails = new SegmentUpdateDetails[0];
    }
    populateMap();
  }

  /**
   * populate the block and its details in a map.
   */
  private void populateMap() {
    blockAndDetailsMap = new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (SegmentUpdateDetails blockDetails : updateDetails) {

      String blockIdentifier = CarbonUpdateUtil
          .getSegmentBlockNameKey(blockDetails.getSegmentName(), blockDetails.getActualBlockName());

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
        .getSegmentBlockNameKey(segID, actualBlockName);

    return blockAndDetailsMap.get(blockIdentifier);

  }

  /**
   *
   * @param key will be like (segid/blockname)  0/0-0-5464654654654
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
   * This will return the lock object used to lock the table update status file before updation.
   *
   * @return
   */
  public ICarbonLock getTableUpdateStatusLock() {
    return CarbonLockFactory.getCarbonLockObj(identifier,
        LockUsage.TABLE_UPDATE_STATUS_LOCK);
  }

  /**
   * Returns all update delta files of specified Segment.
   *
   * @param segmentId
   * @return
   * @throws Exception
   */
  public List<String> getUpdateDeltaFiles(final String segmentId) {
    List<String> updatedDeltaFilesList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    String endTimeStamp = "";
    String startTimeStamp = "";
    String segmentPath = CarbonTablePath.getSegmentPath(
        identifier.getTablePath(), segmentId);
    CarbonFile segDir =
        FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
    for (LoadMetadataDetails eachSeg : segmentDetails) {
      if (eachSeg.getLoadName().equalsIgnoreCase(segmentId)) {
        // if the segment is found then take the start and end time stamp.
        startTimeStamp = eachSeg.getUpdateDeltaStartTimestamp();
        endTimeStamp = eachSeg.getUpdateDeltaEndTimestamp();
      }
    }
    // if start timestamp is empty then no update delta is found. so return empty list.
    if (startTimeStamp.isEmpty()) {
      return updatedDeltaFilesList;
    }
    final Long endTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(endTimeStamp);
    final Long startTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(startTimeStamp);

    // else scan the segment for the delta files with the respective timestamp.
    CarbonFile[] files = segDir.listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile pathName) {
        String fileName = pathName.getName();
        if (fileName.endsWith(CarbonCommonConstants.UPDATE_DELTA_FILE_EXT)) {
          String firstPart = fileName.substring(0, fileName.indexOf('.'));

          long timestamp = Long.parseLong(firstPart
              .substring(firstPart.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                  firstPart.length()));
          if (Long.compare(timestamp, endTimeStampFinal) <= 0
              && Long.compare(timestamp, startTimeStampFinal) >= 0) {

            // if marked for delete then it is invalid.
            if (!isBlockValid(segmentId, fileName)) {
              return false;
            }

            return true;
          }
        }
        return false;
      }
    });

    for (CarbonFile cfile : files) {
      updatedDeltaFilesList.add(cfile.getCanonicalPath());
    }

    return updatedDeltaFilesList;
  }

  /**
   * Below method will be used to get all the delete delta files based on block name
   *
   * @param blockFilePath actual block filePath
   * @return all delete delta files
   * @throws Exception
   */
  public String[] getDeleteDeltaFilePath(String blockFilePath, String segmentId) throws Exception {
    String blockId =
        CarbonUtil.getBlockId(identifier, blockFilePath, segmentId, true, isStandardTable);
    String tupleId;
    if (!isStandardTable) {
      tupleId = CarbonTablePath.getShortBlockIdForPartitionTable(blockId);
    } else {
      tupleId = CarbonTablePath.getShortBlockId(blockId);
    }
    return getDeltaFiles(tupleId, CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
        .toArray(new String[0]);
  }

  /**
   * Returns all delta file paths of specified block
   */
  private List<String> getDeltaFiles(String tupleId, String extension)
      throws Exception {
    String segment = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID);
    String completeBlockName = CarbonTablePath.addDataPartPrefix(
        CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.BLOCK_ID)
            + CarbonCommonConstants.FACT_FILE_EXT);

    String blockPath;
    if (!isStandardTable) {
      blockPath = identifier.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.PART_ID)
          .replace("#", "/") + CarbonCommonConstants.FILE_SEPARATOR + completeBlockName;
    } else {
      String carbonDataDirectoryPath = CarbonTablePath.getSegmentPath(
          identifier.getTablePath(), segment);
      blockPath =
          carbonDataDirectoryPath + CarbonCommonConstants.FILE_SEPARATOR + completeBlockName;
    }
    CarbonFile file = FileFactory.getCarbonFile(blockPath, FileFactory.getFileType(blockPath));
    if (!file.exists()) {
      throw new Exception("Invalid tuple id " + tupleId);
    }
    String blockNameWithoutExtn =
        completeBlockName.substring(0, completeBlockName.lastIndexOf('.'));
    //blockName without timestamp
    final String blockNameFromTuple =
        blockNameWithoutExtn.substring(0, blockNameWithoutExtn.lastIndexOf("-"));
    return getDeltaFiles(file, blockNameFromTuple, extension, segment);
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
  private List<String> getDeltaFiles(CarbonFile blockDir, final String blockNameFromTuple,
      final String extension, String segment) throws IOException {
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

        // final long deltaEndTimeStamp = block.getDeleteDeltaEndTimeAsLong();
        // final long deltaStartTimestamp = block.getDeleteDeltaStartTimeAsLong();
        return getFilePaths(blockDir, blockNameFromTuple, extension, deleteFileList,
            deltaStartTimestamp, deltaEndTimeStamp);
      }
    }
    return deleteFileList;
  }

  private List<String> getFilePaths(CarbonFile blockDir, final String blockNameFromTuple,
      final String extension, List<String> deleteFileList, final long deltaStartTimestamp,
      final long deltaEndTimeStamp) throws IOException {
    if (null != blockDir.getParentFile()) {
      CarbonFile[] files = blockDir.getParentFile().listFiles(new CarbonFileFilter() {

        @Override
        public boolean accept(CarbonFile pathName) {
          String fileName = pathName.getName();
          if (fileName.endsWith(extension) && pathName.getSize() > 0) {
            String firstPart = fileName.substring(0, fileName.lastIndexOf('.'));
            String blockName =
                firstPart.substring(0, firstPart.lastIndexOf(CarbonCommonConstants.HYPHEN));
            long timestamp = Long.parseLong(firstPart
                .substring(firstPart.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                    firstPart.length()));
            if (blockNameFromTuple.equals(blockName) && (
                (Long.compare(timestamp, deltaEndTimeStamp) <= 0) && (
                    Long.compare(timestamp, deltaStartTimestamp) >= 0))) {
              return true;
            }
          }
          return false;
        }
      });

      for (CarbonFile cfile : files) {
        if (null == deleteFileList) {
          deleteFileList = new ArrayList<String>(files.length);
        }
        deleteFileList.add(cfile.getCanonicalPath());
      }
    } else {
      throw new IOException("Parent file could not found");
    }
    return deleteFileList;
  }

  /**
   * Return all delta file for a block.
   * @param segmentId
   * @param blockName
   * @return
   */
  public CarbonFile[] getDeleteDeltaFilesList(final Segment segmentId, final String blockName) {
    String segmentPath = CarbonTablePath.getSegmentPath(
        identifier.getTablePath(), segmentId.getSegmentNo());
    CarbonFile segDir =
        FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
    for (SegmentUpdateDetails block : updateDetails) {
      if ((block.getBlockName().equalsIgnoreCase(blockName)) &&
          (block.getSegmentName().equalsIgnoreCase(segmentId.getSegmentNo()))
          && !CarbonUpdateUtil.isBlockInvalid((block.getSegmentStatus()))) {
        final long deltaStartTimestamp =
            getStartTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);
        final long deltaEndTimeStamp =
            getEndTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);

        return segDir.listFiles(new CarbonFileFilter() {

          @Override public boolean accept(CarbonFile pathName) {
            String fileName = pathName.getName();
            if (fileName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
                && pathName.getSize() > 0) {
              String firstPart = fileName.substring(0, fileName.indexOf('.'));
              String blkName = firstPart.substring(0, firstPart.lastIndexOf("-"));
              long timestamp = Long.parseLong(
                  firstPart.substring(firstPart.lastIndexOf("-") + 1, firstPart.length()));
              if (blockName.equals(blkName) && (Long.compare(timestamp, deltaEndTimeStamp) <= 0)
                  && (Long.compare(timestamp, deltaStartTimestamp) >= 0)) {
                return true;
              }
            }
            return false;
          }
        });
      }
    }
    return null;
  }

  /**
   * Returns all update delta files of specified Segment.
   *
   * @param loadMetadataDetail metadatadetails of segment
   * @param validUpdateFiles if true then only the valid range files will be returned.
   * @return
   */
  public CarbonFile[] getUpdateDeltaFilesList(LoadMetadataDetails loadMetadataDetail,
      final boolean validUpdateFiles, final String fileExtension, final boolean excludeOriginalFact,
      CarbonFile[] allFilesOfSegment, boolean isAbortedFile) {

    String endTimeStamp = "";
    String startTimeStamp = "";
    long factTimeStamp = 0;

    // if the segment is found then take the start and end time stamp.
    startTimeStamp = loadMetadataDetail.getUpdateDeltaStartTimestamp();
    endTimeStamp = loadMetadataDetail.getUpdateDeltaEndTimestamp();
    factTimeStamp = loadMetadataDetail.getLoadStartTime();

    // if start timestamp is empty then no update delta is found. so return empty list.
    if (startTimeStamp.isEmpty()) {
      return new CarbonFile[0];
    }

    final Long endTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(endTimeStamp);
    final Long startTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(startTimeStamp);
    final long factTimeStampFinal = factTimeStamp;

    List<CarbonFile> listOfCarbonFiles =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // else scan the segment for the delta files with the respective timestamp.

    for (CarbonFile eachFile : allFilesOfSegment) {

      String fileName = eachFile.getName();
      if (fileName.endsWith(fileExtension)) {
        String firstPart = fileName.substring(0, fileName.lastIndexOf('.'));

        long timestamp = Long.parseLong(firstPart
            .substring(firstPart.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                firstPart.length()));

        if (excludeOriginalFact) {
          if (Long.compare(factTimeStampFinal, timestamp) == 0) {
            continue;
          }
        }

        if (validUpdateFiles) {
          if (Long.compare(timestamp, endTimeStampFinal) <= 0
              && Long.compare(timestamp, startTimeStampFinal) >= 0) {
            listOfCarbonFiles.add(eachFile);
          }
        } else {
          // invalid cases.
          if (isAbortedFile) {
            if (Long.compare(timestamp, endTimeStampFinal) > 0) {
              listOfCarbonFiles.add(eachFile);
            }
          } else if (Long.compare(timestamp, startTimeStampFinal) < 0
              || Long.compare(timestamp, endTimeStampFinal) > 0) {
            listOfCarbonFiles.add(eachFile);
          }
        }
      }
    }

    return listOfCarbonFiles.toArray(new CarbonFile[listOfCarbonFiles.size()]);
  }

  /**
   * Returns all update delta files of specified Segment.
   *
   * @param segmentId
   * @param validUpdateFiles
   * @param fileExtension
   * @param excludeOriginalFact
   * @param allFilesOfSegment
   * @return
   */
  public CarbonFile[] getUpdateDeltaFilesForSegment(String segmentId,
      final boolean validUpdateFiles, final String fileExtension, final boolean excludeOriginalFact,
      CarbonFile[] allFilesOfSegment) {

    String endTimeStamp = "";
    String startTimeStamp = "";
    long factTimeStamp = 0;

    for (LoadMetadataDetails eachSeg : segmentDetails) {
      if (eachSeg.getLoadName().equalsIgnoreCase(segmentId)) {
        // if the segment is found then take the start and end time stamp.
        startTimeStamp = eachSeg.getUpdateDeltaStartTimestamp();
        endTimeStamp = eachSeg.getUpdateDeltaEndTimestamp();
        factTimeStamp = eachSeg.getLoadStartTime();
      }
    }

    // if start timestamp is empty then no update delta is found. so return empty list.
    if (startTimeStamp.isEmpty()) {
      return new CarbonFile[0];
    }

    final Long endTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(endTimeStamp);
    final Long startTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(startTimeStamp);
    final long factTimeStampFinal = factTimeStamp;

    List<CarbonFile> listOfCarbonFiles =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // else scan the segment for the delta files with the respective timestamp.

    for (CarbonFile eachFile : allFilesOfSegment) {

      String fileName = eachFile.getName();
      if (fileName.endsWith(fileExtension)) {
        String firstPart = fileName.substring(0, fileName.indexOf('.'));

        long timestamp = Long.parseLong(firstPart
            .substring(firstPart.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                firstPart.length()));

        if (excludeOriginalFact) {
          if (Long.compare(factTimeStampFinal, timestamp) == 0) {
            continue;
          }
        }

        if (validUpdateFiles) {
          if (Long.compare(timestamp, endTimeStampFinal) <= 0
              && Long.compare(timestamp, startTimeStampFinal) >= 0) {

            boolean validBlock = true;

            for (SegmentUpdateDetails blockDetails : getUpdateStatusDetails()) {
              if (blockDetails.getActualBlockName().equalsIgnoreCase(eachFile.getName())
                  && CarbonUpdateUtil.isBlockInvalid(blockDetails.getSegmentStatus())) {
                validBlock = false;
              }
            }

            if (validBlock) {
              listOfCarbonFiles.add(eachFile);
            }

          }
        } else {
          // invalid cases.
          if (Long.compare(timestamp, startTimeStampFinal) < 0) {
            listOfCarbonFiles.add(eachFile);
          }
        }
      }
    }

    return listOfCarbonFiles.toArray(new CarbonFile[listOfCarbonFiles.size()]);
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
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    SegmentUpdateDetails[] listOfSegmentUpdateDetailsArray;

    // get the updated status file identifier from the table status.
    String tableUpdateStatusIdentifier = getUpdatedStatusIdentifier();

    if (StringUtils.isEmpty(tableUpdateStatusIdentifier)) {
      return new SegmentUpdateDetails[0];
    }

    String tableUpdateStatusPath =
        CarbonTablePath.getMetadataPath(identifier.getTablePath()) +
            CarbonCommonConstants.FILE_SEPARATOR + tableUpdateStatusIdentifier;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(tableUpdateStatusPath);

    try {
      if (!FileFactory
          .isFileExist(tableUpdateStatusPath, FileFactory.getFileType(tableUpdateStatusPath))) {
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
  private void closeStreams(Closeable... streams) {
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
   * @param segmentId
   * @return
   */
  public UpdateVO getInvalidTimestampRange(String segmentId) {
    UpdateVO range = new UpdateVO();
    for (LoadMetadataDetails segment : segmentDetails) {
      if (segment.getLoadName().equalsIgnoreCase(segmentId)) {
        range.setSegmentId(segmentId);
        range.setFactTimestamp(segment.getLoadStartTime());
        if (!segment.getUpdateDeltaStartTimestamp().isEmpty() && !segment
            .getUpdateDeltaEndTimestamp().isEmpty()) {
          range.setUpdateDeltaStartTimestamp(
              CarbonUpdateUtil.getTimeStampAsLong(segment.getUpdateDeltaStartTimestamp()));
          range.setLatestUpdateTimestamp(
              CarbonUpdateUtil.getTimeStampAsLong(segment.getUpdateDeltaEndTimestamp()));
        }
        return range;
      }
    }
    return range;
  }

  /**
   * Returns the invalid timestamp range of a segment.
   * @return
   */
  public List<UpdateVO> getInvalidTimestampRange() {
    List<UpdateVO> ranges = new ArrayList<UpdateVO>();
    for (LoadMetadataDetails segment : segmentDetails) {
      if ((SegmentStatus.LOAD_FAILURE == segment.getSegmentStatus()
          || SegmentStatus.COMPACTED == segment.getSegmentStatus()
          || SegmentStatus.MARKED_FOR_DELETE == segment.getSegmentStatus())) {
        UpdateVO range = new UpdateVO();
        range.setSegmentId(segment.getLoadName());
        range.setFactTimestamp(segment.getLoadStartTime());
        if (!segment.getUpdateDeltaStartTimestamp().isEmpty() &&
            !segment.getUpdateDeltaEndTimestamp().isEmpty()) {
          range.setUpdateDeltaStartTimestamp(
              CarbonUpdateUtil.getTimeStampAsLong(segment.getUpdateDeltaStartTimestamp()));
          range.setLatestUpdateTimestamp(
              CarbonUpdateUtil.getTimeStampAsLong(segment.getUpdateDeltaEndTimestamp()));
        }
        ranges.add(range);
      }
    }
    return ranges;
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

    List<CarbonFile> files =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

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
            if (Long.compare(timestamp, deltaEndTimestamp) > 0) {
              files.add(eachFile);
            }
          } else if (Long.compare(timestamp, deltaStartTimestamp) < 0
              || Long.compare(timestamp, deltaEndTimestamp) > 0) {
            files.add(eachFile);
          }
        }
      }
    }

    return files.toArray(new CarbonFile[files.size()]);
  }
}
