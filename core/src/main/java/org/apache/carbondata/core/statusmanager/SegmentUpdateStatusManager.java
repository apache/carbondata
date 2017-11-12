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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.SegmentUpdateDetails;
import org.apache.carbondata.core.mutate.TupleIdEnum;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.reader.CarbonDeleteFilesDataReader;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;

/**
 * Manages Segment & block status of carbon table for Delete operation
 */
public class SegmentUpdateStatusManager {

  /**
   * logger
   */
  private static final LogService LOG =
      LogServiceFactory.getLogService(SegmentUpdateStatusManager.class.getName());

  private AbsoluteTableIdentifier absoluteTableIdentifier;
  private LoadMetadataDetails[] segmentDetails;
  private SegmentUpdateDetails[] updateDetails;
  private CarbonTablePath carbonTablePath;
  private Map<String, SegmentUpdateDetails> blockAndDetailsMap;

  /**
   * @param absoluteTableIdentifier
   */
  public SegmentUpdateStatusManager(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
    carbonTablePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
        absoluteTableIdentifier.getCarbonTableIdentifier());
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    // current it is used only for read function scenarios, as file update always requires to work
    // on latest file status.
    segmentDetails =
        segmentStatusManager.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());
    updateDetails = readLoadMetadata();
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
  public SegmentUpdateDetails getDetailsForABlock(String segID, String actualBlockName) {

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
   *
   * @param loadMetadataDetails
   */
  public void setLoadMetadataDetails(LoadMetadataDetails[] loadMetadataDetails) {
    this.segmentDetails = loadMetadataDetails;
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
    return CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier.getCarbonTableIdentifier(),
        LockUsage.TABLE_UPDATE_STATUS_LOCK);
  }

  /**
   * Returns all delete delta files of specified block
   *
   * @param tupleId
   * @return
   * @throws Exception
   */
  public List<String> getDeleteDeltaFiles(String tupleId) throws Exception {
    return getDeltaFiles(tupleId, CarbonCommonConstants.DELETE_DELTA_FILE_EXT);
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
    String segmentPath = carbonTablePath.getCarbonDataDirectoryPath("0", segmentId);
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
   * Returns all deleted records of specified block
   *
   * @param tupleId
   * @return
   * @throws Exception
   */
  public Map<Integer, Integer[]> getDeleteDeltaDataFromAllFiles(String tupleId) throws Exception {
    List<String> deltaFiles = getDeltaFiles(tupleId, CarbonCommonConstants.DELETE_DELTA_FILE_EXT);
    CarbonDeleteFilesDataReader dataReader = new CarbonDeleteFilesDataReader();
    String blockletId = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.BLOCKLET_ID);
    return dataReader.getDeleteDataFromAllFiles(deltaFiles, blockletId);
  }

  /**
   * Below method will be used to get all the delete delta files based on block name
   *
   * @param blockFilePath actual block filePath
   * @return all delete delta files
   * @throws Exception
   */
  public String[] getDeleteDeltaFilePath(String blockFilePath) throws Exception {
    int tableFactPathLength = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier()).getFactDir().length() + 1;
    String blockame = blockFilePath.substring(tableFactPathLength);
    String tupleId = CarbonTablePath.getShortBlockId(blockame);
    return getDeltaFiles(tupleId, CarbonCommonConstants.DELETE_DELTA_FILE_EXT)
        .toArray(new String[0]);
  }

  /**
   * Returns all delta file paths of specified block
   *
   * @param tupleId
   * @param extension
   * @return
   * @throws Exception
   */
  public List<String> getDeltaFiles(String tupleId, String extension) throws Exception {
    try {
      CarbonTablePath carbonTablePath = CarbonStorePath
          .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
              absoluteTableIdentifier.getCarbonTableIdentifier());
      String segment = CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.SEGMENT_ID);
      String carbonDataDirectoryPath = carbonTablePath.getCarbonDataDirectoryPath("0", segment);
      String completeBlockName = CarbonTablePath.addDataPartPrefix(
          CarbonUpdateUtil.getRequiredFieldFromTID(tupleId, TupleIdEnum.BLOCK_ID)
              + CarbonCommonConstants.FACT_FILE_EXT);
      String blockPath =
          carbonDataDirectoryPath + CarbonCommonConstants.FILE_SEPARATOR + completeBlockName;
      CarbonFile file = FileFactory.getCarbonFile(blockPath, FileFactory.getFileType(blockPath));
      if (!file.exists()) {
        throw new Exception("Invalid tuple id " + tupleId);
      }
      String blockNameWithoutExtn = completeBlockName.substring(0, completeBlockName.indexOf('.'));
      //blockName without timestamp
      final String blockNameFromTuple =
          blockNameWithoutExtn.substring(0, blockNameWithoutExtn.lastIndexOf("-"));
      return getDeltaFiles(file, blockNameFromTuple, extension,
          segment);
    } catch (Exception ex) {
      String errorMsg = "Invalid tuple id " + tupleId;
      LOG.error(errorMsg);
      throw new Exception(errorMsg);
    }
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
      final String extension,
      String segment) {
    List<String> deleteFileList = new ArrayList<>();
    for (SegmentUpdateDetails block : updateDetails) {
      if (block.getBlockName().equalsIgnoreCase(blockNameFromTuple) &&
          block.getSegmentName().equalsIgnoreCase(segment) &&
          !CarbonUpdateUtil.isBlockInvalid(block.getSegmentStatus())) {
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
      final long deltaEndTimeStamp) {
    CarbonFile[] files = blockDir.getParentFile().listFiles(new CarbonFileFilter() {

      @Override public boolean accept(CarbonFile pathName) {
        String fileName = pathName.getName();
        if (fileName.endsWith(extension)) {
          String firstPart = fileName.substring(0, fileName.indexOf('.'));
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
    return deleteFileList;
  }

  /**
   * Return all delta file for a block.
   * @param segmentId
   * @param blockName
   * @return
   */
  public CarbonFile[] getDeleteDeltaFilesList(final String segmentId, final String blockName) {

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());

    String segmentPath = carbonTablePath.getCarbonDataDirectoryPath("0", segmentId);

    CarbonFile segDir =
        FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));

    for (SegmentUpdateDetails block : updateDetails) {
      if ((block.getBlockName().equalsIgnoreCase(blockName)) &&
          (block.getSegmentName().equalsIgnoreCase(segmentId))
          && !CarbonUpdateUtil.isBlockInvalid((block.getSegmentStatus()))) {
        final long deltaStartTimestamp =
            getStartTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);
        final long deltaEndTimeStamp =
            getEndTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);

        return segDir.listFiles(new CarbonFileFilter() {

          @Override public boolean accept(CarbonFile pathName) {
            String fileName = pathName.getName();
            if (fileName.endsWith(CarbonCommonConstants.DELETE_DELTA_FILE_EXT)) {
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
   * @param segmentId
   * @param validUpdateFiles if true then only the valid range files will be returned.
   * @return
   */
  public CarbonFile[] getUpdateDeltaFilesList(String segmentId, final boolean validUpdateFiles,
      final String fileExtension, final boolean excludeOriginalFact,
      CarbonFile[] allFilesOfSegment) {

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    String endTimeStamp = "";
    String startTimeStamp = "";
    long factTimeStamp = 0;

    LoadMetadataDetails[] segmentDetails =
        segmentStatusManager.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());

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
            listOfCarbonFiles.add(eachFile);
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

    if (null == tableUpdateStatusIdentifier) {
      return new SegmentUpdateDetails[0];
    }

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());

    String tableUpdateStatusPath =
        carbonTablePath.getMetadataDirectoryPath() + CarbonCommonConstants.FILE_SEPARATOR
            + tableUpdateStatusIdentifier;
    AtomicFileOperations fileOperation = new AtomicFileOperationsImpl(tableUpdateStatusPath,
        FileFactory.getFileType(tableUpdateStatusPath));

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
    SegmentStatusManager ssm = new SegmentStatusManager(absoluteTableIdentifier);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    LoadMetadataDetails[] loadDetails =
        ssm.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());
    if (loadDetails.length == 0) {
      return null;
    }
    return loadDetails[0].getUpdateStatusFileName();
  }

  /**
   * writes segment update details into a given file at @param dataLoadLocation
   *
   * @param listOfSegmentUpdateDetailsArray
   * @throws IOException
   */
  public void writeLoadDetailsIntoFile(List<SegmentUpdateDetails> listOfSegmentUpdateDetailsArray,
      String updateStatusFileIdentifier) throws IOException {

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());

    String fileLocation =
        carbonTablePath.getMetadataDirectoryPath() + CarbonCommonConstants.FILE_SEPARATOR
            + CarbonUpdateUtil.getUpdateStatusFileName(updateStatusFileIdentifier);

    AtomicFileOperations fileWrite =
        new AtomicFileOperationsImpl(fileLocation, FileFactory.getFileType(fileLocation));
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
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

  /**
   * compares passed time stamp with status file delete timestamp and
   * returns latest timestamp from status file if both are not equal
   * returns null otherwise
   *
   * @param completeBlockName
   * @param timestamp
   * @return
   */
  public String getTimestampForRefreshCache(String completeBlockName, String timestamp) {
    long cacheTimestamp = 0;
    if (null != timestamp) {
      cacheTimestamp = CarbonUpdateUtil.getTimeStampAsLong(timestamp);
    }
    String blockName = CarbonTablePath.addDataPartPrefix(CarbonUpdateUtil.getBlockName(
        CarbonUpdateUtil.getRequiredFieldFromTID(completeBlockName, TupleIdEnum.BLOCK_ID)));
    String segmentId =
        CarbonUpdateUtil.getRequiredFieldFromTID(completeBlockName, TupleIdEnum.SEGMENT_ID);
    SegmentUpdateDetails[] listOfSegmentUpdateDetailsArray =
        readLoadMetadata();
    for (SegmentUpdateDetails block : listOfSegmentUpdateDetailsArray) {
      if (segmentId.equalsIgnoreCase(block.getSegmentName()) &&
          block.getBlockName().equalsIgnoreCase(blockName) &&
          !CarbonUpdateUtil.isBlockInvalid(block.getSegmentStatus())) {
        long deleteTimestampFromStatusFile = block.getDeleteDeltaEndTimeAsLong();
        if (Long.compare(deleteTimestampFromStatusFile, cacheTimestamp) == 0) {
          return null;
        } else {
          return block.getDeleteDeltaEndTimestamp();
        }
      }
    }
    return null;
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
   * Get the invalid tasks in that segment.
   * @param segmentId
   * @return
   */
  public List<String> getInvalidBlockList(String segmentId) {

    // get the original fact file timestamp from the table status file.
    List<String> listOfInvalidBlocks = new ArrayList<String>();
    SegmentStatusManager ssm = new SegmentStatusManager(absoluteTableIdentifier);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
    LoadMetadataDetails[] segmentDetails =
        ssm.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());
    long timestampOfOriginalFacts = 0;

    String startTimestampOfUpdate = "" ;
    String endTimestampOfUpdate = "";

    for (LoadMetadataDetails segment : segmentDetails) {
      // find matching segment and return timestamp.
      if (segment.getLoadName().equalsIgnoreCase(segmentId)) {
        timestampOfOriginalFacts = segment.getLoadStartTime();
        startTimestampOfUpdate = segment.getUpdateDeltaStartTimestamp();
        endTimestampOfUpdate = segment.getUpdateDeltaEndTimestamp();
      }
    }

    if (startTimestampOfUpdate.isEmpty()) {
      return listOfInvalidBlocks;

    }

    // now after getting the original fact timestamp, what ever is remaining
    // files need to cross check it with table status file.

    // filter out the fact files.

    String segmentPath = carbonTablePath.getCarbonDataDirectoryPath("0", segmentId);
    CarbonFile segDir =
        FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));

    final Long endTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(endTimestampOfUpdate);
    final Long startTimeStampFinal = CarbonUpdateUtil.getTimeStampAsLong(startTimestampOfUpdate);
    final Long timeStampOriginalFactFinal =
        timestampOfOriginalFacts;

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
            return false;
          }
          if (Long.compare(timestamp, timeStampOriginalFactFinal) == 0) {
            return false;
          }
          // take the rest of files as they are invalid.
          return true;
        }
        return false;
      }
    });

    // gather the task numbers.
    for (CarbonFile updateFiles : files) {
      listOfInvalidBlocks.add(updateFiles.getName());
    }

    return listOfInvalidBlocks;
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
   *
   * @param segmentId
   * @param block
   * @param needCompleteList
   * @return
   */
  public CarbonFile[] getDeleteDeltaInvalidFilesList(final String segmentId,
      final SegmentUpdateDetails block, final boolean needCompleteList,
      CarbonFile[] allSegmentFiles) {

    final long deltaStartTimestamp =
        getStartTimeOfDeltaFile(CarbonCommonConstants.DELETE_DELTA_FILE_EXT, block);

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

        if (block.getBlockName().equalsIgnoreCase(blkName) && (
            Long.compare(timestamp, deltaStartTimestamp) < 0)) {
          files.add(eachFile);
        }
      }
    }

    return files.toArray(new CarbonFile[files.size()]);
  }

  /**
   *
   * @param blockName
   * @param allSegmentFiles
   * @return
   */
  public CarbonFile[] getAllBlockRelatedFiles(String blockName, CarbonFile[] allSegmentFiles,
                                              String actualBlockName) {
    List<CarbonFile> files = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    for (CarbonFile eachFile : allSegmentFiles) {

      // for carbon data.
      if (eachFile.getName().equalsIgnoreCase(actualBlockName)) {
        files.add(eachFile);
      }

      // get carbon index files of the block.
      String indexFileName = CarbonTablePath.getCarbonIndexFileName(actualBlockName);
      if (eachFile.getName().equalsIgnoreCase(indexFileName)) {
        files.add(eachFile);
      }

    }

    return files.toArray(new CarbonFile[files.size()]);
  }
}
