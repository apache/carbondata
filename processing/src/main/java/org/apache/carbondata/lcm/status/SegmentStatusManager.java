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
package org.apache.carbondata.lcm.status;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.lcm.fileoperations.AtomicFileOperations;
import org.apache.carbondata.lcm.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.lcm.fileoperations.FileWriteOperation;
import org.apache.carbondata.lcm.locks.CarbonLockFactory;
import org.apache.carbondata.lcm.locks.CarbonLockUtil;
import org.apache.carbondata.lcm.locks.ICarbonLock;
import org.apache.carbondata.lcm.locks.LockUsage;

import com.google.gson.Gson;
/**
 * Manages Load/Segment status
 */
public class SegmentStatusManager {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  /**
   * This will return the lock object used to lock the table status file before updation.
   *
   * @return
   */
  public static ICarbonLock getTableStatusLock(AbsoluteTableIdentifier identifier) {
    return CarbonLockFactory.getCarbonLockObj(identifier.getCarbonTableIdentifier(),
        LockUsage.TABLE_STATUS_LOCK);
  }

  /**
   * This method will return last modified time of tablestatus file
   */
  public static long getTableStatusLastModifiedTime(AbsoluteTableIdentifier identifier) throws IOException {
    String tableStatusPath = CarbonStorePath.getCarbonTablePath(identifier.getStorePath(),
            identifier.getCarbonTableIdentifier()).getTableStatusFilePath();
    if (!FileFactory.isFileExist(tableStatusPath, FileFactory.getFileType(tableStatusPath))) {
      return 0L;
    } else {
      return FileFactory.getCarbonFile(tableStatusPath, FileFactory.getFileType(tableStatusPath))
              .getLastModifiedTime();
    }
  }


  /**
   * get valid segment for given table
   *
   * @return
   * @throws IOException
   */
  public static SegmentStatus getSegmentStatus(AbsoluteTableIdentifier identifier)
      throws IOException {

    // @TODO: move reading LoadStatus file to separate class
    List<String> validSegments = new ArrayList<String>(10);
    List<String> validUpdatedSegments = new ArrayList<String>(10);
    List<String> invalidSegments = new ArrayList<String>(10);
    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(identifier.getStorePath(),
        identifier.getCarbonTableIdentifier());
    String dataPath = carbonTablePath.getTableStatusFilePath();
    DataInputStream dataInputStream = null;
    Gson gsonObjectToRead = new Gson();
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(dataPath, FileFactory.getFileType(dataPath));
    LoadMetadataDetails[] loadFolderDetailsArray;
    try {
      if (FileFactory.isFileExist(dataPath, FileFactory.getFileType(dataPath))) {
        dataInputStream = fileOperation.openForRead();
        BufferedReader buffReader =
            new BufferedReader(
                    new InputStreamReader(dataInputStream, CarbonCommonConstants.DEFAULT_CHARSET));

        loadFolderDetailsArray = gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
        //just directly iterate Array
        List<LoadMetadataDetails> loadFolderDetails = Arrays.asList(loadFolderDetailsArray);

        for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
          String loadStatus = loadMetadataDetails.getLoadStatus();
          if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS.equalsIgnoreCase(loadStatus)
              || CarbonCommonConstants.MARKED_FOR_UPDATE.equalsIgnoreCase(loadStatus)
              || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS.equalsIgnoreCase(
                  loadStatus)) {
            // check for merged loads.
            if (null != loadMetadataDetails.getMergedLoadName()) {
              if (!validSegments.contains(loadMetadataDetails.getMergedLoadName())) {
                validSegments.add(loadMetadataDetails.getMergedLoadName());
              }
              // if merged load is updated then put it in updated list
              if (CarbonCommonConstants.MARKED_FOR_UPDATE.equalsIgnoreCase(loadStatus)) {
                validUpdatedSegments.add(loadMetadataDetails.getMergedLoadName());
              }
              continue;
            }
            if (CarbonCommonConstants.MARKED_FOR_UPDATE.equalsIgnoreCase(loadStatus)) {
              validUpdatedSegments.add(loadMetadataDetails.getLoadName());
            }
            validSegments.add(loadMetadataDetails.getLoadName());
          } else if (CarbonCommonConstants.STORE_LOADSTATUS_FAILURE.equalsIgnoreCase(loadStatus)
              || CarbonCommonConstants.SEGMENT_COMPACTED.equalsIgnoreCase(loadStatus)
              || CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(loadStatus)) {
            invalidSegments.add(loadMetadataDetails.getLoadName());
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    } finally {
      try {
        if (null != dataInputStream) {
          dataInputStream.close();
        }
      } catch (Exception e) {
        LOG.error(e);
        throw e;
      }
    }

    return new SegmentStatus(validSegments, validUpdatedSegments, invalidSegments);
  }

  /**
   * This method reads the load metadata file
   *
   * @param tableFolderPath
   * @return
   */
  public static LoadMetadataDetails[] readLoadMetadata(String tableFolderPath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    String metadataFileName = tableFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonCommonConstants.LOADMETADATA_FILENAME;
    LoadMetadataDetails[] listOfLoadFolderDetailsArray;

    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(metadataFileName, FileFactory.getFileType(metadataFileName));

    try {
      if (!FileFactory.isFileExist(metadataFileName, FileFactory.getFileType(metadataFileName))) {
        return new LoadMetadataDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      listOfLoadFolderDetailsArray =
          gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      return new LoadMetadataDetails[0];
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }

    return listOfLoadFolderDetailsArray;
  }

  /**
   * returns current time
   *
   * @return
   */
  private static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    return sdf.format(new Date());
  }

  /**
   * compares two given date strings
   *
   * @param loadValue
   * @param userValue
   * @return -1 if first arg is less than second arg, 1 if first arg is greater than second arg,
   * 0 otherwise
   */
  private static Integer compareDateValues(Long loadValue, Long userValue) {
    return loadValue.compareTo(userValue);
  }

  /**
   * updates deletion status
   *
   * @param loadIds
   * @param tableFolderPath
   * @return
   */
  public static List<String> updateDeletionStatus(AbsoluteTableIdentifier identifier,
      List<String> loadIds, String tableFolderPath) throws Exception {
    CarbonTableIdentifier carbonTableIdentifier = identifier.getCarbonTableIdentifier();
    ICarbonLock carbonDeleteSegmentLock =
        CarbonLockFactory.getCarbonLockObj(carbonTableIdentifier, LockUsage.DELETE_SEGMENT_LOCK);
    ICarbonLock carbonTableStatusLock =
        CarbonLockFactory.getCarbonLockObj(carbonTableIdentifier, LockUsage.TABLE_STATUS_LOCK);
    String tableDetails =
        carbonTableIdentifier.getDatabaseName() + "." + carbonTableIdentifier.getTableName();
    List<String> invalidLoadIds = new ArrayList<String>(0);
    try {
      if (carbonDeleteSegmentLock.lockWithRetries()) {
        LOG.info("Delete segment lock has been successfully acquired");

        CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(
            identifier.getStorePath(), identifier.getCarbonTableIdentifier());
        String dataLoadLocation = carbonTablePath.getTableStatusFilePath();
        LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;
        if (!FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation))) {
          // log error.
          LOG.error("Load metadata file is not present.");
          return loadIds;
        }
        // read existing metadata details in load metadata.
        listOfLoadFolderDetailsArray = readLoadMetadata(tableFolderPath);
        if (listOfLoadFolderDetailsArray != null && listOfLoadFolderDetailsArray.length != 0) {
          updateDeletionStatus(loadIds, listOfLoadFolderDetailsArray, invalidLoadIds);
          if (invalidLoadIds.isEmpty()) {
            // All or None , if anything fails then dont write
            if(carbonTableStatusLock.lockWithRetries()) {
              LOG.info("Table status lock has been successfully acquired");
              // To handle concurrency scenarios, always take latest metadata before writing
              // into status file.
              LoadMetadataDetails[] latestLoadMetadataDetails = readLoadMetadata(tableFolderPath);
              updateLatestTableStatusDetails(listOfLoadFolderDetailsArray,
                  latestLoadMetadataDetails);
              writeLoadDetailsIntoFile(dataLoadLocation, listOfLoadFolderDetailsArray);
            }
            else {
              String errorMsg = "Delete segment by id is failed for " + tableDetails
                  + ". Not able to acquire the table status lock due to other operation running "
                  + "in the background.";
              LOG.audit(errorMsg);
              LOG.error(errorMsg);
              throw new Exception(errorMsg + " Please try after some time.");
            }

          } else {
            return invalidLoadIds;
          }

        } else {
          LOG.audit("Delete segment by Id is failed. No matching segment id found.");
          return loadIds;
        }

      } else {
        String errorMsg = "Delete segment by id is failed for " + tableDetails
            + ". Not able to acquire the delete segment lock due to another delete "
            + "operation is running in the background.";
        LOG.audit(errorMsg);
        LOG.error(errorMsg);
        throw new Exception(errorMsg + " Please try after some time.");
      }
    } catch (IOException e) {
      LOG.error("IOException" + e.getMessage());
    } finally {
      CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
      CarbonLockUtil.fileUnlock(carbonDeleteSegmentLock, LockUsage.DELETE_SEGMENT_LOCK);
    }

    return invalidLoadIds;
  }

  /**
   * updates deletion status
   *
   * @param loadDate
   * @param tableFolderPath
   * @return
   */
  public static List<String> updateDeletionStatus(AbsoluteTableIdentifier identifier,
      String loadDate, String tableFolderPath, Long loadStartTime) throws Exception {
    CarbonTableIdentifier carbonTableIdentifier = identifier.getCarbonTableIdentifier();
    ICarbonLock carbonDeleteSegmentLock =
        CarbonLockFactory.getCarbonLockObj(carbonTableIdentifier, LockUsage.DELETE_SEGMENT_LOCK);
    ICarbonLock carbonTableStatusLock =
        CarbonLockFactory.getCarbonLockObj(carbonTableIdentifier, LockUsage.TABLE_STATUS_LOCK);
    String tableDetails =
        carbonTableIdentifier.getDatabaseName() + "." + carbonTableIdentifier.getTableName();
    List<String> invalidLoadTimestamps = new ArrayList<String>(0);
    try {
      if (carbonDeleteSegmentLock.lockWithRetries()) {
        LOG.info("Delete segment lock has been successfully acquired");

        CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(
            identifier.getStorePath(), identifier.getCarbonTableIdentifier());
        String dataLoadLocation = carbonTablePath.getTableStatusFilePath();
        LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;

        if (!FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation))) {
          // log error.
          LOG.error("Error message: " + "Load metadata file is not present.");
          invalidLoadTimestamps.add(loadDate);
          return invalidLoadTimestamps;
        }
        // read existing metadata details in load metadata.
        listOfLoadFolderDetailsArray = readLoadMetadata(tableFolderPath);
        if (listOfLoadFolderDetailsArray != null && listOfLoadFolderDetailsArray.length != 0) {
          updateDeletionStatus(loadDate, listOfLoadFolderDetailsArray, invalidLoadTimestamps,
              loadStartTime);
          if (invalidLoadTimestamps.isEmpty()) {
            if(carbonTableStatusLock.lockWithRetries()) {
              LOG.info("Table status lock has been successfully acquired.");
              // To handle concurrency scenarios, always take latest metadata before writing
              // into status file.
              LoadMetadataDetails[] latestLoadMetadataDetails = readLoadMetadata(tableFolderPath);
              updateLatestTableStatusDetails(listOfLoadFolderDetailsArray,
                  latestLoadMetadataDetails);
              writeLoadDetailsIntoFile(dataLoadLocation, listOfLoadFolderDetailsArray);
            }
            else {

              String errorMsg = "Delete segment by date is failed for " + tableDetails
                  + ". Not able to acquire the table status lock due to other operation running "
                  + "in the background.";
              LOG.audit(errorMsg);
              LOG.error(errorMsg);
              throw new Exception(errorMsg + " Please try after some time.");

            }
          } else {
            return invalidLoadTimestamps;
          }

        } else {
          LOG.audit("Delete segment by date is failed. No matching segment found.");
          invalidLoadTimestamps.add(loadDate);
          return invalidLoadTimestamps;
        }

      } else {
        String errorMsg = "Delete segment by date is failed for " + tableDetails
            + ". Not able to acquire the delete segment lock due to another delete "
            + "operation is running in the background.";
        LOG.audit(errorMsg);
        LOG.error(errorMsg);
        throw new Exception(errorMsg + " Please try after some time.");
      }
    } catch (IOException e) {
      LOG.error("Error message: " + "IOException" + e.getMessage());
    } finally {
      CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
      CarbonLockUtil.fileUnlock(carbonDeleteSegmentLock, LockUsage.DELETE_SEGMENT_LOCK);
    }

    return invalidLoadTimestamps;
  }

  /**
   * writes load details into a given file at @param dataLoadLocation
   *
   * @param dataLoadLocation
   * @param listOfLoadFolderDetailsArray
   * @throws IOException
   */
  public static void writeLoadDetailsIntoFile(String dataLoadLocation,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray) throws IOException {
    AtomicFileOperations fileWrite =
        new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the metadata file.

    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
              Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetailsArray);
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
   * updates deletion status details for each load and returns invalidLoadIds
   *
   * @param loadIds
   * @param listOfLoadFolderDetailsArray
   * @param invalidLoadIds
   * @return invalidLoadIds
   */
  public static List<String> updateDeletionStatus(List<String> loadIds,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray, List<String> invalidLoadIds) {
    for (String loadId : loadIds) {
      boolean loadFound = false;
      // For each load id loop through data and if the
      // load id is found then mark
      // the metadata as deleted.
      for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {

        if (loadId.equalsIgnoreCase(loadMetadata.getLoadName())) {
          // if the segment is compacted then no need to delete that.
          if (CarbonCommonConstants.SEGMENT_COMPACTED
              .equalsIgnoreCase(loadMetadata.getLoadStatus())) {
            LOG.error("Cannot delete the Segment which is compacted. Segment is " + loadId);
            invalidLoadIds.add(loadId);
            return invalidLoadIds;
          }
          if (!CarbonCommonConstants.MARKED_FOR_DELETE.equals(loadMetadata.getLoadStatus())) {
            loadFound = true;
            updateSegmentMetadataDetails(loadMetadata);
            LOG.info("Segment ID " + loadId + " Marked for Delete");
          }
          break;
        }
      }

      if (!loadFound) {
        LOG.audit("Delete segment by ID is failed. No matching segment id found :" + loadId);
        invalidLoadIds.add(loadId);
        return invalidLoadIds;
      }

    }
    return invalidLoadIds;
  }

  /**
   * updates deletion status details for load and returns invalidLoadTimestamps
   *
   * @param loadDate
   * @param listOfLoadFolderDetailsArray
   * @param invalidLoadTimestamps
   * @return invalidLoadTimestamps
   */
  public static List<String> updateDeletionStatus(String loadDate,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray, List<String> invalidLoadTimestamps,
      Long loadStartTime) {
    // For each load timestamp loop through data and if the
    // required load timestamp is found then mark
    // the metadata as deleted.
    boolean loadFound = false;
    String loadStartTimeString = "Load Start Time: ";
    for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
      Integer result = compareDateValues(loadMetadata.getLoadStartTimeAsLong(), loadStartTime);
      if (result < 0) {
        if (CarbonCommonConstants.SEGMENT_COMPACTED
            .equalsIgnoreCase(loadMetadata.getLoadStatus())) {
          LOG.info("Ignoring the segment : " + loadMetadata.getLoadName()
              + "as the segment has been compacted.");
          continue;
        }
        if (!CarbonCommonConstants.MARKED_FOR_DELETE.equals(loadMetadata.getLoadStatus())) {
          loadFound = true;
          updateSegmentMetadataDetails(loadMetadata);
          LOG.info("Info: " +
              loadStartTimeString + loadMetadata.getLoadStartTime() +
              " Marked for Delete");
        }
      }

    }

    if (!loadFound) {
      invalidLoadTimestamps.add(loadDate);
      LOG.audit("Delete segment by date is failed. No matching segment found.");
      return invalidLoadTimestamps;
    }
    return invalidLoadTimestamps;
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
   * updates table status details using latest metadata
   *
   * @param oldMetadata
   * @param newMetadata
   * @return
   */

  public static List<LoadMetadataDetails> updateLatestTableStatusDetails(
      LoadMetadataDetails[] oldMetadata, LoadMetadataDetails[] newMetadata) {

    List<LoadMetadataDetails> newListMetadata =
        new ArrayList<LoadMetadataDetails>(Arrays.asList(newMetadata));
    for (LoadMetadataDetails oldSegment : oldMetadata) {
      if (CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(oldSegment.getLoadStatus())) {
        updateSegmentMetadataDetails(newListMetadata.get(newListMetadata.indexOf(oldSegment)));
      }
    }
    return newListMetadata;
  }

  /**
   * updates segment status and modificaton time details
   *
   * @param loadMetadata
   */
  public static void updateSegmentMetadataDetails(LoadMetadataDetails loadMetadata) {
    // update status only if the segment is not marked for delete
    if (!CarbonCommonConstants.MARKED_FOR_DELETE.equalsIgnoreCase(loadMetadata.getLoadStatus())) {
      loadMetadata.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
      loadMetadata.setModificationOrdeletionTimesStamp(readCurrentTime());
    }
  }


  public static class SegmentStatus {
    private final List<String> validSegments;
    private final List<String> validUpdatedSegments;
    private final List<String> invalidSegments;

    private SegmentStatus(List<String> validSegments, List<String> validUpdatedSegments,
        List<String> invalidSegments) {
      this.validSegments = validSegments;
      this.validUpdatedSegments = validUpdatedSegments;
      this.invalidSegments = invalidSegments;
    }

    public List<String> getValidSegments() {
      return validSegments;
    }

    public List<String> getUpadtedSegments() {
      return validUpdatedSegments;
    }

    public List<String> getInvalidSegments() {
      return invalidSegments;
    }
  }
}
