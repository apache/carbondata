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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
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
import org.apache.carbondata.lcm.locks.ICarbonLock;
import org.apache.carbondata.lcm.locks.LockUsage;

import com.google.gson.Gson;

/**
 * Manages Load/Segment status
 */
public class SegmentStatusManager {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  private AbsoluteTableIdentifier absoluteTableIdentifier;

  public SegmentStatusManager(AbsoluteTableIdentifier absoluteTableIdentifier) {
    this.absoluteTableIdentifier = absoluteTableIdentifier;
  }

  public static class ValidSegmentsInfo {
    public final List<String> listOfValidSegments;
    public final List<String> listOfValidUpdatedSegments;

    public ValidSegmentsInfo(List<String> listOfValidSegments,
        List<String> listOfValidUpdatedSegments) {
      this.listOfValidSegments = listOfValidSegments;
      this.listOfValidUpdatedSegments = listOfValidUpdatedSegments;
    }
  }

  /**
   * This will return the lock object used to lock the table status file before updation.
   *
   * @return
   */
  public ICarbonLock getTableStatusLock() {
    return CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier.getCarbonTableIdentifier(),
        LockUsage.TABLE_STATUS_LOCK);
  }

  /**
   * This method will return last modified time of tablestatus file
   */
  public long getTableStatusLastModifiedTime() throws IOException {
    String tableStatusPath = CarbonStorePath.getCarbonTablePath(
        absoluteTableIdentifier.getStorePath(), absoluteTableIdentifier.getCarbonTableIdentifier())
          .getTableStatusFilePath();
    if (!FileFactory.isFileExist(tableStatusPath, FileFactory.getFileType(tableStatusPath))) {
      return 0L;
    } else {
      return FileFactory.getCarbonFile(tableStatusPath, FileFactory.getFileType(tableStatusPath))
          .getLastModifiedTime();
    }
  }

  /**
   * get valid segment for given table
   * @return
   * @throws IOException
   */
  public ValidSegmentsInfo getValidSegments() throws IOException {

    // @TODO: move reading LoadStatus file to separate class
    List<String> listOfValidSegments = new ArrayList<String>(10);
    List<String> listOfValidUpdatedSegments = new ArrayList<String>(10);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());
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
            new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));

        loadFolderDetailsArray = gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
        //just directly iterate Array
        List<LoadMetadataDetails> loadFolderDetails = Arrays.asList(loadFolderDetailsArray);

        for (LoadMetadataDetails loadMetadataDetails : loadFolderDetails) {
          if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
              || CarbonCommonConstants.MARKED_FOR_UPDATE
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())
              || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS
              .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
            // check for merged loads.
            if (null != loadMetadataDetails.getMergedLoadName()) {
              if (!listOfValidSegments.contains(loadMetadataDetails.getMergedLoadName())) {
                listOfValidSegments.add(loadMetadataDetails.getMergedLoadName());
              }
              // if merged load is updated then put it in updated list
              if (CarbonCommonConstants.MARKED_FOR_UPDATE
                  .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {
                listOfValidUpdatedSegments.add(loadMetadataDetails.getMergedLoadName());
              }
              continue;
            }

            if (CarbonCommonConstants.MARKED_FOR_UPDATE
                .equalsIgnoreCase(loadMetadataDetails.getLoadStatus())) {

              listOfValidUpdatedSegments.add(loadMetadataDetails.getLoadName());
            }
            listOfValidSegments.add(loadMetadataDetails.getLoadName());

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
    return new ValidSegmentsInfo(listOfValidSegments, listOfValidUpdatedSegments);
  }

  /**
   * This method reads the load metadata file
   *
   * @param tableFolderPath
   * @return
   */
  public LoadMetadataDetails[] readLoadMetadata(String tableFolderPath) {
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
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT);
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
   * @return
   */
  private String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  /**
   * compares two given date strings
   *
   * @param loadValue
   * @param userValue
   * @return -1 if first arg is less than second arg, 1 if first arg is greater than second arg,
   * 0 otherwise
   */
  private Integer compareDateValues(Long loadValue, Long userValue) {

    return loadValue.compareTo(userValue);
  }

  /**
   * updates deletion status
   * @param loadIds
   * @param tableFolderPath
   * @return
   */
  public List<String> updateDeletionStatus(List<String> loadIds, String tableFolderPath) {
    ICarbonLock carbonLock = CarbonLockFactory
        .getCarbonLockObj(absoluteTableIdentifier.getCarbonTableIdentifier(),
            LockUsage.METADATA_LOCK);
    List<String> invalidLoadIds = new ArrayList<String>(0);
    try {
      if (carbonLock.lockWithRetries()) {
        LOG.info("Metadata lock has been successfully acquired");

        CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
                absoluteTableIdentifier.getCarbonTableIdentifier());
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
          if(invalidLoadIds.isEmpty())
          {
            // All or None , if anything fails then dont write
            writeLoadDetailsIntoFile(dataLoadLocation, listOfLoadFolderDetailsArray);
          }
          else
          {
            return invalidLoadIds;
          }

        } else {
          LOG.audit("Delete segment by Id is failed. No matching segment id found.");
          return loadIds;
        }

      } else {
        LOG.error("Unable to acquire the metadata lock");
      }
    } catch (IOException e) {
      LOG.error("IOException" + e.getMessage());
    } finally {
      fileUnlock(carbonLock);
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
  public List<String> updateDeletionStatus(String loadDate, String tableFolderPath,
      Long loadStartTime) {
    ICarbonLock carbonLock = CarbonLockFactory
        .getCarbonLockObj(absoluteTableIdentifier.getCarbonTableIdentifier(),
            LockUsage.METADATA_LOCK);
    List<String> invalidLoadTimestamps = new ArrayList<String>(0);
    try {
      if (carbonLock.lockWithRetries()) {
        LOG.info("Metadata lock has been successfully acquired");

        CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
                absoluteTableIdentifier.getCarbonTableIdentifier());
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
          updateDeletionStatus(loadDate, listOfLoadFolderDetailsArray,
              invalidLoadTimestamps, loadStartTime);
          if(invalidLoadTimestamps.isEmpty()) {
            writeLoadDetailsIntoFile(dataLoadLocation, listOfLoadFolderDetailsArray);
          }
          else
          {
            return invalidLoadTimestamps;
          }

        } else {
          LOG.audit("Delete segment by date is failed. No matching segment found.");
          invalidLoadTimestamps.add(loadDate);
          return invalidLoadTimestamps;
        }

      } else {
        LOG.error("Error message: " + "Unable to acquire the metadata lock");
      }
    } catch (IOException e) {
      LOG.error("Error message: " + "IOException" + e.getMessage());
    } finally {
      fileUnlock(carbonLock);
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
  public void writeLoadDetailsIntoFile(String dataLoadLocation,
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
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetailsArray);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
    }

    fileWrite.close();
  }

  /**
   * updates deletion status details for each load and returns invalidLoadIds
   * @param loadIds
   * @param listOfLoadFolderDetailsArray
   * @param invalidLoadIds
   * @return invalidLoadIds
   */
  public List<String> updateDeletionStatus(List<String> loadIds,
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
            loadMetadata.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
            loadMetadata.setModificationOrdeletionTimesStamp(readCurrentTime());
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
  public List<String> updateDeletionStatus(String loadDate,
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
          loadMetadata.setLoadStatus(CarbonCommonConstants.MARKED_FOR_DELETE);
          loadMetadata.setModificationOrdeletionTimesStamp(readCurrentTime());
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
   * unlocks given file
   * @param carbonLock
   */
  private void fileUnlock(ICarbonLock carbonLock) {
    if (carbonLock.unlock()) {
      LOG.info("Metadata lock has been successfully released");
    } else {
      LOG
          .error("Not able to release the metadata lock");
    }
  }



}
