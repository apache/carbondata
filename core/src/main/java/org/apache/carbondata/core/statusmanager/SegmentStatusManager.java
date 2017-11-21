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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.exceptions.TableStatusLockException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DeleteLoadFolders;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;

/**
 * Manages Load/Segment status
 */
public class SegmentStatusManager {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  private AbsoluteTableIdentifier identifier;

  public SegmentStatusManager(AbsoluteTableIdentifier identifier) {
    this.identifier = identifier;
  }

  /**
   * This will return the lock object used to lock the table status file before updation.
   *
   * @return
   */
  public ICarbonLock getTableStatusLock() {
    return CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
  }

  /**
   * This method will return last modified time of tablestatus file
   */
  public static long getTableStatusLastModifiedTime(AbsoluteTableIdentifier identifier)
      throws IOException {
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
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
  public ValidAndInvalidSegmentsInfo getValidAndInvalidSegments() throws IOException {

    // @TODO: move reading LoadStatus file to separate class
    List<String> listOfValidSegments = new ArrayList<>(10);
    List<String> listOfValidUpdatedSegments = new ArrayList<>(10);
    List<String> listOfInvalidSegments = new ArrayList<>(10);
    List<String> listOfStreamSegments = new ArrayList<>(10);
    String dataPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
    DataInputStream dataInputStream = null;

    // Use GSON to deserialize the load information
    Gson gson = new Gson();

    AtomicFileOperations fileOperation =
            new AtomicFileOperationsImpl(dataPath, FileFactory.getFileType(dataPath));
    LoadMetadataDetails[] loadFolderDetailsArray;
    try {
      if (FileFactory.isFileExist(dataPath, FileFactory.getFileType(dataPath))) {
        dataInputStream = fileOperation.openForRead();
        BufferedReader buffReader =
            new BufferedReader(new InputStreamReader(dataInputStream, "UTF-8"));
        loadFolderDetailsArray = gson.fromJson(buffReader, LoadMetadataDetails[].class);
        // if loadFolderDetailsArray is null, assign a empty array
        if (null == loadFolderDetailsArray) {
          loadFolderDetailsArray = new LoadMetadataDetails[0];
        }
        //just directly iterate Array
        for (LoadMetadataDetails segment : loadFolderDetailsArray) {
          if (SegmentStatus.SUCCESS == segment.getSegmentStatus() ||
              SegmentStatus.MARKED_FOR_UPDATE == segment.getSegmentStatus() ||
              SegmentStatus.LOAD_PARTIAL_SUCCESS == segment.getSegmentStatus() ||
              SegmentStatus.STREAMING == segment.getSegmentStatus() ||
              SegmentStatus.STREAMING_FINISH == segment.getSegmentStatus()) {
            // check for merged loads.
            if (null != segment.getMergedLoadName()) {
              if (!listOfValidSegments.contains(segment.getMergedLoadName())) {
                listOfValidSegments.add(segment.getMergedLoadName());
              }
              // if merged load is updated then put it in updated list
              if (SegmentStatus.MARKED_FOR_UPDATE == segment.getSegmentStatus()) {
                listOfValidUpdatedSegments.add(segment.getMergedLoadName());
              }
              continue;
            }

            if (SegmentStatus.MARKED_FOR_UPDATE == segment.getSegmentStatus()) {

              listOfValidUpdatedSegments.add(segment.getLoadName());
            }
            if (SegmentStatus.STREAMING == segment.getSegmentStatus() ||
                SegmentStatus.STREAMING_FINISH == segment.getSegmentStatus()) {
              listOfStreamSegments.add(segment.getLoadName());
              continue;
            }
            listOfValidSegments.add(segment.getLoadName());
          } else if ((SegmentStatus.LOAD_FAILURE == segment.getSegmentStatus() ||
              SegmentStatus.COMPACTED == segment.getSegmentStatus() ||
              SegmentStatus.MARKED_FOR_DELETE == segment.getSegmentStatus())) {
            listOfInvalidSegments.add(segment.getLoadName());
          }
        }
      }
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(dataInputStream);
    }
    return new ValidAndInvalidSegmentsInfo(listOfValidSegments, listOfValidUpdatedSegments,
            listOfInvalidSegments, listOfStreamSegments);
  }

  /**
   * This method reads the load metadata file
   *
   * @param metadataFolderPath
   * @return
   */
  public static LoadMetadataDetails[] readLoadMetadata(String metadataFolderPath) {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    String metadataFileName = metadataFolderPath + CarbonCommonConstants.FILE_SEPARATOR
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
      LOG.error(e, "Failed to read metadata of load");
      return new LoadMetadataDetails[0];
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }

    // if listOfLoadFolderDetailsArray is null, return empty array
    if (null == listOfLoadFolderDetailsArray) {
      return new LoadMetadataDetails[0];
    }

    return listOfLoadFolderDetailsArray;
  }

  /**
   * This method will create new segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  public static int createNewSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = -1;
    for (int i = 0; i < loadMetadataDetails.length; i++) {
      try {
        int loadCount = Integer.parseInt(loadMetadataDetails[i].getLoadName());
        if (newSegmentId < loadCount) {
          newSegmentId = loadCount;
        }
      } catch (NumberFormatException ne) {
        // this case is for compacted folders. For compacted folders Id will be like 0.1, 2.1
        // consider a case when 12 loads are completed and after that major compaction is triggered.
        // In this case new compacted folder will be created with name 12.1 and after query time
        // out all the compacted folders will be deleted and entry will also be removed from the
        // table status file. In that case also if a new load comes the new segment Id assigned
        // should be 13 and not 0
        String loadName = loadMetadataDetails[i].getLoadName();
        if (loadName.contains(".")) {
          int loadCount = Integer.parseInt(loadName.split("\\.")[0]);
          if (newSegmentId < loadCount) {
            newSegmentId = loadCount;
          }
        }
      }
    }
    newSegmentId++;
    return newSegmentId;
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
        CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.DELETE_SEGMENT_LOCK);
    ICarbonLock carbonTableStatusLock =
        CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
    String tableDetails =
        carbonTableIdentifier.getDatabaseName() + "." + carbonTableIdentifier.getTableName();
    List<String> invalidLoadIds = new ArrayList<String>(0);
    try {
      if (carbonDeleteSegmentLock.lockWithRetries()) {
        LOG.info("Delete segment lock has been successfully acquired");

        String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
        LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;
        if (!FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation))) {
          // log error.
          LOG.error("Load metadata file is not present.");
          return loadIds;
        }
        // read existing metadata details in load metadata.
        listOfLoadFolderDetailsArray = readLoadMetadata(tableFolderPath);
        if (listOfLoadFolderDetailsArray.length != 0) {
          updateDeletionStatus(identifier, loadIds, listOfLoadFolderDetailsArray, invalidLoadIds);
          if (invalidLoadIds.isEmpty()) {
            // All or None , if anything fails then dont write
            if (carbonTableStatusLock.lockWithRetries()) {
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
      throw e;
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
        CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.DELETE_SEGMENT_LOCK);
    ICarbonLock carbonTableStatusLock =
        CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
    String tableDetails =
        carbonTableIdentifier.getDatabaseName() + "." + carbonTableIdentifier.getTableName();
    List<String> invalidLoadTimestamps = new ArrayList<String>(0);
    try {
      if (carbonDeleteSegmentLock.lockWithRetries()) {
        LOG.info("Delete segment lock has been successfully acquired");

        String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
        LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;

        if (!FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation))) {
          // Table status file is not present, maybe table is empty, ignore this operation
          LOG.warn("Trying to update table metadata file which is not present.");
          return invalidLoadTimestamps;
        }
        // read existing metadata details in load metadata.
        listOfLoadFolderDetailsArray = readLoadMetadata(tableFolderPath);
        if (listOfLoadFolderDetailsArray.length != 0) {
          updateDeletionStatus(identifier, loadDate, listOfLoadFolderDetailsArray,
              invalidLoadTimestamps, loadStartTime);
          if (invalidLoadTimestamps.isEmpty()) {
            if (carbonTableStatusLock.lockWithRetries()) {
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
      throw e;
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
   * updates deletion status details for each load and returns invalidLoadIds
   *
   * @param loadIds
   * @param listOfLoadFolderDetailsArray
   * @param invalidLoadIds
   * @return invalidLoadIds
   */
  private static List<String> updateDeletionStatus(AbsoluteTableIdentifier absoluteTableIdentifier,
      List<String> loadIds, LoadMetadataDetails[] listOfLoadFolderDetailsArray,
      List<String> invalidLoadIds) {
    SegmentStatus segmentStatus = null;
    for (String loadId : loadIds) {
      boolean loadFound = false;
      // For each load id loop through data and if the
      // load id is found then mark
      // the metadata as deleted.
      for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {

        if (loadId.equalsIgnoreCase(loadMetadata.getLoadName())) {
          segmentStatus = loadMetadata.getSegmentStatus();
          if (SegmentStatus.COMPACTED == segmentStatus) {
            // if the segment is compacted then no need to delete that.
            LOG.error("Cannot delete the Segment which is compacted. Segment is " + loadId);
            invalidLoadIds.add(loadId);
            return invalidLoadIds;
          } else if (SegmentStatus.INSERT_IN_PROGRESS == segmentStatus
              && checkIfValidLoadInProgress(absoluteTableIdentifier, loadId)) {
            // if the segment status is in progress then no need to delete that.
            LOG.error("Cannot delete the segment " + loadId + " which is load in progress");
            invalidLoadIds.add(loadId);
            return invalidLoadIds;
          } else if (SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == segmentStatus
              && checkIfValidLoadInProgress(absoluteTableIdentifier, loadId)) {
            // if the segment status is overwrite in progress, then no need to delete that.
            LOG.error("Cannot delete the segment " + loadId + " which is load overwrite " +
                    "in progress");
            invalidLoadIds.add(loadId);
            return invalidLoadIds;
          } else if (SegmentStatus.STREAMING == segmentStatus) {
            // if the segment status is streaming, the segment can't be deleted directly.
            LOG.error("Cannot delete the segment " + loadId + " which is streaming in progress");
            invalidLoadIds.add(loadId);
            return invalidLoadIds;
          } else if (SegmentStatus.MARKED_FOR_DELETE != segmentStatus) {
            loadFound = true;
            loadMetadata.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
            loadMetadata.setModificationOrdeletionTimesStamp(CarbonUpdateUtil.readCurrentTime());
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
  public static List<String> updateDeletionStatus(AbsoluteTableIdentifier absoluteTableIdentifier,
      String loadDate, LoadMetadataDetails[] listOfLoadFolderDetailsArray,
      List<String> invalidLoadTimestamps, Long loadStartTime) {
    // For each load timestamp loop through data and if the
    // required load timestamp is found then mark
    // the metadata as deleted.
    boolean loadFound = false;
    String loadStartTimeString = "Load Start Time: ";
    SegmentStatus segmentStatus = null;
    for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
      Integer result = compareDateValues(loadMetadata.getLoadStartTimeAsLong(), loadStartTime);
      if (result < 0) {
        segmentStatus = loadMetadata.getSegmentStatus();
        if (SegmentStatus.COMPACTED == segmentStatus) {
          LOG.info("Ignoring the segment : " + loadMetadata.getLoadName()
              + "as the segment has been compacted.");
        } else if (SegmentStatus.STREAMING == segmentStatus) {
          LOG.info("Ignoring the segment : " + loadMetadata.getLoadName()
              + "as the segment is streaming in progress.");
        } else if (SegmentStatus.INSERT_IN_PROGRESS == segmentStatus && checkIfValidLoadInProgress(
            absoluteTableIdentifier, loadMetadata.getLoadName())) {
          LOG.info("Ignoring the segment : " + loadMetadata.getLoadName()
              + "as the segment is insert in progress.");
        } else if (SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == segmentStatus
            && checkIfValidLoadInProgress(absoluteTableIdentifier, loadMetadata.getLoadName())) {
          LOG.info("Ignoring the segment : " + loadMetadata.getLoadName()
              + "as the segment is insert overwrite in progress.");
        } else if (SegmentStatus.MARKED_FOR_DELETE != segmentStatus) {
          loadFound = true;
          updateSegmentMetadataDetails(loadMetadata);
          LOG.info("Info: " + loadStartTimeString + loadMetadata.getLoadStartTime()
              + " Marked for Delete");
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
      if (SegmentStatus.MARKED_FOR_DELETE == oldSegment.getSegmentStatus()) {
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
    if (SegmentStatus.MARKED_FOR_DELETE != loadMetadata.getSegmentStatus()) {
      loadMetadata.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
      loadMetadata.setModificationOrdeletionTimesStamp(CarbonUpdateUtil.readCurrentTime());
    }
  }

  /**
   * This API will return the update status file name.
   * @param segmentList
   * @return
   */
  public String getUpdateStatusFileName(LoadMetadataDetails[] segmentList) {
    if (segmentList.length == 0) {
      return "";
    }
    else {
      for (LoadMetadataDetails eachSeg : segmentList) {
        // file name stored in 0th segment.
        if (eachSeg.getLoadName().equalsIgnoreCase("0")) {
          return eachSeg.getUpdateStatusFileName();
        }
      }
    }
    return "";
  }

  public static class ValidAndInvalidSegmentsInfo {
    private final List<String> listOfValidSegments;
    private final List<String> listOfValidUpdatedSegments;
    private final List<String> listOfInvalidSegments;
    private final List<String> listOfStreamSegments;

    private ValidAndInvalidSegmentsInfo(List<String> listOfValidSegments,
        List<String> listOfValidUpdatedSegments, List<String> listOfInvalidUpdatedSegments,
        List<String> listOfStreamSegments) {
      this.listOfValidSegments = listOfValidSegments;
      this.listOfValidUpdatedSegments = listOfValidUpdatedSegments;
      this.listOfInvalidSegments = listOfInvalidUpdatedSegments;
      this.listOfStreamSegments = listOfStreamSegments;
    }
    public List<String> getInvalidSegments() {
      return listOfInvalidSegments;
    }
    public List<String> getValidSegments() {
      return listOfValidSegments;
    }

    public List<String> getStreamSegments() {
      return listOfStreamSegments;
    }
  }

  /**
   * This function checks if any load or insert overwrite is in progress before dropping that table
   * @return
   */
  public static Boolean checkIfAnyLoadInProgressForTable(CarbonTable carbonTable) {
    Boolean loadInProgress = false;
    String metaPath = carbonTable.getMetadataPath();
    LoadMetadataDetails[] listOfLoadFolderDetailsArray =
              SegmentStatusManager.readLoadMetadata(metaPath);
    if (listOfLoadFolderDetailsArray.length != 0) {
      for (LoadMetadataDetails loaddetail :listOfLoadFolderDetailsArray) {
        SegmentStatus segmentStatus = loaddetail.getSegmentStatus();
        if (segmentStatus == SegmentStatus.INSERT_IN_PROGRESS ||
                segmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
          loadInProgress =
              checkIfValidLoadInProgress(carbonTable.getAbsoluteTableIdentifier(),
                  loaddetail.getLoadName());
        }
      }
    }
    return loadInProgress;
  }

  /**
   * This method will check for valid IN_PROGRESS segments.
   * Tries to acquire a lock on the segment and decide on the stale segments
   * @param absoluteTableIdentifier
   *
   */
  public static Boolean checkIfValidLoadInProgress(AbsoluteTableIdentifier absoluteTableIdentifier,
      String loadId) {
    ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
        CarbonTablePath.addSegmentPrefix(loadId) + LockUsage.LOCK);
    try {
      return !segmentLock.lockWithRetries(1, 0);
    } finally {
      segmentLock.unlock();
    }
  }

  private static boolean isLoadDeletionRequired(String metaDataLocation) {
    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(metaDataLocation);
    if (details != null && details.length > 0) {
      for (LoadMetadataDetails oneRow : details) {
        if ((SegmentStatus.MARKED_FOR_DELETE == oneRow.getSegmentStatus()
            || SegmentStatus.COMPACTED == oneRow.getSegmentStatus()
            || SegmentStatus.INSERT_IN_PROGRESS == oneRow.getSegmentStatus()
            || SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == oneRow.getSegmentStatus())
            && oneRow.getVisibility().equalsIgnoreCase("true")) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * This will update the old table status details before clean files to the latest table status.
   * @param oldList
   * @param newList
   * @return
   */
  public static List<LoadMetadataDetails> updateLoadMetadataFromOldToNew(
      LoadMetadataDetails[] oldList, LoadMetadataDetails[] newList) {

    List<LoadMetadataDetails> newListMetadata =
        new ArrayList<LoadMetadataDetails>(Arrays.asList(newList));
    for (LoadMetadataDetails oldSegment : oldList) {
      if ("false".equalsIgnoreCase(oldSegment.getVisibility())) {
        newListMetadata.get(newListMetadata.indexOf(oldSegment)).setVisibility("false");
      }
    }
    return newListMetadata;
  }

  private static void writeLoadMetadata(AbsoluteTableIdentifier identifier,
      List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());

    DataOutputStream dataOutputStream;
    Gson gsonObjectToWrite = new Gson();
    BufferedWriter brWriter = null;

    AtomicFileOperations writeOperation =
        new AtomicFileOperationsImpl(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));

    try {

      dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray());
      brWriter.write(metadataInstance);
    } finally {
      try {
        if (null != brWriter) {
          brWriter.flush();
        }
      } catch (Exception e) {
        LOG.error("error in  flushing ");

      }
      CarbonUtil.closeStreams(brWriter);
      writeOperation.close();
    }
  }

  public static void deleteLoadsAndUpdateMetadata(
      CarbonTable carbonTable,
      boolean isForceDeletion) throws IOException {
    if (isLoadDeletionRequired(carbonTable.getMetadataPath())) {
      LoadMetadataDetails[] details =
          SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
      AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
      ICarbonLock carbonTableStatusLock = CarbonLockFactory.getCarbonLockObj(
          identifier, LockUsage.TABLE_STATUS_LOCK);

      // Delete marked loads
      boolean isUpdationRequired = DeleteLoadFolders.deleteLoadFoldersFromFileSystem(
          identifier, isForceDeletion, details, carbonTable.getMetadataPath());

      boolean updationCompletionStatus = false;

      if (isUpdationRequired) {
        try {
          // Update load metadate file after cleaning deleted nodes
          if (carbonTableStatusLock.lockWithRetries()) {
            LOG.info("Table status lock has been successfully acquired.");

            // read latest table status again.
            LoadMetadataDetails[] latestMetadata =
                SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());

            // update the metadata details from old to new status.
            List<LoadMetadataDetails> latestStatus =
                updateLoadMetadataFromOldToNew(details, latestMetadata);

            writeLoadMetadata(identifier, latestStatus);
          } else {
            String dbName = identifier.getCarbonTableIdentifier().getDatabaseName();
            String tableName = identifier.getCarbonTableIdentifier().getTableName();
            String errorMsg = "Clean files request is failed for " +
                dbName + "." + tableName +
                ". Not able to acquire the table status lock due to other operation " +
                "running in the background.";
            LOG.audit(errorMsg);
            LOG.error(errorMsg);
            throw new TableStatusLockException(errorMsg + " Please try after some time.");
          }
          updationCompletionStatus = true;
        } finally {
          CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
          if (updationCompletionStatus) {
            DeleteLoadFolders.physicalFactAndMeasureMetadataDeletion(
                identifier, carbonTable.getMetadataPath(), isForceDeletion);
          }
        }
      }
    }
  }

}
