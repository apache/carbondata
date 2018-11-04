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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.common.logging.impl.Audit;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DeleteLoadFolders;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Manages Load/Segment status
 */
public class SegmentStatusManager {

  private static final Logger LOG =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  private AbsoluteTableIdentifier identifier;

  private Configuration configuration;

  public SegmentStatusManager(AbsoluteTableIdentifier identifier) {
    this.identifier = identifier;
    configuration = FileFactory.getConfiguration();
  }

  public SegmentStatusManager(AbsoluteTableIdentifier identifier, Configuration configuration) {
    this.identifier = identifier;
    this.configuration = configuration;
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

  public ValidAndInvalidSegmentsInfo getValidAndInvalidSegments() throws IOException {
    return getValidAndInvalidSegments(null, null);
  }

  /**
   * get valid segment for given load status details.
   */
  public ValidAndInvalidSegmentsInfo getValidAndInvalidSegments(
      LoadMetadataDetails[] loadMetadataDetails, ReadCommittedScope readCommittedScope)
      throws IOException {

    // @TODO: move reading LoadStatus file to separate class
    List<Segment> listOfValidSegments = new ArrayList<>(10);
    List<Segment> listOfValidUpdatedSegments = new ArrayList<>(10);
    List<Segment> listOfInvalidSegments = new ArrayList<>(10);
    List<Segment> listOfStreamSegments = new ArrayList<>(10);
    List<Segment> listOfInProgressSegments = new ArrayList<>(10);

    try {
      if (loadMetadataDetails == null) {
        loadMetadataDetails = readTableStatusFile(
            CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()));
      }

      if (readCommittedScope == null) {
        readCommittedScope = new TableStatusReadCommittedScope(identifier, loadMetadataDetails,
            configuration);
      }
      //just directly iterate Array
      for (LoadMetadataDetails segment : loadMetadataDetails) {
        if (SegmentStatus.SUCCESS == segment.getSegmentStatus()
            || SegmentStatus.MARKED_FOR_UPDATE == segment.getSegmentStatus()
            || SegmentStatus.LOAD_PARTIAL_SUCCESS == segment.getSegmentStatus()
            || SegmentStatus.STREAMING == segment.getSegmentStatus()
            || SegmentStatus.STREAMING_FINISH == segment.getSegmentStatus()) {
          // check for merged loads.
          if (null != segment.getMergedLoadName()) {
            Segment seg = new Segment(segment.getMergedLoadName(), segment.getSegmentFile(),
                readCommittedScope, segment);
            if (!listOfValidSegments.contains(seg)) {
              listOfValidSegments.add(seg);
            }
            // if merged load is updated then put it in updated list
            if (SegmentStatus.MARKED_FOR_UPDATE == segment.getSegmentStatus()) {
              listOfValidUpdatedSegments.add(seg);
            }
            continue;
          }

          if (SegmentStatus.MARKED_FOR_UPDATE == segment.getSegmentStatus()) {

            listOfValidUpdatedSegments.add(
                new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope));
          }
          if (SegmentStatus.STREAMING == segment.getSegmentStatus()
              || SegmentStatus.STREAMING_FINISH == segment.getSegmentStatus()) {
            listOfStreamSegments.add(
                new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope));
            continue;
          }
          listOfValidSegments.add(
              new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope,
                  segment));
        } else if ((SegmentStatus.LOAD_FAILURE == segment.getSegmentStatus()
            || SegmentStatus.COMPACTED == segment.getSegmentStatus()
            || SegmentStatus.MARKED_FOR_DELETE == segment.getSegmentStatus())) {
          listOfInvalidSegments.add(new Segment(segment.getLoadName(), segment.getSegmentFile()));
        } else if (SegmentStatus.INSERT_IN_PROGRESS == segment.getSegmentStatus() ||
            SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == segment.getSegmentStatus()) {
          listOfInProgressSegments.add(
              new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope));
        }
      }
    } catch (IOException e) {
      LOG.error(e);
      throw e;
    }
    return new ValidAndInvalidSegmentsInfo(listOfValidSegments, listOfValidUpdatedSegments,
        listOfInvalidSegments, listOfStreamSegments, listOfInProgressSegments);
  }

  /**
   * This method reads the load metadata file
   *
   * @param metadataFolderPath
   * @return
   */
  public static LoadMetadataDetails[] readLoadMetadata(String metadataFolderPath) {
    String metadataFileName = metadataFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonTablePath.TABLE_STATUS_FILE;
    try {
      return readTableStatusFile(metadataFileName);
    } catch (IOException e) {
      return new LoadMetadataDetails[0];
    }
  }

  /**
   * Reads the table status file with the specified UUID if non empty.
   */
  public static LoadMetadataDetails[] readLoadMetadata(String metaDataFolderPath, String uuid)
      throws IOException {
    String tableStatusFileName;
    if (uuid.isEmpty()) {
      tableStatusFileName = metaDataFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonTablePath.TABLE_STATUS_FILE;
    } else {
      tableStatusFileName = metaDataFolderPath + CarbonCommonConstants.FILE_SEPARATOR
          + CarbonTablePath.TABLE_STATUS_FILE + CarbonCommonConstants.UNDERSCORE + uuid;
    }
    return readTableStatusFile(tableStatusFileName);
  }

  /**
   * This method reads the load history metadata file
   *
   * @param metadataFolderPath
   * @return
   */
  public static LoadMetadataDetails[] readLoadHistoryMetadata(String metadataFolderPath) {
    String metadataFileName = metadataFolderPath + CarbonCommonConstants.FILE_SEPARATOR
        + CarbonTablePath.TABLE_STATUS_HISTORY_FILE;
    try {
      return readTableStatusFile(metadataFileName);
    } catch (IOException e) {
      return new LoadMetadataDetails[0];
    }
  }

  public static LoadMetadataDetails[] readTableStatusFile(String tableStatusPath)
      throws IOException {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    LoadMetadataDetails[] listOfLoadFolderDetailsArray;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(tableStatusPath);

    try {
      if (!FileFactory.isFileExist(tableStatusPath, FileFactory.getFileType(tableStatusPath))) {
        return new LoadMetadataDetails[0];
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      listOfLoadFolderDetailsArray =
          gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
    } catch (IOException e) {
      LOG.error("Failed to read metadata of load", e);
      throw e;
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
   * This method will get the max segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  public static int getMaxSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
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
    return newSegmentId;
  }

  /**
   * This method will create new segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  public static int createNewSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = getMaxSegmentId(loadMetadataDetails);
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
              Audit.log(LOG, errorMsg);
              LOG.error(errorMsg);
              throw new Exception(errorMsg + " Please try after some time.");
            }

          } else {
            return invalidLoadIds;
          }

        } else {
          Audit.log(LOG, "Delete segment by Id is failed. No matching segment id found.");
          return loadIds;
        }

      } else {
        String errorMsg = "Delete segment by id is failed for " + tableDetails
            + ". Not able to acquire the delete segment lock due to another delete "
            + "operation is running in the background.";
        Audit.log(LOG, errorMsg);
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
              Audit.log(LOG, errorMsg);
              LOG.error(errorMsg);
              throw new Exception(errorMsg + " Please try after some time.");

            }
          } else {
            return invalidLoadTimestamps;
          }

        } else {
          Audit.log(LOG, "Delete segment by date is failed. No matching segment found.");
          invalidLoadTimestamps.add(loadDate);
          return invalidLoadTimestamps;
        }

      } else {
        String errorMsg = "Delete segment by date is failed for " + tableDetails
            + ". Not able to acquire the delete segment lock due to another delete "
            + "operation is running in the background.";
        Audit.log(LOG, errorMsg);
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
        AtomicFileOperationFactory.getAtomicFileOperations(dataLoadLocation);
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
              && isLoadInProgress(absoluteTableIdentifier, loadId)) {
            // if the segment status is in progress then no need to delete that.
            LOG.error("Cannot delete the segment " + loadId + " which is load in progress");
            invalidLoadIds.add(loadId);
            return invalidLoadIds;
          } else if (SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == segmentStatus
              && isLoadInProgress(absoluteTableIdentifier, loadId)) {
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
        Audit.log(LOG, "Delete segment by ID is failed. No matching segment id found :" + loadId);
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
        } else if (SegmentStatus.INSERT_IN_PROGRESS == segmentStatus && isLoadInProgress(
            absoluteTableIdentifier, loadMetadata.getLoadName())) {
          LOG.info("Ignoring the segment : " + loadMetadata.getLoadName()
              + "as the segment is insert in progress.");
        } else if (SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == segmentStatus
            && isLoadInProgress(absoluteTableIdentifier, loadMetadata.getLoadName())) {
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
      Audit.log(LOG, "Delete segment by date is failed. No matching segment found.");
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
    private final List<Segment> listOfValidSegments;
    private final List<Segment> listOfValidUpdatedSegments;
    private final List<Segment> listOfInvalidSegments;
    private final List<Segment> listOfStreamSegments;
    private final List<Segment> listOfInProgressSegments;

    private ValidAndInvalidSegmentsInfo(List<Segment> listOfValidSegments,
        List<Segment> listOfValidUpdatedSegments, List<Segment> listOfInvalidUpdatedSegments,
        List<Segment> listOfStreamSegments, List<Segment> listOfInProgressSegments) {
      this.listOfValidSegments = listOfValidSegments;
      this.listOfValidUpdatedSegments = listOfValidUpdatedSegments;
      this.listOfInvalidSegments = listOfInvalidUpdatedSegments;
      this.listOfStreamSegments = listOfStreamSegments;
      this.listOfInProgressSegments = listOfInProgressSegments;
    }
    public List<Segment> getInvalidSegments() {
      return listOfInvalidSegments;
    }
    public List<Segment> getValidSegments() {
      return listOfValidSegments;
    }

    public List<Segment> getStreamSegments() {
      return listOfStreamSegments;
    }

    public List<Segment> getListOfInProgressSegments() {
      return listOfInProgressSegments;
    }
  }

  /**
   * Return true if any load or insert overwrite is in progress for specified table
   */
  public static Boolean isLoadInProgressInTable(CarbonTable carbonTable) {
    if (carbonTable == null) {
      return false;
    }
    boolean loadInProgress = false;
    String metaPath = carbonTable.getMetadataPath();
    LoadMetadataDetails[] listOfLoadFolderDetailsArray =
              SegmentStatusManager.readLoadMetadata(metaPath);
    if (listOfLoadFolderDetailsArray.length != 0) {
      for (LoadMetadataDetails loaddetail :listOfLoadFolderDetailsArray) {
        SegmentStatus segmentStatus = loaddetail.getSegmentStatus();
        if (segmentStatus == SegmentStatus.INSERT_IN_PROGRESS
            || segmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
          loadInProgress =
              isLoadInProgress(carbonTable.getAbsoluteTableIdentifier(),
                  loaddetail.getLoadName());
        }
      }
    }
    return loadInProgress;
  }

  /**
   * Return true if the compaction is in progress for the table
   * @param carbonTable
   * @return
   */
  public static Boolean isCompactionInProgress(CarbonTable carbonTable) {
    if (carbonTable == null) {
      return false;
    }
    boolean compactionInProgress;
    ICarbonLock lock = CarbonLockFactory
        .getCarbonLockObj(carbonTable.getAbsoluteTableIdentifier(), LockUsage.COMPACTION_LOCK);
    try {
      compactionInProgress = !lock.lockWithRetries(1, 0);
    } finally {
      lock.unlock();
    }
    return compactionInProgress;
  }

  /**
   * Return true if insert overwrite is in progress for specified table
   */
  public static Boolean isOverwriteInProgressInTable(CarbonTable carbonTable) {
    if (carbonTable == null) {
      return false;
    }
    boolean loadInProgress = false;
    String metaPath = carbonTable.getMetadataPath();
    LoadMetadataDetails[] listOfLoadFolderDetailsArray =
        SegmentStatusManager.readLoadMetadata(metaPath);
    if (listOfLoadFolderDetailsArray.length != 0) {
      for (LoadMetadataDetails loaddetail :listOfLoadFolderDetailsArray) {
        SegmentStatus segmentStatus = loaddetail.getSegmentStatus();
        if (segmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
          loadInProgress =
              isLoadInProgress(carbonTable.getAbsoluteTableIdentifier(),
                  loaddetail.getLoadName());
        }
      }
    }
    return loadInProgress;
  }

  /**
   * Return true if the specified `loadName` is in progress, by checking the load lock.
   */
  public static Boolean isLoadInProgress(AbsoluteTableIdentifier absoluteTableIdentifier,
      String loadName) {
    ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
        CarbonTablePath.addSegmentPrefix(loadName) + LockUsage.LOCK);
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
        AtomicFileOperationFactory.getAtomicFileOperations(dataLoadLocation);

    try {

      dataOutputStream = writeOperation.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(listOfLoadFolderDetails.toArray());
      brWriter.write(metadataInstance);
    } catch (IOException ie) {
      LOG.error("Error message: " + ie.getLocalizedMessage());
      writeOperation.setFailed();
      throw ie;
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

  private static class ReturnTuple {
    LoadMetadataDetails[] details;
    boolean isUpdateRequired;
    ReturnTuple(LoadMetadataDetails[] details, boolean isUpdateRequired) {
      this.details = details;
      this.isUpdateRequired = isUpdateRequired;
    }
  }

  private static ReturnTuple isUpdationRequired(
      boolean isForceDeletion,
      CarbonTable carbonTable,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    LoadMetadataDetails[] details =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
    // Delete marked loads
    boolean isUpdationRequired =
        DeleteLoadFolders.deleteLoadFoldersFromFileSystem(
            absoluteTableIdentifier,
            isForceDeletion,
            details,
            carbonTable.getMetadataPath()
        );
    return new ReturnTuple(details, isUpdationRequired);
  }

  public static void deleteLoadsAndUpdateMetadata(
      CarbonTable carbonTable,
      boolean isForceDeletion,
      List<PartitionSpec> partitionSpecs) throws IOException {
    // delete the expired segment lock files
    CarbonLockUtil.deleteExpiredSegmentLockFiles(carbonTable);
    if (isLoadDeletionRequired(carbonTable.getMetadataPath())) {
      AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
      boolean updationCompletionStatus = false;
      LoadMetadataDetails[] newAddedLoadHistoryList = null;
      ReturnTuple tuple = isUpdationRequired(isForceDeletion, carbonTable, identifier);
      if (tuple.isUpdateRequired) {
        ICarbonLock carbonTableStatusLock =
            CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
        boolean locked = false;
        try {
          // Update load metadate file after cleaning deleted nodes
          locked = carbonTableStatusLock.lockWithRetries();
          if (locked) {
            LOG.info("Table status lock has been successfully acquired.");
            // Again read status and check to verify updation required or not.
            ReturnTuple tuple2 = isUpdationRequired(isForceDeletion, carbonTable, identifier);
            if (!tuple2.isUpdateRequired) {
              return;
            }
            // read latest table status again.
            LoadMetadataDetails[] latestMetadata =
                SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());

            int invisibleSegmentPreserveCnt =
                CarbonProperties.getInstance().getInvisibleSegmentPreserveCount();
            int maxSegmentId = SegmentStatusManager.getMaxSegmentId(tuple2.details);
            int invisibleSegmentCnt = SegmentStatusManager.countInvisibleSegments(
                tuple2.details, maxSegmentId);
            // if execute command 'clean files' or the number of invisible segment info
            // exceeds the value of 'carbon.invisible.segments.preserve.count',
            // it need to append the invisible segment list to 'tablestatus.history' file.
            if (isForceDeletion || (invisibleSegmentCnt > invisibleSegmentPreserveCnt)) {
              TableStatusReturnTuple tableStatusReturn = separateVisibleAndInvisibleSegments(
                  tuple2.details, latestMetadata, invisibleSegmentCnt, maxSegmentId);
              LoadMetadataDetails[] oldLoadHistoryList = readLoadHistoryMetadata(
                  carbonTable.getMetadataPath());
              LoadMetadataDetails[] newLoadHistoryList = appendLoadHistoryList(
                  oldLoadHistoryList, tableStatusReturn.arrayOfLoadHistoryDetails);
              writeLoadDetailsIntoFile(
                  CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()),
                  tableStatusReturn.arrayOfLoadDetails);
              writeLoadDetailsIntoFile(
                  CarbonTablePath.getTableStatusHistoryFilePath(carbonTable.getTablePath()),
                  newLoadHistoryList);
              // the segments which will be moved to history file need to be deleted
              newAddedLoadHistoryList = tableStatusReturn.arrayOfLoadHistoryDetails;
            } else {
              // update the metadata details from old to new status.
              List<LoadMetadataDetails> latestStatus =
                  updateLoadMetadataFromOldToNew(tuple2.details, latestMetadata);
              writeLoadMetadata(identifier, latestStatus);
            }
            updationCompletionStatus = true;
          } else {
            String dbName = identifier.getCarbonTableIdentifier().getDatabaseName();
            String tableName = identifier.getCarbonTableIdentifier().getTableName();
            String errorMsg = "Clean files request is failed for " +
                dbName + "." + tableName +
                ". Not able to acquire the table status lock due to other operation " +
                "running in the background.";
            Audit.log(LOG, errorMsg);
            LOG.error(errorMsg);
            throw new IOException(errorMsg + " Please try after some time.");
          }
        } finally {
          if (locked) {
            CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
          }
          if (updationCompletionStatus) {
            DeleteLoadFolders
                .physicalFactAndMeasureMetadataDeletion(carbonTable, newAddedLoadHistoryList,
                    isForceDeletion, partitionSpecs);
          }
        }
      }
    }
  }

  /**
   * Get the number of invisible segment info from segment info list.
   */
  public static int countInvisibleSegments(
      LoadMetadataDetails[] segmentList, int maxSegmentId) {
    int invisibleSegmentCnt = 0;
    if (segmentList.length != 0) {
      for (LoadMetadataDetails eachSeg : segmentList) {
        // can not remove segment 0, there are some info will be used later
        // for example: updateStatusFileName
        // also can not remove the max segment id,
        // otherwise will impact the generation of segment id
        if (!eachSeg.getLoadName().equalsIgnoreCase("0")
            && !eachSeg.getLoadName().equalsIgnoreCase(String.valueOf(maxSegmentId))
            && eachSeg.getVisibility().equalsIgnoreCase("false")) {
          invisibleSegmentCnt += 1;
        }
      }
    }
    return invisibleSegmentCnt;
  }

  private static class TableStatusReturnTuple {
    LoadMetadataDetails[] arrayOfLoadDetails;
    LoadMetadataDetails[] arrayOfLoadHistoryDetails;
    TableStatusReturnTuple(LoadMetadataDetails[] arrayOfLoadDetails,
        LoadMetadataDetails[] arrayOfLoadHistoryDetails) {
      this.arrayOfLoadDetails = arrayOfLoadDetails;
      this.arrayOfLoadHistoryDetails = arrayOfLoadHistoryDetails;
    }
  }

  /**
   * Separate visible and invisible segments into two array.
   */
  public static TableStatusReturnTuple separateVisibleAndInvisibleSegments(
      LoadMetadataDetails[] oldList,
      LoadMetadataDetails[] newList,
      int invisibleSegmentCnt,
      int maxSegmentId) {
    int newSegmentsLength = newList.length;
    int visibleSegmentCnt = newSegmentsLength - invisibleSegmentCnt;
    LoadMetadataDetails[] arrayOfVisibleSegments = new LoadMetadataDetails[visibleSegmentCnt];
    LoadMetadataDetails[] arrayOfInvisibleSegments = new LoadMetadataDetails[invisibleSegmentCnt];
    int oldSegmentsLength = oldList.length;
    int visibleIdx = 0;
    int invisibleIdx = 0;
    for (int i = 0; i < newSegmentsLength; i++) {
      LoadMetadataDetails newSegment = newList[i];
      if (i < oldSegmentsLength) {
        LoadMetadataDetails oldSegment = oldList[i];
        if (newSegment.getLoadName().equalsIgnoreCase("0")
            || newSegment.getLoadName().equalsIgnoreCase(String.valueOf(maxSegmentId))) {
          newSegment.setVisibility(oldSegment.getVisibility());
          arrayOfVisibleSegments[visibleIdx] = newSegment;
          visibleIdx++;
        } else if ("false".equalsIgnoreCase(oldSegment.getVisibility())) {
          newSegment.setVisibility("false");
          arrayOfInvisibleSegments[invisibleIdx] = newSegment;
          invisibleIdx++;
        } else {
          arrayOfVisibleSegments[visibleIdx] = newSegment;
          visibleIdx++;
        }
      } else {
        arrayOfVisibleSegments[visibleIdx] = newSegment;
        visibleIdx++;
      }
    }
    return new TableStatusReturnTuple(arrayOfVisibleSegments, arrayOfInvisibleSegments);
  }

  /**
   * Return an array containing all invisible segment entries in appendList and historyList.
   */
  public static LoadMetadataDetails[] appendLoadHistoryList(
      LoadMetadataDetails[] historyList,
      LoadMetadataDetails[] appendList) {
    int historyListLen = historyList.length;
    int appendListLen = appendList.length;
    int newListLen = historyListLen + appendListLen;
    LoadMetadataDetails[] newList = new LoadMetadataDetails[newListLen];
    int newListIdx = 0;
    for (int i = 0; i < historyListLen; i++) {
      newList[newListIdx] = historyList[i];
      newListIdx++;
    }
    for (int i = 0; i < appendListLen; i++) {
      newList[newListIdx] = appendList[i];
      newListIdx++;
    }
    return newList;
  }
}
