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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.exception.ConcurrentOperationException;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.locks.LockUsage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.RelationIdentifier;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.readcommitter.TableStatusReadCommittedScope;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DeleteLoadFolders;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import static org.apache.carbondata.core.constants.CarbonCommonConstants.DEFAULT_CHARSET;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
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

  private static final int READ_TABLE_STATUS_RETRY_COUNT = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK,
                  CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CARBON_LOCK_DEFAULT);

  private static final int READ_TABLE_STATUS_RETRY_TIMEOUT = CarbonLockUtil
          .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CARBON_LOCK,
                  CarbonCommonConstants.MAX_TIMEOUT_FOR_CARBON_LOCK_DEFAULT);

  public SegmentStatusManager(AbsoluteTableIdentifier identifier) {
    this.identifier = identifier;
    configuration = FileFactory.getConfiguration();
  }

  public SegmentStatusManager(AbsoluteTableIdentifier identifier, Configuration configuration) {
    this.identifier = identifier;
    this.configuration = configuration;
  }

  /**
   * This will return the lock object used to lock the table status file before update.
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
    if (!FileFactory.isFileExist(tableStatusPath)) {
      return 0L;
    } else {
      return FileFactory.getCarbonFile(tableStatusPath)
          .getLastModifiedTime();
    }
  }

  public ValidAndInvalidSegmentsInfo getValidAndInvalidSegments() throws IOException {
    return getValidAndInvalidSegments(false, null, null);
  }

  public ValidAndInvalidSegmentsInfo getValidAndInvalidSegments(Boolean isChildTable)
      throws IOException {
    return getValidAndInvalidSegments(isChildTable, null, null);
  }

  /**
   * get valid segment for given load status details.
   */
  public ValidAndInvalidSegmentsInfo getValidAndInvalidSegments(Boolean isChildTable,
      LoadMetadataDetails[] loadMetadataDetails, ReadCommittedScope readCommittedScope)
      throws IOException {

    // @TODO: move reading LoadStatus file to separate class
    List<Segment> listOfValidSegments = new ArrayList<>(10);
    List<Segment> listOfValidUpdatedSegments = new ArrayList<>(10);
    List<Segment> listOfInvalidSegments = new ArrayList<>(10);
    List<Segment> listOfStreamSegments = new ArrayList<>(10);
    List<Segment> listOfInProgressSegments = new ArrayList<>(10);
    Map<String, List<String>> mergedLoadMapping = new HashMap<>();

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
          // In case of child table, during loading, if no record is loaded to the segment, then
          // segmentStatus will be marked as 'Success'. During query, don't need to add that segment
          // to validSegment list, as segment does not exists
          if (isChildTable) {
            if (!segment.getDataSize().equalsIgnoreCase("0") && !segment.getIndexSize()
                .equalsIgnoreCase("0")) {
              listOfValidSegments.add(
                  new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope,
                      segment));
            }
          } else {
            listOfValidSegments.add(
                new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope,
                    segment));
          }
        } else if ((SegmentStatus.LOAD_FAILURE == segment.getSegmentStatus()
            || SegmentStatus.COMPACTED == segment.getSegmentStatus()
            || SegmentStatus.MARKED_FOR_DELETE == segment.getSegmentStatus())) {
          listOfInvalidSegments.add(new Segment(segment.getLoadName(), segment.getSegmentFile()));
          if (SegmentStatus.COMPACTED == segment.getSegmentStatus()) {
            // After main table compaction, segment mapping of child tables may not be updated.
            // In order to check if main table and child table are in sync after compaction,
            // check the main table's merged segment's map. ex: {0.1 -> 0,1,2,3}
            if (null != segment.getMergedLoadName()) {
              if (mergedLoadMapping.containsKey(segment.getMergedLoadName())) {
                mergedLoadMapping.get(segment.getMergedLoadName()).add(segment.getLoadName());
              } else {
                List<String> mergedLoads = new ArrayList<>();
                mergedLoads.add(segment.getLoadName());
                mergedLoadMapping.put(segment.getMergedLoadName(), mergedLoads);
              }
            }
          }
        } else if (SegmentStatus.INSERT_IN_PROGRESS == segment.getSegmentStatus() ||
            SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS == segment.getSegmentStatus()) {
          listOfInProgressSegments.add(
              new Segment(segment.getLoadName(), segment.getSegmentFile(), readCommittedScope));
        }
      }
    } catch (IOException e) {
      LOG.error(e.getMessage(), e);
      throw e;
    }
    return new ValidAndInvalidSegmentsInfo(listOfValidSegments, listOfValidUpdatedSegments,
        listOfInvalidSegments, listOfStreamSegments, listOfInProgressSegments, mergedLoadMapping);
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
   * Returns valid segment list from validAndInvalidSegmentsInfo
   */
  public static List<String> getValidSegmentList(
      ValidAndInvalidSegmentsInfo validAndInvalidSegmentsInfo) {
    List<String> segmentList = new ArrayList<>();
    for (Segment segment : validAndInvalidSegmentsInfo.getValidSegments()) {
      segmentList.add(segment.getSegmentNo());
    }
    return segmentList;
  }

  /**
   * Returns ValidAndInvalidSegmentsInfo for a given RelationIdentifier
   *
   * @param relationIdentifier get list of segments for relation identifier
   * @return validAndInvalidSegmentsInfo
   */
  public static ValidAndInvalidSegmentsInfo getValidAndInvalidSegmentsInfo(
      RelationIdentifier relationIdentifier) throws IOException {
    return new SegmentStatusManager(AbsoluteTableIdentifier
        .from(relationIdentifier.getTablePath(), relationIdentifier.getDatabaseName(),
            relationIdentifier.getTableName())).getValidAndInvalidSegments();
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

  /**
   * Read file and return its content as string
   *
   * @param tableStatusPath path of the table status to read
   * @return file content, null is file does not exist
   * @throws IOException if IO errors
   */
  public static String readFileAsString(String tableStatusPath) throws IOException {
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;

    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(tableStatusPath);

    if (!FileFactory.isFileExist(tableStatusPath)) {
      return null;
    }

    try {
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream, Charset.forName(DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      return buffReader.readLine();
    } catch (EOFException ex) {
      throw ex;
    } catch (IOException e) {
      LOG.error("Failed to read table status file", e);
      throw e;
    } finally {
      closeStreams(buffReader, inStream, dataInputStream);
    }
  }

  /**
   * Read table status file and decoded to segment meta arrays
   *
   * @param tableStatusPath table status file path
   * @return segment metadata
   * @throws IOException if IO errors
   */
  public static LoadMetadataDetails[] readTableStatusFile(String tableStatusPath)
      throws IOException {
    int retry = READ_TABLE_STATUS_RETRY_COUNT;

    // When storing table status file in object store, reading of table status file may
    // fail (receive IOException or JsonSyntaxException)
    // when table status file is being modifying
    // so here we retry multiple times before
    // throwing IOException or JsonSyntaxException
    while (retry > 0) {
      try {
        String content = readFileAsString(tableStatusPath);
        if (content == null) {
          return new LoadMetadataDetails[0];
        }
        return new Gson().fromJson(content, LoadMetadataDetails[].class);
      } catch (JsonSyntaxException | IOException ex) {
        retry--;
        if (retry == 0) {
          // we have retried several times, throw this exception to make the execution failed
          LOG.error("Failed to read table status file:" + tableStatusPath);
          throw ex;
        }
        try {
          LOG.warn("Failed to read table status file, retry count:" + retry);
          // sleep for some time before retry
          TimeUnit.SECONDS.sleep(READ_TABLE_STATUS_RETRY_TIMEOUT);
        } catch (InterruptedException e) {
          // ignored
        }
      }
    }
    return null;
  }

  /**
   * This method will get the max segment id
   *
   * @param loadMetadataDetails
   * @return
   */
  private static int getMaxSegmentId(LoadMetadataDetails[] loadMetadataDetails) {
    int newSegmentId = -1;
    for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
      try {
        int loadCount = Integer.parseInt(loadMetadataDetail.getLoadName());
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
        String loadName = loadMetadataDetail.getLoadName();
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
    ICarbonLock carbonCleanFilesLock =
        CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.CLEAN_FILES_LOCK);
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
        if (carbonCleanFilesLock.lockWithRetries()) {
          LOG.info("Clean Files lock has been successfully acquired");
          String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier
              .getTablePath());
          LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;
          if (!FileFactory.isFileExist(dataLoadLocation)) {
            // log error.
            LOG.error("Load metadata file is not present.");
            return loadIds;
          }
          // read existing metadata details in load metadata.
          listOfLoadFolderDetailsArray = readLoadMetadata(tableFolderPath);
          if (listOfLoadFolderDetailsArray.length != 0) {
            updateDeletionStatus(identifier, loadIds, listOfLoadFolderDetailsArray, invalidLoadIds);
            if (invalidLoadIds.isEmpty()) {
              // All or None , if anything fails then don't write
              if (carbonTableStatusLock.lockWithRetries()) {
                LOG.info("Table status lock has been successfully acquired");
                // To handle concurrency scenarios, always take latest metadata before writing
                // into status file.
                LoadMetadataDetails[] latestLoadMetadataDetails = readLoadMetadata(tableFolderPath);
                writeLoadDetailsIntoFile(dataLoadLocation, updateLatestTableStatusDetails(
                    listOfLoadFolderDetailsArray, latestLoadMetadataDetails).stream()
                    .toArray(LoadMetadataDetails[]::new));
              } else {
                String errorMsg = "Delete segment by id is failed for " + tableDetails
                    + ". Not able to acquire the table status lock due to other operation running "
                    + "in the background.";
                LOG.error(errorMsg);
                throw new Exception(errorMsg + " Please try after some time.");
              }

            } else {
              return invalidLoadIds;
            }

          } else {
            LOG.error("Delete segment by Id is failed. No matching segment id found.");
            return loadIds;
          }
        } else {
          String errorMsg = "Delete segment by id is failed for " + tableDetails
              + " as not able to acquire clean files lock.";
          LOG.error(errorMsg);
          throw new Exception(errorMsg + " Please try after some time.");
        }
      } else {
        String errorMsg = "Delete segment by id is failed for " + tableDetails
            + ". Not able to acquire the delete segment lock due to another delete "
            + "operation is running in the background.";
        LOG.error(errorMsg);
        throw new Exception(errorMsg + " Please try after some time.");
      }
    } catch (IOException e) {
      LOG.error("IOException" + e.getMessage(), e);
      throw e;
    } finally {
      CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
      CarbonLockUtil.fileUnlock(carbonDeleteSegmentLock, LockUsage.DELETE_SEGMENT_LOCK);
      CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK);
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
    ICarbonLock carbonCleanFilesLock =
        CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.CLEAN_FILES_LOCK);
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
        if (carbonCleanFilesLock.lockWithRetries()) {
          LOG.info("Clean Files lock has been successfully acquired");
          String dataLoadLocation = CarbonTablePath.getTableStatusFilePath(identifier
              .getTablePath());
          LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;

          if (!FileFactory.isFileExist(dataLoadLocation)) {
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
                writeLoadDetailsIntoFile(dataLoadLocation, updateLatestTableStatusDetails(
                    listOfLoadFolderDetailsArray, latestLoadMetadataDetails).stream()
                    .toArray(LoadMetadataDetails[]::new));
              } else {

                String errorMsg = "Delete segment by date is failed for " + tableDetails
                    + ". Not able to acquire the table status lock due to other operation running "
                    + "in the background.";
                LOG.error(errorMsg);
                throw new Exception(errorMsg + " Please try after some time.");

              }
            } else {
              return invalidLoadTimestamps;
            }

          } else {
            LOG.error("Delete segment by date is failed. No matching segment found.");
            invalidLoadTimestamps.add(loadDate);
            return invalidLoadTimestamps;
          }
        } else {
          String errorMsg = "Delete segment by id is failed for " + tableDetails
              + " as not able to acquire clean files lock.";
          LOG.error(errorMsg);
          throw new Exception(errorMsg + " Please try after some time.");
        }
      } else {
        String errorMsg = "Delete segment by date is failed for " + tableDetails
            + ". Not able to acquire the delete segment lock due to another delete "
            + "operation is running in the background.";
        LOG.error(errorMsg);
        throw new Exception(errorMsg + " Please try after some time.");
      }
    } catch (IOException e) {
      LOG.error("Error message: " + "IOException" + e.getMessage(), e);
      throw e;
    } finally {
      CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
      CarbonLockUtil.fileUnlock(carbonDeleteSegmentLock, LockUsage.DELETE_SEGMENT_LOCK);
      CarbonLockUtil.fileUnlock(carbonCleanFilesLock, LockUsage.CLEAN_FILES_LOCK);
    }

    return invalidLoadTimestamps;
  }

  /**
   * Backup the table status file as 'tablestatus.backup' in the same path
   *
   * @param tableStatusPath table status file path
   */
  private static void backupTableStatus(String tableStatusPath) throws IOException {
    CarbonFile file = FileFactory.getCarbonFile(tableStatusPath);
    if (file.exists()) {
      String backupPath = tableStatusPath + ".backup";
      String currentContent = readFileAsString(tableStatusPath);
      if (currentContent != null) {
        writeStringIntoFile(backupPath, currentContent);
      }
    }
  }

  /**
   * writes load details to specified path
   *
   * @param tableStatusPath path of the table status file
   * @param listOfLoadFolderDetailsArray segment metadata
   * @throws IOException if IO errors
   */
  public static void writeLoadDetailsIntoFile(
      String tableStatusPath,
      LoadMetadataDetails[] listOfLoadFolderDetailsArray) throws IOException {
    // When overwriting table status file, if process crashed, table status file
    // will be in corrupted state. This can happen in an unstable environment,
    // like in the cloud. To prevent the table corruption, user can enable following
    // property to enable backup of the table status before overwriting it.
    if (tableStatusPath.endsWith(CarbonTablePath.TABLE_STATUS_FILE) &&
        CarbonProperties.isEnableTableStatusBackup()) {
      backupTableStatus(tableStatusPath);
    }
    String content = new Gson().toJson(listOfLoadFolderDetailsArray);
    mockForTest();
    // make the table status file smaller by removing fields that are default value
    for (LoadMetadataDetails loadMetadataDetails : listOfLoadFolderDetailsArray) {
      loadMetadataDetails.removeUnnecessaryField();
    }
    // If process crashed during following write, table status file need to be
    // manually recovered.
    writeStringIntoFile(FileFactory.getUpdatedFilePath(tableStatusPath), content);
  }

  // a dummy func for mocking in testcase, which simulates IOException
  private static void mockForTest() {
  }

  /**
   * writes string content to specified path
   *
   * @param filePath path of the file to write
   * @param content content to write
   * @throws IOException if IO errors
   */
  public static void writeStringIntoFile(String filePath, String content) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(filePath);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(
          dataOutputStream, Charset.forName(DEFAULT_CHARSET)));
      brWriter.write(content);
    } catch (IOException ioe) {
      LOG.error("Write file failed: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
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
            loadMetadata.setModificationOrDeletionTimestamp(CarbonUpdateUtil.readCurrentTime());
            LOG.info("Segment ID " + loadId + " Marked for Delete");
          }
          break;
        }
      }

      if (!loadFound) {
        LOG.error("Delete segment by ID is failed. No matching segment id found :" + loadId);
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
  private static List<String> updateDeletionStatus(AbsoluteTableIdentifier absoluteTableIdentifier,
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
      LOG.error("Delete segment by date is failed. No matching segment found.");
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
  private static List<LoadMetadataDetails> updateLatestTableStatusDetails(
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
   * updates segment status and modification time details
   *
   * @param loadMetadata
   */
  private static void updateSegmentMetadataDetails(LoadMetadataDetails loadMetadata) {
    // update status only if the segment is not marked for delete
    if (SegmentStatus.MARKED_FOR_DELETE != loadMetadata.getSegmentStatus()) {
      loadMetadata.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
      loadMetadata.setModificationOrDeletionTimestamp(CarbonUpdateUtil.readCurrentTime());
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
    Map<String, List<String>> mergedLoadMapping;

    private ValidAndInvalidSegmentsInfo(List<Segment> listOfValidSegments,
        List<Segment> listOfValidUpdatedSegments, List<Segment> listOfInvalidUpdatedSegments,
        List<Segment> listOfStreamSegments, List<Segment> listOfInProgressSegments,
        Map<String, List<String>> mergedLoadMapping) {
      this.listOfValidSegments = listOfValidSegments;
      this.listOfValidUpdatedSegments = listOfValidUpdatedSegments;
      this.listOfInvalidSegments = listOfInvalidUpdatedSegments;
      this.listOfStreamSegments = listOfStreamSegments;
      this.listOfInProgressSegments = listOfInProgressSegments;
      this.mergedLoadMapping = mergedLoadMapping;
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

    public Map<String, List<String>> getMergedLoadMapping() {
      return mergedLoadMapping;
    }
  }

  /**
   * Return true if any load or insert overwrite is in progress for specified table
   */
  public static boolean isLoadInProgressInTable(CarbonTable carbonTable) {
    if (carbonTable == null) {
      return false;
    }
    boolean loadInProgress = false;
    String metaPath = carbonTable.getMetadataPath();
    LoadMetadataDetails[] listOfLoadFolderDetailsArray =
              SegmentStatusManager.readLoadMetadata(metaPath);
    if (listOfLoadFolderDetailsArray.length != 0) {
      for (LoadMetadataDetails loadDetail :listOfLoadFolderDetailsArray) {
        SegmentStatus segmentStatus = loadDetail.getSegmentStatus();
        if (segmentStatus == SegmentStatus.INSERT_IN_PROGRESS
            || segmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
          loadInProgress =
              isLoadInProgress(carbonTable.getAbsoluteTableIdentifier(),
                  loadDetail.getLoadName());
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
  public static boolean isCompactionInProgress(CarbonTable carbonTable) {
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
  public static boolean isOverwriteInProgressInTable(CarbonTable carbonTable) {
    if (carbonTable == null) {
      return false;
    }
    boolean loadInProgress = false;
    String metaPath = carbonTable.getMetadataPath();
    LoadMetadataDetails[] listOfLoadFolderDetailsArray =
        SegmentStatusManager.readLoadMetadata(metaPath);
    if (listOfLoadFolderDetailsArray.length != 0) {
      for (LoadMetadataDetails loadDetail :listOfLoadFolderDetailsArray) {
        SegmentStatus segmentStatus = loadDetail.getSegmentStatus();
        if (segmentStatus == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
          loadInProgress =
              isLoadInProgress(carbonTable.getAbsoluteTableIdentifier(),
                  loadDetail.getLoadName());
        }
      }
    }
    return loadInProgress;
  }

  /**
   * Return true if the specified `loadName` is in progress, by checking the load lock.
   */
  public static boolean isLoadInProgress(AbsoluteTableIdentifier absoluteTableIdentifier,
      String loadName) {
    ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
        CarbonTablePath.addSegmentPrefix(loadName) + LockUsage.LOCK);
    try {
      return !segmentLock.lockWithRetries(1, 0);
    } finally {
      segmentLock.unlock();
    }
  }

  public static boolean isExpiredSegment(LoadMetadataDetails oneLoad, AbsoluteTableIdentifier
      absoluteTableIdentifier) {
    boolean isExpiredSegment = false;
    if (oneLoad.getSegmentStatus() == SegmentStatus.COMPACTED || oneLoad.getSegmentStatus() ==
        SegmentStatus.MARKED_FOR_DELETE) {
      isExpiredSegment = true;
    } else if (oneLoad.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS || oneLoad
        .getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
      // check if lock can be acquired
      ICarbonLock segmentLock = CarbonLockFactory.getCarbonLockObj(absoluteTableIdentifier,
          CarbonTablePath.addSegmentPrefix(oneLoad.getLoadName()) + LockUsage.LOCK);
      try {
        isExpiredSegment = segmentLock.lockWithRetries();
      } finally {
        CarbonLockUtil.fileUnlock(segmentLock, LockUsage.LOCK);
      }
    }
    return isExpiredSegment;
  }

  public static boolean isLoadDeletionRequired(LoadMetadataDetails[] details) {
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
  private static List<LoadMetadataDetails> updateLoadMetadataFromOldToNew(
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

  private static class ReturnTuple {
    LoadMetadataDetails[] details;
    Set<String> loadsToDelete;
    ReturnTuple(LoadMetadataDetails[] details, Set<String> loadsToDelete) {
      this.details = details;
      this.loadsToDelete = loadsToDelete;
    }
  }

  private static ReturnTuple isUpdateRequired(boolean isForceDeletion, CarbonTable carbonTable,
      AbsoluteTableIdentifier absoluteTableIdentifier, LoadMetadataDetails[] details,
      boolean cleanStaleInProgress) {
    // Delete marked loads
    Set<String> loadsToDelete = DeleteLoadFolders
        .deleteLoadFoldersFromFileSystem(absoluteTableIdentifier, isForceDeletion, details,
            carbonTable.getMetadataPath(), cleanStaleInProgress);
    return new ReturnTuple(details, loadsToDelete);
  }

  public static void deleteLoadsAndUpdateMetadata(CarbonTable carbonTable, boolean isForceDeletion,
      List<PartitionSpec> partitionSpecs, boolean cleanStaleInprogress,
      boolean isCleanFilesOperation) throws IOException {
    LoadMetadataDetails[] metadataDetails =
        SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
    // delete the expired segment lock files
    CarbonLockUtil.deleteExpiredSegmentLockFiles(carbonTable);
    if (isLoadDeletionRequired(metadataDetails)) {
      AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
      boolean updateCompletionStatus = false;
      Set<String> loadsToDelete = new HashSet<>();
      LoadMetadataDetails[] newAddedLoadHistoryList = null;
      ReturnTuple tuple =
          isUpdateRequired(isForceDeletion, carbonTable, identifier, metadataDetails,
              cleanStaleInprogress);
      if (!tuple.loadsToDelete.isEmpty()) {
        ICarbonLock carbonTableStatusLock =
            CarbonLockFactory.getCarbonLockObj(identifier, LockUsage.TABLE_STATUS_LOCK);
        boolean locked = false;
        try {
          int retryCount = CarbonLockUtil
              .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
                  CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
          int maxTimeout = CarbonLockUtil
              .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
                  CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
          // Update load metadata file after cleaning deleted nodes
          locked = carbonTableStatusLock.lockWithRetries(retryCount, maxTimeout);
          if (locked) {
            LOG.info("Table status lock has been successfully acquired.");
            // Again read status and check to verify update required or not.
            LoadMetadataDetails[] details =
                SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
            ReturnTuple tuple2 =
                isUpdateRequired(isForceDeletion, carbonTable,
                    identifier, details, cleanStaleInprogress);
            if (tuple2.loadsToDelete.isEmpty()) {
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
            if (isCleanFilesOperation || invisibleSegmentCnt > invisibleSegmentPreserveCnt) {
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
              writeLoadDetailsIntoFile(
                  CarbonTablePath.getTableStatusFilePath(identifier.getTablePath()),
                  latestStatus.toArray(new LoadMetadataDetails[0]));
            }
            updateCompletionStatus = true;
            loadsToDelete = tuple2.loadsToDelete;
          } else {
            String dbName = identifier.getCarbonTableIdentifier().getDatabaseName();
            String tableName = identifier.getCarbonTableIdentifier().getTableName();
            String errorMsg = "Clean files request is failed for " +
                dbName + "." + tableName +
                ". Not able to acquire the table status lock due to other operation " +
                "running in the background.";
            LOG.error(errorMsg);
            throw new IOException(errorMsg + " Please try after some time.");
          }
        } finally {
          if (locked) {
            CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
          }
          if (updateCompletionStatus) {
            DeleteLoadFolders
                .physicalFactAndMeasureMetadataDeletion(carbonTable, newAddedLoadHistoryList,
                  isForceDeletion, partitionSpecs, cleanStaleInprogress, loadsToDelete);
          }
        }
      }
    }
  }

  public static void truncateTable(CarbonTable carbonTable)
      throws ConcurrentOperationException, IOException {
    ICarbonLock carbonTableStatusLock = CarbonLockFactory.getCarbonLockObj(
        carbonTable.getAbsoluteTableIdentifier(), LockUsage.TABLE_STATUS_LOCK);
    boolean locked = false;
    try {
      // Update load metadata file after cleaning deleted nodes
      locked = carbonTableStatusLock.lockWithRetries();
      if (locked) {
        LOG.info("Table status lock has been successfully acquired.");
        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(
                CarbonTablePath.getMetadataPath(carbonTable.getTablePath()));
        for (LoadMetadataDetails listOfLoadFolderDetails : listOfLoadFolderDetailsArray) {
          boolean writing;
          switch (listOfLoadFolderDetails.getSegmentStatus()) {
            case INSERT_IN_PROGRESS:
            case INSERT_OVERWRITE_IN_PROGRESS:
            case STREAMING:
              writing = true;
              break;
            default:
              writing = false;
          }
          if (writing) {
            throw new ConcurrentOperationException(carbonTable, "insert", "truncate");
          }
        }
        for (LoadMetadataDetails listOfLoadFolderDetails : listOfLoadFolderDetailsArray) {
          listOfLoadFolderDetails.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
        }
        SegmentStatusManager
            .writeLoadDetailsIntoFile(
                CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()),
                listOfLoadFolderDetailsArray);
      } else {
        String dbName = carbonTable.getDatabaseName();
        String tableName = carbonTable.getTableName();
        String errorMsg = "truncate table request is failed for " +
            dbName + "." + tableName +
            ". Not able to acquire the table status lock due to other operation " +
            "running in the background.";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        CarbonLockUtil.fileUnlock(carbonTableStatusLock, LockUsage.TABLE_STATUS_LOCK);
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

  /**
   * map loadName -> loadStartTime
   */
  public static Map<String, String> mapSegmentToStartTime(CarbonTable carbonTable) {
    LoadMetadataDetails[] loadMetadataDetails = SegmentStatusManager.readLoadMetadata(
        carbonTable.getMetadataPath());
    if (loadMetadataDetails != null && loadMetadataDetails.length > 0) {
      Map<String, String> map = new HashMap<>(loadMetadataDetails.length);
      for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
        map.put(loadMetadataDetail.getLoadName(),
            String.valueOf(loadMetadataDetail.getLoadStartTime()));
      }
      return map;
    } else {
      return new HashMap<>(0);
    }
  }
}
