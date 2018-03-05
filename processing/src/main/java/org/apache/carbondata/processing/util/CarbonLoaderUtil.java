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
package org.apache.carbondata.processing.util;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.NodeBlockRelation;
import org.apache.carbondata.processing.merger.NodeMultiBlockRelation;
import static org.apache.carbondata.core.enums.EscapeSequences.*;

import com.google.gson.Gson;


public final class CarbonLoaderUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLoaderUtil.class.getName());

  private CarbonLoaderUtil() {
  }

  public static void deleteSegment(CarbonLoadModel loadModel, int currentLoad) {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(loadModel.getTablePath(), carbonTable.getCarbonTableIdentifier());

    for (int i = 0; i < carbonTable.getPartitionCount(); i++) {
      String segmentPath = carbonTablePath.getCarbonDataDirectoryPath(i + "", currentLoad + "");
      deleteStorePath(segmentPath);
    }
  }

  /**
   * the method returns true if the segment has carbondata file else returns false.
   *
   * @param loadModel
   * @param currentLoad
   * @return
   */
  public static boolean isValidSegment(CarbonLoadModel loadModel,
      int currentLoad) {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema()
        .getCarbonTable();
    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(
        loadModel.getTablePath(), carbonTable.getCarbonTableIdentifier());

    int fileCount = 0;
    int partitionCount = carbonTable.getPartitionCount();
    for (int i = 0; i < partitionCount; i++) {
      String segmentPath = carbonTablePath.getCarbonDataDirectoryPath(i + "",
          currentLoad + "");
      CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath,
          FileFactory.getFileType(segmentPath));
      CarbonFile[] files = carbonFile.listFiles(new CarbonFileFilter() {

        @Override
        public boolean accept(CarbonFile file) {
          return file.getName().endsWith(
              CarbonTablePath.getCarbonIndexExtension())
              || file.getName().endsWith(
              CarbonTablePath.getCarbonDataExtension());
        }

      });
      fileCount += files.length;
      if (files.length > 0) {
        return true;
      }
    }
    if (fileCount == 0) {
      return false;
    }
    return true;
  }

  public static void deleteStorePath(String path) {
    try {
      FileType fileType = FileFactory.getFileType(path);
      if (FileFactory.isFileExist(path, fileType)) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(path, fileType);
        CarbonUtil.deleteFoldersAndFiles(carbonFile);
      }
    } catch (IOException | InterruptedException e) {
      LOGGER.error("Unable to delete the given path :: " + e.getMessage());
    }
  }

  /**
   * This API will write the load level metadata for the loadmanagement module inorder to
   * manage the load and query execution management smoothly.
   *
   * @param newMetaEntry
   * @param loadModel
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean recordNewLoadMetadata(LoadMetadataDetails newMetaEntry,
      CarbonLoadModel loadModel, boolean loadStartEntry, boolean insertOverwrite)
      throws IOException {
    return recordNewLoadMetadata(newMetaEntry, loadModel, loadStartEntry, insertOverwrite, "");
  }

  /**
   * This API will write the load level metadata for the loadmanagement module inorder to
   * manage the load and query execution management smoothly.
   *
   * @param newMetaEntry
   * @param loadModel
   * @param uuid
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean recordNewLoadMetadata(LoadMetadataDetails newMetaEntry,
      CarbonLoadModel loadModel, boolean loadStartEntry, boolean insertOverwrite, String uuid)
      throws IOException {
    return recordNewLoadMetadata(newMetaEntry, loadModel, loadStartEntry, insertOverwrite, uuid,
        new ArrayList<Segment>(), new ArrayList<Segment>());
  }

  /**
   * This API will write the load level metadata for the loadmanagement module inorder to
   * manage the load and query execution management smoothly.
   *
   * @param newMetaEntry
   * @param loadModel
   * @param uuid
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean recordNewLoadMetadata(LoadMetadataDetails newMetaEntry,
      CarbonLoadModel loadModel, boolean loadStartEntry, boolean insertOverwrite, String uuid,
      List<Segment> segmentsToBeDeleted, List<Segment> segmentFilesTobeUpdated) throws IOException {
    boolean status = false;
    AbsoluteTableIdentifier absoluteTableIdentifier =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    CarbonTablePath carbonTablePath = CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier);
    String metadataPath = carbonTablePath.getMetadataDirectoryPath();
    FileType fileType = FileFactory.getFileType(metadataPath);
    if (!FileFactory.isFileExist(metadataPath, fileType)) {
      FileFactory.mkdirs(metadataPath, fileType);
    }
    String tableStatusPath;
    if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() && !uuid.isEmpty()) {
      tableStatusPath = carbonTablePath.getTableStatusFilePathWithUUID(uuid);
    } else {
      tableStatusPath = carbonTablePath.getTableStatusFilePath();
    }
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    int retryCount = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
    int maxTimeout = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info(
            "Acquired lock for table" + loadModel.getDatabaseName() + "." + loadModel.getTableName()
                + " for table status updation");
        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());
        List<LoadMetadataDetails> listOfLoadFolderDetails =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<CarbonFile> staleFolders = new ArrayList<>();
        Collections.addAll(listOfLoadFolderDetails, listOfLoadFolderDetailsArray);
        // create a new segment Id if load has just begun else add the already generated Id
        if (loadStartEntry) {
          String segmentId =
              String.valueOf(SegmentStatusManager.createNewSegmentId(listOfLoadFolderDetailsArray));
          loadModel.setLoadMetadataDetails(listOfLoadFolderDetails);
          // Segment id would be provided in case this is compaction flow for aggregate data map.
          // If that is true then used the segment id as the load name.
          if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() && !loadModel
              .getSegmentId().isEmpty()) {
            newMetaEntry.setLoadName(loadModel.getSegmentId());
          } else {
            newMetaEntry.setLoadName(segmentId);
            loadModel.setSegmentId(segmentId);
          }
          // Exception should be thrown if:
          // 1. If insert overwrite is in progress and any other load or insert operation
          // is triggered
          // 2. If load or insert into operation is in progress and insert overwrite operation
          // is triggered
          for (LoadMetadataDetails entry : listOfLoadFolderDetails) {
            if (entry.getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS
                && SegmentStatusManager.isLoadInProgress(
                    absoluteTableIdentifier, entry.getLoadName())) {
              throw new RuntimeException("Already insert overwrite is in progress");
            } else if (newMetaEntry.getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS
                && entry.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS
                && SegmentStatusManager.isLoadInProgress(
                    absoluteTableIdentifier, entry.getLoadName())) {
              throw new RuntimeException("Already insert into or load is in progress");
            }
          }
          listOfLoadFolderDetails.add(newMetaEntry);
        } else {
          newMetaEntry.setLoadName(String.valueOf(loadModel.getSegmentId()));
          // existing entry needs to be overwritten as the entry will exist with some
          // intermediate status
          int indexToOverwriteNewMetaEntry = 0;
          boolean found = false;
          for (LoadMetadataDetails entry : listOfLoadFolderDetails) {
            if (entry.getLoadName().equals(newMetaEntry.getLoadName())
                && entry.getLoadStartTime() == newMetaEntry.getLoadStartTime()) {
              found = true;
              break;
            }
            indexToOverwriteNewMetaEntry++;
          }
          if (insertOverwrite) {
            for (LoadMetadataDetails entry : listOfLoadFolderDetails) {
              if (entry.getSegmentStatus() != SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS) {
                entry.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
                // For insert overwrite, we will delete the old segment folder immediately
                // So collect the old segments here
                addToStaleFolders(carbonTablePath, staleFolders, entry);
              }
            }
          }
          if (!found) {
            LOGGER.error("Entry not found to update " + newMetaEntry + " From list :: "
                + listOfLoadFolderDetails);
          }
          listOfLoadFolderDetails.set(indexToOverwriteNewMetaEntry, newMetaEntry);
        }
        // when no records are inserted then newSegmentEntry will be SegmentStatus.MARKED_FOR_DELETE
        // so empty segment folder should be deleted
        if (newMetaEntry.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE) {
          addToStaleFolders(carbonTablePath, staleFolders, newMetaEntry);
        }

        for (LoadMetadataDetails detail: listOfLoadFolderDetails) {
          // if the segments is in the list of marked for delete then update the status.
          if (segmentsToBeDeleted.contains(new Segment(detail.getLoadName(), null))) {
            detail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          } else if (segmentFilesTobeUpdated.contains(Segment.toSegment(detail.getLoadName()))) {
            detail.setSegmentFile(
                detail.getLoadName() + "_" + newMetaEntry.getUpdateStatusFileName()
                    + CarbonTablePath.SEGMENT_EXT);
          }
        }

        SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetails
            .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
        // Delete all old stale segment folders
        for (CarbonFile staleFolder : staleFolders) {
          // try block is inside for loop because even if there is failure in deletion of 1 stale
          // folder still remaining stale folders should be deleted
          try {
            CarbonUtil.deleteFoldersAndFiles(staleFolder);
          } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to delete stale folder: " + e.getMessage());
          }
        }
        status = true;
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + loadModel
            .getDatabaseName() + "." + loadModel.getTableName());
      };
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table status updation" + loadModel.getDatabaseName()
                + "." + loadModel.getTableName());
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + loadModel.getDatabaseName() + "." + loadModel
                .getTableName() + " during table status updation");
      }
    }
    return status;
  }

  /**
   * This API will update the segmentFile of a passed segment.
   *
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean updateSegmentFile(String tablePath, String segmentId, String segmentFile)
      throws IOException {
    boolean status = false;
    String tableStatusPath = CarbonTablePath.getTableStatusPath(tablePath);
    String metadataPath = CarbonTablePath.getMetadataPath(tablePath);
    AbsoluteTableIdentifier absoluteTableIdentifier =
        AbsoluteTableIdentifier.from(tablePath, null, null);
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    int retryCount = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.NUMBER_OF_TRIES_FOR_CONCURRENT_LOCK_DEFAULT);
    int maxTimeout = CarbonLockUtil
        .getLockProperty(CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK,
            CarbonCommonConstants.MAX_TIMEOUT_FOR_CONCURRENT_LOCK_DEFAULT);
    try {
      if (carbonLock.lockWithRetries(retryCount, maxTimeout)) {
        LOGGER.info("Acquired lock for tablepath" + tablePath + " for table status updation");
        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metadataPath);

        for (LoadMetadataDetails detail : listOfLoadFolderDetailsArray) {
          // if the segments is in the list of marked for delete then update the status.
          if (segmentId.equals(detail.getLoadName())) {
            detail.setSegmentFile(segmentFile);
            break;
          }
        }

        SegmentStatusManager
            .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
        status = true;
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table path " + tablePath);
      }
      ;
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + tablePath);
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + tablePath + " during table status updation");
      }
    }
    return status;
  }

  private static void addToStaleFolders(CarbonTablePath carbonTablePath,
      List<CarbonFile> staleFolders, LoadMetadataDetails entry) throws IOException {
    String path = carbonTablePath.getCarbonDataDirectoryPath("0", entry.getLoadName());
    // add to the deletion list only if file exist else HDFS file system will throw
    // exception while deleting the file if file path does not exist
    if (FileFactory.isFileExist(path, FileFactory.getFileType(path))) {
      staleFolders.add(FileFactory.getCarbonFile(path));
    }
  }

  /**
   * Method to create new entry for load in table status file
   *
   * @param loadMetadataDetails
   * @param loadStatus
   * @param loadStartTime
   * @param addLoadEndTime
   */
  public static void populateNewLoadMetaEntry(LoadMetadataDetails loadMetadataDetails,
      SegmentStatus loadStatus, long loadStartTime, boolean addLoadEndTime) {
    if (addLoadEndTime) {
      long loadEndDate = CarbonUpdateUtil.readCurrentTime();
      loadMetadataDetails.setLoadEndTime(loadEndDate);
    }
    loadMetadataDetails.setSegmentStatus(loadStatus);
    loadMetadataDetails.setLoadStartTime(loadStartTime);
  }

  public static void writeLoadMetadata(AbsoluteTableIdentifier absoluteTableIdentifier,
      List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(absoluteTableIdentifier);
    String dataLoadLocation = carbonTablePath.getTableStatusFilePath();

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
        LOGGER.error("error in  flushing ");

      }
      CarbonUtil.closeStreams(brWriter);
      writeOperation.close();
    }

  }

  public static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  public static boolean isValidEscapeSequence(String escapeChar) {
    return escapeChar.equalsIgnoreCase(NEW_LINE.getName()) ||
        escapeChar.equalsIgnoreCase(CARRIAGE_RETURN.getName()) ||
        escapeChar.equalsIgnoreCase(TAB.getName()) ||
        escapeChar.equalsIgnoreCase(BACKSPACE.getName());
  }

  public static String getEscapeChar(String escapeCharacter) {
    if (escapeCharacter.equalsIgnoreCase(NEW_LINE.getName())) {
      return NEW_LINE.getEscapeChar();
    } else if (escapeCharacter.equalsIgnoreCase(BACKSPACE.getName())) {
      return BACKSPACE.getEscapeChar();
    } else if (escapeCharacter.equalsIgnoreCase(TAB.getName())) {
      return TAB.getEscapeChar();
    } else if (escapeCharacter.equalsIgnoreCase(CARRIAGE_RETURN.getName())) {
      return CARRIAGE_RETURN.getEscapeChar();
    }
    return escapeCharacter;
  }


  public static void readAndUpdateLoadProgressInTableMeta(CarbonLoadModel model,
      boolean insertOverwrite, String uuid) throws IOException {
    LoadMetadataDetails newLoadMetaEntry = new LoadMetadataDetails();
    SegmentStatus status = SegmentStatus.INSERT_IN_PROGRESS;
    if (insertOverwrite) {
      status = SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS;
    }

    // reading the start time of data load.
    if (model.getFactTimeStamp() == 0) {
      long loadStartTime = CarbonUpdateUtil.readCurrentTime();
      model.setFactTimeStamp(loadStartTime);
    }
    CarbonLoaderUtil
        .populateNewLoadMetaEntry(newLoadMetaEntry, status, model.getFactTimeStamp(), false);
    boolean entryAdded = CarbonLoaderUtil
        .recordNewLoadMetadata(newLoadMetaEntry, model, true, insertOverwrite, uuid);
    if (!entryAdded) {
      throw new IOException("Dataload failed due to failure in table status updation for "
          + model.getTableName());
    }
  }

  public static void readAndUpdateLoadProgressInTableMeta(CarbonLoadModel model,
      boolean insertOverwrite) throws IOException {
    readAndUpdateLoadProgressInTableMeta(model, insertOverwrite, "");
  }

  /**
   * This method will update the load failure entry in the table status file
   */
  public static void updateTableStatusForFailure(CarbonLoadModel model, String uuid)
      throws IOException {
    // in case if failure the load status should be "Marked for delete" so that it will be taken
    // care during clean up
    SegmentStatus loadStatus = SegmentStatus.MARKED_FOR_DELETE;
    // always the last entry in the load metadata details will be the current load entry
    LoadMetadataDetails loadMetaEntry = model.getCurrentLoadMetadataDetail();
    if (loadMetaEntry == null) {
      return;
    }
    CarbonLoaderUtil
        .populateNewLoadMetaEntry(loadMetaEntry, loadStatus, model.getFactTimeStamp(), true);
    boolean entryAdded = CarbonLoaderUtil.recordNewLoadMetadata(
        loadMetaEntry, model, false, false, uuid);
    if (!entryAdded) {
      throw new IOException(
          "Failed to update failure entry in table status for " + model.getTableName());
    }
  }

  /**
   * This method will update the load failure entry in the table status file with empty uuid.
   */
  public static void updateTableStatusForFailure(CarbonLoadModel model)
      throws IOException {
    updateTableStatusForFailure(model, "");
  }

  public static Dictionary getDictionary(DictionaryColumnUniqueIdentifier columnIdentifier)
      throws IOException {
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictCache =
        CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY);
    return dictCache.get(columnIdentifier);
  }

  public static Dictionary getDictionary(AbsoluteTableIdentifier absoluteTableIdentifier,
      ColumnIdentifier columnIdentifier, DataType dataType)
      throws IOException {
    return getDictionary(
        new DictionaryColumnUniqueIdentifier(absoluteTableIdentifier, columnIdentifier, dataType));
  }

  /**
   * This method will divide the blocks among the tasks of the nodes as per the data locality
   *
   * @param blockInfos
   * @param noOfNodesInput -1 if number of nodes has to be decided
   *                       based on block location information
   * @param parallelism    total no of tasks to execute in parallel
   * @return
   */
  public static Map<String, List<List<Distributable>>> nodeBlockTaskMapping(
      List<Distributable> blockInfos, int noOfNodesInput, int parallelism,
      List<String> activeNode) {

    Map<String, List<Distributable>> mapOfNodes =
        CarbonLoaderUtil.nodeBlockMapping(blockInfos, noOfNodesInput, activeNode);
    int taskPerNode = parallelism / mapOfNodes.size();
    //assigning non zero value to noOfTasksPerNode
    int noOfTasksPerNode = taskPerNode == 0 ? 1 : taskPerNode;
    // divide the blocks of a node among the tasks of the node.
    return assignBlocksToTasksPerNode(mapOfNodes, noOfTasksPerNode);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @return
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(List<Distributable> blockInfos,
      int noOfNodesInput) {
    return nodeBlockMapping(blockInfos, noOfNodesInput, null);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @return
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(List<Distributable> blockInfos) {
    // -1 if number of nodes has to be decided based on block location information
    return nodeBlockMapping(blockInfos, -1);
  }

  /**
   * the method returns the number of required executors
   *
   * @param blockInfos
   * @return
   */
  public static Map<String, List<Distributable>> getRequiredExecutors(
      List<Distributable> blockInfos) {
    List<NodeBlockRelation> flattenedList =
        new ArrayList<NodeBlockRelation>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (Distributable blockInfo : blockInfos) {
      try {
        for (String eachNode : blockInfo.getLocations()) {
          NodeBlockRelation nbr = new NodeBlockRelation(blockInfo, eachNode);
          flattenedList.add(nbr);
        }
      } catch (IOException e) {
        throw new RuntimeException("error getting location of block: " + blockInfo.toString(), e);
      }
    }
    // sort the flattened data.
    Collections.sort(flattenedList);
    Map<String, List<Distributable>> nodeAndBlockMapping =
        new LinkedHashMap<String, List<Distributable>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // from the flattened list create a mapping of node vs Data blocks.
    createNodeVsBlockMapping(flattenedList, nodeAndBlockMapping);
    return nodeAndBlockMapping;
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @param noOfNodesInput -1 if number of nodes has to be decided
   *                       based on block location information
   * @return
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(List<Distributable> blockInfos,
      int noOfNodesInput, List<String> activeNodes) {

    Map<String, List<Distributable>> nodeBlocksMap =
        new HashMap<String, List<Distributable>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    List<NodeBlockRelation> flattenedList =
        new ArrayList<NodeBlockRelation>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    Set<Distributable> uniqueBlocks =
        new HashSet<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Set<String> nodes = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    createFlattenedListFromMap(blockInfos, flattenedList, uniqueBlocks, nodes);

    int noofNodes = (-1 == noOfNodesInput) ? nodes.size() : noOfNodesInput;
    if (null != activeNodes) {
      noofNodes = activeNodes.size();
    }
    int blocksPerNode = blockInfos.size() / noofNodes;
    blocksPerNode = blocksPerNode <= 0 ? 1 : blocksPerNode;

    // sort the flattened data.
    Collections.sort(flattenedList);

    Map<String, List<Distributable>> nodeAndBlockMapping =
        new LinkedHashMap<String, List<Distributable>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // from the flattened list create a mapping of node vs Data blocks.
    createNodeVsBlockMapping(flattenedList, nodeAndBlockMapping);

    // so now we have a map of node vs blocks. allocate the block as per the order
    createOutputMap(nodeBlocksMap, blocksPerNode, uniqueBlocks, nodeAndBlockMapping, activeNodes);

    // if any blocks remain then assign them to nodes in round robin.
    assignLeftOverBlocks(nodeBlocksMap, uniqueBlocks, blocksPerNode, activeNodes);

    return nodeBlocksMap;
  }

  /**
   * Assigning the blocks of a node to tasks.
   *
   * @param nodeBlocksMap nodeName to list of blocks mapping
   * @param noOfTasksPerNode
   * @return
   */
  private static Map<String, List<List<Distributable>>> assignBlocksToTasksPerNode(
      Map<String, List<Distributable>> nodeBlocksMap, int noOfTasksPerNode) {
    Map<String, List<List<Distributable>>> outputMap =
        new HashMap<String, List<List<Distributable>>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // for each node
    for (Map.Entry<String, List<Distributable>> eachNode : nodeBlocksMap.entrySet()) {

      List<Distributable> blockOfEachNode = eachNode.getValue();
      //sorting the block so same block will be give to same executor
      Collections.sort(blockOfEachNode);
      // create the task list for each node.
      createTaskListForNode(outputMap, noOfTasksPerNode, eachNode.getKey());

      // take all the block of node and divide it among the tasks of a node.
      divideBlockToTasks(outputMap, eachNode.getKey(), blockOfEachNode);
    }

    return outputMap;
  }

  /**
   * This will divide the blocks of a node to tasks of the node.
   *
   * @param outputMap
   * @param key
   * @param blockOfEachNode
   */
  private static void divideBlockToTasks(Map<String, List<List<Distributable>>> outputMap,
      String key, List<Distributable> blockOfEachNode) {

    List<List<Distributable>> taskLists = outputMap.get(key);
    int tasksOfNode = taskLists.size();
    int i = 0;
    for (Distributable block : blockOfEachNode) {

      taskLists.get(i % tasksOfNode).add(block);
      i++;
    }

  }

  /**
   * This will create the empty list for each task of a node.
   *
   * @param outputMap
   * @param noOfTasksPerNode
   * @param key
   */
  private static void createTaskListForNode(Map<String, List<List<Distributable>>> outputMap,
      int noOfTasksPerNode, String key) {
    List<List<Distributable>> nodeTaskList =
        new ArrayList<List<Distributable>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (int i = 0; i < noOfTasksPerNode; i++) {
      List<Distributable> eachTask =
          new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      nodeTaskList.add(eachTask);

    }
    outputMap.put(key, nodeTaskList);

  }

  /**
   * If any left over data blocks are present then assign those to nodes in round robin way.
   *
   * @param outputMap
   * @param uniqueBlocks
   */
  private static void assignLeftOverBlocks(Map<String, List<Distributable>> outputMap,
      Set<Distributable> uniqueBlocks, int noOfBlocksPerNode, List<String> activeNodes) {

    if (activeNodes != null) {
      for (String activeNode : activeNodes) {
        List<Distributable> blockLst = outputMap.get(activeNode);
        if (null == blockLst) {
          blockLst = new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        }
        populateBlocks(uniqueBlocks, noOfBlocksPerNode, blockLst);
        if (blockLst.size() > 0) {
          outputMap.put(activeNode, blockLst);
        }
      }
    } else {
      for (Map.Entry<String, List<Distributable>> entry : outputMap.entrySet()) {
        List<Distributable> blockLst = entry.getValue();
        populateBlocks(uniqueBlocks, noOfBlocksPerNode, blockLst);
      }

    }

    for (Map.Entry<String, List<Distributable>> entry : outputMap.entrySet()) {
      Iterator<Distributable> blocks = uniqueBlocks.iterator();
      if (blocks.hasNext()) {
        Distributable block = blocks.next();
        List<Distributable> blockLst = entry.getValue();
        blockLst.add(block);
        blocks.remove();
      }
    }
  }

  /**
   * The method populate the blockLst to be allocate to a specific node.
   * @param uniqueBlocks
   * @param noOfBlocksPerNode
   * @param blockLst
   */
  private static void populateBlocks(Set<Distributable> uniqueBlocks, int noOfBlocksPerNode,
      List<Distributable> blockLst) {
    Iterator<Distributable> blocks = uniqueBlocks.iterator();
    //if the node is already having the per block nodes then avoid assign the extra blocks
    if (blockLst.size() == noOfBlocksPerNode) {
      return;
    }
    while (blocks.hasNext()) {
      Distributable block = blocks.next();
      blockLst.add(block);
      blocks.remove();
      if (blockLst.size() >= noOfBlocksPerNode) {
        break;
      }
    }
  }

  /**
   * To create the final output of the Node and Data blocks
   *
   * @param outputMap
   * @param blocksPerNode
   * @param uniqueBlocks
   * @param nodeAndBlockMapping
   * @param activeNodes
   */
  private static void createOutputMap(Map<String, List<Distributable>> outputMap, int blocksPerNode,
      Set<Distributable> uniqueBlocks, Map<String, List<Distributable>> nodeAndBlockMapping,
      List<String> activeNodes) {

    ArrayList<NodeMultiBlockRelation> multiBlockRelations =
        new ArrayList<>(nodeAndBlockMapping.size());
    for (Map.Entry<String, List<Distributable>> entry : nodeAndBlockMapping.entrySet()) {
      multiBlockRelations.add(new NodeMultiBlockRelation(entry.getKey(), entry.getValue()));
    }
    // sort nodes based on number of blocks per node, so that nodes having lesser blocks
    // are assigned first
    Collections.sort(multiBlockRelations);

    for (NodeMultiBlockRelation nodeMultiBlockRelation : multiBlockRelations) {
      String nodeName = nodeMultiBlockRelation.getNode();
      //assign the block to the node only if the node is active
      String activeExecutor = nodeName;
      if (null != activeNodes) {
        activeExecutor = getActiveExecutor(activeNodes, nodeName);
        if (null == activeExecutor) {
          continue;
        }
      }
      // this loop will be for each NODE
      int nodeCapacity = 0;
      // loop thru blocks of each Node
      for (Distributable block : nodeMultiBlockRelation.getBlocks()) {

        // check if this is already assigned.
        if (uniqueBlocks.contains(block)) {

          if (null == outputMap.get(activeExecutor)) {
            List<Distributable> list =
                new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
            outputMap.put(activeExecutor, list);
          }
          // assign this block to this node if node has capacity left
          if (nodeCapacity < blocksPerNode) {
            List<Distributable> infos = outputMap.get(activeExecutor);
            infos.add(block);
            nodeCapacity++;
            uniqueBlocks.remove(block);
          } else {
            // No need to continue loop as node is full
            break;
          }
        }
      }
    }
  }

  /**
   * method validates whether the node is active or not.
   *
   * @param activeNode
   * @param nodeName
   * @return returns true if active else false.
   */
  private static String getActiveExecutor(List activeNode, String nodeName) {
    boolean isActiveNode = activeNode.contains(nodeName);
    if (isActiveNode) {
      return nodeName;
    }
    //if localhost then retrieve the localhost name then do the check
    else if (nodeName.equals("localhost")) {
      try {
        String hostName = InetAddress.getLocalHost().getHostName();
        isActiveNode = activeNode.contains(hostName);
        if (isActiveNode) {
          return hostName;
        }
      } catch (UnknownHostException ue) {
        isActiveNode = false;
      }
    } else {
      try {
        String hostAddress = InetAddress.getByName(nodeName).getHostAddress();
        isActiveNode = activeNode.contains(hostAddress);
        if (isActiveNode) {
          return hostAddress;
        }
      } catch (UnknownHostException ue) {
        isActiveNode = false;
      }
    }
    return null;
  }

  /**
   * Create the Node and its related blocks Mapping and put in a Map
   *
   * @param flattenedList
   * @param nodeAndBlockMapping
   */
  private static void createNodeVsBlockMapping(List<NodeBlockRelation> flattenedList,
      Map<String, List<Distributable>> nodeAndBlockMapping) {
    for (NodeBlockRelation nbr : flattenedList) {
      String node = nbr.getNode();
      List<Distributable> list;

      if (null == nodeAndBlockMapping.get(node)) {
        list = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        list.add(nbr.getBlock());
        nodeAndBlockMapping.put(node, list);
      } else {
        list = nodeAndBlockMapping.get(node);
        list.add(nbr.getBlock());
      }
    }
    /*for resolving performance issue, removed values() with entrySet () iterating the values and
    sorting it.entrySet will give the logical view for hashMap and we dont query the map twice for
    each key whereas values () iterate twice*/
    Iterator<Map.Entry<String, List<Distributable>>> iterator =
        nodeAndBlockMapping.entrySet().iterator();
    while (iterator.hasNext()) {
      Collections.sort(iterator.next().getValue());
    }
  }

  /**
   * Create the flat List i.e flattening of the Map.
   *
   * @param blockInfos
   * @param flattenedList
   * @param uniqueBlocks
   */
  private static void createFlattenedListFromMap(List<Distributable> blockInfos,
      List<NodeBlockRelation> flattenedList, Set<Distributable> uniqueBlocks,
      Set<String> nodeList) {
    for (Distributable blockInfo : blockInfos) {
      // put the blocks in the set
      uniqueBlocks.add(blockInfo);

      try {
        for (String eachNode : blockInfo.getLocations()) {
          NodeBlockRelation nbr = new NodeBlockRelation(blockInfo, eachNode);
          flattenedList.add(nbr);
          nodeList.add(eachNode);
        }
      } catch (IOException e) {
        throw new RuntimeException("error getting location of block: " + blockInfo.toString(), e);
      }
    }
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   */
  public static void checkAndCreateCarbonDataLocation(String segmentId, CarbonTable carbonTable) {
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonTable.getTablePath(), carbonTableIdentifier);
    String segmentFolder = carbonTablePath.getCarbonDataDirectoryPath("0", segmentId);
    CarbonUtil.checkAndCreateFolder(segmentFolder);
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

  /*
   * This method will add data size and index size into tablestatus for each segment. And also
   * returns the size of the segment.
   */
  public static Long addDataIndexSizeIntoMetaEntry(LoadMetadataDetails loadMetadataDetails,
      String segmentId, CarbonTable carbonTable) throws IOException {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath((carbonTable.getAbsoluteTableIdentifier()));
    Map<String, Long> dataIndexSize = CarbonUtil.getDataSizeAndIndexSize(carbonTablePath,
        new Segment(segmentId, loadMetadataDetails.getSegmentFile()));
    Long dataSize = dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE);
    loadMetadataDetails.setDataSize(String.valueOf(dataSize));
    Long indexSize = dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE);
    loadMetadataDetails.setIndexSize(String.valueOf(indexSize));
    return dataSize + indexSize;
  }

  /**
   * Merge index files with in the segment of partitioned table
   * @param segmentId
   * @param tablePath
   * @return
   * @throws IOException
   */
  public static String mergeIndexFilesinPartitionedSegment(String segmentId, String tablePath)
      throws IOException {
    CarbonIndexFileMergeWriter.SegmentIndexFIleMergeStatus segmentIndexFIleMergeStatus =
        new CarbonIndexFileMergeWriter().mergeCarbonIndexFilesOfSegment(segmentId, tablePath);
    String uniqueId = "";
    if (segmentIndexFIleMergeStatus != null) {
      uniqueId = System.currentTimeMillis() + "";
      String newSegmentFileName = segmentId + "_" + uniqueId + CarbonTablePath.SEGMENT_EXT;
      String path =
          CarbonTablePath.getSegmentFilesLocation(tablePath) + CarbonCommonConstants.FILE_SEPARATOR
              + newSegmentFileName;
      SegmentFileStore.writeSegmentFile(segmentIndexFIleMergeStatus.getSegmentFile(), path);
      updateSegmentFile(tablePath, segmentId, newSegmentFileName);
      deleteFiles(segmentIndexFIleMergeStatus.getFilesTobeDeleted());
    }
    return uniqueId;
  }

  private static void deleteFiles(List<String> filesToBeDeleted) throws IOException {
    for (String filePath : filesToBeDeleted) {
      FileFactory.deleteFile(filePath, FileFactory.getFileType(filePath));
    }
  }
}
