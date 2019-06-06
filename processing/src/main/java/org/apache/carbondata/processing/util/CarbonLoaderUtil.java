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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonLoadOptionConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.block.Distributable;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory.FileType;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.writer.CarbonIndexFileMergeWriter;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.merger.NodeMultiBlockRelation;

import static org.apache.carbondata.core.enums.EscapeSequences.BACKSPACE;
import static org.apache.carbondata.core.enums.EscapeSequences.CARRIAGE_RETURN;
import static org.apache.carbondata.core.enums.EscapeSequences.NEW_LINE;
import static org.apache.carbondata.core.enums.EscapeSequences.TAB;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

public final class CarbonLoaderUtil {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonLoaderUtil.class.getName());

  private CarbonLoaderUtil() {
  }

  /**
   * strategy for assign blocks to nodes/executors
   */
  public enum BlockAssignmentStrategy {
    BLOCK_NUM_FIRST("Assign blocks to node base on number of blocks"),
    BLOCK_SIZE_FIRST("Assign blocks to node base on data size of blocks"),
    NODE_MIN_SIZE_FIRST("Assign blocks to node base on minimum size of inputs");
    private String name;
    BlockAssignmentStrategy(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + ':' + this.name;
    }
  }

  public static void deleteSegment(CarbonLoadModel loadModel, int currentLoad) {
    String segmentPath = CarbonTablePath.getSegmentPath(
        loadModel.getTablePath(), currentLoad + "");
    deleteStorePath(segmentPath);
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

    int fileCount = 0;
    String segmentPath = CarbonTablePath.getSegmentPath(
        loadModel.getTablePath(), currentLoad + "");
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
      LOGGER.error("Unable to delete the given path :: " + e.getMessage(), e);
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
   * This API deletes the content of the non Transactional Tables when insert overwrite is set true.
   *
   * @param loadModel
   * @throws IOException
   */
  public static void deleteNonTransactionalTableForInsertOverwrite(final CarbonLoadModel loadModel)
      throws IOException {
    // We need to delete the content of the Table Path Folder except the
    // Newly added file.
    List<String> filesToBeDeleted = new ArrayList<>();
    CarbonFile carbonFile = FileFactory.getCarbonFile(loadModel.getTablePath());
    CarbonFile[] filteredList = carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return !file.getName().contains(loadModel.getFactTimeStamp() + "");
      }
    });
    for (CarbonFile file : filteredList) {
      filesToBeDeleted.add(file.getAbsolutePath());
    }

    deleteFiles(filesToBeDeleted);
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
      final CarbonLoadModel loadModel, boolean loadStartEntry, boolean insertOverwrite, String uuid)
      throws IOException {
    // For Non Transactional tables no need to update the the Table Status file.
    if (!loadModel.isCarbonTransactionalTable()) {
      return true;
    }

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
    AbsoluteTableIdentifier identifier =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    if (loadModel.isCarbonTransactionalTable()) {
      String metadataPath = CarbonTablePath.getMetadataPath(identifier.getTablePath());
      FileType fileType = FileFactory.getFileType(metadataPath);
      if (!FileFactory.isFileExist(metadataPath, fileType)) {
        FileFactory.mkdirs(metadataPath, fileType);
      }
    }
    String tableStatusPath;
    if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() && !uuid.isEmpty()) {
      tableStatusPath = CarbonTablePath.getTableStatusFilePathWithUUID(
          identifier.getTablePath(), uuid);
    } else {
      tableStatusPath = CarbonTablePath.getTableStatusFilePath(identifier.getTablePath());
    }
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
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
            SegmentStatusManager.readLoadMetadata(
                CarbonTablePath.getMetadataPath(identifier.getTablePath()));
        List<LoadMetadataDetails> listOfLoadFolderDetails =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        List<CarbonFile> staleFolders = new ArrayList<>();
        Collections.addAll(listOfLoadFolderDetails, listOfLoadFolderDetailsArray);
        // create a new segment Id if load has just begun else add the already generated Id
        if (loadStartEntry) {
          String segmentId =
              String.valueOf(SegmentStatusManager.createNewSegmentId(listOfLoadFolderDetailsArray));
          loadModel.setLoadMetadataDetails(listOfLoadFolderDetails);
          LoadMetadataDetails entryTobeRemoved = null;
          // Segment id would be provided in case this is compaction flow for aggregate data map.
          // If that is true then used the segment id as the load name.
          if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() && !loadModel
              .getSegmentId().isEmpty()) {
            newMetaEntry.setLoadName(loadModel.getSegmentId());
          } else if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildTable()
              && !loadModel.getSegmentId().isEmpty()) {
            for (LoadMetadataDetails entry : listOfLoadFolderDetails) {
              if (entry.getLoadName().equalsIgnoreCase(loadModel.getSegmentId())) {
                newMetaEntry.setLoadName(loadModel.getSegmentId());
                newMetaEntry.setExtraInfo(entry.getExtraInfo());
                entryTobeRemoved = entry;
              }
            }
          } else {
            newMetaEntry.setLoadName(segmentId);
            loadModel.setSegmentId(segmentId);
          }
          listOfLoadFolderDetails.remove(entryTobeRemoved);
          // Exception should be thrown if:
          // 1. If insert overwrite is in progress and any other load or insert operation
          // is triggered
          // 2. If load or insert into operation is in progress and insert overwrite operation
          // is triggered
          for (LoadMetadataDetails entry : listOfLoadFolderDetails) {
            if (entry.getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS
                && SegmentStatusManager.isLoadInProgress(
                    identifier, entry.getLoadName())) {
              throw new RuntimeException("Already insert overwrite is in progress");
            } else if (newMetaEntry.getSegmentStatus() == SegmentStatus.INSERT_OVERWRITE_IN_PROGRESS
                && entry.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS
                && SegmentStatusManager.isLoadInProgress(
                    identifier, entry.getLoadName())) {
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
              newMetaEntry.setExtraInfo(entry.getExtraInfo());
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
                addToStaleFolders(identifier, staleFolders, entry);
              }
            }
          }
          if (!found) {
            LOGGER.error("Entry not found to update " + newMetaEntry + " From list :: "
                + listOfLoadFolderDetails);
            throw new IOException("Entry not found to update in the table status file");
          }
          listOfLoadFolderDetails.set(indexToOverwriteNewMetaEntry, newMetaEntry);
        }
        // when no records are inserted then newSegmentEntry will be SegmentStatus.MARKED_FOR_DELETE
        // so empty segment folder should be deleted
        if (newMetaEntry.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE) {
          addToStaleFolders(identifier, staleFolders, newMetaEntry);
        }

        for (LoadMetadataDetails detail: listOfLoadFolderDetails) {
          // if the segments is in the list of marked for delete then update the status.
          if (segmentsToBeDeleted.contains(new Segment(detail.getLoadName()))) {
            detail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          } else if (segmentFilesTobeUpdated
              .contains(Segment.toSegment(detail.getLoadName(), null))) {
            detail.setSegmentFile(
                detail.getLoadName() + "_" + newMetaEntry.getUpdateStatusFileName()
                    + CarbonTablePath.SEGMENT_EXT);
          }
        }

        if (loadModel.getCarbonDataLoadSchema().getCarbonTable().isChildDataMap() && !loadStartEntry
            && !uuid.isEmpty() && segmentsToBeDeleted.isEmpty() && !insertOverwrite) {
          SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath,
              new LoadMetadataDetails[] { newMetaEntry });
        } else {
          SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetails
              .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));
        }
        // Delete all old stale segment folders
        for (CarbonFile staleFolder : staleFolders) {
          // try block is inside for loop because even if there is failure in deletion of 1 stale
          // folder still remaining stale folders should be deleted
          try {
            CarbonUtil.deleteFoldersAndFiles(staleFolder);
          } catch (IOException | InterruptedException e) {
            LOGGER.error("Failed to delete stale folder: " + e.getMessage(), e);
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

  private static void addToStaleFolders(AbsoluteTableIdentifier identifier,
      List<CarbonFile> staleFolders, LoadMetadataDetails entry) throws IOException {
    String path = CarbonTablePath.getSegmentPath(
        identifier.getTablePath(), entry.getLoadName());
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

  public static boolean isValidEscapeSequence(String escapeChar) {
    return escapeChar.equalsIgnoreCase(NEW_LINE.getName()) ||
        escapeChar.equalsIgnoreCase(CARRIAGE_RETURN.getName()) ||
        escapeChar.equalsIgnoreCase(TAB.getName()) ||
        escapeChar.equalsIgnoreCase(BACKSPACE.getName());
  }

  public static boolean isValidBinaryDecoder(String binaryDecoderChar) {
    return CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_BASE64.equalsIgnoreCase(
        binaryDecoderChar) ||
        CarbonLoadOptionConstants.CARBON_OPTIONS_BINARY_DECODER_HEX.equalsIgnoreCase(
            binaryDecoderChar) ||
        StringUtils.isBlank(binaryDecoderChar);
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
        CarbonLoaderUtil.nodeBlockMapping(blockInfos, noOfNodesInput, activeNode,
            BlockAssignmentStrategy.BLOCK_NUM_FIRST, null);
    int taskPerNode = parallelism / mapOfNodes.size();
    //assigning non zero value to noOfTasksPerNode
    int noOfTasksPerNode = taskPerNode == 0 ? 1 : taskPerNode;
    // divide the blocks of a node among the tasks of the node.
    return assignBlocksToTasksPerNode(mapOfNodes, noOfTasksPerNode);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   * @param activeNodes List of all the active nodes running in cluster, based on these and the
   *                    actual nodes, where blocks are present, the mapping is done
   * @param blockInfos
   * @return
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(List<Distributable> blockInfos,
      int noOfNodesInput, List<String> activeNodes) {
    return nodeBlockMapping(blockInfos, noOfNodesInput, activeNodes,
        BlockAssignmentStrategy.BLOCK_NUM_FIRST,null);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @return
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(List<Distributable> blockInfos) {
    // -1 if number of nodes has to be decided based on block location information
    return nodeBlockMapping(blockInfos, -1, null,
        BlockAssignmentStrategy.BLOCK_NUM_FIRST,null);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos blocks
   * @param numOfNodesInput -1 if number of nodes has to be decided
   *                       based on block location information
   * @param blockAssignmentStrategy strategy used to assign blocks
   * @param expectedMinSizePerNode the property load_min_size_inmb specified by the user
   * @return a map that maps node to blocks
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(
      List<Distributable> blockInfos, int numOfNodesInput, List<String> activeNodes,
      BlockAssignmentStrategy blockAssignmentStrategy, String expectedMinSizePerNode) {
    ArrayList<NodeMultiBlockRelation> rtnNode2Blocks = new ArrayList<>();

    Set<Distributable> uniqueBlocks = new HashSet<>(blockInfos);
    ArrayList<NodeMultiBlockRelation> originNode2Blocks = createNode2BlocksMapping(blockInfos);
    Set<String> nodes = new HashSet<>(originNode2Blocks.size());
    for (NodeMultiBlockRelation relation : originNode2Blocks) {
      nodes.add(relation.getNode());
    }

    int numOfNodes = (-1 == numOfNodesInput) ? nodes.size() : numOfNodesInput;
    if (null != activeNodes) {
      numOfNodes = activeNodes.size();
    }

    // calculate the average expected size for each node
    long sizePerNode = 0;
    long totalFileSize = 0;
    if (BlockAssignmentStrategy.BLOCK_NUM_FIRST == blockAssignmentStrategy) {
      if (blockInfos.size() > 0) {
        sizePerNode = blockInfos.size() / numOfNodes;
      }
      sizePerNode = sizePerNode <= 0 ? 1 : sizePerNode;
    } else if (BlockAssignmentStrategy.BLOCK_SIZE_FIRST == blockAssignmentStrategy
        || BlockAssignmentStrategy.NODE_MIN_SIZE_FIRST == blockAssignmentStrategy) {
      for (Distributable blockInfo : uniqueBlocks) {
        totalFileSize += ((TableBlockInfo) blockInfo).getBlockLength();
      }
      sizePerNode = totalFileSize / numOfNodes;
    }

    // if enable to control the minimum amount of input data for each node
    if (BlockAssignmentStrategy.NODE_MIN_SIZE_FIRST == blockAssignmentStrategy) {
      long expectedMinSizePerNodeInt = 0;
      // validate the property load_min_size_inmb specified by the user
      if (CarbonUtil.validateValidIntType(expectedMinSizePerNode)) {
        expectedMinSizePerNodeInt = Integer.parseInt(expectedMinSizePerNode);
      } else {
        LOGGER.warn("Invalid load_min_size_inmb value found: " + expectedMinSizePerNode
            + ", only int value greater than 0 is supported.");
        expectedMinSizePerNodeInt = Integer.parseInt(
            CarbonCommonConstants.CARBON_LOAD_MIN_SIZE_INMB_DEFAULT);
      }
      // If the average expected size for each node greater than load min size,
      // then fall back to default strategy
      if (expectedMinSizePerNodeInt * 1024 * 1024 < sizePerNode) {
        if (CarbonProperties.getInstance().isLoadSkewedDataOptimizationEnabled()) {
          blockAssignmentStrategy = BlockAssignmentStrategy.BLOCK_SIZE_FIRST;
        } else {
          blockAssignmentStrategy = BlockAssignmentStrategy.BLOCK_NUM_FIRST;
          // fall back to BLOCK_NUM_FIRST strategy need to reset
          // the average expected size for each node
          if (numOfNodes == 0) {
            sizePerNode = 1;
          } else {
            sizePerNode = blockInfos.size() / numOfNodes;
            sizePerNode = sizePerNode <= 0 ? 1 : sizePerNode;
          }
        }
        LOGGER.info("Specified minimum data size to load is less than the average size "
            + "for each node, fallback to default strategy" + blockAssignmentStrategy);
      } else {
        sizePerNode = expectedMinSizePerNodeInt;
      }
    }

    if (BlockAssignmentStrategy.NODE_MIN_SIZE_FIRST == blockAssignmentStrategy) {
      // assign blocks to each node ignore data locality
      assignBlocksIgnoreDataLocality(rtnNode2Blocks, sizePerNode, uniqueBlocks, activeNodes);
    } else {
      // assign blocks to each node
      assignBlocksByDataLocality(rtnNode2Blocks, sizePerNode, uniqueBlocks, originNode2Blocks,
          activeNodes, blockAssignmentStrategy);
    }

    // if any blocks remain then assign them to nodes in round robin.
    assignLeftOverBlocks(rtnNode2Blocks, uniqueBlocks, sizePerNode, activeNodes,
        blockAssignmentStrategy);

    // convert
    Map<String, List<Distributable>> rtnNodeBlocksMap =
        new HashMap<String, List<Distributable>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (NodeMultiBlockRelation relation : rtnNode2Blocks) {
      rtnNodeBlocksMap.put(relation.getNode(), relation.getBlocks());
    }
    return rtnNodeBlocksMap;
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
   * If any left over data blocks are present then assign those to nodes in round robin way. This
   * will not obey the data locality.
   */
  private static void assignLeftOverBlocks(ArrayList<NodeMultiBlockRelation> outputMap,
      Set<Distributable> leftOverBlocks, long expectedSizePerNode, List<String> activeNodes,
      BlockAssignmentStrategy blockAssignmentStrategy) {
    Map<String, Integer> node2Idx = new HashMap<>(outputMap.size());
    for (int idx = 0; idx < outputMap.size(); idx++) {
      node2Idx.put(outputMap.get(idx).getNode(), idx);
    }

    // iterate all the nodes and try to allocate blocks to the nodes
    if (activeNodes != null) {
      for (String activeNode : activeNodes) {
        if (LOGGER.isDebugEnabled()) {
          LOGGER.debug("Second assignment iteration: assign for executor: " + activeNode);
        }

        Integer idx;
        List<Distributable> blockLst;
        if (node2Idx.containsKey(activeNode)) {
          idx = node2Idx.get(activeNode);
          blockLst = outputMap.get(idx).getBlocks();
        } else {
          idx = node2Idx.size();
          blockLst = new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        }
        populateBlocks(leftOverBlocks, expectedSizePerNode, blockLst, blockAssignmentStrategy);

        if (!node2Idx.containsKey(activeNode) && blockLst.size() > 0) {
          outputMap.add(idx, new NodeMultiBlockRelation(activeNode, blockLst));
          node2Idx.put(activeNode, idx);
        }
      }
    } else {
      for (NodeMultiBlockRelation entry : outputMap) {
        List<Distributable> blockLst = entry.getBlocks();
        populateBlocks(leftOverBlocks, expectedSizePerNode, blockLst, blockAssignmentStrategy);
      }
    }

    // if there is still blocks left, allocate them in round robin manner to each nodes
    assignBlocksUseRoundRobin(outputMap, leftOverBlocks, blockAssignmentStrategy);
  }

  /**
   * assign remaining blocks to nodes
   *
   * @param remainingBlocks blocks to be allocated
   * @param expectedSizePerNode expected size for each node
   * @param blockLst destination for the blocks to be allocated
   * @param blockAssignmentStrategy block assignment stretegy
   */
  private static void populateBlocks(Set<Distributable> remainingBlocks,
      long expectedSizePerNode, List<Distributable> blockLst,
      BlockAssignmentStrategy blockAssignmentStrategy) {
    switch (blockAssignmentStrategy) {
      case BLOCK_NUM_FIRST:
        populateBlocksByNum(remainingBlocks, expectedSizePerNode, blockLst);
        break;
      case BLOCK_SIZE_FIRST:
      case NODE_MIN_SIZE_FIRST:
        populateBlocksBySize(remainingBlocks, expectedSizePerNode, blockLst);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported block assignment strategy: " + blockAssignmentStrategy);
    }
  }

  /**
   * Taken N number of distributable blocks from {@param remainingBlocks} and add them to output
   * {@param blockLst}. After added, the total number of {@param blockLst} is less
   * than {@param expectedSizePerNode}.
   */
  private static void populateBlocksByNum(Set<Distributable> remainingBlocks,
      long expectedSizePerNode, List<Distributable> blockLst) {
    Iterator<Distributable> blocks = remainingBlocks.iterator();
    // if the node is already having the per block nodes then avoid assign the extra blocks
    if (blockLst.size() == expectedSizePerNode) {
      return;
    }
    while (blocks.hasNext()) {
      Distributable block = blocks.next();
      blockLst.add(block);
      blocks.remove();
      if (blockLst.size() >= expectedSizePerNode) {
        break;
      }
    }
  }

  /**
   * Taken N number of distributable blocks from {@param remainingBlocks} and add them to output
   * {@param blockLst}. After added, the total accumulated block size of {@param blockLst}
   * is less than {@param expectedSizePerNode}.
   */
  private static void populateBlocksBySize(Set<Distributable> remainingBlocks,
      long expectedSizePerNode, List<Distributable> blockLst) {
    Iterator<Distributable> blocks = remainingBlocks.iterator();
    //if the node is already having the avg node size then avoid assign the extra blocks
    long fileSize = 0;
    for (Distributable block : blockLst) {
      fileSize += ((TableBlockInfo) block).getBlockLength();
    }
    if (fileSize >= expectedSizePerNode) {
      LOGGER.debug("Capacity is full, skip allocate blocks on this node");
      return;
    }

    while (blocks.hasNext()) {
      Distributable block = blocks.next();
      long thisBlockSize = ((TableBlockInfo) block).getBlockLength();
      if (fileSize < expectedSizePerNode) {
        // `fileSize==0` means there are no blocks assigned to this node before
        if (fileSize == 0 || fileSize + thisBlockSize <= expectedSizePerNode * 1.1D) {
          blockLst.add(block);
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Second Assignment iteration: "
                + ((TableBlockInfo) block).getFilePath() + "-"
                + ((TableBlockInfo) block).getBlockLength() + "-->currentNode");
          }
          fileSize += thisBlockSize;
          blocks.remove();
        }
      } else {
        break;
      }
    }
  }

  /**
   * allocate the blocks in round robin manner
   */
  private static void assignBlocksUseRoundRobin(ArrayList<NodeMultiBlockRelation> node2Blocks,
      Set<Distributable> remainingBlocks, BlockAssignmentStrategy blockAssignmentStrategy) {
    switch (blockAssignmentStrategy) {
      case BLOCK_NUM_FIRST:
        roundRobinAssignBlocksByNum(node2Blocks, remainingBlocks);
        break;
      case BLOCK_SIZE_FIRST:
      case NODE_MIN_SIZE_FIRST:
        roundRobinAssignBlocksBySize(node2Blocks, remainingBlocks);
        break;
      default:
        throw new IllegalArgumentException("Unsupported block assignment strategy: "
            + blockAssignmentStrategy);
    }
  }

  private static void roundRobinAssignBlocksByNum(ArrayList<NodeMultiBlockRelation> outputMap,
      Set<Distributable> remainingBlocks) {
    for (NodeMultiBlockRelation relation: outputMap) {
      Iterator<Distributable> blocks = remainingBlocks.iterator();
      if (blocks.hasNext()) {
        Distributable block = blocks.next();
        List<Distributable> blockLst = relation.getBlocks();
        blockLst.add(block);
        blocks.remove();
      }
    }
  }

  private static void roundRobinAssignBlocksBySize(ArrayList<NodeMultiBlockRelation> outputMap,
      Set<Distributable> remainingBlocks) {
    Iterator<Distributable> blocks = remainingBlocks.iterator();
    while (blocks.hasNext()) {
      // sort the allocated node-2-blocks in ascending order, the total data size of first one is
      // the smallest, so we assign this block to it.
      Collections.sort(outputMap, NodeMultiBlockRelation.DATA_SIZE_ASC_COMPARATOR);
      Distributable block = blocks.next();
      List<Distributable> blockLst = outputMap.get(0).getBlocks();
      blockLst.add(block);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("RoundRobin assignment iteration: "
            + ((TableBlockInfo) block).getFilePath() + "-"
            + ((TableBlockInfo) block).getBlockLength() + "-->" + outputMap.get(0).getNode());
      }
      blocks.remove();
    }
  }
  /**
   * allocate distributable blocks to nodes based on data locality
   */
  private static void assignBlocksByDataLocality(
      ArrayList<NodeMultiBlockRelation> outputNode2Blocks,
      long expectedSizePerNode, Set<Distributable> remainingBlocks,
      List<NodeMultiBlockRelation> inputNode2Blocks, List<String> activeNodes,
      BlockAssignmentStrategy blockAssignmentStrategy) {
    if (BlockAssignmentStrategy.BLOCK_SIZE_FIRST == blockAssignmentStrategy) {
      // sort nodes based on data size of all blocks per node, so that nodes having bigger size
      // are assigned first
      Collections.sort(inputNode2Blocks, NodeMultiBlockRelation.DATA_SIZE_DESC_COMPARATOR);
    } else {
      // sort nodes based on number of blocks per node, so that nodes having lesser blocks
      // are assigned first
      Collections.sort(inputNode2Blocks);
    }

    Map<String, Integer> executor2Idx = new HashMap<>();
    for (NodeMultiBlockRelation nodeMultiBlockRelation : inputNode2Blocks) {
      String nodeName = nodeMultiBlockRelation.getNode();
      // assign the block to the node only if the node is active
      String activeExecutor = nodeName;
      if (null != activeNodes) {
        activeExecutor = getActiveExecutor(activeNodes, nodeName);
        if (null == activeExecutor) {
          continue;
        }
      }
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("First Assignment iteration: assign for executor: " + activeExecutor);
      }

      List<Distributable> blocksInThisNode = nodeMultiBlockRelation.getBlocks();
      if (BlockAssignmentStrategy.BLOCK_SIZE_FIRST == blockAssignmentStrategy) {
        // sort blocks based on block size, so that bigger blocks will be assigned first
        Collections.sort(blocksInThisNode, TableBlockInfo.DATA_SIZE_DESC_COMPARATOR);
      }

      long nodeCapacity = 0;
      // loop thru blocks of each Node
      for (Distributable block : nodeMultiBlockRelation.getBlocks()) {
        if (!remainingBlocks.contains(block)) {
          // this block has been added before
          continue;
        }
        // this is the first time to add block to this node, initialize it
        if (!executor2Idx.containsKey(activeExecutor)) {
          Integer idx = executor2Idx.size();
          outputNode2Blocks.add(idx, new NodeMultiBlockRelation(activeExecutor,
              new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE)));
          executor2Idx.put(activeExecutor, idx);
        }

        // assign this block to this node if node has capacity left
        if (BlockAssignmentStrategy.BLOCK_NUM_FIRST == blockAssignmentStrategy) {
          if (nodeCapacity < expectedSizePerNode) {
            Integer idx = executor2Idx.get(activeExecutor);
            List<Distributable> infos = outputNode2Blocks.get(idx).getBlocks();
            infos.add(block);
            nodeCapacity++;
            if (LOGGER.isDebugEnabled()) {
              try {
                LOGGER.debug("First Assignment iteration: block("
                    + StringUtils.join(block.getLocations(), ", ")
                    + ")-->" + activeExecutor);
              } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
              }
            }
            remainingBlocks.remove(block);
          } else {
            // No need to continue loop as node is full
            break;
          }
        } else if (BlockAssignmentStrategy.BLOCK_SIZE_FIRST == blockAssignmentStrategy) {
          long thisBlockSize = ((TableBlockInfo) block).getBlockLength();
          // `nodeCapacity == 0` means that there is a huge block that already exceed the
          // `expectedSize` of the node, so we have to assign it to some node, otherwise it will
          // be assigned in the last RoundRobin iteration.
          if (nodeCapacity == 0 || nodeCapacity < expectedSizePerNode) {
            if (nodeCapacity == 0 || nodeCapacity + thisBlockSize <= expectedSizePerNode * 1.05D) {
              Integer idx = executor2Idx.get(activeExecutor);
              List<Distributable> blocks = outputNode2Blocks.get(idx).getBlocks();
              blocks.add(block);
              nodeCapacity += thisBlockSize;
              if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                    "First Assignment iteration: " + ((TableBlockInfo) block).getFilePath() + '-'
                        + ((TableBlockInfo) block).getBlockLength() + "-->" + activeExecutor);
              }
              remainingBlocks.remove(block);
            }
            // this block is too big for current node and there are still capacity left
            // for small files, so continue to allocate block on this node in next iteration.
          } else {
            // No need to continue loop as node is full
            break;
          }
        } else {
          throw new IllegalArgumentException(
              "Unsupported block assignment strategy: " + blockAssignmentStrategy);
        }
      }
    }
  }

  /**
   * allocate distributable blocks to nodes based on ignore data locality
   */
  private static void assignBlocksIgnoreDataLocality(
          ArrayList<NodeMultiBlockRelation> outputNode2Blocks,
          long expectedSizePerNode, Set<Distributable> remainingBlocks,
          List<String> activeNodes) {
    // get all blocks
    Set<Distributable> uniqueBlocks = new HashSet<>(remainingBlocks);
    // shuffle activeNodes ignore data locality
    List<String> shuffleNodes  = new ArrayList<>(activeNodes);
    Collections.shuffle(shuffleNodes);

    for (String activeNode : shuffleNodes) {
      long nodeCapacity = 0;
      NodeMultiBlockRelation nodeBlock = new NodeMultiBlockRelation(activeNode,
          new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE));
      // loop thru blocks of each Node
      for (Distributable block : uniqueBlocks) {
        if (!remainingBlocks.contains(block)) {
          // this block has been added before
          continue;
        }

        long thisBlockSize = ((TableBlockInfo) block).getBlockLength();
        if (nodeCapacity == 0
            || nodeCapacity + thisBlockSize <= expectedSizePerNode * 1024 * 1024) {
          nodeBlock.getBlocks().add(block);
          nodeCapacity += thisBlockSize;
          if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                "First Assignment iteration: " + ((TableBlockInfo) block).getFilePath() + '-'
                    + ((TableBlockInfo) block).getBlockLength() + "-->" + activeNode);
          }
          remainingBlocks.remove(block);
          // this block is too big for current node and there are still capacity left
          // for small files, so continue to allocate block on this node in next iteration.
        } else {
          // No need to continue loop as node is full
          break;
        }
      }
      if (nodeBlock.getBlocks().size() != 0) {
        outputNode2Blocks.add(nodeBlock);
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
   * Create node to blocks mapping
   *
   * @param blockInfos input block info
   */
  private static ArrayList<NodeMultiBlockRelation> createNode2BlocksMapping(
      List<Distributable> blockInfos) {
    Map<String, Integer> node2Idx = new HashMap<>();
    ArrayList<NodeMultiBlockRelation> node2Blocks = new ArrayList<>();

    for (Distributable blockInfo : blockInfos) {
      try {
        for (final String eachNode : blockInfo.getLocations()) {
          if (node2Idx.containsKey(eachNode)) {
            Integer idx = node2Idx.get(eachNode);
            List<Distributable> blocks = node2Blocks.get(idx).getBlocks();
            blocks.add(blockInfo);
          } else {
            // add blocks to this node for the first time
            Integer idx = node2Idx.size();
            List<Distributable> blocks = new ArrayList<>();
            blocks.add(blockInfo);
            node2Blocks.add(idx, new NodeMultiBlockRelation(eachNode, blocks));
            node2Idx.put(eachNode, idx);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException("error getting location of block: " + blockInfo.toString(), e);
      }
    }

    return node2Blocks;
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   */
  public static void checkAndCreateCarbonDataLocation(String segmentId, CarbonTable carbonTable) {
    String segmentFolder = CarbonTablePath.getSegmentPath(
        carbonTable.getTablePath(), segmentId);
    CarbonUtil.checkAndCreateFolder(segmentFolder);
  }

  /*
   * This method will add data size and index size into tablestatus for each segment. And also
   * returns the size of the segment.
   */
  public static Long addDataIndexSizeIntoMetaEntry(LoadMetadataDetails loadMetadataDetails,
      String segmentId, CarbonTable carbonTable) throws IOException {
    Map<String, Long> dataIndexSize = CarbonUtil.getDataSizeAndIndexSize(
        carbonTable.getTablePath(),
        new Segment(segmentId, loadMetadataDetails.getSegmentFile()));
    Long dataSize = dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE);
    loadMetadataDetails.setDataSize(String.valueOf(dataSize));
    Long indexSize = dataIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE);
    loadMetadataDetails.setIndexSize(String.valueOf(indexSize));
    return dataSize + indexSize;
  }

  /**
   * Merge index files with in the segment of partitioned table
   *
   * @param table
   * @param segmentId
   * @param uuid
   * @return
   * @throws IOException
   */
  public static String mergeIndexFilesInPartitionedSegment(CarbonTable table, String segmentId,
      String uuid, String partitionPath) throws IOException {
    String tablePath = table.getTablePath();
    return new CarbonIndexFileMergeWriter(table)
        .mergeCarbonIndexFilesOfSegment(segmentId, uuid, tablePath, partitionPath);
  }

  private static void deleteFiles(List<String> filesToBeDeleted) throws IOException {
    for (String filePath : filesToBeDeleted) {
      FileFactory.deleteFile(filePath, FileFactory.getFileType(filePath));
    }
  }

  /**
   * Update specified segment status for load to MarkedForDelete in case of failure
   */
  public static void updateTableStatusInCaseOfFailure(String loadName,
      AbsoluteTableIdentifier absoluteTableIdentifier, String tableName, String databaseName,
      String tablePath, String metaDataPath) throws IOException {
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for table" + databaseName + "." + tableName
            + " for table status updation");
        LoadMetadataDetails[] loadMetadataDetails =
            SegmentStatusManager.readLoadMetadata(metaDataPath);
        boolean ifTableStatusUpdateRequired = false;
        for (LoadMetadataDetails loadMetadataDetail : loadMetadataDetails) {
          if (loadMetadataDetail.getSegmentStatus() == SegmentStatus.INSERT_IN_PROGRESS && loadName
              .equalsIgnoreCase(loadMetadataDetail.getLoadName())) {
            loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
            ifTableStatusUpdateRequired = true;
          }
        }
        if (ifTableStatusUpdateRequired) {
          SegmentStatusManager
              .writeLoadDetailsIntoFile(CarbonTablePath.getTableStatusFilePath(tablePath),
                  loadMetadataDetails);
        }
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table " + databaseName + "."
                + tableName);
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + databaseName + "."
            + tableName);
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + databaseName + "." + tableName
            + " during table status updation");
      }
    }
  }
}
