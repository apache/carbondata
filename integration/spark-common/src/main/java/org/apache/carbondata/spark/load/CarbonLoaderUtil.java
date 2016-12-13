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
package org.apache.carbondata.spark.load;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonDataLoadSchema;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.ColumnIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.Distributable;
import org.apache.carbondata.core.carbon.metadata.CarbonMetadata;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.carbon.path.CarbonStorePath;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.apache.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.apache.carbondata.core.load.LoadMetadataDetails;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;
import org.apache.carbondata.lcm.fileoperations.AtomicFileOperations;
import org.apache.carbondata.lcm.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.lcm.fileoperations.FileWriteOperation;
import org.apache.carbondata.lcm.locks.ICarbonLock;
import org.apache.carbondata.lcm.status.SegmentStatusManager;
import org.apache.carbondata.processing.api.dataloader.DataLoadModel;
import org.apache.carbondata.processing.api.dataloader.SchemaInfo;
import org.apache.carbondata.processing.csvload.DataGraphExecuter;
import org.apache.carbondata.processing.dataprocessor.DataProcessTaskStatus;
import org.apache.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.apache.carbondata.processing.graphgenerator.GraphGenerator;
import org.apache.carbondata.processing.graphgenerator.GraphGeneratorException;
import org.apache.carbondata.processing.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonDataProcessorUtil;
import org.apache.carbondata.spark.merger.NodeBlockRelation;
import org.apache.carbondata.spark.merger.NodeMultiBlockRelation;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.util.Utils;


public final class CarbonLoaderUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLoaderUtil.class.getName());
  /**
   * minimum no of blocklet required for distribution
   */
  private static int minBlockLetsReqForDistribution = 0;

  static {
    String property = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_BLOCKLETDISTRIBUTION_MIN_REQUIRED_SIZE);
    try {
      minBlockLetsReqForDistribution = Integer.parseInt(property);
    } catch (NumberFormatException ne) {
      LOGGER.info("Invalid configuration. Consisering the defaul");
      minBlockLetsReqForDistribution =
          CarbonCommonConstants.DEFAULT_CARBON_BLOCKLETDISTRIBUTION_MIN_REQUIRED_SIZE;
    }
  }

  private CarbonLoaderUtil() {

  }

  private static void generateGraph(IDataProcessStatus dataProcessTaskStatus, SchemaInfo info,
      CarbonLoadModel loadModel, String outputLocation)
      throws GraphGeneratorException {
    DataLoadModel model = new DataLoadModel();
    model.setCsvLoad(null != dataProcessTaskStatus.getCsvFilePath()
            || null != dataProcessTaskStatus.getFilesToProcess());
    model.setSchemaInfo(info);
    model.setTableName(dataProcessTaskStatus.getTableName());
    List<LoadMetadataDetails> loadMetadataDetails = loadModel.getLoadMetadataDetails();
    if (null != loadMetadataDetails && !loadMetadataDetails.isEmpty()) {
      model.setLoadNames(
          CarbonDataProcessorUtil.getLoadNameFromLoadMetaDataDetails(loadMetadataDetails));
      model.setModificationOrDeletionTime(CarbonDataProcessorUtil
          .getModificationOrDeletionTimesFromLoadMetadataDetails(loadMetadataDetails));
    }
    model.setBlocksID(dataProcessTaskStatus.getBlocksID());
    model.setEscapeCharacter(dataProcessTaskStatus.getEscapeCharacter());
    model.setQuoteCharacter(dataProcessTaskStatus.getQuoteCharacter());
    model.setCommentCharacter(dataProcessTaskStatus.getCommentCharacter());
    model.setRddIteratorKey(dataProcessTaskStatus.getRddIteratorKey());
    model.setTaskNo(loadModel.getTaskNo());
    model.setFactTimeStamp(loadModel.getFactTimeStamp());
    model.setMaxColumns(loadModel.getMaxColumns());
    model.setDateFormat(loadModel.getDateFormat());
    boolean hdfsReadMode =
        dataProcessTaskStatus.getCsvFilePath() != null
                && dataProcessTaskStatus.getCsvFilePath().startsWith("hdfs:");
    int allocate =
            null != dataProcessTaskStatus.getCsvFilePath()
                    ? 1 : dataProcessTaskStatus.getFilesToProcess().size();
    GraphGenerator generator = new GraphGenerator(model, hdfsReadMode, loadModel.getPartitionId(),
        loadModel.getStorePath(), allocate,
        loadModel.getCarbonDataLoadSchema(), loadModel.getSegmentId(), outputLocation);
    generator.generateGraph();
  }

  public static void executeGraph(CarbonLoadModel loadModel, String storeLocation,
      String storePath, String kettleHomePath) throws Exception {
    System.setProperty("KETTLE_HOME", kettleHomePath);
    if (!new File(storeLocation).mkdirs()) {
      LOGGER.error("Error while creating the temp store path: " + storeLocation);
    }
    String outPutLoc = storeLocation + "/etl";
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = databaseName + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + loadModel.getTaskNo();
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, storePath);
    // CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
    CarbonProperties.getInstance().addProperty("send.signal.load", "false");

    String fileNamePrefix = "";
    if (loadModel.isAggLoadRequest()) {
      fileNamePrefix = "graphgenerator";
    }
    String graphPath =
        outPutLoc + File.separator + databaseName + File.separator + tableName + File.separator
            + loadModel.getSegmentId() + File.separator + loadModel.getTaskNo() + File.separator
            + tableName + fileNamePrefix + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      path.delete();
    }

    DataProcessTaskStatus dataProcessTaskStatus
            = new DataProcessTaskStatus(databaseName, tableName);
    dataProcessTaskStatus.setCsvFilePath(loadModel.getFactFilePath());
    dataProcessTaskStatus.setDimCSVDirLoc(loadModel.getDimFolderPath());
    if (loadModel.isDirectLoad()) {
      dataProcessTaskStatus.setFilesToProcess(loadModel.getFactFilesToProcess());
      dataProcessTaskStatus.setDirectLoad(true);
      dataProcessTaskStatus.setCsvDelimiter(loadModel.getCsvDelimiter());
      dataProcessTaskStatus.setCsvHeader(loadModel.getCsvHeader());
    }

    dataProcessTaskStatus.setBlocksID(loadModel.getBlocksID());
    dataProcessTaskStatus.setEscapeCharacter(loadModel.getEscapeChar());
    dataProcessTaskStatus.setQuoteCharacter(loadModel.getQuoteChar());
    dataProcessTaskStatus.setCommentCharacter(loadModel.getCommentChar());
    dataProcessTaskStatus.setRddIteratorKey(loadModel.getRddIteratorKey());
    dataProcessTaskStatus.setDateFormat(loadModel.getDateFormat());
    SchemaInfo info = new SchemaInfo();

    info.setDatabaseName(databaseName);
    info.setTableName(tableName);
    info.setAutoAggregateRequest(loadModel.isAggLoadRequest());
    info.setComplexDelimiterLevel1(loadModel.getComplexDelimiterLevel1());
    info.setComplexDelimiterLevel2(loadModel.getComplexDelimiterLevel2());
    info.setSerializationNullFormat(loadModel.getSerializationNullFormat());
    info.setBadRecordsLoggerEnable(loadModel.getBadRecordsLoggerEnable());
    info.setBadRecordsLoggerAction(loadModel.getBadRecordsAction());

    generateGraph(dataProcessTaskStatus, info, loadModel, outPutLoc);

    DataGraphExecuter graphExecuter = new DataGraphExecuter(dataProcessTaskStatus);
    graphExecuter
        .executeGraph(graphPath, new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN),
            info, loadModel.getPartitionId(), loadModel.getCarbonDataLoadSchema());
  }

  public static List<String> addNewSliceNameToList(String newSlice, List<String> activeSlices) {
    activeSlices.add(newSlice);
    return activeSlices;
  }

  public static void deleteSegment(CarbonLoadModel loadModel, int currentLoad) {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(loadModel.getStorePath(), carbonTable.getCarbonTableIdentifier());

    for (int i = 0; i < carbonTable.getPartitionCount(); i++) {
      String segmentPath = carbonTablePath.getCarbonDataDirectoryPath(i + "", currentLoad + "");
      deleteStorePath(segmentPath);
    }
  }

  public static void deletePartialLoadDataIfExist(CarbonLoadModel loadModel,
      final boolean isCompactionFlow) throws IOException {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    String metaDataLocation = carbonTable.getMetaDataFilepath();
    final LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(metaDataLocation);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(loadModel.getStorePath(), carbonTable.getCarbonTableIdentifier());

    //delete folder which metadata no exist in tablestatus
    for (int i = 0; i < carbonTable.getPartitionCount(); i++) {
      final String partitionCount = i + "";
      String partitionPath = carbonTablePath.getPartitionDir(partitionCount);
      FileType fileType = FileFactory.getFileType(partitionPath);
      if (FileFactory.isFileExist(partitionPath, fileType)) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(partitionPath, fileType);
        CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile path) {
            String segmentId =
                CarbonTablePath.DataPathUtil.getSegmentId(path.getAbsolutePath() + "/dummy");
            boolean found = false;
            for (int j = 0; j < details.length; j++) {
              if (details[j].getLoadName().equals(segmentId) && details[j].getPartitionCount()
                  .equals(partitionCount)) {
                found = true;
                break;
              }
            }
            return !found;
          }
        });
        for (int k = 0; k < listFiles.length; k++) {
          String segmentId =
              CarbonTablePath.DataPathUtil.getSegmentId(listFiles[k].getAbsolutePath() + "/dummy");
          if (isCompactionFlow) {
            if (segmentId.contains(".")) {
              deleteStorePath(listFiles[k].getAbsolutePath());
            }
          } else {
            if (!segmentId.contains(".")) {
              deleteStorePath(listFiles[k].getAbsolutePath());
            }
          }
        }
      }
    }
  }

  public static void deleteStorePath(String path) {
    try {
      FileType fileType = FileFactory.getFileType(path);
      if (FileFactory.isFileExist(path, fileType)) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(path, fileType);
        CarbonUtil.deleteFoldersAndFiles(carbonFile);
      }
    } catch (IOException e) {
      LOGGER.error("Unable to delete the given path :: " + e.getMessage());
    } catch (CarbonUtilException e) {
      LOGGER.error("Unable to delete the given path :: " + e.getMessage());
    }
  }

  public static List<String> getListOfValidSlices(LoadMetadataDetails[] details) {
    List<String> activeSlices =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails oneLoad : details) {
      if (CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS.equals(oneLoad.getLoadStatus())
          || CarbonCommonConstants.STORE_LOADSTATUS_PARTIAL_SUCCESS.equals(oneLoad.getLoadStatus())
          || CarbonCommonConstants.MARKED_FOR_UPDATE.equals(oneLoad.getLoadStatus())) {
        if (null != oneLoad.getMergedLoadName()) {
          String loadName = CarbonCommonConstants.LOAD_FOLDER + oneLoad.getMergedLoadName();
          activeSlices.add(loadName);
        } else {
          String loadName = CarbonCommonConstants.LOAD_FOLDER + oneLoad.getLoadName();
          activeSlices.add(loadName);
        }
      }
    }
    return activeSlices;
  }

  public static List<String> getListOfUpdatedSlices(LoadMetadataDetails[] details) {
    List<String> updatedSlices =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails oneLoad : details) {
      if (CarbonCommonConstants.MARKED_FOR_UPDATE.equals(oneLoad.getLoadStatus())) {
        if (null != oneLoad.getMergedLoadName()) {
          updatedSlices.add(oneLoad.getMergedLoadName());
        } else {
          updatedSlices.add(oneLoad.getLoadName());
        }
      }
    }
    return updatedSlices;
  }

  public static void removeSliceFromMemory(String databaseName, String tableName, String loadName) {
    // TODO: Remove from memory
  }

  /**
   * This method will delete the local data load folder location after data load is complete
   *
   * @param loadModel
   */
  public static void deleteLocalDataLoadFolderLocation(CarbonLoadModel loadModel,
      boolean isCompactionFlow) {
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = databaseName + CarbonCommonConstants.UNDERSCORE + tableName
        + CarbonCommonConstants.UNDERSCORE + loadModel.getTaskNo();
    if (isCompactionFlow) {
      tempLocationKey = CarbonCommonConstants.COMPACTION_KEY_WORD + '_' + tempLocationKey;
    }
    // form local store location
    String localStoreLocation = CarbonProperties.getInstance()
        .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
    try {
      CarbonUtil.deleteFoldersAndFiles(new File[] { new File(localStoreLocation).getParentFile() });
      LOGGER.info("Deleted the local store location" + localStoreLocation);
    } catch (CarbonUtilException e) {
      LOGGER.error(e, "Failed to delete local data load folder location");
    }

  }

  /**
   * This method will get the store location for the given path, segemnt id and partition id
   *
   * @param storePath
   * @param carbonTableIdentifier
   * @param segmentId
   * @param partitionId
   * @return
   */
  public static String getStoreLocation(String storePath,
      CarbonTableIdentifier carbonTableIdentifier, String segmentId, String partitionId) {
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(storePath, carbonTableIdentifier);
    String carbonDataFilePath = carbonTablePath.getCarbonDataDirectoryPath(partitionId, segmentId);
    return carbonDataFilePath;
  }

  /**
   * This API will write the load level metadata for the loadmanagement module inorder to
   * manage the load and query execution management smoothly.
   *
   * @param loadCount
   * @param loadMetadataDetails
   * @param loadModel
   * @param loadStatus
   * @param startLoadTime
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean recordLoadMetadata(int loadCount, LoadMetadataDetails loadMetadataDetails,
      CarbonLoadModel loadModel, String loadStatus, String startLoadTime) throws IOException {

    boolean status = false;

    String metaDataFilepath =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getMetaDataFilepath();

    AbsoluteTableIdentifier absoluteTableIdentifier =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();

    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(absoluteTableIdentifier.getStorePath(),
            absoluteTableIdentifier.getCarbonTableIdentifier());

    String tableStatusPath = carbonTablePath.getTableStatusFilePath();
    ICarbonLock carbonLock = SegmentStatusManager.getTableStatusLock(absoluteTableIdentifier);

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + loadModel.getDatabaseName() + "." + loadModel.getTableName()
                + " for table status updation");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        String loadEnddate = readCurrentTime();
        loadMetadataDetails.setTimestamp(loadEnddate);
        loadMetadataDetails.setLoadStatus(loadStatus);
        loadMetadataDetails.setLoadName(String.valueOf(loadCount));
        loadMetadataDetails.setLoadStartTime(startLoadTime);

        List<LoadMetadataDetails> listOfLoadFolderDetails =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        if (null != listOfLoadFolderDetailsArray) {
          for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
            listOfLoadFolderDetails.add(loadMetadata);
          }
        }
        listOfLoadFolderDetails.add(loadMetadataDetails);

        SegmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetails
            .toArray(new LoadMetadataDetails[listOfLoadFolderDetails.size()]));

        status = true;
      } else {
        LOGGER.error("Not able to acquire the lock for Table status updation for table " + loadModel
            .getDatabaseName() + "." + loadModel.getTableName());
      }
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

  public static void writeLoadMetadata(CarbonDataLoadSchema schema, String databaseName,
      String tableName, List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(schema.getCarbonTable().getStorePath(),
            schema.getCarbonTable().getCarbonTableIdentifier());
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

  public static String extractLoadMetadataFileLocation(CarbonLoadModel loadModel) {
    CarbonTable carbonTable =
        org.apache.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
            .getCarbonTable(loadModel.getDatabaseName() + '_' + loadModel.getTableName());
    return carbonTable.getMetaDataFilepath();
  }

  public static Dictionary getDictionary(DictionaryColumnUniqueIdentifier columnIdentifier,
      String carbonStorePath) throws CarbonUtilException {
    Cache dictCache =
        CacheProvider.getInstance().createCache(CacheType.REVERSE_DICTIONARY, carbonStorePath);
    return (Dictionary) dictCache.get(columnIdentifier);
  }

  public static Dictionary getDictionary(CarbonTableIdentifier tableIdentifier,
      ColumnIdentifier columnIdentifier, String carbonStorePath, DataType dataType)
      throws CarbonUtilException {
    return getDictionary(
        new DictionaryColumnUniqueIdentifier(tableIdentifier, columnIdentifier, dataType),
        carbonStorePath);
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
    blocksPerNode = blocksPerNode <=0 ? 1 : blocksPerNode;

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
        if(isActiveNode){
          return hostName;
        }
      } catch (UnknownHostException ue) {
        isActiveNode = false;
      }
    } else {
      try {
        String hostAddress = InetAddress.getByName(nodeName).getHostAddress();
        isActiveNode = activeNode.contains(hostAddress);
        if(isActiveNode){
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
        Collections.sort(list);
        nodeAndBlockMapping.put(node, list);
      } else {
        list = nodeAndBlockMapping.get(node);
        list.add(nbr.getBlock());
        Collections.sort(list);
      }
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
   *
   * @param carbonStorePath
   * @param dbName
   * @param tableName
   * @param segmentId
   */
  public static void checkAndCreateCarbonDataLocation(String carbonStorePath, String dbName,
      String tableName, String segmentId) {
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(dbName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTableIdentifier);
    String carbonDataDirectoryPath =
        carbonTablePath.getCarbonDataDirectoryPath("0", segmentId);
    CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
  }

  /**
   * return the Array of available local-dirs
   *
   * @param conf
   * @return
   */
  public static String[] getConfiguredLocalDirs(SparkConf conf) {
    return Utils.getConfiguredLocalDirs(conf);
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

}
