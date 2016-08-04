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
package org.carbondata.spark.load;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.ColumnIdentifier;
import org.carbondata.core.carbon.datastore.block.Distributable;
import org.carbondata.core.carbon.metadata.CarbonMetadata;
import org.carbondata.core.carbon.metadata.datatype.DataType;
import org.carbondata.core.carbon.metadata.schema.table.CarbonTable;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.carbondata.core.carbon.metadata.schema.table.column.CarbonMeasure;
import org.carbondata.core.carbon.path.CarbonStorePath;
import org.carbondata.core.carbon.path.CarbonTablePath;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperations;
import org.carbondata.core.datastorage.store.fileperations.AtomicFileOperationsImpl;
import org.carbondata.core.datastorage.store.fileperations.FileWriteOperation;
import org.carbondata.core.datastorage.store.filesystem.CarbonFile;
import org.carbondata.core.datastorage.store.filesystem.CarbonFileFilter;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.datastorage.store.impl.FileFactory.FileType;
import org.carbondata.core.load.LoadMetadataDetails;
import org.carbondata.core.locks.ICarbonLock;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.lcm.status.SegmentStatusManager;
import org.carbondata.processing.api.dataloader.DataLoadModel;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.csvload.DataGraphExecuter;
import org.carbondata.processing.dataprocessor.DataProcessTaskStatus;
import org.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.carbondata.processing.graphgenerator.GraphGenerator;
import org.carbondata.processing.graphgenerator.GraphGeneratorException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.spark.merger.NodeBlockRelation;
import org.carbondata.spark.merger.NodeMultiBlockRelation;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.util.Utils;

public final class CarbonLoaderUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLoaderUtil.class.getName());

  /**
   * dfs.bytes-per-checksum
   * HDFS checksum length, block size for a file should be exactly divisible
   * by this value
   */
  private static final int HDFS_CHECKSUM_LENGTH = 512;

  private CarbonLoaderUtil() {

  }

  private static void generateGraph(IDataProcessStatus schmaModel, SchemaInfo info,
      int currentRestructNumber, CarbonLoadModel loadModel, String outputLocation)
      throws GraphGeneratorException {
    DataLoadModel model = new DataLoadModel();
    model.setCsvLoad(null != schmaModel.getCsvFilePath() || null != schmaModel.getFilesToProcess());
    model.setSchemaInfo(info);
    model.setTableName(schmaModel.getTableName());
    List<LoadMetadataDetails> loadMetadataDetails = loadModel.getLoadMetadataDetails();
    if (null != loadMetadataDetails && !loadMetadataDetails.isEmpty()) {
      model.setLoadNames(
          CarbonDataProcessorUtil.getLoadNameFromLoadMetaDataDetails(loadMetadataDetails));
      model.setModificationOrDeletionTime(CarbonDataProcessorUtil
          .getModificationOrDeletionTimesFromLoadMetadataDetails(loadMetadataDetails));
    }
    model.setBlocksID(schmaModel.getBlocksID());
    model.setEscapeCharacter(schmaModel.getEscapeCharacter());
    model.setTaskNo(loadModel.getTaskNo());
    model.setFactTimeStamp(loadModel.getFactTimeStamp());
    boolean hdfsReadMode =
        schmaModel.getCsvFilePath() != null && schmaModel.getCsvFilePath().startsWith("hdfs:");
    int allocate = null != schmaModel.getCsvFilePath() ? 1 : schmaModel.getFilesToProcess().size();
    GraphGenerator generator = new GraphGenerator(model, hdfsReadMode, loadModel.getPartitionId(),
        loadModel.getStorePath(), currentRestructNumber, allocate,
        loadModel.getCarbonDataLoadSchema(), loadModel.getSegmentId(), outputLocation);
    generator.generateGraph();
  }

  public static void executeGraph(CarbonLoadModel loadModel, String storeLocation,
      String hdfsStoreLocation, String kettleHomePath, int currentRestructNumber) throws Exception {
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
        .addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, hdfsStoreLocation);
    // CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
    CarbonProperties.getInstance().addProperty("send.signal.load", "false");

    String fileNamePrefix = "";
    if (loadModel.isAggLoadRequest()) {
      fileNamePrefix = "graphgenerator";
    }
    String graphPath =
        outPutLoc + File.separator + databaseName + File.separator + tableName + File.separator
            + loadModel.getSegmentId() + File.separator + loadModel.getTaskNo() + File.separator
            + tableName +
            fileNamePrefix + ".ktr";
    File path = new File(graphPath);
    if (path.exists()) {
      path.delete();
    }

    DataProcessTaskStatus schmaModel =
        new DataProcessTaskStatus(databaseName, tableName, loadModel.getTableName());
    schmaModel.setCsvFilePath(loadModel.getFactFilePath());
    schmaModel.setDimCSVDirLoc(loadModel.getDimFolderPath());
    if (loadModel.isDirectLoad()) {
      schmaModel.setFilesToProcess(loadModel.getFactFilesToProcess());
      schmaModel.setDirectLoad(true);
      schmaModel.setCsvDelimiter(loadModel.getCsvDelimiter());
      schmaModel.setCsvHeader(loadModel.getCsvHeader());
    }

    schmaModel.setBlocksID(loadModel.getBlocksID());
    schmaModel.setEscapeCharacter(loadModel.getEscapeChar());
    SchemaInfo info = new SchemaInfo();

    info.setSchemaName(databaseName);
    info.setCubeName(tableName);
    info.setAutoAggregateRequest(loadModel.isAggLoadRequest());
    info.setComplexDelimiterLevel1(loadModel.getComplexDelimiterLevel1());
    info.setComplexDelimiterLevel2(loadModel.getComplexDelimiterLevel2());
    info.setSerializationNullFormat(loadModel.getSerializationNullFormat());

    generateGraph(schmaModel, info, currentRestructNumber, loadModel, outPutLoc);

    DataGraphExecuter graphExecuter = new DataGraphExecuter(schmaModel);
    graphExecuter
        .executeGraph(graphPath, new ArrayList<String>(CarbonCommonConstants.CONSTANT_SIZE_TEN),
            info, loadModel.getPartitionId(), loadModel.getCarbonDataLoadSchema());
  }

  public static String[] getStorelocs(String schemaName, String cubeName, String factTableName,
      String hdfsStoreLocation, int currentRestructNumber) {
    String[] loadFolders;

    String baseStorelocation =
        hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;

    String factStorepath =
        baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
            + currentRestructNumber + File.separator + factTableName;

    // Change to LOCAL for testing in local
    CarbonFile file =
        FileFactory.getCarbonFile(factStorepath, FileFactory.getFileType(factStorepath));

    if (!file.exists()) {
      return new String[0];
    }
    CarbonFile[] listFiles = file.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile path) {
        return path.getName().startsWith(CarbonCommonConstants.LOAD_FOLDER) && !path.getName()
            .endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS);
      }
    });

    loadFolders = new String[listFiles.length];
    int count = 0;

    for (CarbonFile loadFile : listFiles) {
      loadFolders[count++] = loadFile.getAbsolutePath();
    }

    return loadFolders;
  }

  public static List<String> addNewSliceNameToList(String newSlice, List<String> activeSlices) {
    activeSlices.add(newSlice);
    return activeSlices;
  }

  public static String getAggLoadFolderLocation(String loadFolderName, String schemaName,
      String cubeName, String aggTableName, String hdfsStoreLocation, int currentRestructNumber) {
    for (int i = currentRestructNumber; i >= 0; i--) {
      String aggTableLocation =
          getTableLocation(schemaName, cubeName, aggTableName, hdfsStoreLocation, i);
      String aggStorepath = aggTableLocation + File.separator + loadFolderName;
      try {
        if (FileFactory.isFileExist(aggStorepath, FileFactory.getFileType(aggStorepath))) {
          return aggStorepath;
        }
      } catch (IOException e) {
        LOGGER.error("Problem checking file existence :: " + e.getMessage());
      }
    }
    return null;
  }

  public static String getTableLocation(String schemaName, String cubeName, String aggTableName,
      String hdfsStoreLocation, int currentRestructNumber) {
    String baseStorelocation =
        hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;
    String aggTableLocation =
        baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
            + currentRestructNumber + File.separator + aggTableName;
    return aggTableLocation;
  }

  public static void deleteTable(int partitionCount, String schemaName, String cubeName,
      String aggTableName, String hdfsStoreLocation, int currentRestructNumber) {
    String aggTableLoc = null;
    String partitionSchemaName = null;
    String partitionCubeName = null;
    for (int i = 0; i < partitionCount; i++) {
      partitionSchemaName = schemaName + '_' + i;
      partitionCubeName = cubeName + '_' + i;
      for (int j = currentRestructNumber; j >= 0; j--) {
        aggTableLoc = getTableLocation(partitionSchemaName, partitionCubeName, aggTableName,
            hdfsStoreLocation, j);
        deleteStorePath(aggTableLoc);
      }
    }
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

  public static void deleteSlice(int partitionCount, String schemaName, String cubeName,
      String tableName, String hdfsStoreLocation, int currentRestructNumber, String loadFolder) {
    String tableLoc = null;
    String partitionSchemaName = null;
    String partitionCubeName = null;
    for (int i = 0; i < partitionCount; i++) {
      partitionSchemaName = schemaName + '_' + i;
      partitionCubeName = cubeName + '_' + i;
      tableLoc =
          getTableLocation(partitionSchemaName, partitionCubeName, tableName, hdfsStoreLocation,
              currentRestructNumber);
      tableLoc = tableLoc + File.separator + loadFolder;
      deleteStorePath(tableLoc);
    }
  }

  public static void deletePartialLoadDataIfExist(CarbonLoadModel loadModel,
      final boolean isCompactionFlow) throws IOException {
    CarbonTable carbonTable = loadModel.getCarbonDataLoadSchema().getCarbonTable();
    String metaDataLocation = carbonTable.getMetaDataFilepath();
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier());
    LoadMetadataDetails[] details = segmentStatusManager.readLoadMetadata(metaDataLocation);
    CarbonTablePath carbonTablePath = CarbonStorePath
        .getCarbonTablePath(loadModel.getStorePath(), carbonTable.getCarbonTableIdentifier());
    final List<String> loadFolders = new ArrayList<String>();
    for (LoadMetadataDetails loadMetadata : details) {
      loadFolders.add(carbonTablePath
          .getCarbonDataDirectoryPath(loadMetadata.getPartitionCount(), loadMetadata.getLoadName())
          .replace("\\", "/"));
    }

    //delete folder which metadata no exist in tablestatus
    for (int i = 0; i < carbonTable.getPartitionCount(); i++) {
      String partitionPath = carbonTablePath.getPartitionDir(i + "");
      FileType fileType = FileFactory.getFileType(partitionPath);
      if (FileFactory.isFileExist(partitionPath, fileType)) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(partitionPath, fileType);
        CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile path) {
            return !loadFolders.contains(path.getAbsolutePath().replace("\\", "/"));
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

  public static boolean isSliceValid(String loc, List<String> activeSlices,
      List<String> updatedSlices, String factTableName) {
    String loadFolderName = loc.substring(loc.indexOf(CarbonCommonConstants.LOAD_FOLDER));
    String sliceNum = loadFolderName.substring(CarbonCommonConstants.LOAD_FOLDER.length());
    if (activeSlices.contains(loadFolderName) || updatedSlices.contains(sliceNum)) {
      String factFileLoc =
          loc + File.separator + factTableName + "_0" + CarbonCommonConstants.FACT_FILE_EXT;
      try {
        if (FileFactory.isFileExist(factFileLoc, FileFactory.getFileType(factFileLoc))) {
          return true;
        }
      } catch (IOException e) {
        LOGGER.error("Problem checking file existence :: " + e.getMessage());
      }
    }
    return false;
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

  public static void removeSliceFromMemory(String schemaName, String cubeName, String loadName) {
    // TODO: Remove from memory
  }

  public static void createEmptyLoadFolder(CarbonLoadModel model, String factLoadFolderLocation,
      String hdfsStoreLocation, int currentRestructNumber) {
    String loadFolderName = factLoadFolderLocation
        .substring(factLoadFolderLocation.indexOf(CarbonCommonConstants.LOAD_FOLDER));
    String aggLoadFolderLocation =
        getTableLocation(model.getDatabaseName(), model.getTableName(), model.getAggTableName(),
            hdfsStoreLocation, currentRestructNumber);
    aggLoadFolderLocation = aggLoadFolderLocation + File.separator + loadFolderName;
    FileType fileType = FileFactory.getFileType(hdfsStoreLocation);
    try {
      FileFactory.mkdirs(aggLoadFolderLocation, fileType);
    } catch (IOException e) {
      LOGGER
          .error("Problem creating empty folder created for aggregation table: " + e.getMessage());
    }
    LOGGER.info("Empty folder created for aggregation table");
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
      CarbonUtil.deleteFoldersAndFiles(new File[] { new File(localStoreLocation).getParentFile()});
      LOGGER.info("Deleted the local store location" + localStoreLocation);
    } catch (CarbonUtilException e) {
      LOGGER.error(e, "Failed to delete local data load folder location");
    }

  }

  /**
   * This method will copy the current segment load to cgiven carbon store path
   *
   * @param loadModel
   * @param segmentName
   * @param updatedSlices
   * @throws IOException
   * @throws CarbonUtilException
   */
  public static void copyCurrentLoadToHDFS(CarbonLoadModel loadModel, String segmentName,
      List<String> updatedSlices) throws IOException, CarbonUtilException {
    //Copy the current load folder to carbon store
    boolean copyStore =
        Boolean.valueOf(CarbonProperties.getInstance().getProperty("dataload.hdfs.copy", "true"));
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String aggTableName = loadModel.getAggTableName();
    if (copyStore) {
      CarbonTableIdentifier carbonTableIdentifier =
          loadModel.getCarbonDataLoadSchema().getCarbonTable().getCarbonTableIdentifier();
      String segmentId = segmentName.substring(CarbonCommonConstants.LOAD_FOLDER.length());
      // form carbon store location
      String carbonStoreLocation = getStoreLocation(
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS),
          carbonTableIdentifier, segmentId, loadModel.getPartitionId());
      String tempLocationKey = databaseName + '_' + tableName;
      // form local store location
      String localStoreLocation = getStoreLocation(CarbonProperties.getInstance()
              .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL),
          carbonTableIdentifier, segmentId, loadModel.getPartitionId());
      localStoreLocation = localStoreLocation + File.separator + loadModel.getTaskNo();
      boolean isUpdate = false;
      if (loadModel.isAggLoadRequest() && null != aggTableName) {
        if (updatedSlices.contains(segmentId)) {
          isUpdate = true;
        }
      }
      copyToHDFS(carbonStoreLocation, localStoreLocation, isUpdate);
      CarbonUtil.deleteFoldersAndFiles(new File[] { new File(localStoreLocation) });
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
   * This method will copy the carbon data files form local store segment folder
   * to carbon store segment location
   *
   * @param carbonStoreLocation
   * @param localStoreLocation
   * @param isUpdate
   * @throws IOException
   */
  private static void copyToHDFS(String carbonStoreLocation, String localStoreLocation,
      boolean isUpdate) throws IOException {
    //If the carbon store and the local store configured differently, then copy
    if (carbonStoreLocation != null && !carbonStoreLocation.equals(localStoreLocation)) {
      long copyStartTime = System.currentTimeMillis();
      // isUpdate will be true only when the below 2 conditions are satisfied
      // 1. slice is marked for update, 2. request is for aggregate table creation
      if (isUpdate) {
        renameFactFile(localStoreLocation);
      }
      LOGGER.info("Copying " + localStoreLocation + " --> " + carbonStoreLocation);
      CarbonUtil.checkAndCreateFolder(carbonStoreLocation);
      long blockSize = getBlockSize();
      int bufferSize = CarbonCommonConstants.BYTEBUFFER_SIZE;
      CarbonFile carbonFile = FileFactory
          .getCarbonFile(localStoreLocation, FileFactory.getFileType(localStoreLocation));
      CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile path) {
          return path.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT) && !path.getName()
              .endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS);
        }
      });
      for (int i = 0; i < listFiles.length; i++) {
        String localFilePath = listFiles[i].getCanonicalPath();
        CarbonFile localCarbonFile =
            FileFactory.getCarbonFile(localFilePath, FileFactory.getFileType(localFilePath));
        String carbonFilePath = carbonStoreLocation + localFilePath
            .substring(localFilePath.lastIndexOf(File.separator));
        copyLocalFileToHDFS(carbonFilePath, localFilePath, bufferSize,
            getMaxOfBlockAndFileSize(blockSize, localCarbonFile.getSize()));
      }
      LOGGER.info("Total copy time (ms):  " + (System.currentTimeMillis() - copyStartTime));
    } else {
      LOGGER.info("Separate carbon.storelocation.hdfs is not configured for carbon store path");
    }
  }

  /**
   * This method will return max of block size and file size
   *
   * @param blockSize
   * @param fileSize
   * @return
   */
  private static long getMaxOfBlockAndFileSize(long blockSize, long fileSize) {
    long maxSize = blockSize;
    if (fileSize > blockSize) {
      maxSize = fileSize;
    }
    // block size should be exactly divisible by 512 which is  maintained by HDFS as bytes
    // per checksum, dfs.bytes-per-checksum=512 must divide block size
    long remainder = maxSize % HDFS_CHECKSUM_LENGTH;
    if (remainder > 0) {
      maxSize = maxSize + HDFS_CHECKSUM_LENGTH - remainder;
    }
    return maxSize;
  }

  /**
   * This method will return the block size for file to be copied in HDFS
   *
   * @return
   */
  private static long getBlockSize() {
    CarbonProperties carbonProperties = CarbonProperties.getInstance();
    long blockSizeInBytes = Long.parseLong(carbonProperties
        .getProperty(CarbonCommonConstants.MAX_FILE_SIZE,
            CarbonCommonConstants.MAX_FILE_SIZE_DEFAULT_VAL))
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR * 1L;
    return blockSizeInBytes;
  }

  /**
   * This method will read the local carbon data file and write to carbon data file in HDFS
   *
   * @param carbonStoreFilePath
   * @param localFilePath
   * @param bufferSize
   * @param blockSize
   * @throws IOException
   */
  private static void copyLocalFileToHDFS(String carbonStoreFilePath, String localFilePath,
      int bufferSize, long blockSize) throws IOException {
    DataOutputStream dataOutputStream = null;
    DataInputStream dataInputStream = null;
    try {
      LOGGER.debug(
          "HDFS file block size for file: " + carbonStoreFilePath + " is " + blockSize + " (bytes");
      dataOutputStream = FileFactory
          .getDataOutputStream(carbonStoreFilePath, FileFactory.getFileType(carbonStoreFilePath),
              bufferSize, blockSize);
      dataInputStream = FileFactory
          .getDataInputStream(localFilePath, FileFactory.getFileType(localFilePath), bufferSize);
      IOUtils.copyBytes(dataInputStream, dataOutputStream, bufferSize);
    } finally {
      CarbonUtil.closeStreams(dataInputStream, dataOutputStream);
    }
  }

  private static void renameFactFile(String localStoreLocation) {
    FileType fileType = FileFactory.getFileType(localStoreLocation);
    try {
      if (FileFactory.isFileExist(localStoreLocation, fileType)) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(localStoreLocation, fileType);
        CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile path) {
            return path.getName().endsWith(CarbonCommonConstants.FACT_FILE_EXT) && !path.getName()
                .endsWith(CarbonCommonConstants.FILE_INPROGRESS_STATUS);
          }
        });
        for (int i = 0; i < listFiles.length; i++) {
          carbonFile = listFiles[i];
          String factFilePath = carbonFile.getCanonicalPath();
          String changedFileName = factFilePath.replace(CarbonCommonConstants.FACT_FILE_EXT,
              CarbonCommonConstants.FACT_UPDATE_EXTENSION);
          carbonFile.renameTo(changedFileName);
        }
      }
    } catch (IOException e) {
      LOGGER.error("Inside renameFactFile. Problem checking file existence :: " + e.getMessage());
    }
  }

  /**
   * API will provide the load number inorder to record the same in metadata file.
   */
  public static int getLoadCount(CarbonLoadModel loadModel, int currentRestructNumber)
      throws IOException {

    String hdfsLoadedTable = getLoadFolderPath(loadModel, null, null, currentRestructNumber);
    int loadCounter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(hdfsLoadedTable);

    String hdfsStoreLoadFolder =
        hdfsLoadedTable + File.separator + CarbonCommonConstants.LOAD_FOLDER + loadCounter;
    hdfsStoreLoadFolder = hdfsStoreLoadFolder.replace("\\", "/");

    String loadFolerCount = hdfsStoreLoadFolder
        .substring(hdfsStoreLoadFolder.lastIndexOf('_') + 1, hdfsStoreLoadFolder.length());
    return Integer.parseInt(loadFolerCount) + 1;
  }

  /**
   * API will provide the load folder path for the store inorder to store the same
   * in the metadata.
   */
  public static String getLoadFolderPath(CarbonLoadModel loadModel, String cubeName,
      String schemaName, int currentRestructNumber) {

    //CHECKSTYLE:OFF    Approval No:Approval-V1R2C10_005

    boolean copyStore =
        Boolean.valueOf(CarbonProperties.getInstance().getProperty("dataload.hdfs.copy", "true"));

    // CHECKSTYLE:ON
    if (null == cubeName && null == schemaName) {
      schemaName = loadModel.getDatabaseName();
      cubeName = loadModel.getTableName();
    }
    String factTable = loadModel.getTableName();
    String hdfsLoadedTable = null;
    if (copyStore) {
      String hdfsLocation =
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);
      String localStore = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.STORE_LOCATION,
              CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);

      if (!hdfsLocation.equals(localStore)) {
        String hdfsStoreLocation = hdfsLocation;
        hdfsStoreLocation =
            hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;

        int rsCounter = currentRestructNumber;
        if (rsCounter == -1) {
          rsCounter = 0;
        }

        hdfsLoadedTable =
            hdfsStoreLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCounter
                + File.separator + factTable;

        hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");
      }

    }
    return hdfsLoadedTable;

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

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info(
            "Acquired lock for table" + loadModel.getDatabaseName() + "." + loadModel.getTableName()
                + " for table status updation");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            segmentStatusManager.readLoadMetadata(metaDataFilepath);

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

        segmentStatusManager.writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetails
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

  public static void writeLoadMetadata(CarbonDataLoadSchema schema, String schemaName,
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
          CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT));

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

    }
    writeOperation.close();

  }

  public static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  public static String extractLoadMetadataFileLocation(CarbonLoadModel loadModel) {
    CarbonTable carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
        .getCarbonTable(loadModel.getDatabaseName() + '_' + loadModel.getTableName());
    return carbonTable.getMetaDataFilepath();
  }

  /**
   * This method will provide the dimension column list for a given aggregate
   * table
   */
  public static Set<String> getColumnListFromAggTable(CarbonLoadModel model) {
    Set<String> columnList = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    CarbonTable carbonTable = org.carbondata.core.carbon.metadata.CarbonMetadata.getInstance()
        .getCarbonTable(model.getDatabaseName() + '_' + model.getTableName());
    List<CarbonDimension> dimensions = carbonTable.getDimensionByTableName(model.getAggTableName());
    List<CarbonMeasure> measures = carbonTable.getMeasureByTableName(model.getAggTableName());
    for (CarbonDimension carbonDimension : dimensions) {
      columnList.add(carbonDimension.getColName());
    }
    for (CarbonMeasure carbonMeasure : measures) {
      columnList.add(carbonMeasure.getColName());
    }
    return columnList;
  }

  public static void copyMergedLoadToHDFS(CarbonLoadModel loadModel, int currentRestructNumber,
      String mergedLoadName) {
    //Copy the current load folder to HDFS
    boolean copyStore =
        Boolean.valueOf(CarbonProperties.getInstance().getProperty("dataload.hdfs.copy", "true"));

    String schemaName = loadModel.getDatabaseName();
    String cubeName = loadModel.getTableName();
    String factTable = loadModel.getTableName();
    String aggTableName = loadModel.getAggTableName();

    if (copyStore) {
      String hdfsLocation =
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS);

      String localStore = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.STORE_LOCATION,
              CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL);
      if (!loadModel.isAggLoadRequest()) {
        copyMergeToHDFS(schemaName, cubeName, factTable, hdfsLocation, localStore,
            currentRestructNumber, mergedLoadName);
      }
      if (null != aggTableName) {
        copyMergeToHDFS(schemaName, cubeName, aggTableName, hdfsLocation, localStore,
            currentRestructNumber, mergedLoadName);
      }
      try {
        CarbonUtil.deleteFoldersAndFiles(new File[] {
            new File(localStore + File.separator + schemaName + File.separator + cubeName) });
      } catch (CarbonUtilException e) {
        LOGGER.error("Error while CarbonUtil.deleteFoldersAndFiles ");
      }
    }
  }

  public static void copyMergeToHDFS(String schemaName, String cubeName, String factTable,
      String hdfsLocation, String localStore, int currentRestructNumber, String mergedLoadName) {
    try {
      //If the hdfs store and the local store configured differently, then copy
      if (hdfsLocation != null && !hdfsLocation.equals(localStore)) {
        /**
         * Identify the Load_X folder from the local store folder
         */
        String currentloadedStore = localStore;
        currentloadedStore =
            currentloadedStore + File.separator + schemaName + File.separator + cubeName;

        int rsCounter = currentRestructNumber;

        if (rsCounter == -1) {
          LOGGER.info("Unable to find the local store details (RS_-1) " + currentloadedStore);
          return;
        }
        String localLoadedTable =
            currentloadedStore + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
                + rsCounter + File.separator + factTable;

        localLoadedTable = localLoadedTable.replace("\\", "/");

        int loadCounter = CarbonUtil.checkAndReturnCurrentLoadFolderNumber(localLoadedTable);

        if (loadCounter == -1) {
          LOGGER.info("Unable to find the local store details (Load_-1) " + currentloadedStore);

          return;
        }

        String localLoadName = CarbonCommonConstants.LOAD_FOLDER + mergedLoadName;
        String localLoadFolder =
            localLoadedTable + File.separator + CarbonCommonConstants.LOAD_FOLDER + mergedLoadName;

        LOGGER.info("Local data loaded folder ... = " + localLoadFolder);

        //Identify the Load_X folder in the HDFS store
        String hdfsStoreLocation = hdfsLocation;
        hdfsStoreLocation =
            hdfsStoreLocation + File.separator + schemaName + File.separator + cubeName;

        rsCounter = currentRestructNumber;
        if (rsCounter == -1) {
          rsCounter = 0;
        }

        String hdfsLoadedTable =
            hdfsStoreLocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER + rsCounter
                + File.separator + factTable;

        hdfsLoadedTable = hdfsLoadedTable.replace("\\", "/");

        String hdfsStoreLoadFolder = hdfsLoadedTable + File.separator + localLoadName;

        LOGGER.info("HDFS data load folder ... = " + hdfsStoreLoadFolder);

        // Copy the data created through latest ETL run, to the HDFS store
        LOGGER.info("Copying " + localLoadFolder + " --> " + hdfsStoreLoadFolder);

        hdfsStoreLoadFolder = hdfsStoreLoadFolder.replace("\\", "/");
        Path path = new Path(hdfsStoreLocation);

        FileSystem fs = path.getFileSystem(FileFactory.getConfiguration());
        fs.copyFromLocalFile(true, true, new Path(localLoadFolder), new Path(hdfsStoreLoadFolder));

        LOGGER.info("Copying sliceMetaData from " + localLoadedTable + " --> " + hdfsLoadedTable);

      } else {
        LOGGER.info("Separate carbon.storelocation.hdfs is not configured for hdfs store path");
      }
    } catch (RuntimeException e) {
      LOGGER.info(e.getMessage());
    } catch (Exception e) {
      LOGGER.info(e.getMessage());
    }
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
  public static Map<String, List<Distributable>> nodeBlockMapping(
      List<Distributable> blockInfos, int noOfNodesInput) {
    return nodeBlockMapping(blockInfos, noOfNodesInput, null);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @return
   */
  public static Map<String, List<Distributable>> nodeBlockMapping(
      List<Distributable> blockInfos) {
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
      for (String eachNode : blockInfo.getLocations()) {
        NodeBlockRelation nbr = new NodeBlockRelation(blockInfo, eachNode);
        flattenedList.add(nbr);
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
    assignLeftOverBlocks(nodeBlocksMap, uniqueBlocks, blocksPerNode);

    return nodeBlocksMap;
  }

  /**
   * Assigning the blocks of a node to tasks.
   *
   * @param nodeBlocksMap
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
      Set<Distributable> uniqueBlocks, int noOfBlocksPerNode) {

    for (Map.Entry<String, List<Distributable>> entry : outputMap.entrySet()) {
      Iterator<Distributable> blocks = uniqueBlocks.iterator();
      List<Distributable> blockLst = entry.getValue();
      //if the node is already having the per block nodes then avoid assign the extra blocks
      if (blockLst.size() == noOfBlocksPerNode){
        continue;
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
   * To create the final output of the Node and Data blocks
   *
   * @param outputMap
   * @param blocksPerNode
   * @param uniqueBlocks
   * @param nodeAndBlockMapping
   * @param activeNodes
   */
  private static void createOutputMap(Map<String, List<Distributable>> outputMap,
      int blocksPerNode, Set<Distributable> uniqueBlocks,
      Map<String, List<Distributable>> nodeAndBlockMapping, List<String> activeNodes) {

    ArrayList<NodeMultiBlockRelation> multiBlockRelations =
        new ArrayList<>(nodeAndBlockMapping.size());
    for (Map.Entry<String, List<Distributable>> entry : nodeAndBlockMapping.entrySet()) {
      multiBlockRelations.add(new NodeMultiBlockRelation(entry.getKey(), entry.getValue()));
    }
    // sort nodes based on number of blocks per node, so that nodes having lesser blocks
    // are assigned first
    Collections.sort(multiBlockRelations);

    Set<String> validActiveNodes = new HashSet<String>();
    // find all the valid active nodes
    for (NodeMultiBlockRelation nodeMultiBlockRelation : multiBlockRelations) {
      String nodeName = nodeMultiBlockRelation.getNode();
      //assign the block to the node only if the node is active
      if (null != activeNodes && isActiveExecutor(activeNodes, nodeName)) {
        validActiveNodes.add(nodeName);
      }
    }

    for (NodeMultiBlockRelation nodeMultiBlockRelation : multiBlockRelations) {
      String nodeName = nodeMultiBlockRelation.getNode();
      //assign the block to the node only if the node is active
      if (!validActiveNodes.isEmpty() && !validActiveNodes.contains(nodeName)) {
        continue;
      }
      // this loop will be for each NODE
      int nodeCapacity = 0;
      // loop thru blocks of each Node
      for (Distributable block : nodeMultiBlockRelation.getBlocks()) {

        // check if this is already assigned.
        if (uniqueBlocks.contains(block)) {

          if (null == outputMap.get(nodeName)) {
            List<Distributable> list =
                new ArrayList<Distributable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
            outputMap.put(nodeName, list);
          }
          // assign this block to this node if node has capacity left
          if (nodeCapacity < blocksPerNode) {
            List<Distributable> infos = outputMap.get(nodeName);
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
  private static boolean isActiveExecutor(List activeNode, String nodeName) {
    boolean isActiveNode = activeNode.contains(nodeName);
    if (isActiveNode) {
      return isActiveNode;
    }
    //if localhost then retrieve the localhost name then do the check
    else if (nodeName.equals("localhost")) {
      try {
        String hostName = InetAddress.getLocalHost().getHostName();
        isActiveNode = activeNode.contains(hostName);
      } catch (UnknownHostException ue) {
        isActiveNode = false;
      }
    } else {
      try {
        String hostAddress = InetAddress.getLocalHost().getHostAddress();
        isActiveNode = activeNode.contains(hostAddress);
      } catch (UnknownHostException ue) {
        isActiveNode = false;
      }
    }
    return isActiveNode;
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

      for (String eachNode : blockInfo.getLocations()) {
        NodeBlockRelation nbr = new NodeBlockRelation(blockInfo, eachNode);
        flattenedList.add(nbr);
        nodeList.add(eachNode);
      }
    }
  }

  /**
   * This method will get the store location for the given path, segment id and partition id
   *
   * @param carbonStorePath
   * @param dbName
   * @param tableName
   * @param partitionCount
   * @param segmentId
   */
  public static void checkAndCreateCarbonDataLocation(String carbonStorePath, String dbName,
      String tableName, int partitionCount, String segmentId) {
    CarbonTable carbonTable = CarbonMetadata.getInstance()
        .getCarbonTable(dbName + CarbonCommonConstants.UNDERSCORE + tableName);
    CarbonTableIdentifier carbonTableIdentifier = carbonTable.getCarbonTableIdentifier();
    CarbonTablePath carbonTablePath =
        CarbonStorePath.getCarbonTablePath(carbonStorePath, carbonTableIdentifier);
    for (int i = 0; i < partitionCount; i++) {
      String carbonDataDirectoryPath =
          carbonTablePath.getCarbonDataDirectoryPath(String.valueOf(i), segmentId);
      CarbonUtil.checkAndCreateFolder(carbonDataDirectoryPath);
    }
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
}
