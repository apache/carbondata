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
package org.carbondata.integration.spark.load;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.cache.Cache;
import org.carbondata.core.cache.CacheProvider;
import org.carbondata.core.cache.CacheType;
import org.carbondata.core.cache.dictionary.Dictionary;
import org.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.carbondata.core.carbon.CarbonDataLoadSchema;
import org.carbondata.core.carbon.CarbonDef;
import org.carbondata.core.carbon.CarbonDef.AggLevel;
import org.carbondata.core.carbon.CarbonDef.AggMeasure;
import org.carbondata.core.carbon.CarbonDef.AggName;
import org.carbondata.core.carbon.CarbonDef.AggTable;
import org.carbondata.core.carbon.CarbonDef.CubeDimension;
import org.carbondata.core.carbon.CarbonDef.Schema;
import org.carbondata.core.carbon.CarbonTableIdentifier;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
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
import org.carbondata.core.metadata.CarbonMetadata;
import org.carbondata.core.metadata.CarbonMetadata.Cube;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;
import org.carbondata.integration.spark.merger.NodeBlockRelation;
import org.carbondata.processing.api.dataloader.DataLoadModel;
import org.carbondata.processing.api.dataloader.SchemaInfo;
import org.carbondata.processing.csvload.DataGraphExecuter;
import org.carbondata.processing.dataprocessor.DataProcessTaskStatus;
import org.carbondata.processing.dataprocessor.IDataProcessStatus;
import org.carbondata.processing.globalsurrogategenerator.GlobalSurrogateGenerator;
import org.carbondata.processing.globalsurrogategenerator.GlobalSurrogateGeneratorInfo;
import org.carbondata.processing.graphgenerator.GraphGenerator;
import org.carbondata.processing.graphgenerator.GraphGeneratorException;
import org.carbondata.processing.util.CarbonDataProcessorUtil;
import org.carbondata.processing.util.CarbonSchemaParser;
import org.carbondata.query.datastorage.InMemoryTable;
import org.carbondata.query.datastorage.InMemoryTableStore;

import com.google.gson.Gson;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

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
      int currentRestructNumber, CarbonLoadModel loadModel) throws GraphGeneratorException {
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
    model.setTaskNo(loadModel.getTaskNo());
    model.setFactTimeStamp(loadModel.getFactTimeStamp());
    boolean hdfsReadMode =
        schmaModel.getCsvFilePath() != null && schmaModel.getCsvFilePath().startsWith("hdfs:");
    int allocate = null != schmaModel.getCsvFilePath() ? 1 : schmaModel.getFilesToProcess().size();
    GraphGenerator generator = new GraphGenerator(model, hdfsReadMode, loadModel.getPartitionId(),
        loadModel.getFactStoreLocation(), currentRestructNumber, allocate,
        loadModel.getCarbonDataLoadSchema(), loadModel.getSegmentId());
    generator.generateGraph();
  }

  public static void executeGraph(CarbonLoadModel loadModel, String storeLocation,
      String hdfsStoreLocation, String kettleHomePath, int currentRestructNumber) throws Exception {
    System.setProperty("KETTLE_HOME", kettleHomePath);
    if (!new File(storeLocation).mkdirs()) {
      LOGGER.error("Error while new File(storeLocation).mkdirs() ");
    }
    String outPutLoc = storeLocation + "/etl";
    String databaseName = loadModel.getDatabaseName();
    String tableName = loadModel.getTableName();
    String tempLocationKey = databaseName + '_' + tableName;
    CarbonProperties.getInstance().addProperty(tempLocationKey, storeLocation);
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.STORE_LOCATION_HDFS, hdfsStoreLocation);
    CarbonProperties.getInstance().addProperty("store_output_location", outPutLoc);
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
    SchemaInfo info = new SchemaInfo();

    info.setSchemaName(databaseName);
    info.setSrcDriverName(loadModel.getDriverClass());
    info.setSrcConUrl(loadModel.getJdbcUrl());
    info.setSrcUserName(loadModel.getDbUserName());
    info.setSrcPwd(loadModel.getDbPwd());
    info.setCubeName(tableName);
    info.setSchemaPath(loadModel.getSchemaPath());
    info.setAutoAggregateRequest(loadModel.isAggLoadRequest());
    info.setComplexDelimiterLevel1(loadModel.getComplexDelimiterLevel1());
    info.setComplexDelimiterLevel2(loadModel.getComplexDelimiterLevel2());

    generateGraph(schmaModel, info, currentRestructNumber, loadModel);

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
    int restructFolderNumber = currentRestructNumber/*CarbonUtil
                .checkAndReturnNextRestructFolderNumber(baseStorelocation,
                        "RS_")*/;

    String aggTableLocation =
        baseStorelocation + File.separator + CarbonCommonConstants.RESTRUCTRE_FOLDER
            + restructFolderNumber + File.separator + aggTableName;
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

  public static void deletePartialLoadDataIfExist(int partitionCount, String schemaName,
      String cubeName, String tableName, String hdfsStoreLocation, int currentRestructNumber,
      int loadFolder) throws IOException {
    String tableLoc = null;
    String partitionSchemaName = null;
    String partitionCubeName = null;
    for (int i = 0; i < partitionCount; i++) {
      partitionSchemaName = schemaName + '_' + i;
      partitionCubeName = cubeName + '_' + i;
      tableLoc =
          getTableLocation(partitionSchemaName, partitionCubeName, tableName, hdfsStoreLocation,
              currentRestructNumber);
      //tableLoc = tableLoc + File.separator + loadFolder;

      final List<String> loadFolders = new ArrayList<String>();
      for (int j = 0; j < loadFolder; j++) {
        loadFolders.add(
            (tableLoc + File.separator + CarbonCommonConstants.LOAD_FOLDER + j).replace("\\", "/"));
      }
      if (loadFolder != 0) {
        loadFolders.add(
            (tableLoc + File.separator + CarbonCommonConstants.SLICE_METADATA_FILENAME + "."
                + currentRestructNumber).replace("\\", "/"));
      }
      FileType fileType = FileFactory.getFileType(tableLoc);
      if (FileFactory.isFileExist(tableLoc, fileType)) {
        CarbonFile carbonFile = FileFactory.getCarbonFile(tableLoc, fileType);
        CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
          @Override public boolean accept(CarbonFile path) {
            return !loadFolders.contains(path.getAbsolutePath().replace("\\", "/")) && !path
                .getName().contains(CarbonCommonConstants.MERGERD_EXTENSION);
          }
        });
        for (int k = 0; k < listFiles.length; k++) {
          deleteStorePath(listFiles[k].getAbsolutePath());
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

  public static String getMetaDataFilePath(String schemaName, String cubeName,
      String hdfsStoreLocation) {
    String basePath = hdfsStoreLocation;
    String schemaPath = basePath.substring(0, basePath.lastIndexOf("/"));
    String metadataFilePath = schemaPath + "/schemas/" + schemaName + '/' + cubeName;
    return metadataFilePath;
  }

  public static void removeSliceFromMemory(String schemaName, String cubeName, String loadName) {
    List<InMemoryTable> activeSlices =
        InMemoryTableStore.getInstance().getActiveSlices(schemaName + '_' + cubeName);
    Iterator<InMemoryTable> sliceItr = activeSlices.iterator();
    InMemoryTable slice = null;
    while (sliceItr.hasNext()) {
      slice = sliceItr.next();
      if (loadName.equals(slice.getLoadName())) {
        sliceItr.remove();
      }
    }
  }

  public static boolean aggTableAlreadyExistWithSameMeasuresndLevels(AggName aggName,
      AggTable[] aggTables) {
    AggMeasure[] aggMeasures = null;
    AggLevel[] aggLevels = null;
    for (int i = 0; i < aggTables.length; i++) {
      aggLevels = aggTables[i].levels;
      aggMeasures = aggTables[i].measures;
      if (aggLevels.length == aggName.levels.length
          && aggMeasures.length == aggName.measures.length) {
        if (checkforLevels(aggLevels, aggName.levels) && checkforMeasures(aggMeasures,
            aggName.measures)) {
          return true;
        }
      }
    }
    return false;
  }

  private static boolean checkforLevels(AggLevel[] aggTableLevels, AggLevel[] newTableLevels) {
    int count = 0;
    for (int i = 0; i < aggTableLevels.length; i++) {
      for (int j = 0; j < newTableLevels.length; j++) {
        if (aggTableLevels[i].name.equals(newTableLevels[j].name)) {
          count++;
          break;
        }
      }
    }
    if (count == aggTableLevels.length) {
      return true;
    }
    return false;
  }

  private static boolean checkforMeasures(AggMeasure[] aggMeasures, AggMeasure[] newTableMeasures) {
    int count = 0;
    for (int i = 0; i < aggMeasures.length; i++) {
      for (int j = 0; j < newTableMeasures.length; j++) {
        if (aggMeasures[i].name.equals(newTableMeasures[j].name) && aggMeasures[i].aggregator
            .equals(newTableMeasures[j].aggregator)) {
          count++;
          break;
        }
      }
    }
    if (count == aggMeasures.length) {
      return true;
    }
    return false;
  }

  public static String getAggregateTableName(AggTable table) {
    return ((CarbonDef.AggName) table).getNameAttribute();
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
      LOGGER.error("Problem creating empty folder created for aggregation table: "
          + e.getMessage());
    }
    LOGGER.info("Empty folder created for aggregation table");
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
          new CarbonTableIdentifier(databaseName, tableName);
      String segmentId = segmentName.substring(CarbonCommonConstants.LOAD_FOLDER.length());
      // form carbon store location
      String carbonStoreLocation = getStoreLocation(
          CarbonProperties.getInstance().getProperty(CarbonCommonConstants.STORE_LOCATION_HDFS),
          carbonTableIdentifier, Integer.parseInt(segmentId), loadModel.getPartitionId());
      String tempLocationKey = databaseName + '_' + tableName;
      // form local store location
      String localStoreLocation = getStoreLocation(CarbonProperties.getInstance()
              .getProperty(tempLocationKey, CarbonCommonConstants.STORE_LOCATION_DEFAULT_VAL),
          carbonTableIdentifier, Integer.parseInt(segmentId), loadModel.getPartitionId());
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
  private static String getStoreLocation(String storePath,
      CarbonTableIdentifier carbonTableIdentifier, int segmentId, String partitionId) {
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

  public static void generateGlobalSurrogates(CarbonLoadModel loadModel, String storeLocation,
      int numberOfPartiiton, String[] partitionColumn, CubeDimension[] dims,
      int currentRestructNumber) {
    GlobalSurrogateGeneratorInfo generatorInfo = new GlobalSurrogateGeneratorInfo();
    generatorInfo.setCubeName(loadModel.getTableName());
    generatorInfo.setSchema(loadModel.getSchema());
    generatorInfo.setStoreLocation(storeLocation);
    generatorInfo.setTableName(loadModel.getTableName());
    generatorInfo.setNumberOfPartition(numberOfPartiiton);
    generatorInfo.setPartiontionColumnName(partitionColumn[0]);
    generatorInfo.setCubeDimensions(dims);
    GlobalSurrogateGenerator generator = new GlobalSurrogateGenerator(generatorInfo);
    generator.generateGlobalSurrogates(currentRestructNumber);
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
   */
  public static void recordLoadMetadata(int loadCount, LoadMetadataDetails loadMetadataDetails,
      CarbonLoadModel loadModel, String loadStatus, String startLoadTime) throws IOException {

    String dataLoadLocation = null;
    //String dataLoadLocation = getLoadFolderPath(loadModel);
    dataLoadLocation =
        loadModel.getCarbonDataLoadSchema().getCarbonTable().getMetaDataFilepath() + File.separator
            + CarbonCommonConstants.LOADMETADATA_FILENAME;
    Gson gsonObjectToRead = new Gson();
    List<LoadMetadataDetails> listOfLoadFolderDetails = null;
    DataInputStream dataInputStream = null;
    String loadEnddate = readCurrentTime();
    loadMetadataDetails.setTimestamp(loadEnddate);
    loadMetadataDetails.setLoadStatus(loadStatus);
    loadMetadataDetails.setLoadName(String.valueOf(loadCount));
    loadMetadataDetails.setLoadStartTime(startLoadTime);
    LoadMetadataDetails[] listOfLoadFolderDetailsArray = null;
    try {
      if (FileFactory.isFileExist(dataLoadLocation, FileFactory.getFileType(dataLoadLocation))) {

        dataInputStream = FileFactory
            .getDataInputStream(dataLoadLocation, FileFactory.getFileType(dataLoadLocation));

        BufferedReader buffReader = new BufferedReader(new InputStreamReader(dataInputStream,
            CarbonCommonConstants.CARBON_DEFAULT_STREAM_ENCODEFORMAT));
        listOfLoadFolderDetailsArray =
            gsonObjectToRead.fromJson(buffReader, LoadMetadataDetails[].class);
      }
      listOfLoadFolderDetails =
          new ArrayList<LoadMetadataDetails>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

      if (null != listOfLoadFolderDetailsArray) {
        for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
          listOfLoadFolderDetails.add(loadMetadata);
        }
      }
      listOfLoadFolderDetails.add(loadMetadataDetails);

    } finally {

      CarbonUtil.closeStreams(dataInputStream);
    }
    writeLoadMetadata(loadModel.getCarbonDataLoadSchema(), loadModel.getDatabaseName(),
        loadModel.getTableName(), listOfLoadFolderDetails);

  }

  public static void writeLoadMetadata(CarbonDataLoadSchema schema, String schemaName,
      String cubeName, List<LoadMetadataDetails> listOfLoadFolderDetails) throws IOException {
    String dataLoadLocation = schema.getCarbonTable().getMetaDataFilepath() + File.separator
        + CarbonCommonConstants.LOADMETADATA_FILENAME;

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

  public static String extractLoadMetadataFileLocation(Schema schema, String schemaName,
      String cubeName) {
    Cube cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    if (null == cube) {
      //Schema schema = loadModel.getSchema();
      CarbonDef.Cube mondrianCube = CarbonSchemaParser.getMondrianCube(schema, cubeName);
      CarbonMetadata.getInstance().loadCube(schema, schema.name, mondrianCube.name, mondrianCube);
      cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    }

    return cube.getMetaDataFilepath();
  }

  public static String readCurrentTime() {
    SimpleDateFormat sdf = new SimpleDateFormat(CarbonCommonConstants.CARBON_TIMESTAMP);
    String date = null;

    date = sdf.format(new Date());

    return date;
  }

  public static CarbonDimension[][] getDimensionSplit(CarbonDataLoadSchema schema, String cubeName,
      int numberOfPartition) {

    List<CarbonDimension> allDims = schema.getCarbonTable().getDimensionByTableName(cubeName);
    List<CarbonMeasure> msrs = schema.getCarbonTable().getMeasureByTableName(cubeName);
    List<CarbonDimension> selectedDims =
        new ArrayList<CarbonDimension>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 0; i < allDims.size(); i++) {
      for (int j = 0; j < msrs.size(); j++) {
        if (selectedDims.get(j).getColName().equals(msrs.get(j).getColName())) {
          selectedDims.add(allDims.get(i));
        }
      }
    }
    CarbonDimension[] allDimsArr = selectedDims.toArray(new CarbonDimension[selectedDims.size()]);
    if (allDimsArr.length < 1) {
      return new CarbonDimension[0][0];
    }
    int[] numberOfNodeToScanForEachThread =
        getNumberOfNodeToScanForEachThread(allDimsArr.length, numberOfPartition);
    CarbonDimension[][] out = new CarbonDimension[numberOfNodeToScanForEachThread.length][];
    int counter = 0;

    for (int i = 0; i < numberOfNodeToScanForEachThread.length; i++) {
      out[i] = new CarbonDimension[numberOfNodeToScanForEachThread[i]];
      for (int j = 0; j < numberOfNodeToScanForEachThread[i]; j++) {
        out[i][j] = allDimsArr[counter++];
      }
    }

    return out;
  }

  private static int[] getNumberOfNodeToScanForEachThread(int numberOfNodes, int numberOfCores) {
    int div = numberOfNodes / numberOfCores;
    int mod = numberOfNodes % numberOfCores;
    int[] numberOfNodeToScan = null;
    if (div > 0) {
      numberOfNodeToScan = new int[numberOfCores];
      Arrays.fill(numberOfNodeToScan, div);
    } else if (mod > 0) {
      numberOfNodeToScan = new int[(int) mod];
    }
    for (int i = 0; i < mod; i++) {
      numberOfNodeToScan[i] = numberOfNodeToScan[i] + 1;
    }
    return numberOfNodeToScan;
  }

  public static String extractLoadMetadataFileLocation(CarbonLoadModel loadModel) {
    Cube cube = CarbonMetadata.getInstance()
        .getCube(loadModel.getDatabaseName() + '_' + loadModel.getTableName());
    if (null == cube) {
      Schema schema = loadModel.getSchema();
      CarbonDef.Cube mondrianCube =
          CarbonSchemaParser.getMondrianCube(schema, loadModel.getTableName());
      CarbonMetadata.getInstance().loadCube(schema, schema.name, mondrianCube.name, mondrianCube);
      cube = CarbonMetadata.getInstance()
          .getCube(loadModel.getDatabaseName() + '_' + loadModel.getTableName());
    }

    return cube.getMetaDataFilepath();
  }

  public static int getCurrentRestructFolder(String schemaName, String cubeName, Schema schema) {
    Cube cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    if (null == cube) {
      CarbonDef.Cube mondrianCube = CarbonSchemaParser.getMondrianCube(schema, cubeName);
      CarbonMetadata.getInstance().loadCube(schema, schema.name, mondrianCube.name, mondrianCube);
      cube = CarbonMetadata.getInstance().getCube(schemaName + '_' + cubeName);
    }

    String metaDataPath = cube.getMetaDataFilepath();
    int currentRestructNumber =
        CarbonUtil.checkAndReturnCurrentRestructFolderNumber(metaDataPath, "RS_", false);
    if (-1 == currentRestructNumber) {
      currentRestructNumber = 0;
    }

    return currentRestructNumber;
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
      String columnIdentifier, String carbonStorePath, DataType dataType)
      throws CarbonUtilException {
    return getDictionary(
        new DictionaryColumnUniqueIdentifier(tableIdentifier, columnIdentifier, dataType),
        carbonStorePath);
  }

  /**
   * This method will divide the blocks among the tasks of the nodes as per the data locality
   *
   * @param blockInfos
   * @param noOfNodesInput   -1 if number of nodes has to be decided
   *                         based on block location information
   * @param parallelism total no of tasks to execute in parallel
   * @return
   */
  public static Map<String, List<List<TableBlockInfo>>> nodeBlockTaskMapping(
      List<TableBlockInfo> blockInfos, int noOfNodesInput, int parallelism) {

    Map<String, List<TableBlockInfo>> mapOfNodes = CarbonLoaderUtil.nodeBlockMapping(
        blockInfos, noOfNodesInput);
    int noOfTasksPerNode = parallelism/mapOfNodes.size();
    // divide the blocks of a node among the tasks of the node.
    return assignBlocksToTasksPerNode(mapOfNodes, noOfTasksPerNode);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @return
   *
   */
  public static Map<String, List<TableBlockInfo>> nodeBlockMapping(
      List<TableBlockInfo> blockInfos) {
    // -1 if number of nodes has to be decided based on block location information
    return nodeBlockMapping(blockInfos, -1);
  }

  /**
   * This method will divide the blocks among the nodes as per the data locality
   *
   * @param blockInfos
   * @param noOfNodesInput -1 if number of nodes has to be decided
   *                       based on block location information
   * @return
   */
  public static Map<String, List<TableBlockInfo>> nodeBlockMapping(
      List<TableBlockInfo> blockInfos, int noOfNodesInput) {

    Map<String, List<TableBlockInfo>> nodeBlocksMap =
        new HashMap<String, List<TableBlockInfo>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    Map<String, List<List<TableBlockInfo>>> outputMap =
        new HashMap<String, List<List<TableBlockInfo>>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    List<NodeBlockRelation> flattenedList =
        new ArrayList<NodeBlockRelation>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    Set<TableBlockInfo> uniqueBlocks =
        new HashSet<TableBlockInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Set<String> nodes = new HashSet<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    createFlattenedListFromMap(blockInfos, flattenedList, uniqueBlocks, nodes);

    int noofNodes = (-1 == noOfNodesInput) ? nodes.size() : noOfNodesInput;
    int blocksPerNode = blockInfos.size() / noofNodes;

    // sort the flattened data.
    Collections.sort(flattenedList);

    Map<String, List<TableBlockInfo>> nodeAndBlockMapping =
        new LinkedHashMap<String, List<TableBlockInfo>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // from the flattened list create a mapping of node vs Data blocks.
    createNodeVsBlockMapping(flattenedList, nodeAndBlockMapping);

    // so now we have a map of node vs blocks. allocate the block as per the order
    createOutputMap(nodeBlocksMap, blocksPerNode, uniqueBlocks, nodeAndBlockMapping);

    // if any blocks remain then assign them to nodes in round robin.
    assignLeftOverBlocks(nodeBlocksMap, uniqueBlocks, blocksPerNode);

    return nodeBlocksMap;
  }

  /**
   *  Assigning the blocks of a node to tasks.
   * @param nodeBlocksMap
   * @param outputMap
   * @param noOfTasksPerNode
   * @return
   */
  private static Map<String, List<List<TableBlockInfo>>> assignBlocksToTasksPerNode(
      Map<String, List<TableBlockInfo>> nodeBlocksMap, int noOfTasksPerNode) {
    Map<String, List<List<TableBlockInfo>>> outputMap = new HashMap
        <String, List<List<TableBlockInfo>>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

    // for each node
    for(Map.Entry<String,List<TableBlockInfo>> eachNode : nodeBlocksMap.entrySet()){

      List<TableBlockInfo> blockOfEachNode = eachNode.getValue();

      // create the task list for each node.
      createTaskListForNode(outputMap,noOfTasksPerNode,eachNode.getKey());

      // take all the block of node and divide it among the tasks of a node.
      divideBlockToTasks(outputMap,eachNode.getKey(),blockOfEachNode);
    }

    return outputMap;
  }

  /**
   * This will divide the blocks of a node to tasks of the node.
   * @param outputMap
   * @param key
   * @param blockOfEachNode
   */
  private static void divideBlockToTasks(Map<String, List<List<TableBlockInfo>>> outputMap,
      String key, List<TableBlockInfo> blockOfEachNode) {

    List<List<TableBlockInfo>> taskLists = outputMap.get(key);
    int tasksOfNode = taskLists.size();
    int i = 0;
    for(TableBlockInfo block : blockOfEachNode){

      taskLists.get(i%tasksOfNode).add(block);
      i++;
    }

  }

  /**
   * This will create the empty list for each task of a node.
   * @param outputMap
   * @param noOfTasksPerNode
   * @param key
   */
  private static void createTaskListForNode(Map<String, List<List<TableBlockInfo>>> outputMap,
      int noOfTasksPerNode, String key) {
    List<List<TableBlockInfo>> nodeTaskList =
        new ArrayList<List<TableBlockInfo>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for(int i = 0 ; i < noOfTasksPerNode; i++){
      List<TableBlockInfo> eachTask = new ArrayList<TableBlockInfo>(
          CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      nodeTaskList.add(eachTask);

    }
    outputMap.put(key,nodeTaskList);

  }

  /**
   * If any left over data blocks are present then assign those to nodes in round robin way.
   *
   * @param outputMap
   * @param uniqueBlocks
   */
  private static void assignLeftOverBlocks(Map<String, List<TableBlockInfo>> outputMap,
      Set<TableBlockInfo> uniqueBlocks, int noOfBlocksPerNode) {


    for (Map.Entry<String, List<TableBlockInfo>> entry : outputMap.entrySet()) {
      Iterator<TableBlockInfo> blocks = uniqueBlocks.iterator();
      List<TableBlockInfo> blockLst = entry.getValue();
      while (blocks.hasNext()) {
        TableBlockInfo block = blocks.next();
        blockLst.add(block);
        blocks.remove();
        if(blockLst.size() >= noOfBlocksPerNode){
          break;
        }
      }
    }

    for (Map.Entry<String, List<TableBlockInfo>> entry : outputMap.entrySet()) {
      Iterator<TableBlockInfo> blocks = uniqueBlocks.iterator();
      if (blocks.hasNext()) {
        TableBlockInfo block = blocks.next();
        List<TableBlockInfo> blockLst = entry.getValue();
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
   */
  private static void createOutputMap(Map<String, List<TableBlockInfo>> outputMap,
      int blocksPerNode, Set<TableBlockInfo> uniqueBlocks,
      Map<String, List<TableBlockInfo>> nodeAndBlockMapping) {
    for (Map.Entry<String, List<TableBlockInfo>> entry : nodeAndBlockMapping.entrySet()) {

      // this loop will be for each NODE
      int nodeCapacity = 0;

      List<TableBlockInfo> blocksInEachNode = entry.getValue();

      // loop thru blocks of each Node
      for (TableBlockInfo block : blocksInEachNode) {

        // check if this is already assigned.
        if (uniqueBlocks.contains(block)) {

          // assign this block to this node if node has capacity left
          if (nodeCapacity < blocksPerNode) {

            List<TableBlockInfo> list;
            if (null == outputMap.get(entry.getKey())) {

              list = new ArrayList<TableBlockInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
              list.add(block);
              outputMap.put(entry.getKey(), list);

            } else {
              list = outputMap.get(entry.getKey());
              list.add(block);

            }
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
   * Create the Node and its related blocks Mapping and put in a Map
   *
   * @param flattenedList
   * @param nodeAndBlockMapping
   */
  private static void createNodeVsBlockMapping(List<NodeBlockRelation> flattenedList,
      Map<String, List<TableBlockInfo>> nodeAndBlockMapping) {
    for (NodeBlockRelation nbr : flattenedList) {
      String node = nbr.getNode();
      List<TableBlockInfo> list;

      if (null == nodeAndBlockMapping.get(node)) {
        list = new ArrayList<TableBlockInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
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
   *  @param blockInfos
   * @param flattenedList
   * @param uniqueBlocks
   */
  private static void createFlattenedListFromMap(List<TableBlockInfo> blockInfos,
      List<NodeBlockRelation> flattenedList, Set<TableBlockInfo> uniqueBlocks,
      Set<String> nodeList) {
    for (TableBlockInfo blockInfo : blockInfos) {
      // put the blocks in the set
      uniqueBlocks.add(blockInfo);

      for (String eachNode : blockInfo.getLocations()) {
        NodeBlockRelation nbr = new NodeBlockRelation(blockInfo, eachNode);
        flattenedList.add(nbr);
        nodeList.add(eachNode);
      }
    }
  }

}
