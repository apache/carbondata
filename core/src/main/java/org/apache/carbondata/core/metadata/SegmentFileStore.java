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

package org.apache.carbondata.core.metadata;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.index.IndexStoreManager;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.index.TableIndex;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.segmentmeta.SegmentColumnMetaDataInfo;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfo;
import org.apache.carbondata.core.segmentmeta.SegmentMetaDataInfoStats;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.TrashUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

/**
 * Provide read and write support for segment file associated with each segment
 */
public class SegmentFileStore {

  private static final Logger LOGGER = LogServiceFactory.getLogService(
      SegmentFileStore.class.getCanonicalName());

  private SegmentFile segmentFile;

  /**
   * Here key folder path and values are index files in it.
   */
  private Map<String, List<String>> indexFilesMap;

  private String tablePath;

  public SegmentFileStore(String tablePath, String segmentFileName) throws IOException {
    this.tablePath = tablePath;
    this.segmentFile = readSegment(tablePath, segmentFileName);
  }

  /**
   * Write segment information to the segment folder with index file name and
   * corresponding partitions.
   */
  public static void writeSegmentFileForPartitionTable(String tablePath, final String taskNo,
      String location, String timeStamp, List<String> partitionNames) throws IOException {
    writeSegmentFileForPartitionTable(tablePath, taskNo, location, timeStamp, partitionNames,
        false);
  }

  /**
   * Method to create and write the segment file, removes the temporary directories from all the
   * respective partition directories. This method is invoked only when {@link
   * CarbonCommonConstants#CARBON_MERGE_INDEX_IN_SEGMENT} is disabled.
   * @param tablePath Table path
   * @param segmentId Segment id
   * @param timeStamp FactTimeStamp
   * @param partitionNames Partition names list
   * @param indexFileNames Index files map with partition as key and index file names set as value
   * @throws IOException
   */
  public static void writeSegmentFile(String tablePath, String segmentId, String timeStamp,
      List<String> partitionNames, Map<String, Set<String>> indexFileNames) throws IOException {
    SegmentFileStore.SegmentFile finalSegmentFile = null;
    boolean isRelativePath;
    String partitionLoc;
    for (String partition : partitionNames) {
      isRelativePath = false;
      partitionLoc = partition;
      if (partitionLoc.startsWith(tablePath)) {
        partitionLoc = partitionLoc.substring(tablePath.length());
        isRelativePath = true;
      }
      SegmentFileStore.SegmentFile segmentFile = new SegmentFileStore.SegmentFile();
      SegmentFileStore.FolderDetails folderDetails = new SegmentFileStore.FolderDetails();
      folderDetails.setFiles(indexFileNames.get(partition));
      folderDetails.setPartitions(
          Collections.singletonList(partitionLoc.substring(partitionLoc.indexOf("/") + 1)));
      folderDetails.setRelative(isRelativePath);
      folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
      segmentFile.getLocationMap().put(partitionLoc, folderDetails);
      if (finalSegmentFile != null) {
        finalSegmentFile = finalSegmentFile.merge(segmentFile);
      } else {
        finalSegmentFile = segmentFile;
      }
    }
    Objects.requireNonNull(finalSegmentFile);
    String segmentFilesLocation = CarbonTablePath.getSegmentFilesLocation(tablePath);
    CarbonFile locationFile = FileFactory.getCarbonFile(segmentFilesLocation);
    if (!locationFile.exists()) {
      locationFile.mkdirs();
    }
    String segmentFileName = SegmentFileStore.genSegmentFileName(segmentId, timeStamp);
    SegmentFileStore.writeSegmentFile(finalSegmentFile,
        segmentFilesLocation + "/" + segmentFileName + CarbonTablePath.SEGMENT_EXT);
    SegmentFileStore
        .moveFromTempFolder(finalSegmentFile, segmentId + "_" + timeStamp + ".tmp", tablePath);
  }

  /**
   * Write segment information to the segment folder with index file name and
   * corresponding partitions.
   */
  public static void writeSegmentFileForPartitionTable(String tablePath, final String taskNo,
      String location, String timeStamp, List<String> partitionNames, boolean isMergeIndexFlow)
      throws IOException {
    String tempFolderLoc = timeStamp + ".tmp";
    String writePath = CarbonTablePath.getSegmentFilesLocation(tablePath) + "/" + tempFolderLoc;
    CarbonFile carbonFile = FileFactory.getCarbonFile(writePath);
    if (!carbonFile.exists()) {
      carbonFile.mkdirs();
    }
    CarbonFile tempFolder;
    if (isMergeIndexFlow) {
      // To get the index files from the partition path directly.
      // For old stores after merge index is triggered, we wont have '.tmp' folder.
      tempFolder = FileFactory.getCarbonFile(location);
    } else {
      // When mergeindex property is disabled during compaction, we write temporary segment files
      // with index files present in '.tmp' folder of partition path.
      tempFolder = FileFactory
          .getCarbonFile(location + CarbonCommonConstants.FILE_SEPARATOR + tempFolderLoc);
    }

    if ((tempFolder.exists() && partitionNames.size() > 0) || (isMergeIndexFlow
        && partitionNames.size() > 0)) {
      CarbonFile[] carbonFiles = tempFolder.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          return file.getName().startsWith(taskNo) && (
              file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
                  .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT));
        }
      });
      if (carbonFiles != null && carbonFiles.length > 0) {
        boolean isRelative = false;
        if (location.startsWith(tablePath)) {
          location = location.substring(tablePath.length());
          isRelative = true;
        }
        SegmentFile segmentFile = new SegmentFile();
        FolderDetails folderDetails = new FolderDetails();
        folderDetails.setRelative(isRelative);
        folderDetails.setPartitions(partitionNames);
        folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
        setIndexFileNamesToFolderDetails(folderDetails, carbonFiles);
        segmentFile.addPath(location, folderDetails);
        String path = null;
        if (isMergeIndexFlow) {
          // in case of merge index flow, tasks are launched per partition and all the tasks
          // will be written to the same tmp folder, in that case taskNo is not unique.
          // To generate a unique fileName UUID is used
          path = writePath + "/" + CarbonUtil.generateUUID() + CarbonTablePath.SEGMENT_EXT;
        } else {
          path = writePath + "/" + taskNo + CarbonTablePath.SEGMENT_EXT;
        }
        // write segment info to new file.
        writeSegmentFile(segmentFile, path);
      }
    }
  }

  /**
   * Generate Segment file name
   * @param segmentId segment id
   * @param UUID unique string, typically caller can use the loading start
   *             timestamp in CarbonLoadModel
   * @return
   */
  public static String genSegmentFileName(String segmentId, String UUID) {
    return segmentId + "_" + UUID;
  }

  public static String writeSegmentFile(CarbonTable carbonTable, String segmentId, String uuid,
      SegmentMetaDataInfo segmentMetaDataInfo) throws IOException {
    return writeSegmentFile(carbonTable, segmentId, uuid, null, segmentMetaDataInfo);
  }

  public static String writeSegmentFile(CarbonTable carbonTable, String segmentId, String uuid)
      throws IOException {
    return writeSegmentFile(carbonTable, segmentId, uuid, null, null);
  }

  public static String writeSegmentFile(CarbonTable carbonTable, String segmentId, String uuid,
      String segPath) throws IOException {
    return writeSegmentFile(carbonTable, segmentId, uuid, segPath, null);
  }

  /**
   * Returns the list of index files
   *
   * @param segmentPath
   * @return
   */
  public static CarbonFile[] getListOfCarbonIndexFiles(String segmentPath) {
    CarbonFile segmentFolder = FileFactory.getCarbonFile(segmentPath);
    return segmentFolder.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        return (file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) ||
            file.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT));
      }
    });
  }

  /**
   * Write segment file to the metadata folder of the table.
   *
   * @param carbonTable CarbonTable
   * @param segment segment
   * @return boolean , whether write is success or fail.
   */
  public static boolean writeSegmentFile(CarbonTable carbonTable, Segment segment)
      throws IOException {
    String tablePath = carbonTable.getTablePath();
    CarbonFile[] indexFiles = getListOfCarbonIndexFiles(segment.getSegmentPath());
    if (indexFiles != null && indexFiles.length > 0) {
      SegmentFile segmentFile = new SegmentFile();
      segmentFile.setOptions(segment.getOptions());
      FolderDetails folderDetails = new FolderDetails();
      folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
      folderDetails.setRelative(false);
      segmentFile.addPath(segment.getSegmentPath(), folderDetails);
      setIndexFileNamesToFolderDetails(folderDetails, indexFiles);
      String segmentFileFolder = CarbonTablePath.getSegmentFilesLocation(tablePath);
      CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFileFolder);
      if (!carbonFile.exists()) {
        carbonFile.mkdirs();
      }
      // write segment info to new file.
      writeSegmentFile(segmentFile,
          segmentFileFolder + File.separator + segment.getSegmentFileName());

      return true;
    }
    return false;
  }

  public static void setIndexFileNamesToFolderDetails(FolderDetails folderDetails,
      CarbonFile[] indexFiles) {
    for (CarbonFile file : indexFiles) {
      if (file.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        folderDetails.setMergeFileName(file.getName());
      } else {
        folderDetails.getFiles().add(file.getName());
      }
    }
  }

  public static boolean writeSegmentFileForOthers(
      CarbonTable carbonTable,
      Segment segment,
      PartitionSpec partitionSpec,
      List<FileStatus> partitionDataFiles)
      throws IOException {
    String tablePath = carbonTable.getTablePath();
    CarbonFile[] dataFiles = null;
    if (partitionDataFiles.isEmpty()) {
      CarbonFile segmentFolder = FileFactory.getCarbonFile(segment.getSegmentPath());
      dataFiles = segmentFolder.listFiles(
          file -> (!file.getName().equals("_SUCCESS") && !file.getName().endsWith(".crc")));
    } else {
      dataFiles = partitionDataFiles.stream().map(
          fileStatus -> FileFactory.getCarbonFile(
              fileStatus.getPath().toString())).toArray(CarbonFile[]::new);
    }
    if (dataFiles != null && dataFiles.length > 0) {
      SegmentFile segmentFile = new SegmentFile();
      segmentFile.setOptions(segment.getOptions());
      FolderDetails folderDetails = new FolderDetails();
      folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
      folderDetails.setRelative(false);
      if (!partitionDataFiles.isEmpty()) {
        folderDetails.setPartitions(partitionSpec.getPartitions());
        segmentFile.addPath(partitionSpec.getLocation().toString(), folderDetails);
      } else {
        segmentFile.addPath(segment.getSegmentPath(), folderDetails);
      }
      for (CarbonFile file : dataFiles) {
        folderDetails.getFiles().add(file.getName());
      }
      String segmentFileFolder = CarbonTablePath.getSegmentFilesLocation(tablePath);
      CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFileFolder);
      if (!carbonFile.exists()) {
        carbonFile.mkdirs();
      }
      // write segment info to new file.
      writeSegmentFile(segmentFile,
          segmentFileFolder + File.separator + segment.getSegmentFileName());

      return true;
    }
    return false;
  }

  /**
   * Write segment file to the metadata folder of the table selecting only the current load files
   *
   * @param carbonTable
   * @param segmentId
   * @param uuid
   * @return
   * @throws IOException
   */
  public static String writeSegmentFile(CarbonTable carbonTable, String segmentId, String uuid,
        String absSegPath, SegmentMetaDataInfo segmentMetaDataInfo)
      throws IOException {
    String tablePath = carbonTable.getTablePath();
    boolean supportFlatFolder = carbonTable.isSupportFlatFolder();
    String segmentPath = absSegPath;
    if (absSegPath == null) {
      segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
    }
    CarbonFile segmentFolder = FileFactory.getCarbonFile(segmentPath);
    CarbonFile[] indexFiles = segmentFolder.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        if (null != uuid) {
          return file.getName().contains(uuid) && (
              file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
                  .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT));
        }
        return file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
            .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT);
      }
    });
    if (indexFiles != null && indexFiles.length > 0) {
      SegmentFile segmentFile = new SegmentFile();
      FolderDetails folderDetails = new FolderDetails();
      folderDetails.setRelative(absSegPath == null);
      folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
      setIndexFileNamesToFolderDetails(folderDetails, indexFiles);
      String segmentRelativePath = "/";
      if (!supportFlatFolder) {
        if (absSegPath != null) {
          segmentRelativePath = absSegPath;
        } else {
          segmentRelativePath = segmentPath.substring(tablePath.length());
        }
      }
      segmentFile.addPath(segmentRelativePath, folderDetails);
      // set segmentMinMax to segmentFile
      if (null != segmentMetaDataInfo) {
        segmentFile.setSegmentMetaDataInfo(segmentMetaDataInfo);
      }
      String segmentFileFolder = CarbonTablePath.getSegmentFilesLocation(tablePath);
      CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFileFolder);
      if (!carbonFile.exists()) {
        carbonFile.mkdirs();
      }
      String segmentFileName = genSegmentFileName(segmentId, uuid) + CarbonTablePath.SEGMENT_EXT;
      // write segment info to new file.
      writeSegmentFile(segmentFile, segmentFileFolder + File.separator + segmentFileName);

      // Move all files to table path from segment folder.
      if (supportFlatFolder) {
        moveFromTempFolder(segmentPath, tablePath);
      }

      return segmentFileName;
    }
    return null;
  }

  /**
   * Get stale and invalid index files that have already been merged.
   * As we are not deleting index files immediately after updating old tables,
   * we will have old index files in segment folder.
   * This method is called in following scenarios:
   * 1. During read, when segment file is not present or gets deleted.
   * 2. When writing segment index size in tablestatus file.
   */
  public static Set<String> getInvalidAndMergedIndexFiles(List<String> indexFiles)
      throws IOException {
    SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
    Set<String> mergedAndInvalidIndexFiles = new HashSet<>();
    long lastModifiedTime = 0L;
    String validMergeIndexFile = null;
    List<String> mergeIndexFileNames = new ArrayList<>();
    boolean isIndexFilesPresent = false;
    for (String indexFile : indexFiles) {
      if (indexFile.endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        isIndexFilesPresent = true;
      }
      if (indexFile.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        long indexFileTimeStamp =
            Long.parseLong(CarbonTablePath.DataFileUtil.getTimeStampFromFileName(indexFile));
        // In case there are more than 1 mergeindex files present, latest one is considered as valid
        if (indexFileTimeStamp > lastModifiedTime) {
          lastModifiedTime = indexFileTimeStamp;
          validMergeIndexFile = indexFile;
        }
        mergeIndexFileNames.add(indexFile);
      }
    }
    //  Possible scenarios where we have >1 merge index file:
    //  1. SI load when MT has stale index files. As during SI load MT segment file is not updated,
    //  we directly read from segment directory.
    //  2. In SI table, after small files index merge(refresh command) we will have more than one
    //  mergeindex file as we are not deleting old file immediately. If segment file gets deleted,
    //  then it reads from segment directory.
    if (mergeIndexFileNames.size() > 1 && validMergeIndexFile != null) {
      final String validIndexFileName = validMergeIndexFile;
      // get the invalid mergeindex files by excluding the valid file.
      mergedAndInvalidIndexFiles.addAll(
          mergeIndexFileNames.stream().filter(file -> !file.equalsIgnoreCase(validIndexFileName))
              .collect(Collectors.toSet()));
    }
    // If both index and merge index files are present, read the valid merge index file and add its
    // mapped index files to the excluding list.
    // Example: We have xxx.index and xxx.mergeindex files in a segment directory.
    // Here the index file (xxx.index) is considered invalid.
    if (isIndexFilesPresent && validMergeIndexFile != null) {
      indexFileStore.readMergeFile(validMergeIndexFile);
      Map<String, List<String>> carbonMergeFileToIndexFilesMap =
          indexFileStore.getCarbonMergeFileToIndexFilesMap();
      String segmentPath =
          validMergeIndexFile.substring(0, validMergeIndexFile.lastIndexOf(File.separator) + 1);
      mergedAndInvalidIndexFiles
          .addAll(carbonMergeFileToIndexFilesMap.get(validMergeIndexFile).stream()
          .map(file -> segmentPath + file).collect(Collectors.toSet()));
    }
    if (mergeIndexFileNames.size() == 0 && indexFiles.size() > 1) {
      // if more than one index file present with different timestamps, then stale/invalid
      // data is present. Latest one is considered as valid.
      Long validFile = indexFiles.stream()
          .map(file -> Long.parseLong(CarbonTablePath.DataFileUtil.getTimeStampFromFileName(file)))
          .max(Long::compareTo).get();
      mergedAndInvalidIndexFiles.addAll(
          indexFiles.stream().filter(file -> !file.contains(validFile.toString()))
              .collect(Collectors.toSet()));
    }
    return mergedAndInvalidIndexFiles;
  }

  /**
   * Get valid index files excluding the invalid ones.
   * This method is called while calculating index size using segment directory.
   */
  public static Object getValidCarbonIndexFiles(Object carbonFiles) throws IOException {
    // For supporting local or default file system.
    if (carbonFiles instanceof CarbonFile[]) {
      Set<String> mergedAndInvalidIndexFiles = getInvalidAndMergedIndexFiles(
          Arrays.stream((CarbonFile[]) carbonFiles).map(file -> file.getAbsolutePath())
              .collect(Collectors.toList()));
      if (!mergedAndInvalidIndexFiles.isEmpty()) {
        return Arrays.stream((CarbonFile[]) carbonFiles)
            .filter(file -> !mergedAndInvalidIndexFiles.contains(file.getAbsolutePath()))
            .toArray(CarbonFile[]::new);
      }
    }
    // For supporting HDFS/S3/other file systems.
    else if (carbonFiles instanceof FileStatus[]) {
      Set<String> mergedAndInvalidIndexFiles = getInvalidAndMergedIndexFiles(
          Arrays.stream((FileStatus[]) carbonFiles).map(file -> file.getPath().toString())
              .collect(Collectors.toList()));
      if (!mergedAndInvalidIndexFiles.isEmpty()) {
        return Arrays.stream((FileStatus[]) carbonFiles)
            .filter(file -> !mergedAndInvalidIndexFiles.contains(file.getPath().toString()))
            .toArray(FileStatus[]::new);
      }
    }
    return carbonFiles;
  }

  /**
   * Move the loaded data from source folder to destination folder.
   */
  private static void moveFromTempFolder(String source, String dest) {

    CarbonFile oldFolder = FileFactory.getCarbonFile(source);
    CarbonFile[] oldFiles = oldFolder.listFiles();
    for (CarbonFile file : oldFiles) {
      file.renameForce(dest + CarbonCommonConstants.FILE_SEPARATOR + file.getName());
    }
    oldFolder.delete();
  }

  /**
   * Writes the segment file in json format
   * @param segmentFile
   * @param path
   * @throws IOException
   */
  public static void writeSegmentFile(SegmentFile segmentFile, String path) throws IOException {
    AtomicFileOperations fileWrite =
        AtomicFileOperationFactory.getAtomicFileOperations(path);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(segmentFile);
      brWriter.write(metadataInstance);
      brWriter.flush();
    } catch (IOException ie) {
      LOGGER.error("Error message: " + ie.getLocalizedMessage());
      fileWrite.setFailed();
      throw ie;
    } finally {
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }
  }

  /**
   * Merge all segment files in a segment to single file.
   *
   * @throws IOException
   */
  public static SegmentFile mergeSegmentFiles(String readPath, String mergeFileName,
      String writePath)
      throws IOException {
    CarbonFile[] segmentFiles = getSegmentFiles(readPath);
    if (segmentFiles != null && segmentFiles.length > 0) {
      SegmentFile segmentFile = mergeSegmentFiles(mergeFileName, writePath, segmentFiles);
      FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(readPath));
      return segmentFile;
    }
    return null;
  }

  public static SegmentFile mergeSegmentFiles(String mergeFileName, String writePath,
      CarbonFile[] segmentFiles) throws IOException {
    SegmentFile segmentFile = null;
    for (CarbonFile file : segmentFiles) {
      SegmentFile localSegmentFile = readSegmentFile(file.getAbsolutePath());
      if (segmentFile == null && localSegmentFile != null) {
        segmentFile = localSegmentFile;
      }
      if (localSegmentFile != null) {
        segmentFile = segmentFile.merge(localSegmentFile);
      }
    }
    if (segmentFile != null) {
      String path = writePath + "/" + mergeFileName + CarbonTablePath.SEGMENT_EXT;
      writeSegmentFile(segmentFile, path);
    }
    return segmentFile;
  }

  public static String getSegmentFilePath(String tablePath, String segmentFileName) {
    return CarbonTablePath.getSegmentFilesLocation(tablePath) +
        CarbonCommonConstants.FILE_SEPARATOR + segmentFileName;
  }

  /**
   * This API will update the segmentFile of a passed segment.
   *
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean updateTableStatusFile(CarbonTable carbonTable, String segmentId,
      String segmentFile, String tableId, SegmentFileStore segmentFileStore) throws IOException {
    return updateTableStatusFile(carbonTable, segmentId, segmentFile, tableId, segmentFileStore,
        null);
  }

  /**
   * This API will update the table status file with specified segment.
   *
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean updateTableStatusFile(CarbonTable carbonTable, String segmentId,
      String segmentFile, String tableId, SegmentFileStore segmentFileStore,
      SegmentStatus segmentStatus) throws IOException {
    boolean status = false;
    String tablePath = carbonTable.getTablePath();
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(tablePath);
    if (!FileFactory.isFileExist(tableStatusPath)) {
      return status;
    }
    String metadataPath = CarbonTablePath.getMetadataPath(tablePath);
    AbsoluteTableIdentifier absoluteTableIdentifier =
        AbsoluteTableIdentifier.from(tablePath, null, null, tableId);
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
        LOGGER.info("Acquired lock for table path" + tablePath + " for table status update");
        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metadataPath);

        for (LoadMetadataDetails detail : listOfLoadFolderDetailsArray) {
          // if the segments is in the list of marked for delete then update the status.
          if (segmentId.equals(detail.getLoadName())) {
            detail.setLoadEndTime(System.currentTimeMillis());
            detail.setSegmentFile(segmentFile);
            if (segmentStatus != null) {
              HashMap<String, Long> dataSizeAndIndexSize =
                  CarbonUtil.getDataSizeAndIndexSize(segmentFileStore, detail.isCarbonFormat());
              detail.setDataSize(
                  dataSizeAndIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_DATA_SIZE)
                      .toString());
              detail.setIndexSize(
                  dataSizeAndIndexSize.get(CarbonCommonConstants.CARBON_TOTAL_INDEX_SIZE)
                      .toString());
              detail.setSegmentStatus(segmentStatus);
            } else {
              detail.setIndexSize(String.valueOf(CarbonUtil
                  .getCarbonIndexSize(segmentFileStore, segmentFileStore.getLocationMap())));
            }
            break;
          }
        }

        SegmentStatusManager
            .writeLoadDetailsIntoFile(tableStatusPath, listOfLoadFolderDetailsArray);
        // clear index cache for the segmentId for which the table status file is getting updated
        clearBlockIndexCache(carbonTable, segmentId);
        status = true;
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status update for table path " + tablePath);
      }

    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status update" + tablePath);
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + tablePath + " during table status update");
      }
    }
    return status;
  }

  /**
   * After updating table status file clear the index cache for all segmentId's on which
   * index is being created because flows like merge index file creation involves modification of
   * segment file and once segment file is modified the cache for that segment need to be cleared
   * otherwise the old cache will be used which is stale
   *
   * @param carbonTable
   * @param segmentId
   */
  public static void clearBlockIndexCache(CarbonTable carbonTable, String segmentId) {
    TableIndex defaultIndex = IndexStoreManager.getInstance().getDefaultIndex(carbonTable);
    LOGGER.info(
        "clearing cache while updating segment file entry in table status file for segmentId: "
            + segmentId);
    defaultIndex.getIndexFactory().clear(segmentId);
  }

  private static CarbonFile[] getSegmentFiles(String segmentPath) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    if (carbonFile.exists()) {
      return carbonFile.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          return file.getName().endsWith(CarbonTablePath.SEGMENT_EXT);
        }
      });
    }
    return null;
  }

  /**
   * It provides segment file only for the partitions which has physical index files.
   *
   * @param partitionSpecs
   */
  public static SegmentFile getSegmentFileForPhysicalDataPartitions(String tablePath,
      List<PartitionSpec> partitionSpecs) throws IOException {
    SegmentFile segmentFile = null;
    for (PartitionSpec spec : partitionSpecs) {
      String location = spec.getLocation().toString();
      CarbonFile carbonFile = FileFactory.getCarbonFile(location);

      CarbonFile[] listFiles =
          carbonFile.listFiles(file -> CarbonTablePath.isCarbonIndexFile(file.getAbsolutePath()));
      if (listFiles != null && listFiles.length > 0) {
        Set<String> mergedAndInvalidIndexFiles = getInvalidAndMergedIndexFiles(
            Arrays.stream(listFiles).map(CarbonFile::getAbsolutePath).collect(Collectors.toList()));
        // In add hive partition after merge index is written, delete index files that are merged.
        for (CarbonFile indexFile : listFiles) {
          if (mergedAndInvalidIndexFiles.contains(indexFile.getAbsolutePath())) {
            indexFile.delete();
          }
        }
        CarbonFile[] carbonIndexFiles = listFiles;
        if (!mergedAndInvalidIndexFiles.isEmpty()) {
          carbonIndexFiles = Arrays.stream(listFiles)
              .filter(file -> !mergedAndInvalidIndexFiles.contains(file.getAbsolutePath()))
              .toArray(CarbonFile[]::new);
        }
        boolean isRelative = false;
        if (location.startsWith(tablePath)) {
          location = location.substring(tablePath.length());
          isRelative = true;
        }
        SegmentFile localSegmentFile = new SegmentFile();
        FolderDetails folderDetails = new FolderDetails();
        folderDetails.setRelative(isRelative);
        folderDetails.setPartitions(spec.getPartitions());
        folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
        setIndexFileNamesToFolderDetails(folderDetails, carbonIndexFiles);
        localSegmentFile.addPath(location, folderDetails);
        if (segmentFile == null) {
          segmentFile = localSegmentFile;
        } else {
          segmentFile = segmentFile.merge(localSegmentFile);
        }
      }
    }
    return segmentFile;
  }

  /**
   * This method reads the segment file which is written in json format
   *
   * @param segmentFilePath
   * @return
   */
  public static SegmentFile readSegmentFile(String segmentFilePath) throws IOException {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    SegmentFile segmentFile;
    AtomicFileOperations fileOperation =
        AtomicFileOperationFactory.getAtomicFileOperations(segmentFilePath);

    try {
      if (!FileFactory.isFileExist(segmentFilePath)) {
        return null;
      }
      dataInputStream = fileOperation.openForRead();
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      segmentFile = gsonObjectToRead.fromJson(buffReader, SegmentFile.class);
    } finally {
      if (inStream != null) {
        CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
      }
    }

    return segmentFile;
  }

  /**
   * Reads segment file.
   */
  private SegmentFile readSegment(String tablePath, String segmentFileName) throws IOException {
    String segmentFilePath =
        CarbonTablePath.getSegmentFilesLocation(tablePath) + CarbonCommonConstants.FILE_SEPARATOR
            + segmentFileName;
    return readSegmentFile(segmentFilePath);
  }

  public String getTablePath() {
    return tablePath;
  }

  /**
   * Gets all the index files and related carbondata files from this segment. First user needs to
   * call @readIndexFiles method before calling it.
   * @return
   */
  public Map<String, List<String>> getIndexFilesMap() {
    return indexFilesMap;
  }

  /**
   * Reads all index files which are located in this segment. First user needs to call
   * @readSegment method before calling it.
   * @throws IOException
   */
  public void readIndexFiles(Configuration configuration) throws IOException {
    readIndexFiles(SegmentStatus.SUCCESS, false, configuration);
  }

  public SegmentFile getSegmentFile() {
    return segmentFile;
  }

  /**
   * Reads all index files as per the status of the file. In case of @ignoreStatus is true it just
   * reads all index files
   * @param status
   * @param ignoreStatus
   * @throws IOException
   */
  public List<String> readIndexFiles(SegmentStatus status, boolean ignoreStatus,
      Configuration configuration) throws IOException {
    if (indexFilesMap != null) {
      return new ArrayList<>();
    }
    List<String> indexOrMergeFiles = new ArrayList<>();
    SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
    indexFilesMap = new HashMap<>();
    indexFileStore.readAllIIndexOfSegment(this.segmentFile, tablePath, status, ignoreStatus);
    Map<String, byte[]> carbonIndexMap = indexFileStore.getCarbonIndexMapWithFullPath();
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter(configuration);
    for (Map.Entry<String, byte[]> entry : carbonIndexMap.entrySet()) {
      List<DataFileFooter> indexInfo =
          fileFooterConverter.getIndexInfo(entry.getKey(), entry.getValue());
      // carbon index file stores blocklets so block filename will be duplicated, use set to remove
      // duplicates
      Set<String> blocks = new LinkedHashSet<>();
      for (DataFileFooter footer : indexInfo) {
        blocks.add(footer.getBlockInfo().getFilePath());
      }
      indexFilesMap.put(entry.getKey(), new ArrayList<>(blocks));
      boolean added = false;
      for (Map.Entry<String, List<String>> mergeFile : indexFileStore
          .getCarbonMergeFileToIndexFilesMap().entrySet()) {
        if (mergeFile.getValue().contains(entry.getKey()
            .substring(entry.getKey().lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR) + 1))) {
          indexOrMergeFiles.add(mergeFile.getKey());
          added = true;
          break;
        }
      }
      if (!added) {
        indexOrMergeFiles.add(entry.getKey());
      }
    }
    return indexOrMergeFiles;
  }

  /**
   * Reads all index files and get the schema of each index file
   * @throws IOException
   */
  public static Map<String, List<ColumnSchema>> getSchemaFiles(SegmentFile segmentFile,
      String tablePath) throws IOException {
    Map<String, List<ColumnSchema>> schemaMap = new HashMap<>();
    if (segmentFile == null) {
      return schemaMap;
    }
    SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
    indexFileStore.readAllIIndexOfSegment(segmentFile, tablePath, SegmentStatus.SUCCESS, true);
    Map<String, byte[]> carbonIndexMap = indexFileStore.getCarbonIndexMapWithFullPath();
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    for (Map.Entry<String, byte[]> entry : carbonIndexMap.entrySet()) {
      List<DataFileFooter> indexInfo = fileFooterConverter
          .getIndexInfo(entry.getKey(), entry.getValue());
      if (indexInfo.size() > 0) {
        schemaMap.put(entry.getKey(), indexInfo.get(0).getColumnInTable());
      }
    }
    return schemaMap;
  }

  /**
   * Gets all index files from this segment
   * @return
   */
  public Set<String> getIndexFiles() {
    Set<String> indexFiles = new HashSet<>();
    if (segmentFile != null) {
      for (Map.Entry<String, FolderDetails> entry : getLocationMap().entrySet()) {
        String location = entry.getKey();
        if (entry.getValue().isRelative) {
          location = tablePath + location;
        }
        if (entry.getValue().status.equals(SegmentStatus.SUCCESS.getMessage())) {
          for (String indexFile : entry.getValue().getFiles()) {
            indexFiles.add(location + CarbonCommonConstants.FILE_SEPARATOR + indexFile);
          }
        }
      }
    }
    return indexFiles;
  }

  /**
   * Gets all index files from this segment
   * @return
   */
  public Map<String, String> getIndexAndMergeFiles() throws IOException {
    Map<String, String> indexFiles = new HashMap<>();
    if (segmentFile != null) {
      for (Map.Entry<String, FolderDetails> entry : getLocationMap().entrySet()) {
        String location = entry.getKey();
        if (entry.getValue().isRelative) {
          if (location.equals("/")) {
            // in case of flat folder, the relative segment location is '/',
            // so don't append it as we again add file separator for file names.
            location = tablePath;
          } else {
            location = tablePath + location;
          }
        }
        if (entry.getValue().status.equals(SegmentStatus.SUCCESS.getMessage())) {
          String mergeFileName = entry.getValue().getMergeFileName();
          if (null != mergeFileName) {
            indexFiles.put(location + CarbonCommonConstants.FILE_SEPARATOR + mergeFileName,
                entry.getValue().mergeFileName);
          }
          Set<String> files = entry.getValue().getFiles();
          if (null != files && !files.isEmpty()) {
            for (String indexFile : files) {
              String indexFilePath = location + CarbonCommonConstants.FILE_SEPARATOR + indexFile;
              // In the 1.3 store, files field contain the carbonindex files names
              // even if they are merged to a carbonindexmerge file. In that case we have to check
              // for the physical existence of the file to decide
              // on whether it is already merged or not.
              if (FileFactory.isFileExist(indexFilePath)) {
                indexFiles.put(indexFilePath, null);
              }
            }
          }
        }
      }
    }
    return indexFiles;
  }

  /**
   * Gets all carbon index files from this segment
   * @return
   */
  public List<CarbonFile> getIndexCarbonFiles() {
    Set<String> indexFiles = getIndexFiles();
    List<CarbonFile> carbonFiles = new ArrayList<>();
    for (String indexFile : indexFiles) {
      CarbonFile carbonFile = FileFactory.getCarbonFile(indexFile);
      if (carbonFile.exists()) {
        carbonFiles.add(carbonFile);
      }
    }
    return carbonFiles;
  }

  /**
   * Drops the partition related files from the segment file of the segment and writes
   * to a new file. First iterator over segment file and check the path it needs to be dropped.
   * And update the status with delete if it found.
   *
   * @param uniqueId
   * @throws IOException
   */
  public void dropPartitions(String segmentNo, List<String> partitionLocations,
      String uniqueId, List<String> toBeDeletedSegments, List<String> toBeUpdatedSegments)
      throws IOException {
    boolean updateSegment = false;
    for (Map.Entry<String, FolderDetails> entry : segmentFile.getLocationMap().entrySet()) {
      String location = entry.getKey();
      if (entry.getValue().isRelative) {
        location = tablePath + CarbonCommonConstants.FILE_SEPARATOR + location;
      }
      Path path = new Path(location);
      // Update the status to delete if path equals
      if (null != partitionLocations) {
        for (String partitionLocation : partitionLocations) {
          if (path.toString().equals(partitionLocation)) {
            entry.getValue().setStatus(SegmentStatus.MARKED_FOR_DELETE.getMessage());
            updateSegment = true;
            break;
          }
        }
      }
    }
    if (updateSegment) {
      String writePath = CarbonTablePath.getSegmentFilesLocation(tablePath);
      writePath =
          writePath + CarbonCommonConstants.FILE_SEPARATOR +
              SegmentFileStore.genSegmentFileName(segmentNo,  String.valueOf(uniqueId))
              + CarbonTablePath.SEGMENT_EXT;
      writeSegmentFile(segmentFile, writePath);
    }
    // Check whether we can completely remove the segment.
    boolean deleteSegment = true;
    for (Map.Entry<String, FolderDetails> entry : segmentFile.getLocationMap().entrySet()) {
      if (entry.getValue().getStatus().equals(SegmentStatus.SUCCESS.getMessage())) {
        deleteSegment = false;
        break;
      }
    }
    if (deleteSegment) {
      toBeDeletedSegments.add(segmentNo);
    }
    if (updateSegment) {
      toBeUpdatedSegments.add(segmentNo);
    }
  }

  /**
   * Update the table status file with the dropped partitions information
   *
   * @param carbonTable
   * @param uniqueId
   * @param toBeUpdatedSegments
   * @param toBeDeleteSegments
   * @throws IOException
   */
  public static void commitDropPartitions(CarbonTable carbonTable, String uniqueId,
      List<String> toBeUpdatedSegments, List<String> toBeDeleteSegments,
      String uuid) throws IOException {
    if (toBeDeleteSegments.size() > 0 || toBeUpdatedSegments.size() > 0) {
      Set<Segment> segmentSet = new HashSet<>(
          new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier())
              .getValidAndInvalidSegments(carbonTable.isMV()).getValidSegments());
      CarbonUpdateUtil.updateTableMetadataStatus(segmentSet, carbonTable, uniqueId,
          true, false,
          Segment.toSegmentList(toBeDeleteSegments, null),
          Segment.toSegmentList(toBeUpdatedSegments, null), uuid);
    }
  }

  /**
   * Clean up invalid data after drop partition in partial segments of table
   *
   * @param table the table need to cleaned stale data
   * @param details the segments need to cleaned stale data
   * @param partitionSpecs the partitions need to cleaned stale data
   * @param forceDelete Whether it should be deleted force or check the time for an hour creation
   *                    to delete data.
   * @throws IOException
   */
  public static void cleanSegments(CarbonTable table,
      LoadMetadataDetails[] details,
      List<PartitionSpec> partitionSpecs,
      boolean forceDelete) throws IOException {
    // scan through each segment.
    for (LoadMetadataDetails segment : details) {
      // if this segment is valid then only we will go for deletion of related
      // dropped partition files. if the segment is mark for delete or compacted then any way
      // it will get deleted.

      if ((segment.getSegmentStatus() == SegmentStatus.SUCCESS
          || segment.getSegmentStatus() == SegmentStatus.LOAD_PARTIAL_SUCCESS)
          && segment.getSegmentFile() != null) {
        List<String> toBeDeletedIndexFiles = new ArrayList<>();
        List<String> toBeDeletedDataFiles = new ArrayList<>();
        // take the list of files from this segment.
        SegmentFileStore fileStore =
            new SegmentFileStore(table.getTablePath(), segment.getSegmentFile());
        List<String> indexOrMergeFiles = fileStore
            .readIndexFiles(SegmentStatus.MARKED_FOR_DELETE, false, FileFactory.getConfiguration());
        if (forceDelete) {
          deletePhysicalPartition(
              partitionSpecs,
              fileStore.getIndexFilesMap(),
              indexOrMergeFiles,
              table.getTablePath());
        }
        for (Map.Entry<String, List<String>> entry : fileStore.indexFilesMap.entrySet()) {
          String indexFile = entry.getKey();
          // Check the partition information in the partition mapper
          Long fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(indexFile
              .substring(indexFile.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                  indexFile.length() - CarbonTablePath.INDEX_FILE_EXT.length()));
          if (TrashUtil.isDataOutsideTrashIsExpired(fileTimestamp) || forceDelete) {
            // Add the corresponding carbondata files to the delete list.
            toBeDeletedDataFiles.addAll(entry.getValue());
          }
        }
        for (String indexFile : indexOrMergeFiles) {
          Long fileTimestamp = 0L;
          if (indexFile.endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
            fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(indexFile
                .substring(indexFile.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                    indexFile.length() - CarbonTablePath.INDEX_FILE_EXT.length()));
          } else if (indexFile.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
            fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(indexFile
                .substring(indexFile.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1,
                    indexFile.length() - CarbonTablePath.MERGE_INDEX_FILE_EXT.length()));
          }
          if (TrashUtil.isDataOutsideTrashIsExpired(fileTimestamp) || forceDelete) {
            toBeDeletedIndexFiles.add(indexFile);
          }
        }
        if (toBeDeletedIndexFiles.size() > 0) {
          for (String dataFile : toBeDeletedIndexFiles) {
            FileFactory.deleteFile(dataFile);
          }
          for (String dataFile : toBeDeletedDataFiles) {
            FileFactory.deleteFile(dataFile);
          }
        }
      }
    }
  }

  /**
   * Clean up invalid data after drop partition in all segments of table
   *
   * @param table the table need to clean stale data
   * @param partitionSpecs the partitionSpecs need to clean
   * @param forceDelete Whether it should be deleted force or check the time for an hour creation
   *                    to delete data.
   * @throws IOException
   */
  public static void cleanSegments(CarbonTable table, List<PartitionSpec> partitionSpecs,
      boolean forceDelete) throws IOException {
    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(table.getMetadataPath());
    cleanSegments(table, details, partitionSpecs, forceDelete);
  }

  /**
   * Clean up invalid data after drop partition in partial segments of table
   *
   * @param table the table need to clean stale data
   * @param segments the segments need to clean stale data
   * @param forceDelete Whether it should be deleted force or check the time for an hour creation
   *                    to delete data.
   * @throws IOException
   */
  public static void cleanSegments(CarbonTable table,
      Set<String> segments,
      List<PartitionSpec> partitionSpecs,
      boolean forceDelete) throws IOException {
    LoadMetadataDetails[] details = Arrays
        .stream(SegmentStatusManager.readLoadMetadata(table.getMetadataPath()))
        .filter(detail -> segments.contains(detail.getLoadName()))
        .toArray(LoadMetadataDetails[]::new);
    cleanSegments(table, details, partitionSpecs, forceDelete);
  }

  public static void deleteSegmentFile(String tablePath, Segment segment) throws Exception {
    String segmentFilePath =
        CarbonTablePath.getSegmentFilePath(tablePath, segment.getSegmentFileName());
    // Deletes the physical segment file
    FileFactory.deleteFile(segmentFilePath);
  }

  /**
   * Deletes the segment file and its physical files like partition folders from disk
   * @param tablePath
   * @param segment
   * @param partitionSpecs
   * @throws IOException
   */
  public static void deleteSegment(String tablePath, Segment segment,
      List<PartitionSpec> partitionSpecs,
      SegmentUpdateStatusManager updateStatusManager) throws Exception {
    SegmentFileStore fileStore = new SegmentFileStore(tablePath, segment.getSegmentFileName());
    List<String> indexOrMergeFiles = fileStore.readIndexFiles(SegmentStatus.SUCCESS, true,
        FileFactory.getConfiguration());
    Map<String, List<String>> indexFilesMap = fileStore.getIndexFilesMap();
    List<String> deletedFiles = new ArrayList<>();
    for (String indexFilePath : indexOrMergeFiles) {
      FileFactory.deleteFile(indexFilePath);
      deletedFiles.add(indexFilePath);
    }
    for (Map.Entry<String, List<String>> entry : indexFilesMap.entrySet()) {
      for (String file : entry.getValue()) {
        String[] deltaFilePaths =
            updateStatusManager.getDeleteDeltaFilePath(file, segment.getSegmentNo());
        for (String deltaFilePath : deltaFilePaths) {
          FileFactory.deleteFile(deltaFilePath);
          deletedFiles.add(deltaFilePath);
        }
        FileFactory.deleteFile(file);
        deletedFiles.add(file);
      }
    }
    LOGGER.info("Deleted the files: " + String.join(",", deletedFiles) + " on clean" +
        " files operation");
    deletePhysicalPartition(partitionSpecs, indexFilesMap, indexOrMergeFiles, tablePath);
  }

  /**
   * If partition specs are available, then check the location map for any index file path which is
   * not present in the partitionSpecs. If found then delete that index file.
   * If the partition directory is empty, then delete the directory also.
   * If partition specs are null, then directly delete parent directory in locationMap.
   */
  private static void deletePhysicalPartition(List<PartitionSpec> partitionSpecs,
      Map<String, List<String>> locationMap, List<String> indexOrMergeFiles, String tablePath) {
    LOGGER.info("Deleting files: ");
    for (String indexOrMergeFile : indexOrMergeFiles) {
      if (null != partitionSpecs) {
        Path location = new Path(indexOrMergeFile);
        boolean exists = pathExistsInPartitionSpec(partitionSpecs, location);
        if (!exists) {
          FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location.toString()));
          LOGGER.info(location.toString());
        }
      } else {
        Path location = new Path(indexOrMergeFile);
        FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location.toString()));
        LOGGER.info(location.toString());
      }
    }
    for (Map.Entry<String, List<String>> entry : locationMap.entrySet()) {
      if (partitionSpecs != null) {
        Path location = new Path(entry.getKey());
        boolean exists = pathExistsInPartitionSpec(partitionSpecs, location);
        if (!exists) {
          FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location.toString()));
          for (String carbonDataFile : entry.getValue()) {
            FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(carbonDataFile));
          }
        }
        CarbonFile path = FileFactory.getCarbonFile(location.getParent().toString());
        deleteEmptyPartitionFolders(path);
      } else {
        Path location = new Path(entry.getKey()).getParent();
        // delete the segment folder
        CarbonFile segmentPath = FileFactory.getCarbonFile(location.toString());
        if (null != segmentPath && segmentPath.exists() &&
            !new Path(tablePath).equals(new Path(segmentPath.getAbsolutePath()))) {
          FileFactory.deleteAllCarbonFilesOfDir(segmentPath);
        }
      }
    }
  }

  /**
   * This method deletes the directories recursively if there are no files under corresponding
   * folder.
   * Ex: If partition folder is year=2015, month=2,day=5 and drop partition is day=5, it will delete
   * till year partition folder if there are no other folder or files present under each folder till
   * year partition
   */
  public static void deleteEmptyPartitionFolders(CarbonFile path) {
    if (path != null && path.listFiles().length == 0) {
      FileFactory.deleteAllCarbonFilesOfDir(path);
      Path parentsLocation = new Path(path.getAbsolutePath()).getParent();
      deleteEmptyPartitionFolders(
          FileFactory.getCarbonFile(parentsLocation.toString()));
    }
  }

  public static boolean pathExistsInPartitionSpec(List<PartitionSpec> partitionSpecs,
      Path partitionPath) {
    for (PartitionSpec spec : partitionSpecs) {
      if (spec.getLocation().equals(partitionPath)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Get the partition specs of the segment
   * @param segmentId
   * @param tablePath
   * @return
   * @throws IOException
   */
  public static List<PartitionSpec> getPartitionSpecs(String segmentId, String tablePath,
      String segmentFilePath, String loadStartTime) throws IOException {
    SegmentFileStore fileStore = new SegmentFileStore(tablePath, segmentFilePath);
    List<PartitionSpec> partitionSpecs = fileStore.getPartitionSpecs();
    for (PartitionSpec spec : partitionSpecs) {
      spec.setUuid(segmentId + "_" + loadStartTime);
    }
    return partitionSpecs;
  }

  /**
   * Get the partition specs of the segment
   * @param segmentId
   * @param tablePath
   * @return
   * @throws IOException
   */
  public static List<PartitionSpec> getPartitionSpecs(String segmentId, String tablePath,
      LoadMetadataDetails[] details)
      throws IOException {
    LoadMetadataDetails segEntry = null;
    for (LoadMetadataDetails entry : details) {
      if (entry.getLoadName().equals(segmentId)) {
        segEntry = entry;
        break;
      }
    }
    if (segEntry != null && segEntry.getSegmentFile() != null) {
      SegmentFileStore fileStore = new SegmentFileStore(tablePath, segEntry.getSegmentFile());
      List<PartitionSpec> partitionSpecs = fileStore.getPartitionSpecs();
      for (PartitionSpec spec : partitionSpecs) {
        spec.setUuid(segmentId + "_" + segEntry.getLoadStartTime());
      }
      return partitionSpecs;
    }
    return new ArrayList<>();
  }

  /**
   * Move the loaded data from temp folder to respective partition folder.
   * @param segmentFile
   * @param tmpFolder
   * @param tablePath
   */
  public static void moveFromTempFolder(SegmentFile segmentFile, String tmpFolder,
      String tablePath) {

    for (Map.Entry<String, SegmentFileStore.FolderDetails> entry : segmentFile.getLocationMap()
        .entrySet()) {
      String location = entry.getKey();
      if (entry.getValue().isRelative()) {
        location = tablePath + CarbonCommonConstants.FILE_SEPARATOR + location;
      }
      CarbonFile oldFolder =
          FileFactory.getCarbonFile(location + CarbonCommonConstants.FILE_SEPARATOR + tmpFolder);
      CarbonFile[] oldFiles = oldFolder.listFiles();
      for (CarbonFile file : oldFiles) {
        file.renameForce(location + CarbonCommonConstants.FILE_SEPARATOR + file.getName());
      }
      oldFolder.delete();
    }
  }

  /**
   * Remove temp stage folder in case of job aborted.
   *
   * @param locationMap
   * @param tmpFolder
   * @param tablePath
   */
  public static void removeTempFolder(Map<String, FolderDetails> locationMap, String tmpFolder,
      String tablePath) {
    if (locationMap == null) {
      return;
    }
    for (Map.Entry<String, SegmentFileStore.FolderDetails> entry : locationMap.entrySet()) {
      String location = entry.getKey();
      if (entry.getValue().isRelative()) {
        location = tablePath + CarbonCommonConstants.FILE_SEPARATOR + location;
      }
      CarbonFile oldFolder =
          FileFactory.getCarbonFile(location + CarbonCommonConstants.FILE_SEPARATOR + tmpFolder);
      if (oldFolder.exists()) {
        FileFactory.deleteAllCarbonFilesOfDir(oldFolder);
      }
    }
  }

  /**
   * Returns content of segment
   * @return
   */
  public Map<String, FolderDetails> getLocationMap() {
    if (segmentFile == null) {
      return new HashMap<>();
    }
    return segmentFile.getLocationMap();
  }

  /**
   * get the current partition specs of this segment
   * @return
   */
  public List<PartitionSpec> getPartitionSpecs() {
    List<PartitionSpec> partitionSpecs = new ArrayList<>();
    if (segmentFile != null) {
      for (Map.Entry<String, FolderDetails> entry : segmentFile.getLocationMap().entrySet()) {
        String location = entry.getKey();
        if (entry.getValue().isRelative) {
          location = tablePath + CarbonCommonConstants.FILE_SEPARATOR + location;
        }
        if (entry.getValue().getStatus().equals(SegmentStatus.SUCCESS.getMessage())) {
          partitionSpecs.add(new PartitionSpec(entry.getValue().partitions, location));
        }
      }
    }
    return partitionSpecs;
  }

  /**
   * It contains the segment information like location, partitions and related index files
   */
  public static class SegmentFile implements Serializable {

    private static final long serialVersionUID = 3582245668420401089L;

    /**
     * mapping of index file parent folder to the index file folder info
     */
    private Map<String, FolderDetails> locationMap;

    /**
     * Segment option properties
     */
    private Map<String, String> options;

    /**
     * Segment metadata information such as column_id, min-max, alter properties
     */
    private String segmentMetaDataInfo;

    public SegmentFile() {
      locationMap = new HashMap<>();
    }

    public SegmentFile merge(SegmentFile segmentFile) throws IOException {
      if (this == segmentFile) {
        return this;
      }
      if (locationMap != null && segmentFile.locationMap != null) {
        for (Map.Entry<String, FolderDetails> entry : segmentFile.locationMap.entrySet()) {
          FolderDetails folderDetails = locationMap.get(entry.getKey());
          if (folderDetails != null) {
            folderDetails.merge(entry.getValue());
          } else {
            locationMap.put(entry.getKey(), entry.getValue());
          }
        }
        if (segmentMetaDataInfo != null) {
          SegmentMetaDataInfo currentSegmentMetaDataInfo =
              (SegmentMetaDataInfo) ObjectSerializationUtil
                  .convertStringToObject(segmentMetaDataInfo);
          if (null != segmentFile.getSegmentMetaDataInfo()) {
            // get updated segmentColumnMetaDataInfo based on comparing block min-max values
            Map<String, SegmentColumnMetaDataInfo> previousBlockColumnMetaDataInfo =
                segmentFile.getSegmentMetaDataInfo().getSegmentColumnMetaDataInfoMap();
            for (Map.Entry<String, SegmentColumnMetaDataInfo> entry :
                previousBlockColumnMetaDataInfo.entrySet()) {
              if (currentSegmentMetaDataInfo.getSegmentColumnMetaDataInfoMap()
                  .containsKey(entry.getKey())) {
                SegmentColumnMetaDataInfo currentBlockMinMaxInfo =
                    currentSegmentMetaDataInfo.getSegmentColumnMetaDataInfoMap()
                        .get(entry.getKey());
                byte[] blockMaxValue = SegmentMetaDataInfoStats.getInstance()
                    .compareAndUpdateMinMax(currentBlockMinMaxInfo.getColumnMaxValue(),
                        entry.getValue().getColumnMaxValue(), false);
                byte[] blockMinValue = SegmentMetaDataInfoStats.getInstance()
                    .compareAndUpdateMinMax(currentBlockMinMaxInfo.getColumnMinValue(),
                        entry.getValue().getColumnMinValue(), true);
                currentSegmentMetaDataInfo.getSegmentColumnMetaDataInfoMap().get(entry.getKey())
                    .setColumnMaxValue(blockMaxValue);
                currentSegmentMetaDataInfo.getSegmentColumnMetaDataInfoMap().get(entry.getKey())
                    .setColumnMinValue(blockMinValue);
              }
            }
          }
          segmentMetaDataInfo =
              ObjectSerializationUtil.convertObjectToString(currentSegmentMetaDataInfo);
        }
      }
      if (locationMap == null) {
        locationMap = segmentFile.locationMap;
      }
      return this;
    }

    public Map<String, FolderDetails> getLocationMap() {
      return locationMap;
    }

    /**
     * Add index file parent folder and the index file folder info
     */
    void addPath(String path, FolderDetails details) {
      locationMap.put(path, details);
    }

    public Map<String, String> getOptions() {
      return options;
    }

    public void setOptions(Map<String, String> options) {
      this.options = options;
    }

    public SegmentMetaDataInfo getSegmentMetaDataInfo() {
      SegmentMetaDataInfo newSegmentMetaDataInfo = null;
      try {
        newSegmentMetaDataInfo = (SegmentMetaDataInfo) ObjectSerializationUtil
            .convertStringToObject(segmentMetaDataInfo);
      } catch (IOException e) {
        LOGGER.error("Error while getting segment metadata info");
      }
      return newSegmentMetaDataInfo;
    }

    public void setSegmentMetaDataInfo(SegmentMetaDataInfo segmentMetaDataInfo) throws IOException {
      this.segmentMetaDataInfo = ObjectSerializationUtil.convertObjectToString(
          segmentMetaDataInfo);
    }
  }

  public static SegmentFile createSegmentFile(String partitionPath, FolderDetails folderDetails) {
    SegmentFile segmentFile = new SegmentFile();
    segmentFile.addPath(partitionPath, folderDetails);
    return segmentFile;
  }

  /**
   * Represents one partition folder
   */
  public static class FolderDetails implements Serializable {

    private static final long serialVersionUID = 501021868886928553L;

    /**
     * Based on isRelative variable:
     * 1. if it is relative, it is relative path to the table path, for all index files
     * 2. if it is not relative, it is the full path of all index files
     */
    private Set<String> files = new HashSet<>();

    /**
     * all partition names
     */
    private List<String> partitions = new ArrayList<>();

    /**
     * status for the partition, success or mark for delete
     */
    private String status;

    /**
     * file name for merge index file in this folder
     */
    private String mergeFileName;

    /**
     * true if it is relative path, for example, if user give partition location when
     * adding the partition, it will be false
     */
    private boolean isRelative;

    public FolderDetails merge(FolderDetails folderDetails) {
      if (this == folderDetails || folderDetails == null) {
        return this;
      }
      if (folderDetails.files != null) {
        files.addAll(folderDetails.files);
      }
      if (files == null) {
        files = folderDetails.files;
      }
      partitions = folderDetails.partitions;
      return this;
    }

    public Set<String> getFiles() {
      return files;
    }

    public void setFiles(Set<String> files) {
      this.files = files;
    }

    public List<String> getPartitions() {
      return partitions;
    }

    public void setPartitions(List<String> partitions) {
      this.partitions = partitions;
    }

    public boolean isRelative() {
      return isRelative;
    }

    public void setRelative(boolean relative) {
      isRelative = relative;
    }

    public String getMergeFileName() {
      return mergeFileName;
    }

    public void setMergeFileName(String mergeFileName) {
      this.mergeFileName = mergeFileName;
    }

    public String getStatus() {
      return status;
    }

    public void setStatus(String status) {
      this.status = status;
    }
  }
}
