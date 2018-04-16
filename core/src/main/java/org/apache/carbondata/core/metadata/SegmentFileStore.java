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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationsImpl;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.locks.CarbonLockUtil;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import com.google.gson.Gson;
import org.apache.hadoop.fs.Path;

/**
 * Provide read and write support for segment file associated with each segment
 */
public class SegmentFileStore {

  private static LogService LOGGER = LogServiceFactory.getLogService(
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
   * Write segment information to the segment folder with indexfilename and
   * corresponding partitions.
   */
  public static void writeSegmentFile(String tablePath, final String taskNo, String location,
      String timeStamp, List<String> partionNames) throws IOException {
    String tempFolderLoc = timeStamp + ".tmp";
    String writePath = CarbonTablePath.getSegmentFilesLocation(tablePath) + "/" + tempFolderLoc;
    CarbonFile carbonFile = FileFactory.getCarbonFile(writePath);
    if (!carbonFile.exists()) {
      carbonFile.mkdirs(writePath, FileFactory.getFileType(writePath));
    }
    CarbonFile tempFolder =
        FileFactory.getCarbonFile(location + CarbonCommonConstants.FILE_SEPARATOR + tempFolderLoc);

    if (tempFolder.exists() && partionNames.size() > 0) {
      CarbonFile[] carbonFiles = tempFolder.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return file.getName().startsWith(taskNo) && file.getName()
              .endsWith(CarbonTablePath.INDEX_FILE_EXT);
        }
      });
      if (carbonFiles != null && carbonFiles.length > 0) {
        boolean isRelative = false;
        if (location.startsWith(tablePath)) {
          location = location.substring(tablePath.length(), location.length());
          isRelative = true;
        }
        SegmentFile segmentFile = new SegmentFile();
        FolderDetails folderDetails = new FolderDetails();
        folderDetails.setRelative(isRelative);
        folderDetails.setPartitions(partionNames);
        folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
        for (CarbonFile file : carbonFiles) {
          folderDetails.getFiles().add(file.getName());
        }
        segmentFile.addPath(location, folderDetails);
        String path = writePath + "/" + taskNo + CarbonTablePath.SEGMENT_EXT;
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

  /**
   * Write segment file to the metadata folder of the table
   * @param tablePath table path
   * @param segmentId segment id
   * @param UUID a UUID string used to construct the segment file name
   * @return segment file name
   */
  public static String writeSegmentFile(String tablePath, String segmentId, String UUID)
      throws IOException {
    String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
    CarbonFile segmentFolder = FileFactory.getCarbonFile(segmentPath);
    CarbonFile[] indexFiles = segmentFolder.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT);
      }
    });
    if (indexFiles != null && indexFiles.length > 0) {
      SegmentFile segmentFile = new SegmentFile();
      FolderDetails folderDetails = new FolderDetails();
      folderDetails.setRelative(true);
      folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
      for (CarbonFile file : indexFiles) {
        folderDetails.getFiles().add(file.getName());
      }
      String segmentRelativePath = segmentPath.substring(tablePath.length(), segmentPath.length());
      segmentFile.addPath(segmentRelativePath, folderDetails);
      String segmentFileFolder =  CarbonTablePath.getSegmentFilesLocation(tablePath);
      CarbonFile carbonFile = FileFactory.getCarbonFile(segmentFileFolder);
      if (!carbonFile.exists()) {
        carbonFile.mkdirs(segmentFileFolder, FileFactory.getFileType(segmentFileFolder));
      }
      String segmentFileName = genSegmentFileName(segmentId, UUID) + CarbonTablePath.SEGMENT_EXT;
      // write segment info to new file.
      writeSegmentFile(segmentFile, segmentFileFolder + File.separator + segmentFileName);
      return segmentFileName;
    }
    return null;
  }

  /**
   * Writes the segment file in json format
   * @param segmentFile
   * @param path
   * @throws IOException
   */
  public static void writeSegmentFile(SegmentFile segmentFile, String path) throws IOException {
    AtomicFileOperations fileWrite =
        new AtomicFileOperationsImpl(path, FileFactory.getFileType(path));
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
        FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(readPath));
      }
      return segmentFile;
    }
    return null;
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
    String tableStatusPath = CarbonTablePath.getTableStatusFilePath(tablePath);
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

  private static CarbonFile[] getSegmentFiles(String segmentPath) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    if (carbonFile.exists()) {
      return carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
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

      CarbonFile[] listFiles = carbonFile.listFiles(new CarbonFileFilter() {
        @Override public boolean accept(CarbonFile file) {
          return CarbonTablePath.isCarbonIndexFile(file.getAbsolutePath());
        }
      });
      if (listFiles != null && listFiles.length > 0) {
        boolean isRelative = false;
        if (location.startsWith(tablePath)) {
          location = location.substring(tablePath.length(), location.length());
          isRelative = true;
        }
        SegmentFile localSegmentFile = new SegmentFile();
        FolderDetails folderDetails = new FolderDetails();
        folderDetails.setRelative(isRelative);
        folderDetails.setPartitions(spec.getPartitions());
        folderDetails.setStatus(SegmentStatus.SUCCESS.getMessage());
        for (CarbonFile file : listFiles) {
          if (file.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
            List<String> indexFiles =
                new SegmentIndexFileStore().getIndexFilesFromMergeFile(file.getAbsolutePath());
            folderDetails.getFiles().addAll(indexFiles);
            folderDetails.setMergeFileName(file.getName());
          } else {
            folderDetails.getFiles().add(file.getName());
          }
        }
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
  private static SegmentFile readSegmentFile(String segmentFilePath) throws IOException {
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    SegmentFile segmentFile;
    AtomicFileOperations fileOperation =
        new AtomicFileOperationsImpl(segmentFilePath, FileFactory.getFileType(segmentFilePath));

    try {
      if (!FileFactory.isFileExist(segmentFilePath, FileFactory.getFileType(segmentFilePath))) {
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
  public void readIndexFiles() throws IOException {
    readIndexFiles(SegmentStatus.SUCCESS, false);
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
  private void readIndexFiles(SegmentStatus status, boolean ignoreStatus) throws IOException {
    if (indexFilesMap != null) {
      return;
    }
    SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
    indexFilesMap = new HashMap<>();
    indexFileStore.readAllIIndexOfSegment(this.segmentFile, tablePath, status, ignoreStatus);
    Map<String, byte[]> carbonIndexMap = indexFileStore.getCarbonIndexMapWithFullPath();
    DataFileFooterConverter fileFooterConverter = new DataFileFooterConverter();
    for (Map.Entry<String, byte[]> entry : carbonIndexMap.entrySet()) {
      List<DataFileFooter> indexInfo =
          fileFooterConverter.getIndexInfo(entry.getKey(), entry.getValue());
      List<String> blocks = new ArrayList<>();
      for (DataFileFooter footer : indexInfo) {
        blocks.add(footer.getBlockInfo().getTableBlockInfo().getFilePath());
      }
      indexFilesMap.put(entry.getKey(), blocks);
    }
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
      List<DataFileFooter> indexInfo =
          fileFooterConverter.getIndexInfo(entry.getKey(), entry.getValue());
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
  public Map<String, String> getIndexFiles() {
    Map<String, String> indexFiles = new HashMap<>();
    if (segmentFile != null) {
      for (Map.Entry<String, FolderDetails> entry : getLocationMap().entrySet()) {
        String location = entry.getKey();
        if (entry.getValue().isRelative) {
          location = tablePath + location;
        }
        if (entry.getValue().status.equals(SegmentStatus.SUCCESS.getMessage())) {
          for (String indexFile : entry.getValue().getFiles()) {
            indexFiles.put(location + CarbonCommonConstants.FILE_SEPARATOR + indexFile,
                entry.getValue().mergeFileName);
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
    Map<String, String> indexFiles = getIndexFiles();
    Set<String> files = new HashSet<>();
    for (Map.Entry<String, String> entry: indexFiles.entrySet()) {
      Path path = new Path(entry.getKey());
      files.add(entry.getKey());
      if (entry.getValue() != null) {
        files.add(new Path(path.getParent(), entry.getValue()).toString());
      }
    }
    List<CarbonFile> carbonFiles = new ArrayList<>();
    for (String indexFile : files) {
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
  public void dropPartitions(Segment segment, List<PartitionSpec> partitionSpecs,
      String uniqueId, List<String> toBeDeletedSegments, List<String> toBeUpdatedSegments)
      throws IOException {
    readSegment(tablePath, segment.getSegmentFileName());
    boolean updateSegment = false;
    for (Map.Entry<String, FolderDetails> entry : segmentFile.getLocationMap().entrySet()) {
      String location = entry.getKey();
      if (entry.getValue().isRelative) {
        location = tablePath + CarbonCommonConstants.FILE_SEPARATOR + location;
      }
      Path path = new Path(location);
      // Update the status to delete if path equals
      if (null != partitionSpecs) {
        for (PartitionSpec spec : partitionSpecs) {
          if (path.equals(spec.getLocation())) {
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
              SegmentFileStore.genSegmentFileName(segment.getSegmentNo(),  String.valueOf(uniqueId))
              + CarbonTablePath.SEGMENT_EXT;
      writeSegmentFile(segmentFile, writePath);
    }
    // Check whether we can completly remove the segment.
    boolean deleteSegment = true;
    for (Map.Entry<String, FolderDetails> entry : segmentFile.getLocationMap().entrySet()) {
      if (entry.getValue().getStatus().equals(SegmentStatus.SUCCESS.getMessage())) {
        deleteSegment = false;
        break;
      }
    }
    if (deleteSegment) {
      toBeDeletedSegments.add(segment.getSegmentNo());
    }
    if (updateSegment) {
      toBeUpdatedSegments.add(segment.getSegmentNo());
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
              .getValidAndInvalidSegments().getValidSegments());
      CarbonUpdateUtil.updateTableMetadataStatus(segmentSet, carbonTable, uniqueId, true,
          Segment.toSegmentList(toBeDeleteSegments), Segment.toSegmentList(toBeUpdatedSegments),
          uuid);
    }
  }

  /**
   * Clean up invalid data after drop partition in all segments of table
   *
   * @param table
   * @param forceDelete Whether it should be deleted force or check the time for an hour creation
   *                    to delete data.
   * @throws IOException
   */
  public static void cleanSegments(CarbonTable table, List<PartitionSpec> partitionSpecs,
      boolean forceDelete) throws IOException {

    LoadMetadataDetails[] details = SegmentStatusManager.readLoadMetadata(table.getMetadataPath());
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
        fileStore.readIndexFiles(SegmentStatus.MARKED_FOR_DELETE, false);
        if (forceDelete) {
          deletePhysicalPartition(partitionSpecs, fileStore.getIndexFilesMap());
        }
        for (Map.Entry<String, List<String>> entry : fileStore.indexFilesMap.entrySet()) {
          String indexFile = entry.getKey();
          // Check the partition information in the partiton mapper
          Long fileTimestamp = CarbonUpdateUtil.getTimeStampAsLong(indexFile
              .substring(indexFile.lastIndexOf(CarbonCommonConstants.HYPHEN) + 1,
                  indexFile.length() - CarbonTablePath.INDEX_FILE_EXT.length()));
          if (CarbonUpdateUtil.isMaxQueryTimeoutExceeded(fileTimestamp) || forceDelete) {
            toBeDeletedIndexFiles.add(indexFile);
            // Add the corresponding carbondata files to the delete list.
            toBeDeletedDataFiles.addAll(entry.getValue());
          }
        }
        if (toBeDeletedIndexFiles.size() > 0) {
          for (String dataFile : toBeDeletedIndexFiles) {
            FileFactory.deleteFile(dataFile, FileFactory.getFileType(dataFile));
          }
          for (String dataFile : toBeDeletedDataFiles) {
            FileFactory.deleteFile(dataFile, FileFactory.getFileType(dataFile));
          }
        }
      }
    }
  }

  /**
   * Deletes the segment file and its physical files like partition folders from disk
   * @param tablePath
   * @param segmentFile
   * @param partitionSpecs
   * @throws IOException
   */
  public static void deleteSegment(String tablePath, String segmentFile,
      List<PartitionSpec> partitionSpecs) throws IOException {
    SegmentFileStore fileStore = new SegmentFileStore(tablePath, segmentFile);
    fileStore.readIndexFiles(SegmentStatus.SUCCESS, true);
    Map<String, List<String>> indexFilesMap = fileStore.getIndexFilesMap();
    for (Map.Entry<String, List<String>> entry : indexFilesMap.entrySet()) {
      FileFactory.deleteFile(entry.getKey(), FileFactory.getFileType(entry.getKey()));
      for (String file : entry.getValue()) {
        FileFactory.deleteFile(file, FileFactory.getFileType(file));
      }
    }
    deletePhysicalPartition(partitionSpecs, indexFilesMap);
    String segmentFilePath =
        CarbonTablePath.getSegmentFilesLocation(tablePath) + CarbonCommonConstants.FILE_SEPARATOR
            + segmentFile;
    // Deletes the physical segment file
    FileFactory.deleteFile(segmentFilePath, FileFactory.getFileType(segmentFilePath));
  }

  private static void deletePhysicalPartition(List<PartitionSpec> partitionSpecs,
      Map<String, List<String>> locationMap) throws IOException {
    for (Map.Entry<String, List<String>> entry : locationMap.entrySet()) {
      Path location = new Path(entry.getKey()).getParent();
      if (partitionSpecs != null) {
        boolean exists = pathExistsInPartitionSpec(partitionSpecs, location);
        if (!exists) {
          FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(location.toString()));
        }
      } else {
        // delete the segment folder
        CarbonFile file = FileFactory.getCarbonFile(location.toString());
        if (null != file) {
          FileFactory.deleteAllCarbonFilesOfDir(file);
        }
      }
    }
  }

  private static boolean pathExistsInPartitionSpec(List<PartitionSpec> partitionSpecs,
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
  public static List<PartitionSpec> getPartitionSpecs(String segmentId, String tablePath)
      throws IOException {
    LoadMetadataDetails segEntry = null;
    LoadMetadataDetails[] details =
        SegmentStatusManager.readLoadMetadata(CarbonTablePath.getMetadataPath(tablePath));
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
    return null;
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
   * Returs the current partition specs of this segment
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

    SegmentFile() {
      locationMap = new HashMap<>();
    }

    SegmentFile merge(SegmentFile mapper) {
      if (this == mapper) {
        return this;
      }
      if (locationMap != null && mapper.locationMap != null) {
        for (Map.Entry<String, FolderDetails> entry : mapper.locationMap.entrySet()) {
          FolderDetails folderDetails = locationMap.get(entry.getKey());
          if (folderDetails != null) {
            folderDetails.merge(entry.getValue());
          } else {
            locationMap.put(entry.getKey(), entry.getValue());
          }
        }
      }
      if (locationMap == null) {
        locationMap = mapper.locationMap;
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
