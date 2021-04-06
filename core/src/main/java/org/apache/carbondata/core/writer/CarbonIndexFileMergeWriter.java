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

package org.apache.carbondata.core.writer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.index.Segment;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.schema.indextable.IndexMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.ObjectSerializationUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.MergedBlockIndex;
import org.apache.carbondata.format.MergedBlockIndexHeader;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

public class CarbonIndexFileMergeWriter {

  /**
   * table handle
   */
  private CarbonTable table;

  /**
   * thrift writer object
   */
  private ThriftWriter thriftWriter;

  private Logger LOGGER = LogServiceFactory.getLogService(this.getClass().getCanonicalName());

  public CarbonIndexFileMergeWriter(CarbonTable table) {
    this.table = table;
  }

  /**
   * Merge all the carbon index files of segment to a  merged file
   * @param tablePath
   * @param indexFileNamesTobeAdded while merging, it considers only these files.
   *                                If null, then consider all
   * @param isOldStoreIndexFilesPresent flag to read file footer information from carbondata
   *                                         file. This will used in case of upgrade from version
   *                                         which do not store the blocklet info to current version
   * @throws IOException
   */
  private String mergeCarbonIndexFilesOfSegment(String segmentId, String tablePath,
      List<String> indexFileNamesTobeAdded, boolean isOldStoreIndexFilesPresent, String uuid,
      String partitionPath) {
    Segment segment = Segment.getSegment(segmentId, tablePath);
    String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
    try {
      List<CarbonFile> indexFiles = new ArrayList<>();
      SegmentFileStore sfs = null;
      if (segment != null && segment.getSegmentFileName() != null) {
        sfs = new SegmentFileStore(tablePath, segment.getSegmentFileName());
        List<CarbonFile> indexCarbonFiles = sfs.getIndexCarbonFiles();
        if (table.isHivePartitionTable()) {
          // in case of partition table, merge index files of a partition
          List<CarbonFile> indexFilesInPartition = new ArrayList<>();
          for (CarbonFile indexCarbonFile : indexCarbonFiles) {
            if (FileFactory.getUpdatedFilePath(indexCarbonFile.getParentFile().getPath())
                .equals(partitionPath)) {
              indexFilesInPartition.add(indexCarbonFile);
            }
          }
          indexFiles = indexFilesInPartition;
        } else {
          indexFiles = indexCarbonFiles;
        }
      }
      if (sfs == null || indexFiles.isEmpty()) {
        if (table.isHivePartitionTable()) {
          segmentPath = partitionPath;
        }
        return writeMergeIndexFileBasedOnSegmentFolder(indexFileNamesTobeAdded,
            isOldStoreIndexFilesPresent, segmentPath, segmentId, uuid, true);
      } else {
        return writeMergeIndexFileBasedOnSegmentFile(segmentId, indexFileNamesTobeAdded,
            isOldStoreIndexFilesPresent, sfs,
            indexFiles.toArray(new CarbonFile[0]), uuid, partitionPath);
      }
    } catch (Exception e) {
      String message =
          "Failed to merge index files in path: " + segmentPath + ". " + e.getMessage();
      LOGGER.error(message);
      throw new RuntimeException(message, e);
    }
  }

  /**
   * merge index files and return the index details
   */
  public SegmentFileStore.FolderDetails mergeCarbonIndexFilesOfSegment(String segmentId,
      String tablePath, String partitionPath, List<String> partitionInfo, String uuid,
      String tempFolderPath, String currPartitionSpec) throws IOException {
    SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
    String partitionTempPath = "";
    for (String partition : partitionInfo) {
      if (partitionPath.equalsIgnoreCase(partition)) {
        partitionTempPath = partition + "/" + tempFolderPath;
        break;
      }
    }
    if (null != partitionPath && !partitionTempPath.isEmpty()) {
      fileStore.readAllIIndexOfSegment(partitionTempPath, uuid);
    }
    Map<String, Map<String, byte[]>> indexLocationMap =
        groupIndexesBySegment(fileStore.getCarbonIndexMapWithFullPath());
    SegmentFileStore.FolderDetails folderDetails = null;
    for (Map.Entry<String, Map<String, byte[]>> entry : indexLocationMap.entrySet()) {
      String mergeIndexFile =
          writeMergeIndexFile(null, partitionPath, entry.getValue(), segmentId, uuid);
      folderDetails = new SegmentFileStore.FolderDetails();
      folderDetails.setMergeFileName(mergeIndexFile);
      folderDetails.setStatus("Success");
      if (partitionPath.startsWith(tablePath)) {
        partitionPath = partitionPath.substring(tablePath.length() + 1);
        List<String> partitions = new ArrayList<>(Arrays.asList(partitionPath.split("/")));

        folderDetails.setPartitions(partitions);
        folderDetails.setRelative(true);
      } else {
        List<PartitionSpec> partitionSpecs;
        if (currPartitionSpec != null) {
          partitionSpecs = (ArrayList<PartitionSpec>) ObjectSerializationUtil
              .convertStringToObject(currPartitionSpec);
          PartitionSpec writeSpec = new PartitionSpec(null, partitionPath);
          int index = partitionSpecs.indexOf(writeSpec);
          if (index > -1) {
            PartitionSpec spec = partitionSpecs.get(index);
            folderDetails.setPartitions(spec.getPartitions());
            folderDetails.setRelative(false);
          } else {
            throw new IOException("Unable to get PartitionSpec for: " + partitionPath);
          }
        } else {
          throw new IOException("Unable to get PartitionSpec for: " + partitionPath);
        }
      }
    }
    return folderDetails;
  }

  private Map<String, Map<String, byte[]>> groupIndexesBySegment(Map<String, byte[]> indexMap) {
    Map<String, Map<String, byte[]>> indexLocationMap = new HashMap<>();
    for (Map.Entry<String, byte[]> entry : indexMap.entrySet()) {
      Path path = new Path(entry.getKey());
      indexLocationMap
          .computeIfAbsent(path.getParent().toString(), k -> new HashMap<>())
          .put(path.getName(), entry.getValue());
    }
    return indexLocationMap;
  }

  public String writeMergeIndexFileBasedOnSegmentFolder(List<String> indexFileNamesTobeAdded,
      boolean isOldStoreIndexFilesPresent, String segmentPath,
      String segmentId, String uuid, boolean readBasedOnUUID) throws IOException {
    CarbonFile[] indexFiles = null;
    SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
    if (isOldStoreIndexFilesPresent) {
      // this case will be used in case of upgrade where old store will not have the blocklet
      // info in the index file and therefore blocklet info need to be read from the file footer
      // in the carbondata file
      fileStore.readAllIndexAndFillBlockletInfo(segmentPath, null);
    } else {
      if (readBasedOnUUID) {
        indexFiles = SegmentIndexFileStore
            .getCarbonIndexFiles(segmentPath, FileFactory.getConfiguration(), uuid);
      } else {
        // The uuid can be different, when we add load from external path.
        indexFiles =
            SegmentIndexFileStore.getCarbonIndexFiles(segmentPath, FileFactory.getConfiguration());
      }
      fileStore.readAllIIndexOfSegment(indexFiles);
    }
    Map<String, byte[]> indexMap = fileStore.getCarbonIndexMap();
    Map<String, List<String>> mergeToIndexFileMap = fileStore.getCarbonMergeFileToIndexFilesMap();
    if (!mergeToIndexFileMap.containsValue(new ArrayList<>(indexMap.keySet())))   {
      writeMergeIndexFile(indexFileNamesTobeAdded, segmentPath, indexMap, segmentId, uuid);
      // If Alter merge index for old tables is triggered, do not delete index files immediately
      // to avoid index file not found during concurrent queries
      if (!isOldStoreIndexFilesPresent && indexFiles != null) {
        for (CarbonFile indexFile : indexFiles) {
          indexFile.delete();
        }
      }
    }
    return null;
  }

  public String writeMergeIndexFileBasedOnSegmentFile(String segmentId,
      List<String> indexFileNamesTobeAdded, boolean isOldStoreIndexFilesPresent,
      SegmentFileStore segmentFileStore, CarbonFile[] indexFiles, String uuid, String partitionPath)
      throws IOException {
    SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
    String newSegmentFileName = null;
    // in case of partition table, merge index file to be created for each partition
    if (null != partitionPath) {
      for (CarbonFile indexFile : indexFiles) {
        fileStore.readIndexFile(indexFile);
      }
    } else {
      fileStore.readAllIIndexOfSegment(segmentFileStore.getSegmentFile(),
          segmentFileStore.getTablePath(), SegmentStatus.SUCCESS, true);
    }
    Map<String, Map<String, byte[]>> indexLocationMap =
        groupIndexesBySegment(fileStore.getCarbonIndexMapWithFullPath());
    List<PartitionSpec> partitionSpecs = SegmentFileStore
        .getPartitionSpecs(segmentId, table.getTablePath(), SegmentStatusManager
            .readLoadMetadata(CarbonTablePath.getMetadataPath(table.getTablePath())));
    List<String> mergeIndexFiles = new ArrayList<>();
    for (Map.Entry<String, Map<String, byte[]>> entry : indexLocationMap.entrySet()) {
      String mergeIndexFile = writeMergeIndexFile(indexFileNamesTobeAdded,
          entry.getKey(), entry.getValue(), segmentId, uuid);
      for (Map.Entry<String, SegmentFileStore.FolderDetails> segment : segmentFileStore
          .getLocationMap().entrySet()) {
        String location = segment.getKey();
        if (segment.getValue().isRelative()) {
          location =
              segmentFileStore.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + location;
        }
        if (FileFactory.getCarbonFile(entry.getKey()).equals(FileFactory.getCarbonFile(location))) {
          segment.getValue().setMergeFileName(mergeIndexFile);
          mergeIndexFiles
              .add(entry.getKey() + CarbonCommonConstants.FILE_SEPARATOR + mergeIndexFile);
          segment.getValue().setFiles(new HashSet<>());
          break;
        }
      }
      if (table.isHivePartitionTable()) {
        for (PartitionSpec partitionSpec : partitionSpecs) {
          if (partitionSpec.getLocation().toString().equals(partitionPath)) {
            try {
              SegmentFileStore
                  .writeSegmentFileForPartitionTable(table.getTablePath(), mergeIndexFile,
                      partitionPath, segmentId + CarbonCommonConstants.UNDERSCORE + uuid + "",
                      partitionSpec.getPartitions(), true);
            } catch (Exception ex) {
              // delete merge index file if created,
              // keep only index files as segment file writing is failed
              FileFactory.getCarbonFile(mergeIndexFile).delete();
              LOGGER.error(
                  "unable to write segment file during merge index writing: " + ex.getMessage());
              throw ex;
            }
          }
        }
      }
    }
    if (table.isIndexTable()) {
      // To maintain same segment file name mapping between parent and SI table.
      IndexMetadata indexMetadata = table.getIndexMetadata();
      LoadMetadataDetails[] loadDetails = SegmentStatusManager
          .readLoadMetadata(CarbonTablePath.getMetadataPath(indexMetadata.getParentTablePath()));
      LoadMetadataDetails loadMetaDetail = Arrays.stream(loadDetails)
          .filter(loadDetail -> loadDetail.getLoadName().equals(segmentId)).findFirst().get();
      newSegmentFileName = loadMetaDetail.getSegmentFile();
    } else {
      // Instead of modifying existing segment file, generate uuid to write into new segment file.
      uuid = String.valueOf(System.currentTimeMillis());
      newSegmentFileName = SegmentFileStore.genSegmentFileName(segmentId, uuid)
          + CarbonTablePath.SEGMENT_EXT;
    }
    String path = CarbonTablePath.getSegmentFilesLocation(table.getTablePath())
        + CarbonCommonConstants.FILE_SEPARATOR + newSegmentFileName;
    if (!table.isHivePartitionTable()) {
      try {
        SegmentFileStore.writeSegmentFile(segmentFileStore.getSegmentFile(), path);
      } catch (Exception ex) {
        // delete merge index file if created,
        // keep only index files as segment file writing is failed
        for (String mergeIndexFile : mergeIndexFiles) {
          FileFactory.getCarbonFile(mergeIndexFile).delete();
        }
        LOGGER.error("unable to write segment file during merge index writing: " + ex.getMessage());
        throw ex;
      }
      boolean status = SegmentFileStore.updateTableStatusFile(table, segmentId, newSegmentFileName,
          table.getCarbonTableIdentifier().getTableId(), segmentFileStore);
      if (!status) {
        throw new IOException("Table status update with mergeIndex file has failed");
      }
    }
    // If Alter merge index for old tables is triggered,
    // do not delete index files immediately to avoid index file not found during concurrent queries
    if (!isOldStoreIndexFilesPresent) {
      for (CarbonFile file : indexFiles) {
        file.delete();
      }
    }
    return uuid;
  }

  private String writeMergeIndexFile(List<String> indexFileNamesTobeAdded, String segmentPath,
      Map<String, byte[]> indexMap, String segment_id, String uuid) throws IOException {
    MergedBlockIndexHeader indexHeader = new MergedBlockIndexHeader();
    MergedBlockIndex mergedBlockIndex = new MergedBlockIndex();
    List<String> fileNames = new ArrayList<>(indexMap.size());
    List<ByteBuffer> data = new ArrayList<>(indexMap.size());
    for (Map.Entry<String, byte[]> entry : indexMap.entrySet()) {
      if (indexFileNamesTobeAdded == null ||
          indexFileNamesTobeAdded.contains(entry.getKey())) {
        fileNames.add(entry.getKey());
        data.add(ByteBuffer.wrap(entry.getValue()));
      }
    }
    if (fileNames.size() > 0) {
      String mergeIndexName =
          segment_id + '_' + uuid + CarbonTablePath.MERGE_INDEX_FILE_EXT;
      openThriftWriter(segmentPath + "/" + mergeIndexName);
      indexHeader.setFile_names(fileNames);
      mergedBlockIndex.setFileData(data);
      writeMergedBlockIndexHeader(indexHeader);
      writeMergedBlockIndex(mergedBlockIndex);
      close();
      return mergeIndexName;
    }
    return null;
  }

  /**
   * Merge all the carbon index files of segment to a  merged file
   *
   * @param segmentId
   */
  public String mergeCarbonIndexFilesOfSegment(String segmentId, String uuid, String tablePath,
      String partitionPath, boolean isOldStoreIndexFilesPresent) {
    return mergeCarbonIndexFilesOfSegment(segmentId, tablePath, null,
        isOldStoreIndexFilesPresent, uuid, partitionPath);
  }

  /**
   * Merge all the carbon index files of segment to a  merged file
   *
   * @param segmentId
   * @param readFileFooterFromCarbonDataFile
   */
  public String mergeCarbonIndexFilesOfSegment(String segmentId, String tablePath,
      boolean readFileFooterFromCarbonDataFile, String uuid) {
    return mergeCarbonIndexFilesOfSegment(segmentId, tablePath, null,
        readFileFooterFromCarbonDataFile, uuid, null);
  }

  /**
   * It writes thrift object to file
   *
   * @throws IOException
   */
  private void writeMergedBlockIndexHeader(MergedBlockIndexHeader indexObject) throws IOException {
    thriftWriter.write(indexObject);
  }

  /**
   * It writes thrift object to file
   *
   * @throws IOException
   */
  private void writeMergedBlockIndex(MergedBlockIndex indexObject) throws IOException {
    thriftWriter.write(indexObject);
  }

  /**
   * Below method will be used to open the thrift writer
   *
   * @param filePath file path where data need to be written
   * @throws IOException throws io exception in case of any failure
   */
  private void openThriftWriter(String filePath) throws IOException {
    // create thrift writer instance
    thriftWriter = new ThriftWriter(filePath, false);
    // open the file stream
    thriftWriter.open(FileWriteOperation.OVERWRITE);
  }

  /**
   * Below method will be used to close the thrift object
   */
  private void close() throws IOException {
    thriftWriter.close();
  }

  public static class SegmentIndexFIleMergeStatus implements Serializable {

    private SegmentFileStore.SegmentFile segmentFile;

    private List<String> filesTobeDeleted;

    public SegmentIndexFIleMergeStatus(SegmentFileStore.SegmentFile segmentFile,
        List<String> filesTobeDeleted) {
      this.segmentFile = segmentFile;
      this.filesTobeDeleted = filesTobeDeleted;
    }

    public SegmentFileStore.SegmentFile getSegmentFile() {
      return segmentFile;
    }

    public List<String> getFilesTobeDeleted() {
      return filesTobeDeleted;
    }
  }
}
