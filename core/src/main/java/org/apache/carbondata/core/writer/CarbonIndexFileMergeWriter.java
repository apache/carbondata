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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2270
    this.table = table;
  }

  /**
   * Merge all the carbonindex files of segment to a  merged file
   * @param tablePath
   * @param indexFileNamesTobeAdded while merging it comsiders only these files.
   *                                If null then consider all
   * @param readFileFooterFromCarbonDataFile flag to read file footer information from carbondata
   *                                         file. This will used in case of upgrade from version
   *                                         which do not store the blocklet info to current version
   * @throws IOException
   */
  private String mergeCarbonIndexFilesOfSegment(String segmentId,
      String tablePath, List<String> indexFileNamesTobeAdded,
      boolean readFileFooterFromCarbonDataFile, String uuid, String partitionPath) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3262
    try {
      Segment segment = Segment.getSegment(segmentId, tablePath);
      String segmentPath = CarbonTablePath.getSegmentPath(tablePath, segmentId);
      CarbonFile[] indexFiles;
      SegmentFileStore sfs = null;
      if (segment != null && segment.getSegmentFileName() != null) {
        sfs = new SegmentFileStore(tablePath, segment.getSegmentFileName());
        List<CarbonFile> indexCarbonFiles = sfs.getIndexCarbonFiles();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3415
        if (table.isHivePartitionTable()) {
          // in case of partition table, merge index files of a partition
          List<CarbonFile> indexFilesInPartition = new ArrayList<>();
          for (CarbonFile indexCarbonFile : indexCarbonFiles) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3641
            if (FileFactory.getUpdatedFilePath(indexCarbonFile.getParentFile().getPath())
                .equals(partitionPath)) {
              indexFilesInPartition.add(indexCarbonFile);
            }
          }
          indexFiles = indexFilesInPartition.toArray(new CarbonFile[indexFilesInPartition.size()]);
        } else {
          indexFiles = indexCarbonFiles.toArray(new CarbonFile[indexCarbonFiles.size()]);
        }
      } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2909
        indexFiles =
            SegmentIndexFileStore.getCarbonIndexFiles(segmentPath, FileFactory.getConfiguration());
      }
      if (isCarbonIndexFilePresent(indexFiles) || indexFileNamesTobeAdded != null) {
        if (sfs == null) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2321
          return writeMergeIndexFileBasedOnSegmentFolder(indexFileNamesTobeAdded,
              readFileFooterFromCarbonDataFile, segmentPath, indexFiles, segmentId);
        } else {
          return writeMergeIndexFileBasedOnSegmentFile(segmentId, indexFileNamesTobeAdded, sfs,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3415
              indexFiles, uuid, partitionPath);
        }
      }
    } catch (Exception e) {
      LOGGER.error(
          "Failed to merge index files in path: " + tablePath, e);
    }
    return null;
  }

  /**
   * merge index files and return the index details
   */
  public SegmentFileStore.FolderDetails mergeCarbonIndexFilesOfSegment(String segmentId,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3641
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
      fileStore.readAllIIndexOfSegment(partitionTempPath);
    }
    Map<String, byte[]> indexMap = fileStore.getCarbonIndexMapWithFullPath();
    Map<String, Map<String, byte[]>> indexLocationMap = new HashMap<>();
    for (Map.Entry<String, byte[]> entry : indexMap.entrySet()) {
      Path path = new Path(entry.getKey());
      Map<String, byte[]> map = indexLocationMap.get(path.getParent().toString());
      if (map == null) {
        map = new HashMap<>();
        indexLocationMap.put(path.getParent().toString(), map);
      }
      map.put(path.getName(), entry.getValue());
    }
    SegmentFileStore.FolderDetails folderDetails = null;
    for (Map.Entry<String, Map<String, byte[]>> entry : indexLocationMap.entrySet()) {
      String mergeIndexFile = writeMergeIndexFile(null, partitionPath, entry.getValue(), segmentId);
      folderDetails = new SegmentFileStore.FolderDetails();
      folderDetails.setMergeFileName(mergeIndexFile);
      folderDetails.setStatus("Success");
      List<String> partitions = new ArrayList<>();
      if (partitionPath.startsWith(tablePath)) {
        partitionPath = partitionPath.substring(tablePath.length() + 1, partitionPath.length());
        partitions.addAll(Arrays.asList(partitionPath.split("/")));

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

  private String writeMergeIndexFileBasedOnSegmentFolder(List<String> indexFileNamesTobeAdded,
      boolean readFileFooterFromCarbonDataFile, String segmentPath, CarbonFile[] indexFiles,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2321
      String segmentId) throws IOException {
    SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
    if (readFileFooterFromCarbonDataFile) {
      // this case will be used in case of upgrade where old store will not have the blocklet
      // info in the index file and therefore blocklet info need to be read from the file footer
      // in the carbondata file
      fileStore.readAllIndexAndFillBolckletInfo(segmentPath);
    } else {
      fileStore.readAllIIndexOfSegment(segmentPath);
    }
    Map<String, byte[]> indexMap = fileStore.getCarbonIndexMap();
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2321
    writeMergeIndexFile(indexFileNamesTobeAdded, segmentPath, indexMap, segmentId);
    for (CarbonFile indexFile : indexFiles) {
      indexFile.delete();
    }
    return null;
  }

  public String writeMergeIndexFileBasedOnSegmentFile(String segmentId,
      List<String> indexFileNamesTobeAdded, SegmentFileStore segmentFileStore,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3415
      CarbonFile[] indexFiles, String uuid, String partitionPath) throws IOException {
    SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
    // in case of partition table, merge index file to be created for each partition
    if (null != partitionPath) {
      for (CarbonFile indexFile : indexFiles) {
        fileStore.readIndexFile(indexFile);
      }
    } else {
      fileStore.readAllIIndexOfSegment(segmentFileStore.getSegmentFile(),
          segmentFileStore.getTablePath(), SegmentStatus.SUCCESS, true);
    }
    Map<String, byte[]> indexMap = fileStore.getCarbonIndexMapWithFullPath();
    Map<String, Map<String, byte[]>> indexLocationMap = new HashMap<>();
    for (Map.Entry<String, byte[]> entry: indexMap.entrySet()) {
      Path path = new Path(entry.getKey());
      Map<String, byte[]> map = indexLocationMap.get(path.getParent().toString());
      if (map == null) {
        map = new HashMap<>();
        indexLocationMap.put(path.getParent().toString(), map);
      }
      map.put(path.getName(), entry.getValue());
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3415
    List<PartitionSpec> partitionSpecs = SegmentFileStore
        .getPartitionSpecs(segmentId, table.getTablePath(), SegmentStatusManager
            .readLoadMetadata(CarbonTablePath.getMetadataPath(table.getTablePath())));
    for (Map.Entry<String, Map<String, byte[]>> entry : indexLocationMap.entrySet()) {
      String mergeIndexFile =
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2321
          writeMergeIndexFile(indexFileNamesTobeAdded, entry.getKey(), entry.getValue(), segmentId);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2704
      for (Map.Entry<String, SegmentFileStore.FolderDetails> segentry : segmentFileStore
          .getLocationMap().entrySet()) {
        String location = segentry.getKey();
        if (segentry.getValue().isRelative()) {
          location =
              segmentFileStore.getTablePath() + CarbonCommonConstants.FILE_SEPARATOR + location;
        }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3659
        if (FileFactory.getCarbonFile(entry.getKey()).equals(FileFactory.getCarbonFile(location))) {
          segentry.getValue().setMergeFileName(mergeIndexFile);
          segentry.getValue().setFiles(new HashSet<String>());
          break;
        }
      }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3415
      if (table.isHivePartitionTable()) {
        for (PartitionSpec partitionSpec : partitionSpecs) {
          if (partitionSpec.getLocation().toString().equals(partitionPath)) {
            SegmentFileStore.writeSegmentFile(table.getTablePath(), mergeIndexFile, partitionPath,
                segmentId + "_" + uuid + "", partitionSpec.getPartitions(), true);
          }
        }
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2482
    String newSegmentFileName = SegmentFileStore.genSegmentFileName(segmentId, uuid)
        + CarbonTablePath.SEGMENT_EXT;
    String path = CarbonTablePath.getSegmentFilesLocation(table.getTablePath())
        + CarbonCommonConstants.FILE_SEPARATOR + newSegmentFileName;
    if (!table.isHivePartitionTable()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2704
      SegmentFileStore.writeSegmentFile(segmentFileStore.getSegmentFile(), path);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3566
      SegmentFileStore.updateTableStatusFile(table, segmentId, newSegmentFileName,
          table.getCarbonTableIdentifier().getTableId(), segmentFileStore);
    }
    for (CarbonFile file : indexFiles) {
      file.delete();
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2482
    return uuid;
  }

  private String writeMergeIndexFile(List<String> indexFileNamesTobeAdded, String segmentPath,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2321
      Map<String, byte[]> indexMap, String segment_id) throws IOException {
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2321
      String mergeIndexName =
          segment_id + '_' + System.currentTimeMillis() + CarbonTablePath.MERGE_INDEX_FILE_EXT;
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
   * Merge all the carbonindex files of segment to a  merged file
   *
   * @param segmentId
   */
  public String mergeCarbonIndexFilesOfSegment(String segmentId, String uuid, String tablePath,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3575
      String partitionPath) {
    return mergeCarbonIndexFilesOfSegment(segmentId, tablePath, null, false, uuid, partitionPath);
  }

  /**
   * Merge all the carbonindex files of segment to a  merged file
   *
   * @param segmentId
   * @param readFileFooterFromCarbonDataFile
   */
  public String mergeCarbonIndexFilesOfSegment(String segmentId, String tablePath,
      boolean readFileFooterFromCarbonDataFile, String uuid) {
    return mergeCarbonIndexFilesOfSegment(segmentId, tablePath, null,
        readFileFooterFromCarbonDataFile, uuid, null);
  }

  private boolean isCarbonIndexFilePresent(CarbonFile[] indexFiles) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1617
    for (CarbonFile file : indexFiles) {
      if (file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        return true;
      }
    }
    return false;
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1617
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2209
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
