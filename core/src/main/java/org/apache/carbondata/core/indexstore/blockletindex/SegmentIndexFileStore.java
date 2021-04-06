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

package org.apache.carbondata.core.indexstore.blockletindex;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.metadata.blocklet.BlockletInfo;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.reader.CarbonIndexFileReader;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.util.CarbonMetadataUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataFileFooterConverter;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.format.MergedBlockIndex;
import org.apache.carbondata.format.MergedBlockIndexHeader;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * This class manages reading of index files with in the segment. The files it read can be
 * carbonindex or carbonindexmerge files.
 */
public class SegmentIndexFileStore {

  /**
   * Logger constant
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SegmentIndexFileStore.class.getName());
  /**
   * Stores the index file name and related binary file data in it.
   */
  private Map<String, byte[]> carbonIndexMap;

  /**
   * Stores the index file name and related binary file data in it.
   */
  private Map<String, byte[]> carbonIndexMapWithFullPath;

  /**
   * Stores the list of index files in a merge file
   */
  private Map<String, List<String>> carbonMergeFileToIndexFilesMap;

  private Configuration configuration;

  public SegmentIndexFileStore() {
    carbonIndexMap = new HashMap<>();
    carbonIndexMapWithFullPath = new TreeMap<>();
    carbonMergeFileToIndexFilesMap = new HashMap<>();
    configuration = FileFactory.getConfiguration();
  }

  public SegmentIndexFileStore(Configuration configuration) {
    carbonIndexMap = new HashMap<>();
    carbonIndexMapWithFullPath = new TreeMap<>();
    carbonMergeFileToIndexFilesMap = new HashMap<>();
    this.configuration = configuration;
  }

  /**
   * Read all index files and keep the cache in it.
   *
   * @param segmentPath the getAbsolutePath of segment folder
   * @param uuid the loadstarttime of segment
   * @throws IOException
   */
  public void readAllIIndexOfSegment(String segmentPath, String uuid) throws IOException {
    CarbonFile[] carbonIndexFiles = getCarbonIndexFiles(segmentPath, configuration, uuid);
    for (CarbonFile carbonIndexFile : carbonIndexFiles) {
      if (carbonIndexFile.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        readMergeFile(carbonIndexFile.getCanonicalPath());
      } else if (carbonIndexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        readIndexFile(carbonIndexFile);
      }
    }
  }

  /**
   * Read all index files and keep the cache in it.
   *
   * @param segmentPath
   * @throws IOException
   */
  public void readAllIIndexOfSegment(String segmentPath) throws IOException {
    readAllIIndexOfSegment(segmentPath, null);
  }

  /**
   * Read all index files and keep the cache in it.
   *
   * @param segmentFile
   * @throws IOException
   */
  public void readAllIIndexOfSegment(SegmentFileStore.SegmentFile segmentFile, String tablePath,
      SegmentStatus status, boolean ignoreStatus) throws IOException {
    List<CarbonFile> carbonIndexFiles = new ArrayList<>();
    Set<String> indexFiles = new HashSet<>();
    if (segmentFile == null) {
      return;
    }
    for (Map.Entry<String, SegmentFileStore.FolderDetails> locations : segmentFile
        .getLocationMap().entrySet()) {
      String location = locations.getKey();

      if (locations.getValue().getStatus().equals(status.getMessage()) || ignoreStatus) {
        if (locations.getValue().isRelative()) {
          location = tablePath + CarbonCommonConstants.FILE_SEPARATOR + location;
        }
        String mergeFileName = locations.getValue().getMergeFileName();
        if (mergeFileName != null) {
          CarbonFile mergeFile = FileFactory
              .getCarbonFile(location + CarbonCommonConstants.FILE_SEPARATOR + mergeFileName);
          if (mergeFile.exists() && !indexFiles.contains(mergeFile.getAbsolutePath())) {
            carbonIndexFiles.add(mergeFile);
            indexFiles.add(mergeFile.getAbsolutePath());
          }
        }
        for (String indexFile : locations.getValue().getFiles()) {
          CarbonFile carbonFile = FileFactory
              .getCarbonFile(location + CarbonCommonConstants.FILE_SEPARATOR + indexFile);
          if (carbonFile.exists() && !indexFiles.contains(carbonFile.getAbsolutePath())) {
            carbonIndexFiles.add(carbonFile);
            indexFiles.add(carbonFile.getAbsolutePath());
          }
        }
      }
    }
    for (CarbonFile carbonIndexFile : carbonIndexFiles) {
      if (carbonIndexFile.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        readMergeFile(carbonIndexFile.getCanonicalPath());
      } else if (carbonIndexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        readIndexFile(carbonIndexFile);
      }
    }
  }

  /**
   * read index file and fill the blocklet information
   *
   * @param segmentPath
   * @throws IOException
   */
  public void readAllIndexAndFillBlockletInfo(String segmentPath, String uuid) throws IOException {
    CarbonFile[] carbonIndexFiles =
        getCarbonIndexFiles(segmentPath, FileFactory.getConfiguration(), uuid);
    for (CarbonFile carbonIndexFile : carbonIndexFiles) {
      if (carbonIndexFile.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        readMergeFile(carbonIndexFile.getCanonicalPath());
      } else if (carbonIndexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        readIndexAndFillBlockletInfo(carbonIndexFile);
      }
    }
  }

  /**
   * Read all index files and keep the cache in it.
   *
   * @param carbonFiles
   * @throws IOException
   */
  public void readAllIIndexOfSegment(CarbonFile[] carbonFiles) throws IOException {
    for (CarbonFile carbonFile : carbonFiles) {
      if (carbonFile.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        readMergeFile(carbonFile.getCanonicalPath());
      } else if (carbonFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        readIndexFile(carbonFile);
      }
    }
  }

  /**
   * Read all index file names of the segment
   *
   * @param segmentPath
   * @return
   * @throws IOException
   */
  public Map<String, String> getIndexFilesFromSegment(String segmentPath) throws IOException {
    CarbonFile[] carbonIndexFiles =
        getCarbonIndexFiles(segmentPath, FileFactory.getConfiguration());
    Map<String, String> indexFiles = new HashMap<>();
    for (CarbonFile carbonIndexFile : carbonIndexFiles) {
      if (carbonIndexFile.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        List<String> indexFilesFromMergeFile =
            getIndexFilesFromMergeFile(carbonIndexFile.getCanonicalPath());
        for (String file : indexFilesFromMergeFile) {
          indexFiles.put(carbonIndexFile.getParentFile().getAbsolutePath()
              + CarbonCommonConstants.FILE_SEPARATOR + file, carbonIndexFile.getName());
        }
      } else if (carbonIndexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        indexFiles.put(carbonIndexFile.getAbsolutePath(), null);
      }
    }
    return indexFiles;
  }

  /**
   * Read all index file names of the segment
   *
   * @param segmentPath
   * @return
   */
  public Map<String, String> getMergeOrIndexFilesFromSegment(String segmentPath) {
    CarbonFile[] carbonIndexFiles =
        getCarbonIndexFiles(segmentPath, FileFactory.getConfiguration());
    Map<String, String> indexFiles = new HashMap<>();
    for (CarbonFile carbonIndexFile : carbonIndexFiles) {
      if (carbonIndexFile.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        indexFiles.put(carbonIndexFile.getAbsolutePath(), carbonIndexFile.getAbsolutePath());
      } else if (carbonIndexFile.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        indexFiles.put(carbonIndexFile.getAbsolutePath(), null);
      }
    }
    return indexFiles;
  }

  /**
   * List all the index files inside merge file.
   * @param mergeFile
   * @return
   * @throws IOException
   */
  public List<String> getIndexFilesFromMergeFile(String mergeFile) throws IOException {
    ThriftReader thriftReader = new ThriftReader(mergeFile);
    thriftReader.open();
    MergedBlockIndexHeader indexHeader = readMergeBlockIndexHeader(thriftReader);
    List<String> fileNames = indexHeader.getFile_names();
    thriftReader.close();
    return fileNames;
  }

  /**
   * Read carbonindexmerge file and update the map
   *
   * @param mergeFilePath
   * @throws IOException
   */
  public void readMergeFile(String mergeFilePath) throws IOException {
    ThriftReader thriftReader = new ThriftReader(mergeFilePath, configuration);
    try {
      thriftReader.open();
      MergedBlockIndexHeader indexHeader = readMergeBlockIndexHeader(thriftReader);
      MergedBlockIndex mergedBlockIndex = readMergeBlockIndex(thriftReader);
      List<String> file_names = indexHeader.getFile_names();
      carbonMergeFileToIndexFilesMap.put(mergeFilePath, file_names);
      List<ByteBuffer> fileData = mergedBlockIndex.getFileData();
      CarbonFile mergeFile = FileFactory.getCarbonFile(mergeFilePath, configuration);
      String mergeFileAbsolutePath = mergeFile.getParentFile().getAbsolutePath();
      assert (file_names.size() == fileData.size());
      for (int i = 0; i < file_names.size(); i++) {
        byte[] data = fileData.get(i).array();
        carbonIndexMap.put(file_names.get(i), data);
        carbonIndexMapWithFullPath
            .put(mergeFileAbsolutePath + CarbonCommonConstants.FILE_SEPARATOR + file_names.get(i),
                data);
      }
    } finally {
      thriftReader.close();
    }
  }

  /**
   * Read carbon index file and convert to stream and add to map
   *
   * @param indexFile
   * @throws IOException
   */
  public void readIndexFile(CarbonFile indexFile) throws IOException {
    String indexFilePath = indexFile.getCanonicalPath();
    try (DataInputStream dataInputStream =
        FileFactory.getDataInputStream(indexFilePath, configuration)) {
      byte[] bytes = new byte[(int) indexFile.getSize()];
      dataInputStream.readFully(bytes);
      carbonIndexMap.put(indexFile.getName(), bytes);
      carbonIndexMapWithFullPath.put(indexFile.getAbsolutePath(), bytes);
    }
  }

  private MergedBlockIndexHeader readMergeBlockIndexHeader(ThriftReader thriftReader)
      throws IOException {
    return (MergedBlockIndexHeader) thriftReader.read(MergedBlockIndexHeader::new);
  }

  private MergedBlockIndex readMergeBlockIndex(ThriftReader thriftReader) throws IOException {
    return (MergedBlockIndex) thriftReader.read(MergedBlockIndex::new);
  }

  /**
   * Get the carbon index file content
   *
   * @param fileName
   * @return
   */
  public byte[] getFileData(String fileName) {
    return carbonIndexMap.get(fileName);
  }

  /**
   * List all the index files of the segment.
   *
   * @param carbonFile directory
   */
  public static void getCarbonIndexFilesRecursively(CarbonFile carbonFile,
      List<CarbonFile> indexFiles) {
    CarbonFile[] carbonFiles = carbonFile.listFiles();
    for (CarbonFile file : carbonFiles) {
      if (file.isDirectory()) {
        getCarbonIndexFilesRecursively(file, indexFiles);
      } else if ((file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
          .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) && file.getSize() > 0) {
        indexFiles.add(file);
      }
    }
  }

  /**
   * List all the index files of the segment.
   *
   * @param segmentPath
   * @return
   */
  public static CarbonFile[] getCarbonIndexFiles(String segmentPath,
      Configuration configuration, String uuid) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath, configuration);
    if (null != uuid) {
      return carbonFile.listFiles(new CarbonFileFilter() {
        @Override
        public boolean accept(CarbonFile file) {
          return file.getName().contains(uuid)
              && (file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
              .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) && file.getSize() > 0;
        }
      });
    }
    return carbonFile.listFiles(new CarbonFileFilter() {
      @Override
      public boolean accept(CarbonFile file) {
        return ((file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
            .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) && file.getSize() > 0);
      }
    });
  }

  /**
   * List all the index files of the segment.
   *
   * @param segmentPath
   * @return
   */
  public static CarbonFile[] getCarbonIndexFiles(String segmentPath, Configuration configuration) {
    return getCarbonIndexFiles(segmentPath, configuration, null);
  }

  /**
   * Return the map that contain index file name and content of the file.
   *
   * @return
   */
  public Map<String, byte[]> getCarbonIndexMap() {
    return carbonIndexMap;
  }

  public Map<String, byte[]> getCarbonIndexMapWithFullPath() {
    return carbonIndexMapWithFullPath;
  }

  /**
   * This method will read the index information from carbon index file
   *
   * @param indexFile
   * @return
   * @throws IOException
   */
  private void readIndexAndFillBlockletInfo(CarbonFile indexFile) throws IOException {
    // flag to take decision whether carbondata file footer reading is required.
    // If the index file does not contain the file footer then carbondata file footer
    // read is required else not required
    boolean isCarbonDataFileFooterReadRequired = true;
    List<BlockletInfo> blockletInfoList = null;
    List<BlockIndex> blockIndexThrift =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
    try {
      indexReader.openThriftReader(indexFile.getCanonicalPath());
      // get the index header
      org.apache.carbondata.format.IndexHeader indexHeader = indexReader.readIndexHeader();
      DataFileFooterConverter fileFooterConverter =
          new DataFileFooterConverter(FileFactory.getConfiguration());
      String filePath = FileFactory.getUpdatedFilePath(indexFile.getCanonicalPath());
      String parentPath =
          filePath.substring(0, filePath.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
      while (indexReader.hasNext()) {
        BlockIndex blockIndex = indexReader.readBlockIndexInfo();
        if (blockIndex.isSetBlocklet_info()) {
          // this case will come in case segment index compaction property is set to false from the
          // application and alter table segment index compaction is run manually. In that case
          // blocklet info will be present in the index but read carbon data file footer property
          // will be true
          isCarbonDataFileFooterReadRequired = false;
          break;
        } else {
          TableBlockInfo blockInfo =
              fileFooterConverter.getTableBlockInfo(blockIndex, indexHeader, parentPath);
          blockletInfoList = getBlockletInfoFromIndexInfo(blockInfo);
        }
        // old store which does not have the blocklet info will have 1 count per part file but in
        // the current code, the number of entries in the index file is equal to the total number
        // of blocklets in all part files for 1 task. So to make it compatible with new structure,
        // the same entry with different blocklet info need to be repeated
        for (BlockletInfo info : blockletInfoList) {
          BlockIndex blockIndexReplica = blockIndex.deepCopy();
          BlockletInfo blockletInfo = info;
          blockIndexReplica
              .setBlock_index(CarbonMetadataUtil.getBlockletIndex(blockletInfo.getBlockletIndex()));
          blockIndexReplica.setBlocklet_info(CarbonMetadataUtil.getBlockletInfo3(blockletInfo));
          blockIndexThrift.add(blockIndexReplica);
        }
      }
      // read complete file at once
      if (!isCarbonDataFileFooterReadRequired) {
        readIndexFile(indexFile);
      } else {
        int totalSize = 0;
        List<byte[]> blockIndexByteArrayList =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        byte[] indexHeaderBytes = CarbonUtil.getByteArray(indexHeader);
        totalSize += indexHeaderBytes.length;
        blockIndexByteArrayList.add(indexHeaderBytes);
        for (BlockIndex blockIndex : blockIndexThrift) {
          byte[] indexInfoBytes = CarbonUtil.getByteArray(blockIndex);
          totalSize += indexInfoBytes.length;
          blockIndexByteArrayList.add(indexInfoBytes);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(totalSize);
        for (byte[] blockIndexBytes : blockIndexByteArrayList) {
          byteBuffer.put(blockIndexBytes);
        }
        carbonIndexMap.put(indexFile.getName(), byteBuffer.array());
      }
    } finally {
      indexReader.closeThriftReader();
    }
  }

  /**
   * This method will read the blocklet info from carbon data file and fill it to index info
   *
   * @param blockInfo
   * @return
   * @throws IOException
   */
  private List<BlockletInfo> getBlockletInfoFromIndexInfo(TableBlockInfo blockInfo)
      throws IOException {
    long startTime = System.currentTimeMillis();
    DataFileFooter carbondataFileFooter = CarbonUtil.readMetadataFile(blockInfo);
    LOGGER.info(
        "Time taken to read carbondata file footer to get blocklet info " + blockInfo.getFilePath()
            + " is " + (System.currentTimeMillis() - startTime));
    return carbondataFileFooter.getBlockletList();
  }

  public Map<String, List<String>> getCarbonMergeFileToIndexFilesMap() {
    return carbonMergeFileToIndexFilesMap;
  }

  public static IndexHeader readIndexHeader(String indexFilePath, Configuration configuration) {
    byte[] indexContent = null;
    if (indexFilePath.toLowerCase().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore(configuration);
      try {
        indexFileStore.readMergeFile(indexFilePath);
      } catch (IOException ex) {
        LOGGER.error(ex);
      }
      Iterator<Map.Entry<String, byte[]>> iterator =
          indexFileStore.getCarbonIndexMap().entrySet().iterator();
      if (iterator.hasNext()) {
        indexContent = iterator.next().getValue();
      }
    }
    CarbonIndexFileReader indexReader = new CarbonIndexFileReader();
    IndexHeader indexHeader = null;
    try {
      if (indexContent == null) {
        indexReader.openThriftReader(indexFilePath);
      } else {
        indexReader.openThriftReader(indexContent);
      }
      // get the index header
      indexHeader = indexReader.readIndexHeader();
    } catch (IOException ex) {
      LOGGER.error(ex);
    } finally {
      indexReader.closeThriftReader();
    }
    return indexHeader;
  }

}
