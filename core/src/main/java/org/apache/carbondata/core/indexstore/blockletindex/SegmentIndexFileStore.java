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
import java.util.*;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.MergedBlockIndex;
import org.apache.carbondata.format.MergedBlockIndexHeader;

import org.apache.thrift.TBase;

/**
 * This class manages reading of index files with in the segment. The files it read can be
 * carbonindex or carbonindexmerge files.
 */
public class SegmentIndexFileStore {

  /**
   * Stores the indexfile name and related binary file data in it.
   */
  private Map<String, byte[]> carbonIndexMap;

  public SegmentIndexFileStore() throws IOException {
    carbonIndexMap = new HashMap<>();
  }

  /**
   * Read all index files and keep the cache in it.
   *
   * @param segmentPath
   * @throws IOException
   */
  public void readAllIIndexOfSegment(String segmentPath) throws IOException {
    CarbonFile[] carbonIndexFiles = getCarbonIndexFiles(segmentPath);
    for (int i = 0; i < carbonIndexFiles.length; i++) {
      if (carbonIndexFiles[i].getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        readMergeFile(carbonIndexFiles[i].getCanonicalPath());
      } else if (carbonIndexFiles[i].getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        readIndexFile(carbonIndexFiles[i]);
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
  public List<String> getIndexFilesFromSegment(String segmentPath) throws IOException {
    CarbonFile[] carbonIndexFiles = getCarbonIndexFiles(segmentPath);
    Set<String> indexFiles = new HashSet<>();
    for (int i = 0; i < carbonIndexFiles.length; i++) {
      if (carbonIndexFiles[i].getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
        indexFiles.addAll(getIndexFilesFromMergeFile(carbonIndexFiles[i].getCanonicalPath()));
      } else if (carbonIndexFiles[i].getName().endsWith(CarbonTablePath.INDEX_FILE_EXT)) {
        indexFiles.add(carbonIndexFiles[i].getName());
      }
    }
    return new ArrayList<>(indexFiles);
  }

  /**
   * List all the index files inside merge file.
   * @param mergeFile
   * @return
   * @throws IOException
   */
  public List<String> getIndexFilesFromMergeFile(String mergeFile) throws IOException {
    List<String> indexFiles = new ArrayList<>();
    ThriftReader thriftReader = new ThriftReader(mergeFile);
    thriftReader.open();
    MergedBlockIndexHeader indexHeader = readMergeBlockIndexHeader(thriftReader);
    List<String> file_names = indexHeader.getFile_names();
    indexFiles.addAll(file_names);
    thriftReader.close();
    return indexFiles;
  }

  /**
   * Read carbonindexmerge file and update the map
   *
   * @param mergeFilePath
   * @throws IOException
   */
  private void readMergeFile(String mergeFilePath) throws IOException {
    ThriftReader thriftReader = new ThriftReader(mergeFilePath);
    thriftReader.open();
    MergedBlockIndexHeader indexHeader = readMergeBlockIndexHeader(thriftReader);
    MergedBlockIndex mergedBlockIndex = readMergeBlockIndex(thriftReader);
    List<String> file_names = indexHeader.getFile_names();
    List<ByteBuffer> fileData = mergedBlockIndex.getFileData();
    assert (file_names.size() == fileData.size());
    for (int i = 0; i < file_names.size(); i++) {
      carbonIndexMap.put(file_names.get(i), fileData.get(i).array());
    }
    thriftReader.close();
  }

  /**
   * Read carbonindex file and convert to stream and add to map
   *
   * @param indexFile
   * @throws IOException
   */
  private void readIndexFile(CarbonFile indexFile) throws IOException {
    String indexFilePath = indexFile.getCanonicalPath();
    DataInputStream dataInputStream =
        FileFactory.getDataInputStream(indexFilePath, FileFactory.getFileType(indexFilePath));
    byte[] bytes = new byte[(int) indexFile.getSize()];
    dataInputStream.readFully(bytes);
    carbonIndexMap.put(indexFile.getName(), bytes);
    dataInputStream.close();
  }

  private MergedBlockIndexHeader readMergeBlockIndexHeader(ThriftReader thriftReader)
      throws IOException {
    return (MergedBlockIndexHeader) thriftReader.read(new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new MergedBlockIndexHeader();
      }
    });
  }

  private MergedBlockIndex readMergeBlockIndex(ThriftReader thriftReader) throws IOException {
    return (MergedBlockIndex) thriftReader.read(new ThriftReader.TBaseCreator() {
      @Override public TBase create() {
        return new MergedBlockIndex();
      }
    });
  }

  /**
   * Get the carbonindex file content
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
   * @param segmentPath
   * @return
   */
  public static CarbonFile[] getCarbonIndexFiles(String segmentPath) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    return carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(CarbonTablePath.INDEX_FILE_EXT) || file.getName()
            .endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT);
      }
    });
  }

  /**
   * Return the map that contain index file name and content of the file.
   *
   * @return
   */
  public Map<String, byte[]> getCarbonIndexMap() {
    return carbonIndexMap;
  }
}
