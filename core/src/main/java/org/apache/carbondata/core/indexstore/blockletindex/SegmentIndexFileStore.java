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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.filesystem.CarbonFileFilter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.reader.ThriftReader;
import org.apache.carbondata.format.MergedBlockIndex;
import org.apache.carbondata.format.MergedBlockIndexHeader;

import org.apache.thrift.TBase;

public class SegmentIndexFileStore {

  private Map<String, byte[]> carbonIndexMap;

  public SegmentIndexFileStore() throws IOException {
    carbonIndexMap = new HashMap<>();
  }

  public void readAllIIndexOfSegment(String segmentPath) throws IOException {
    CarbonFile[] carbonIndexFiles = getCarbonIndexFiles(segmentPath);
    for (int i = 0; i < carbonIndexFiles.length; i++) {
      if (carbonIndexFiles[i].getName().endsWith(".carbonindexmerge")) {
        readMergeFile(carbonIndexFiles[i].getCanonicalPath());
      } else if (carbonIndexFiles[i].getName().endsWith(".carbonindex")) {
        readIndexFile(carbonIndexFiles[i]);
      }
    }
  }

  public List<String> getIndexFiles(String segmentPath) throws IOException {
    CarbonFile[] carbonIndexFiles = getCarbonIndexFiles(segmentPath);
    Set<String> indexFiles = new HashSet<>();
    for (int i = 0; i < carbonIndexFiles.length; i++) {
      if (carbonIndexFiles[i].getName().endsWith(".carbonindexmerge")) {
        ThriftReader thriftReader = new ThriftReader(carbonIndexFiles[i].getCanonicalPath());
        thriftReader.open();
        MergedBlockIndexHeader indexHeader = readMergeBlockIndexHeader(thriftReader);
        List<String> file_names = indexHeader.getFile_names();
        indexFiles.addAll(file_names);
        thriftReader.close();
      } else if (carbonIndexFiles[i].getName().endsWith(".carbonindex")) {
        indexFiles.add(carbonIndexFiles[i].getName());
      }
    }
    return new ArrayList<>(indexFiles);
  }

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

  private void readIndexFile(CarbonFile indexFile) throws IOException {
    String indexFilePath = indexFile.getCanonicalPath();
    DataInputStream dataInputStream =
        FileFactory.getDataInputStream(indexFilePath, FileFactory.getFileType(indexFilePath));
    byte[] bytes = new byte[(int) indexFile.getSize()];
    dataInputStream.readFully(bytes);
    carbonIndexMap.put(indexFile.getName(), bytes);
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

  public byte[] getFileData(String fileName) {
    return carbonIndexMap.get(fileName);
  }

  public static CarbonFile[] getCarbonIndexFiles(String segmentPath) {
    CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
    return carbonFile.listFiles(new CarbonFileFilter() {
      @Override public boolean accept(CarbonFile file) {
        return file.getName().endsWith(".carbonindex") || file.getName()
            .endsWith(".carbonindexmerge");
      }
    });
  }

  public Map<String, byte[]> getCarbonIndexMap() {
    return carbonIndexMap;
  }
}
