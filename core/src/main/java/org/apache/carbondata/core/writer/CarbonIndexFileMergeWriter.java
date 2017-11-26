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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.MergedBlockIndex;
import org.apache.carbondata.format.MergedBlockIndexHeader;

public class CarbonIndexFileMergeWriter {

  /**
   * thrift writer object
   */
  private ThriftWriter thriftWriter;

  /**
   * Merge all the carbonindex files of segment to a  merged file
   * @param segmentPath
   * @throws IOException
   */
  public void mergeCarbonIndexFilesOfSegment(String segmentPath) throws IOException {
    CarbonFile[] indexFiles = SegmentIndexFileStore.getCarbonIndexFiles(segmentPath);
    if (isCarbonIndexFilePresent(indexFiles)) {
      SegmentIndexFileStore fileStore = new SegmentIndexFileStore();
      fileStore.readAllIIndexOfSegment(segmentPath);
      openThriftWriter(
          segmentPath + "/" +
              System.currentTimeMillis() + CarbonTablePath.MERGE_INDEX_FILE_EXT);
      Map<String, byte[]> indexMap = fileStore.getCarbonIndexMap();
      MergedBlockIndexHeader indexHeader = new MergedBlockIndexHeader();
      MergedBlockIndex mergedBlockIndex = new MergedBlockIndex();
      List<String> fileNames = new ArrayList<>(indexMap.size());
      List<ByteBuffer> data = new ArrayList<>(indexMap.size());
      for (Map.Entry<String, byte[]> entry : indexMap.entrySet()) {
        fileNames.add(entry.getKey());
        data.add(ByteBuffer.wrap(entry.getValue()));
      }
      indexHeader.setFile_names(fileNames);
      mergedBlockIndex.setFileData(data);
      writeMergedBlockIndexHeader(indexHeader);
      writeMergedBlockIndex(mergedBlockIndex);
      close();
      for (CarbonFile indexFile : indexFiles) {
        indexFile.delete();
      }
    }
  }

  private boolean isCarbonIndexFilePresent(CarbonFile[] indexFiles) {
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
    thriftWriter.open(FileWriteOperation.OVERWRITE);
  }

  /**
   * Below method will be used to close the thrift object
   */
  private void close() throws IOException {
    thriftWriter.close();
  }

}
