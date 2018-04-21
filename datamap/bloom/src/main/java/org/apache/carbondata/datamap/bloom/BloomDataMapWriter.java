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
package org.apache.carbondata.datamap.bloom;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datamap.DataMapMeta;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * BloomDataMap is constructed in blocklet level. For each indexed column, a bloom filter is
 * constructed to indicate whether a value belongs to this blocklet. Bloom filter of blocklet that
 * belongs to same block will be written to one index file suffixed with .bloomindex. So the number
 * of bloom index file will be equal to that of the blocks.
 */
@InterfaceAudience.Internal
public class BloomDataMapWriter extends DataMapWriter {
  private String dataMapName;
  private List<String> indexedColumns;
  private int bloomFilterSize;
  // map column name to ordinal in pages
  private Map<String, Integer> col2Ordianl;
  private Map<String, DataType> col2DataType;
  private String currentBlockId;
  private int currentBlockletId;
  private List<String> currentDMFiles;
  private List<DataOutputStream> currentDataOutStreams;
  private List<ObjectOutputStream> currentObjectOutStreams;
  private List<BloomFilter<byte[]>> indexBloomFilters;

  @InterfaceAudience.Internal
  public BloomDataMapWriter(AbsoluteTableIdentifier identifier, DataMapMeta dataMapMeta,
      int bloomFilterSize, Segment segment, String writeDirectoryPath) {
    super(identifier, segment, writeDirectoryPath);
    dataMapName = dataMapMeta.getDataMapName();
    indexedColumns = dataMapMeta.getIndexedColumns();
    this.bloomFilterSize = bloomFilterSize;
    col2Ordianl = new HashMap<String, Integer>(indexedColumns.size());
    col2DataType = new HashMap<String, DataType>(indexedColumns.size());

    currentDMFiles = new ArrayList<String>(indexedColumns.size());
    currentDataOutStreams = new ArrayList<DataOutputStream>(indexedColumns.size());
    currentObjectOutStreams = new ArrayList<ObjectOutputStream>(indexedColumns.size());

    indexBloomFilters = new ArrayList<BloomFilter<byte[]>>(indexedColumns.size());
  }

  @Override
  public void onBlockStart(String blockId, long taskId) throws IOException {
    this.currentBlockId = blockId;
    this.currentBlockletId = 0;
    currentDMFiles.clear();
    currentDataOutStreams.clear();
    currentObjectOutStreams.clear();
    initDataMapFile();
  }

  @Override
  public void onBlockEnd(String blockId) throws IOException {
    for (int indexColId = 0; indexColId < indexedColumns.size(); indexColId++) {
      CarbonUtil.closeStreams(this.currentDataOutStreams.get(indexColId),
          this.currentObjectOutStreams.get(indexColId));
      commitFile(this.currentDMFiles.get(indexColId));
    }
  }

  @Override
  public void onBlockletStart(int blockletId) {
    this.currentBlockletId = blockletId;
    indexBloomFilters.clear();
    for (int i = 0; i < indexedColumns.size(); i++) {
      indexBloomFilters.add(BloomFilter.create(Funnels.byteArrayFunnel(),
          bloomFilterSize, 0.00001d));
    }
  }

  @Override
  public void onBlockletEnd(int blockletId) {
    try {
      writeBloomDataMapFile();
    } catch (Exception e) {
      for (ObjectOutputStream objectOutputStream : currentObjectOutStreams) {
        CarbonUtil.closeStreams(objectOutputStream);
      }
      for (DataOutputStream dataOutputStream : currentDataOutStreams) {
        CarbonUtil.closeStreams(dataOutputStream);
      }
      throw new RuntimeException(e);
    }
  }

  // notice that the input pages only contains the indexed columns
  @Override
  public void onPageAdded(int blockletId, int pageId, ColumnPage[] pages)
      throws IOException {
    col2Ordianl.clear();
    col2DataType.clear();
    for (int colId = 0; colId < pages.length; colId++) {
      String columnName = pages[colId].getColumnSpec().getFieldName().toLowerCase();
      col2Ordianl.put(columnName, colId);
      DataType columnType = pages[colId].getColumnSpec().getSchemaDataType();
      col2DataType.put(columnName, columnType);
    }

    // for each row
    for (int rowId = 0; rowId < pages[0].getPageSize(); rowId++) {
      // for each indexed column
      for (int indexColId = 0; indexColId < indexedColumns.size(); indexColId++) {
        String indexedCol = indexedColumns.get(indexColId);
        byte[] indexValue;
        if (DataTypes.STRING == col2DataType.get(indexedCol)
            || DataTypes.BYTE_ARRAY == col2DataType.get(indexedCol)) {
          byte[] originValue = (byte[]) pages[col2Ordianl.get(indexedCol)].getData(rowId);
          indexValue = new byte[originValue.length - 2];
          System.arraycopy(originValue, 2, indexValue, 0, originValue.length - 2);
        } else {
          Object originValue = pages[col2Ordianl.get(indexedCol)].getData(rowId);
          indexValue = CarbonUtil.getValueAsBytes(col2DataType.get(indexedCol), originValue);
        }

        indexBloomFilters.get(indexColId).put(indexValue);
      }
    }
  }

  private void initDataMapFile() throws IOException {
    String dataMapDir = genDataMapStorePath(this.writeDirectoryPath, this.dataMapName);
    for (int indexColId = 0; indexColId < indexedColumns.size(); indexColId++) {
      String dmFile = dataMapDir + File.separator + this.currentBlockId
          + '.' + indexedColumns.get(indexColId) + BloomCoarseGrainDataMap.BLOOM_INDEX_SUFFIX;
      DataOutputStream dataOutStream = null;
      ObjectOutputStream objectOutStream = null;
      try {
        FileFactory.createNewFile(dmFile, FileFactory.getFileType(dmFile));
        dataOutStream = FileFactory.getDataOutputStream(dmFile,
            FileFactory.getFileType(dmFile));
        objectOutStream = new ObjectOutputStream(dataOutStream);
      } catch (IOException e) {
        CarbonUtil.closeStreams(objectOutStream, dataOutStream);
        throw new IOException(e);
      }

      this.currentDMFiles.add(dmFile);
      this.currentDataOutStreams.add(dataOutStream);
      this.currentObjectOutStreams.add(objectOutStream);
    }
  }

  private void writeBloomDataMapFile() throws IOException {
    for (int indexColId = 0; indexColId < indexedColumns.size(); indexColId++) {
      BloomDMModel model = new BloomDMModel(this.currentBlockId, this.currentBlockletId,
          indexBloomFilters.get(indexColId));
      // only in higher version of guava-bloom-filter, it provides readFrom/writeTo interface.
      // In lower version, we use default java serializer to write bloomfilter.
      this.currentObjectOutStreams.get(indexColId).writeObject(model);
      this.currentObjectOutStreams.get(indexColId).flush();
      this.currentDataOutStreams.get(indexColId).flush();
    }
  }

  @Override
  public void finish() throws IOException {

  }

  @Override
  protected void commitFile(String dataMapFile) throws IOException {
    super.commitFile(dataMapFile);
  }

  /**
   * create and return path that will store the datamap
   *
   * @param dataPath patch to store the carbondata factdata
   * @param dataMapName datamap name
   * @return path to store the datamap
   * @throws IOException
   */
  public static String genDataMapStorePath(String dataPath, String dataMapName)
      throws IOException {
    String dmDir = dataPath + File.separator + dataMapName;
    Path dmPath = FileFactory.getPath(dmDir);
    FileSystem fs = FileFactory.getFileSystem(dmPath);
    if (!fs.exists(dmPath)) {
      fs.mkdirs(dmPath);
    }
    return dmDir;
  }
}
