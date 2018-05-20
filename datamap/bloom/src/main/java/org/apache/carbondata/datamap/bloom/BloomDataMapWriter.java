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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.util.bloom.CarbonBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * BloomDataMap is constructed in CG level (blocklet level).
 * For each indexed column, a bloom filter is constructed to indicate whether a value
 * belongs to this blocklet. Bloom filter of blocklet that belongs to same block will
 * be written to one index file suffixed with .bloomindex. So the number
 * of bloom index file will be equal to that of the blocks.
 */
@InterfaceAudience.Internal
public class BloomDataMapWriter extends DataMapWriter {
  private static final LogService LOG = LogServiceFactory.getLogService(
      BloomDataMapWriter.class.getCanonicalName());
  private int bloomFilterSize;
  private double bloomFilterFpp;
  private boolean compressBloom;
  protected int currentBlockletId;
  private List<String> currentDMFiles;
  private List<DataOutputStream> currentDataOutStreams;
  protected List<CarbonBloomFilter> indexBloomFilters;

  BloomDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, int bloomFilterSize, double bloomFilterFpp,
      boolean compressBloom)
      throws IOException {
    super(tablePath, dataMapName, indexColumns, segment, shardName);
    this.bloomFilterSize = bloomFilterSize;
    this.bloomFilterFpp = bloomFilterFpp;
    this.compressBloom = compressBloom;
    currentDMFiles = new ArrayList<>(indexColumns.size());
    currentDataOutStreams = new ArrayList<>(indexColumns.size());
    indexBloomFilters = new ArrayList<>(indexColumns.size());
    initDataMapFile();
    resetBloomFilters();
  }

  @Override
  public void onBlockStart(String blockId) throws IOException {
  }

  @Override
  public void onBlockEnd(String blockId) throws IOException {
  }

  @Override
  public void onBlockletStart(int blockletId) {
  }

  protected void resetBloomFilters() {
    indexBloomFilters.clear();
    List<CarbonColumn> indexColumns = getIndexColumns();
    int[] stats = calculateBloomStats();
    for (int i = 0; i < indexColumns.size(); i++) {
      indexBloomFilters
          .add(new CarbonBloomFilter(stats[0], stats[1], Hash.MURMUR_HASH, compressBloom));
    }
  }

  /**
   * It calculates the bits size and number of hash functions to calculate bloom.
   */
  private int[] calculateBloomStats() {
    /*
     * n: how many items you expect to have in your filter
     * p: your acceptable false positive rate
     * Number of bits (m) = -n*ln(p) / (ln(2)^2)
     * Number of hashes(k) = m/n * ln(2)
     */
    double sizeinBits = -bloomFilterSize * Math.log(bloomFilterFpp) / (Math.pow(Math.log(2), 2));
    double numberOfHashes = sizeinBits / bloomFilterSize * Math.log(2);
    int[] stats = new int[2];
    stats[0] = (int) Math.ceil(sizeinBits);
    stats[1] = (int) Math.ceil(numberOfHashes);
    return stats;
  }

  @Override
  public void onBlockletEnd(int blockletId) {
    writeBloomDataMapFile();
    currentBlockletId++;
  }

  @Override
  public void onPageAdded(int blockletId, int pageId, int pageSize, ColumnPage[] pages) {
    List<CarbonColumn> indexColumns = getIndexColumns();
    for (int rowId = 0; rowId < pageSize; rowId++) {
      // for each indexed column, add the data to bloom filter
      for (int i = 0; i < indexColumns.size(); i++) {
        Object data = pages[i].getData(rowId);
        DataType dataType = indexColumns.get(i).getDataType();
        byte[] indexValue;
        if (DataTypes.STRING == dataType) {
          indexValue = getStringData(data);
        } else if (DataTypes.BYTE_ARRAY == dataType) {
          byte[] originValue = (byte[]) data;
          // String and byte array is LV encoded, L is short type
          indexValue = new byte[originValue.length - 2];
          System.arraycopy(originValue, 2, indexValue, 0, originValue.length - 2);
        } else {
          indexValue = CarbonUtil.getValueAsBytes(dataType, data);
        }
        indexBloomFilters.get(i).add(new Key(indexValue));
      }
    }
  }

  protected byte[] getStringData(Object data) {
    byte[] lvData = (byte[]) data;
    byte[] indexValue = new byte[lvData.length - 2];
    System.arraycopy(lvData, 2, indexValue, 0, lvData.length - 2);
    return indexValue;
  }

  private void initDataMapFile() throws IOException {
    if (!FileFactory.isFileExist(dataMapPath)) {
      if (!FileFactory.mkdirs(dataMapPath, FileFactory.getFileType(dataMapPath))) {
        throw new IOException("Failed to create directory " + dataMapPath);
      }
    }
    List<CarbonColumn> indexColumns = getIndexColumns();
    for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
      String dmFile = BloomCoarseGrainDataMap.getBloomIndexFile(dataMapPath,
          indexColumns.get(indexColId).getColName());
      DataOutputStream dataOutStream = null;
      try {
        FileFactory.createNewFile(dmFile, FileFactory.getFileType(dmFile));
        dataOutStream = FileFactory.getDataOutputStream(dmFile,
            FileFactory.getFileType(dmFile));
      } catch (IOException e) {
        CarbonUtil.closeStreams(dataOutStream);
        throw new IOException(e);
      }

      this.currentDMFiles.add(dmFile);
      this.currentDataOutStreams.add(dataOutStream);
    }
  }

  protected void writeBloomDataMapFile() {
    List<CarbonColumn> indexColumns = getIndexColumns();
    try {
      for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
        BloomDMModel model =
            new BloomDMModel(this.currentBlockletId, indexBloomFilters.get(indexColId));
        // only in higher version of guava-bloom-filter, it provides readFrom/writeTo interface.
        // In lower version, we use default java serializer to write bloomfilter.
        model.write(this.currentDataOutStreams.get(indexColId));
        this.currentDataOutStreams.get(indexColId).flush();
      }
    } catch (Exception e) {
      for (DataOutputStream dataOutputStream : currentDataOutStreams) {
        CarbonUtil.closeStreams(dataOutputStream);
      }
      throw new RuntimeException(e);
    } finally {
      resetBloomFilters();
    }
  }

  @Override
  public void finish() throws IOException {
    if (indexBloomFilters.size() > 0) {
      writeBloomDataMapFile();
    }
    releaseResouce();
  }

  protected void releaseResouce() {
    List<CarbonColumn> indexColumns = getIndexColumns();
    for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
      CarbonUtil.closeStreams(
          currentDataOutStreams.get(indexColId));
    }
  }

}
