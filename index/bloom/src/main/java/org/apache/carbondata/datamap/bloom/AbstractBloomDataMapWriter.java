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
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.DataMapWriter;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.util.bloom.CarbonBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

@InterfaceAudience.Internal
public abstract class AbstractBloomDataMapWriter extends DataMapWriter {
  private int bloomFilterSize;
  private double bloomFilterFpp;
  private boolean compressBloom;
  protected int currentBlockletId;
  private List<String> currentDMFiles;
  private List<DataOutputStream> currentDataOutStreams;
  protected List<CarbonBloomFilter> indexBloomFilters;

  AbstractBloomDataMapWriter(String tablePath, String dataMapName, List<CarbonColumn> indexColumns,
      Segment segment, String shardName, SegmentProperties segmentProperties,
      int bloomFilterSize, double bloomFilterFpp, boolean compressBloom)
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
  public void onBlockStart(String blockId) {
  }

  @Override
  public void onBlockEnd(String blockId) {
  }

  @Override
  public void onBlockletStart(int blockletId) {
  }

  protected void resetBloomFilters() {
    indexBloomFilters.clear();
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
    for (int rowId = 0; rowId < pageSize; rowId++) {
      // for each indexed column, add the data to index
      for (int i = 0; i < indexColumns.size(); i++) {
        Object data = pages[i].getData(rowId);
        addValue2BloomIndex(i, data);
      }
    }
  }

  protected void addValue2BloomIndex(int indexColIdx, Object value) {
    byte[] indexValue;
    // convert measure to bytes
    // convert non-dict dimensions to simple bytes without length
    // convert internal-dict dimensions to simple bytes without any encode
    if (indexColumns.get(indexColIdx).isMeasure()) {
      // NULL value of all measures are already processed in `ColumnPage.getData`
      // or `RawBytesReadSupport.readRow` with actual data type

      // Carbon stores boolean as byte. Here we convert it for `getValueAsBytes`
      if (indexColumns.get(indexColIdx).getDataType().equals(DataTypes.BOOLEAN)) {
        value = BooleanConvert.boolean2Byte((Boolean)value);
      }
      indexValue = CarbonUtil.getValueAsBytes(indexColumns.get(indexColIdx).getDataType(), value);
    } else {
      if (indexColumns.get(indexColIdx).getDataType() == DataTypes.DATE) {
        indexValue = convertDictionaryValue(indexColIdx, value);
      } else {
        indexValue = convertNonDictionaryValue(indexColIdx, value);
      }
    }
    if (indexValue.length == 0) {
      indexValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    indexBloomFilters.get(indexColIdx).add(new Key(indexValue));
  }

  protected abstract byte[] convertDictionaryValue(int indexColIdx, Object value);

  protected abstract byte[] convertNonDictionaryValue(int indexColIdx, Object value);

  private void initDataMapFile() throws IOException {
    if (!FileFactory.isFileExist(dataMapPath)) {
      if (!FileFactory.mkdirs(dataMapPath)) {
        throw new IOException("Failed to create directory " + dataMapPath);
      }
    }
    for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
      String dmFile = BloomIndexFileStore.getBloomIndexFile(dataMapPath,
          indexColumns.get(indexColId).getColName());
      DataOutputStream dataOutStream = null;
      try {
        FileFactory.createNewFile(dmFile);
        dataOutStream = FileFactory.getDataOutputStream(dmFile);
      } catch (IOException e) {
        CarbonUtil.closeStreams(dataOutStream);
        throw new IOException(e);
      }

      this.currentDMFiles.add(dmFile);
      this.currentDataOutStreams.add(dataOutStream);
    }
  }

  protected void writeBloomDataMapFile() {
    try {
      for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
        CarbonBloomFilter bloomFilter = indexBloomFilters.get(indexColId);
        bloomFilter.setBlockletNo(currentBlockletId);
        // only in higher version of guava-bloom-filter, it provides readFrom/writeTo interface.
        // In lower version, we use default java serializer to write bloomfilter.
        bloomFilter.write(this.currentDataOutStreams.get(indexColId));
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
  public void finish() {
    if (!isWritingFinished()) {
      releaseResouce();
      setWritingFinished(true);
    }
  }

  protected void releaseResouce() {
    for (int indexColId = 0; indexColId < indexColumns.size(); indexColId++) {
      CarbonUtil.closeStreams(
          currentDataOutStreams.get(indexColId));
    }
  }
}
