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
package org.apache.carbondata.core.scan.filter.executer;

import java.io.IOException;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.BitMapDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

public class ExcludeFilterExecuterImpl extends AbstractFilterExecuter {

  protected DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  protected DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  protected SegmentProperties segmentProperties;

  public ExcludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      SegmentProperties segmentProperties) {
    this.dimColEvaluatorInfo = dimColEvaluatorInfo;
    dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();
    this.segmentProperties = segmentProperties;
    FilterUtil.prepareKeysFromSurrogates(dimColEvaluatorInfo.getFilterValues(), segmentProperties,
        dimColEvaluatorInfo.getDimension(), dimColumnExecuterInfo);
  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder) throws IOException {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());
    if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    DimensionRawColumnChunk dimensionRawColumnChunk =
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
    DimensionColumnDataChunk[] dimensionColumnDataChunks =
        dimensionRawColumnChunk.convertToDimColDataChunks();
    BitSetGroup bitSetGroup =
        new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
    for (int i = 0; i < dimensionColumnDataChunks.length; i++) {
      BitSet bitSet = getFilteredIndexes(dimensionColumnDataChunks[i],
          dimensionRawColumnChunk.getRowCount()[i]);
      bitSetGroup.setBitSet(bitSet, i);
    }

    return bitSetGroup;
  }

  protected BitSet getFilteredIndexes(DimensionColumnDataChunk dimColumnDataChunk,
      int numerOfRows) {
    // For high cardinality dimensions.
    if (dimColumnDataChunk.isNoDicitionaryColumn()
        && dimColumnDataChunk instanceof VariableLengthDimensionDataChunk) {
      return setDirectKeyFilterIndexToBitSet((VariableLengthDimensionDataChunk) dimColumnDataChunk,
          numerOfRows);
    }
    if (dimColumnDataChunk.isExplicitSorted()
        && dimColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(
          (FixedLengthDimensionDataChunk) dimColumnDataChunk, numerOfRows);
    }
    if (dimColumnDataChunk instanceof BitMapDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex((BitMapDimensionDataChunk) dimColumnDataChunk,
          numerOfRows);
    }
    return setFilterdIndexToBitSet((FixedLengthDimensionDataChunk) dimColumnDataChunk, numerOfRows);
  }

  private BitSet setDirectKeyFilterIndexToBitSet(
      VariableLengthDimensionDataChunk dimColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      byte[] filterVal = filterValues[i];
      if (dimColumnDataChunk.isExplicitSorted()) {
        for (int index = 0; index < numerOfRows; index++) {
          if (dimColumnDataChunk.compareTo(index, filterVal) == 0) {
            bitSet.flip(dimColumnDataChunk.getInvertedIndex(index));
          }
        }
      } else {
        for (int index = 0; index < numerOfRows; index++) {
          if (dimColumnDataChunk.compareTo(index, filterVal) == 0) {
            bitSet.flip(index);
          }
        }
      }
    }
    return bitSet;

  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      BitMapDimensionDataChunk dimensionColumnDataChunk, int numerOfRows) {

    return dimensionColumnDataChunk.applyFilter(dimColumnExecuterInfo.getFilterKeys(),
        FilterOperator.NOT_IN, numerOfRows);
  }
  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      FixedLengthDimensionDataChunk dimColumnDataChunk, int numerOfRows) {
    int startKey = 0;
    int last = 0;
    int startIndex = 0;
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      startKey = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[i], false);
      if (startKey < 0) {
        continue;
      }
      bitSet.flip(dimColumnDataChunk.getInvertedIndex(startKey));
      last = startKey;
      for (int j = startKey + 1; j < numerOfRows; j++) {
        if (dimColumnDataChunk.compareTo(j, filterValues[i]) == 0) {
          bitSet.flip(dimColumnDataChunk.getInvertedIndex(j));
          last++;
        } else {
          break;
        }
      }
      startIndex = last;
      if (startIndex >= numerOfRows) {
        break;
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(FixedLengthDimensionDataChunk dimColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int k = 0; k < filterValues.length; k++) {
      for (int j = 0; j < numerOfRows; j++) {
        if (dimColumnDataChunk.compareTo(j, filterValues[k]) == 0) {
          bitSet.flip(j);
        }
      }
    }
    return bitSet;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.flip(0, 1);
    return bitSet;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());
    if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
  }
  @Override public boolean isReadRequired(BlocksChunkHolder blockChunkHolder) {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());
    List<Integer> bitMapEncodedDictionaries = blockChunkHolder.getDataBlock()
        .getBitMapEncodedDictionariesInfoList().get(blockIndex).getBitmap_encoded_dictionary_list();
    byte[][] filterKeys = dimColumnExecuterInfo.getFilterKeys();
    return isReadRequired(bitMapEncodedDictionaries, filterKeys);
  }

  private boolean isReadRequired(List<Integer> bitMapEncodedDictionaries, byte[][] filterKeys) {
    if (bitMapEncodedDictionaries != null && bitMapEncodedDictionaries.size() > 0) {
      boolean result = false;
      for (Integer dict : bitMapEncodedDictionaries) {
        int index = CarbonUtil.binarySearch(filterKeys, 0, filterKeys.length - 1,
            ByteUtil.convertIntToByteArray(dict, filterKeys[0].length));
        if (index < 0) {
          result = true;
          break;
        }
      }
      return result;
    } else {
      return true;
    }
  }
}
