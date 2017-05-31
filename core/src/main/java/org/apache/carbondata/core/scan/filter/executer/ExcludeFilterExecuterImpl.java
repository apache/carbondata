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

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.CarbonUtil;

public class ExcludeFilterExecuterImpl implements FilterExecuter {

  protected DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  protected DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  protected SegmentProperties segmentProperties;
  /**
   * is dimension column data is natural sorted
   */
  private boolean isNaturalSorted;
  public ExcludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      SegmentProperties segmentProperties) {
    this.dimColEvaluatorInfo = dimColEvaluatorInfo;
    dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();
    this.segmentProperties = segmentProperties;
    FilterUtil.prepareKeysFromSurrogates(dimColEvaluatorInfo.getFilterValues(), segmentProperties,
        dimColEvaluatorInfo.getDimension(), dimColumnExecuterInfo);
    isNaturalSorted = dimColEvaluatorInfo.getDimension().isUseInvertedIndex() && dimColEvaluatorInfo
        .getDimension().isSortColumn();
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
    if (dimColumnDataChunk.isExplicitSorted()) {
      return setFilterdIndexToBitSetWithColumnIndex(dimColumnDataChunk, numerOfRows);
    }
    return setFilterdIndexToBitSet(dimColumnDataChunk, numerOfRows);
  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      DimensionColumnDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    int startIndex = 0;
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      if (startIndex >= numerOfRows) {
        break;
      }
      int[] rangeIndex = CarbonUtil
          .getRangeIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[i]);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
        bitSet.flip(dimensionColumnDataChunk.getInvertedIndex(j));
      }
      if (rangeIndex[1] >= 0) {
        startIndex = rangeIndex[1] + 1;
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    // binary search can only be applied if column is sorted
    if (isNaturalSorted) {
      int startIndex = 0;
      for (int i = 0; i < filterValues.length; i++) {
        if (startIndex >= numerOfRows) {
          break;
        }
        int[] rangeIndex = CarbonUtil
            .getRangeIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
                filterValues[i]);
        for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
          bitSet.flip(j);
        }
        if (rangeIndex[1] >= 0) {
          startIndex = rangeIndex[1] + 1;
        }
      }
    } else {
      if (filterValues.length > 1) {
        for (int i = 0; i < numerOfRows; i++) {
          int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
              dimensionColumnDataChunk.getChunkData(i));
          if (index >= 0) {
            bitSet.flip(i);
          }
        }
      } else {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnDataChunk.compareTo(j, filterValues[0]) == 0) {
            bitSet.flip(j);
          }
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
}
