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
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

public class IncludeFilterExecuterImpl implements FilterExecuter {

  protected DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;
  protected DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  protected SegmentProperties segmentProperties;

  public IncludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      SegmentProperties segmentProperties) {
    this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
    this.segmentProperties = segmentProperties;
    dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();
    FilterUtil.prepareKeysFromSurrogates(dimColumnEvaluatorInfo.getFilterValues(),
        segmentProperties, dimColumnEvaluatorInfo.getDimension(), dimColumnExecuterInfo);

  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder) throws IOException {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColumnEvaluatorInfo.getColumnIndex());
    if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    DimensionRawColumnChunk dimensionRawColumnChunk =
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
    BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
    for (int i = 0; i < dimensionRawColumnChunk.getPagesCount(); i++) {
      if (dimensionRawColumnChunk.getMaxValues() != null) {
        if (isScanRequired(dimensionRawColumnChunk.getMaxValues()[i],
            dimensionRawColumnChunk.getMinValues()[i], dimColumnExecuterInfo.getFilterKeys())) {
          BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.convertToDimColDataChunk(i),
              dimensionRawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      } else {
        BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.convertToDimColDataChunk(i),
            dimensionRawColumnChunk.getRowCount()[i]);
        bitSetGroup.setBitSet(bitSet, i);
      }
    }
    return bitSetGroup;
  }

  protected BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    if (dimensionColumnDataChunk.isNoDicitionaryColumn()
        && dimensionColumnDataChunk instanceof VariableLengthDimensionDataChunk) {
      return setDirectKeyFilterIndexToBitSet(
          (VariableLengthDimensionDataChunk) dimensionColumnDataChunk, numerOfRows);
    } else if (dimensionColumnDataChunk.isExplicitSorted()
        && dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(
          (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, numerOfRows);
    }

    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows);
  }

  private BitSet setDirectKeyFilterIndexToBitSet(
      VariableLengthDimensionDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      byte[] filterVal = filterValues[i];
      if (dimensionColumnDataChunk.isExplicitSorted()) {
        for (int index = 0; index < numerOfRows; index++) {
          if (dimensionColumnDataChunk.compareTo(index, filterVal) == 0) {
            bitSet.set(dimensionColumnDataChunk.getInvertedIndex(index));
          }
        }
      } else {
        for (int index = 0; index < numerOfRows; index++) {
          if (dimensionColumnDataChunk.compareTo(index, filterVal) == 0) {
            bitSet.set(index);
          }
        }
      }
    }
    return bitSet;

  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      FixedLengthDimensionDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    int startIndex = 0;
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      int[] rangeIndex = CarbonUtil.getRangeIndexUsingBinarySearch(dimensionColumnDataChunk,
          startIndex, numerOfRows - 1, filterValues[i]);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {

        bitSet.set(dimensionColumnDataChunk.getInvertedIndex(j));
      }

      if (rangeIndex[1] >= 0) {
        startIndex = rangeIndex[1];
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      if (filterValues.length > 1) {
        for (int i = 0; i < numerOfRows; i++) {
          int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
              dimensionColumnDataChunk.getChunkData(i));
          if (index >= 0) {
            bitSet.set(i);
          }
        }
      } else if (filterValues.length == 1) {
        for (int i = 0; i < numerOfRows; i++) {
          if (dimensionColumnDataChunk.compareTo(i, filterValues[0]) == 0) {
            bitSet.set(i);
          }
        }
      }
    }
    return bitSet;
  }

  public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    int columnIndex = dimColumnEvaluatorInfo.getColumnIndex();
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping().get(columnIndex);

    boolean isScanRequired = blockIndex >= blkMaxVal.length ||
        isScanRequired(blkMaxVal[blockIndex], blkMinVal[blockIndex], filterValues);
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  private boolean isScanRequired(byte[] blkMaxVal, byte[] blkMinVal, byte[][] filterValues) {
    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      int maxCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blkMaxVal);
      // and filter-min should be positive
      int minCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blkMinVal);

      // if any filter value is in range than this block needs to be
      // scanned
      if (maxCompare <= 0 && minCompare >= 0) {
        isScanRequired = true;
        break;
      }
    }
    return isScanRequired;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColumnEvaluatorInfo.getColumnIndex());
    if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
  }
}
