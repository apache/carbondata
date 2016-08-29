/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.filter.executer;

import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

public class IncludeFilterExecuterImpl implements FilterExecuter {

  protected DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;
  protected DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  protected SegmentProperties segmentProperties;

  public IncludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      SegmentProperties segmentProperties) {
    this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
    this.segmentProperties = segmentProperties;
    dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();
    FilterUtil
        .prepareKeysFromSurrogates(dimColumnEvaluatorInfo.getFilterValues(), segmentProperties,
            dimColumnEvaluatorInfo.getDimension(), dimColumnExecuterInfo);

  }

  @Override public BitSet applyFilter(BlocksChunkHolder blockChunkHolder) {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColumnEvaluatorInfo.getColumnIndex());
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    return getFilteredIndexes(blockChunkHolder.getDimensionDataChunk()[blockIndex],
        blockChunkHolder.getDataBlock().nodeSize());
  }

  protected BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    if (dimensionColumnDataChunk.getAttributes().isNoDictionary()
        && dimensionColumnDataChunk instanceof VariableLengthDimensionDataChunk) {
      return setDirectKeyFilterIndexToBitSet(
          (VariableLengthDimensionDataChunk) dimensionColumnDataChunk, numerOfRows);
    } else if (null != dimensionColumnDataChunk.getAttributes().getInvertedIndexes()
        && dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(
          (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, numerOfRows);
    }

    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows);
  }

  private BitSet setDirectKeyFilterIndexToBitSet(
      VariableLengthDimensionDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    List<byte[]> listOfColumnarKeyBlockDataForNoDictionaryVals =
        dimensionColumnDataChunk.getCompleteDataChunk();
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    int[] columnIndexArray = dimensionColumnDataChunk.getAttributes().getInvertedIndexes();
    int[] columnReverseIndexArray =
        dimensionColumnDataChunk.getAttributes().getInvertedIndexesReverse();
    for (int i = 0; i < filterValues.length; i++) {
      byte[] filterVal = filterValues[i];
      if (null != listOfColumnarKeyBlockDataForNoDictionaryVals) {
        if (null != columnIndexArray) {
          for (int index : columnIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVals.get(columnReverseIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.set(index);
            }
          }
        } else if (null != columnReverseIndexArray) {
          for (int index : columnReverseIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVals.get(columnReverseIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.set(index);
            }
          }
        } else {
          for (int index = 0;
               index < listOfColumnarKeyBlockDataForNoDictionaryVals.size(); index++) {
            if (ByteUtil.UnsafeComparer.INSTANCE
                .compareTo(filterVal, listOfColumnarKeyBlockDataForNoDictionaryVals.get(index))
                == 0) {
              bitSet.set(index);
            }
          }
        }
      }
    }
    return bitSet;

  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      FixedLengthDimensionDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    int[] columnIndex = dimensionColumnDataChunk.getAttributes().getInvertedIndexes();
    int start = 0;
    int last = 0;
    int startIndex = 0;
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[i], false);
      if (start < 0) {
        continue;
      }
      bitSet.set(columnIndex[start]);
      last = start;
      for (int j = start + 1; j < numerOfRows; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(dimensionColumnDataChunk.getCompleteDataChunk(), j * filterValues[i].length,
                filterValues[i].length, filterValues[i], 0, filterValues[i].length) == 0) {
          bitSet.set(columnIndex[j]);
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

  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      FixedLengthDimensionDataChunk fixedDimensionChunk =
          (FixedLengthDimensionDataChunk) dimensionColumnDataChunk;
      byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      for (int k = 0; k < filterValues.length; k++) {
        for (int j = 0; j < numerOfRows; j++) {
          if (ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(fixedDimensionChunk.getCompleteDataChunk(), j * filterValues[k].length,
                  filterValues[k].length, filterValues[k], 0, filterValues[k].length) == 0) {
            bitSet.set(j);
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

    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      int maxCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blkMaxVal[blockIndex]);
      // and filter-min should be positive
      int minCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blkMinVal[blockIndex]);

      // if any filter value is in range than this block needs to be
      // scanned
      if (maxCompare <= 0 && minCompare >= 0) {
        isScanRequired = true;
        break;
      }
    }
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

}
