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

public class ExcludeFilterExecuterImpl implements FilterExecuter {

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

  @Override public BitSet applyFilter(BlocksChunkHolder blockChunkHolder) {
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    return getFilteredIndexes(blockChunkHolder.getDimensionDataChunk()[blockIndex],
        blockChunkHolder.getDataBlock().nodeSize());
  }

  protected BitSet getFilteredIndexes(DimensionColumnDataChunk dimColumnDataChunk,
      int numerOfRows) {
    // For high cardinality dimensions.
    if (dimColumnDataChunk.getAttributes().isNoDictionary()
        && dimColumnDataChunk instanceof VariableLengthDimensionDataChunk) {
      return setDirectKeyFilterIndexToBitSet((VariableLengthDimensionDataChunk) dimColumnDataChunk,
          numerOfRows);
    }
    if (null != dimColumnDataChunk.getAttributes().getInvertedIndexes()
        && dimColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(
          (FixedLengthDimensionDataChunk) dimColumnDataChunk, numerOfRows);
    }
    return setFilterdIndexToBitSet((FixedLengthDimensionDataChunk) dimColumnDataChunk, numerOfRows);
  }

  private BitSet setDirectKeyFilterIndexToBitSet(
      VariableLengthDimensionDataChunk dimColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    List<byte[]> listOfColumnarKeyBlockDataForNoDictionaryVal =
        dimColumnDataChunk.getCompleteDataChunk();
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    int[] columnIndexArray = dimColumnDataChunk.getAttributes().getInvertedIndexes();
    int[] columnReverseIndexArray = dimColumnDataChunk.getAttributes().getInvertedIndexesReverse();
    for (int i = 0; i < filterValues.length; i++) {
      byte[] filterVal = filterValues[i];
      if (null != listOfColumnarKeyBlockDataForNoDictionaryVal) {

        if (null != columnReverseIndexArray) {
          for (int index : columnIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVal.get(columnReverseIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.flip(index);
            }
          }
        } else if (null != columnIndexArray) {

          for (int index : columnIndexArray) {
            byte[] noDictionaryVal =
                listOfColumnarKeyBlockDataForNoDictionaryVal.get(columnIndexArray[index]);
            if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterVal, noDictionaryVal) == 0) {
              bitSet.flip(index);
            }
          }
        } else {
          for (int index = 0;
               index < listOfColumnarKeyBlockDataForNoDictionaryVal.size(); index++) {
            if (ByteUtil.UnsafeComparer.INSTANCE
                .compareTo(filterVal, listOfColumnarKeyBlockDataForNoDictionaryVal.get(index))
                == 0) {
              bitSet.flip(index);
            }
          }

        }

      }
    }
    return bitSet;

  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      FixedLengthDimensionDataChunk dimColumnDataChunk, int numerOfRows) {
    int[] columnIndex = dimColumnDataChunk.getAttributes().getInvertedIndexes();
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
      bitSet.flip(columnIndex[startKey]);
      last = startKey;
      for (int j = startKey + 1; j < numerOfRows; j++) {
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(dimColumnDataChunk.getCompleteDataChunk(), j * filterValues[i].length,
                filterValues[i].length, filterValues[i], 0, filterValues[i].length) == 0) {
          bitSet.flip(columnIndex[j]);
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
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(dimColumnDataChunk.getCompleteDataChunk(), j * filterValues[k].length,
                filterValues[k].length, filterValues[k], 0, filterValues[k].length) == 0) {
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
}
