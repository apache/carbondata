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
package org.carbondata.query.filter.executer;

import java.util.BitSet;
import java.util.List;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.carbondata.core.carbon.metadata.encoder.Encoding;
import org.carbondata.core.util.ByteUtil;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.expression.Expression;
import org.carbondata.query.expression.exception.FilterUnsupportedException;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;

public class RowLevelRangeGrtrThanEquaToFilterExecuterImpl extends RowLevelFilterExecuterImpl {

  protected byte[][] filterRangeValues;

  public RowLevelRangeGrtrThanEquaToFilterExecuterImpl(
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, byte[][] filterRangeValues,
      SegmentProperties segmentProperties) {
    super(dimColEvaluatorInfoList, msrColEvalutorInfoList, exp, tableIdentifier, segmentProperties,
        null);
    this.filterRangeValues = filterRangeValues;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = this.filterRangeValues;
    int columnIndex = this.dimColEvaluatorInfoList.get(0).getColumnIndex();
    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      int maxCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blockMaxValue[columnIndex]);
      // if any filter value is in range than this block needs to be
      // scanned less than equal to max range.
      if (maxCompare <= 0) {
        isScanRequired = true;
        break;
      }
    }
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;

  }

  @Override public BitSet applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException {
    if (!dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY)) {
      return super.applyFilter(blockChunkHolder);
    }
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColEvaluatorInfoList.get(0).getColumnIndex());
    if (null == blockChunkHolder.getDimensionDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    return getFilteredIndexes(blockChunkHolder.getDimensionDataChunk()[blockIndex],
        blockChunkHolder.getDataBlock().nodeSize());
  }

  private BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    if (null != dimensionColumnDataChunk.getAttributes().getInvertedIndexes()
        && dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(
          (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, numerOfRows);
    }
    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows);
  }

  /**
   * Method will scan the block and finds the range start index from which all members
   * will be considered for applying range filters. this method will be called if the
   * column is not supported by default so column index mapping  will be present for
   * accesing the members from the block.
   *
   * @param dimensionColumnDataChunk
   * @param numerOfRows
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      FixedLengthDimensionDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    int[] columnIndex = dimensionColumnDataChunk.getAttributes().getInvertedIndexes();
    int start = 0;
    int last = 0;
    int startIndex = 0;
    byte[][] filterValues = this.filterRangeValues;
    for (int i = 0; i < filterValues.length; i++) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[i], false);
      if (start < 0) {
        start = -(start + 1);
        if (start == numerOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its >= filter then from that
        // index the bitset will be considered for filtering process.
        if (ByteUtil
            .compare(filterValues[i], dimensionColumnDataChunk.getChunkData(columnIndex[start]))
            >= 0) {
          start = start + 1;
        }
      }
      last = start;
      for (int j = start; j < numerOfRows; j++) {

        bitSet.set(columnIndex[j]);
        last++;
      }
      startIndex = last;
      if (startIndex >= numerOfRows) {
        break;
      }
    }
    return bitSet;
  }

  /**
   * Method will scan the block and finds the range start index from which all
   * members will be considered for applying range filters. this method will
   * be called if the column is sorted default so column index
   * mapping will be present for accesing the members from the block.
   *
   * @param dimensionColumnDataChunk
   * @param numerOfRows
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      int start = 0;
      int last = 0;
      int startIndex = 0;
      byte[][] filterValues = this.filterRangeValues;
      for (int k = 0; k < filterValues.length; k++) {
        start = CarbonUtil.getFirstIndexUsingBinarySearch(
            (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, startIndex, numerOfRows - 1,
            filterValues[k], false);
        if (start < 0) {
          start = -(start + 1);
          if (start == numerOfRows) {
            start = start - 1;
          }
          // Method will compare the tentative index value after binary search, this tentative
          // index needs to be compared by the filter member if its >= filter then from that
          // index the bitset will be considered for filtering process.
          if (ByteUtil.compare(filterValues[k], dimensionColumnDataChunk.getChunkData(start))
              >= 0) {
            start = start + 1;
          }
        }

        last = start;
        for (int j = start; j < numerOfRows; j++) {
          bitSet.set(j);
          last++;
        }
        startIndex = last;
        if (startIndex >= numerOfRows) {
          break;
        }
      }
    }
    return bitSet;
  }
}
