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

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.expression.Expression;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

public class RowLevelRangeLessThanFiterExecuterImpl extends RowLevelFilterExecuterImpl {
  private byte[][] filterRangeValues;

  public RowLevelRangeLessThanFiterExecuterImpl(
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
      // and filter-min should be positive
      int minCompare =
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blockMinValue[columnIndex]);

      // if any filter applied is not in range of min and max of block
      // then since its a less than fiter validate whether the block
      // min range is less  than applied filter member
      if (minCompare > 0) {
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
    byte[] defaultValue = null;
    if (dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(
              dimColEvaluatorInfoList.get(0).getDimension().getDataType());
      int key = directDictionaryGenerator.generateDirectSurrogateKey(null) + 1;
      defaultValue = FilterUtil.getMaskKey(key, dimColEvaluatorInfoList.get(0).getDimension(),
          this.segmentProperties.getDimensionKeyGenerator());
    }
    if (null != dimensionColumnDataChunk.getAttributes().getInvertedIndexes()
        && dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(
          (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, numerOfRows, defaultValue);
    }
    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows, defaultValue);
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
      FixedLengthDimensionDataChunk dimensionColumnDataChunk, int numerOfRows,
      byte[] defaultValue) {
    BitSet bitSet = new BitSet(numerOfRows);
    int[] columnIndex = dimensionColumnDataChunk.getAttributes().getInvertedIndexes();
    int start = 0;
    int last = 0;
    int startIndex = 0;
    int skip = 0;
    byte[][] filterValues = this.filterRangeValues;

    //find the number of default values to skip the null value in case of direct dictionary
    if (null != defaultValue) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              defaultValue, false);
      if (start < 0) {
        skip = -(start + 1);
        // end of block
        if (skip == numerOfRows) {
          return bitSet;
        }
      } else {
        skip = start;
      }
      startIndex = skip;
    }

    for (int i = 0; i < filterValues.length; i++) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[i], false);
      // Logic will handle the case where the range filter member is not present in block
      // in this case the binary search will return the index from where the bit sets will be
      // set inorder to apply filters. this is Lesser than filter so the range will be taken
      // from the prev element which is Lesser than filter member.
      start = CarbonUtil.nextLesserValueToTarget(start, dimensionColumnDataChunk, filterValues[i]);
      if (start < 0) {
        start = -(start + 1);
        if (start == numerOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its < filter then from that
        // index the bitset will be considered for filtering process.
        if (ByteUtil
            .compare(filterValues[i], dimensionColumnDataChunk.getChunkData(columnIndex[start]))
            < 0) {
          start = start - 1;
        }
      }
      last = start;
      for (int j = start; j >= skip; j--) {
        bitSet.set(columnIndex[j]);
        last--;
      }
      startIndex = last;
      if (startIndex >= 0) {
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
      int numerOfRows, byte[] defaultValue) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      int start = 0;
      int last = 0;
      int startIndex = 0;
      int skip = 0;
      byte[][] filterValues = this.filterRangeValues;
      //find the number of default values to skip the null value in case of direct dictionary
      if (null != defaultValue) {
        start = CarbonUtil.getFirstIndexUsingBinarySearch(
            (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, startIndex, numerOfRows - 1,
            defaultValue, false);
        if (start < 0) {
          skip = -(start + 1);
          // end of block
          if (skip == numerOfRows) {
            return bitSet;
          }
        } else {
          skip = start;
        }
        startIndex = skip;
      }
      for (int k = 0; k < filterValues.length; k++) {
        start = CarbonUtil.getFirstIndexUsingBinarySearch(
            (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, startIndex, numerOfRows - 1,
            filterValues[k], false);
        if(start >= 0) {
          start = CarbonUtil.nextLesserValueToTarget(start,
              (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, filterValues[k]);
        }
        if (start < 0) {
          start = -(start + 1);

          if (start >= numerOfRows) {
            start = numerOfRows - 1;
          }
          // Method will compare the tentative index value after binary search, this tentative
          // index needs to be compared by the filter member if its < filter then from that
          // index the bitset will be considered for filtering process.
          if (ByteUtil.compare(filterValues[k], dimensionColumnDataChunk.getChunkData(start)) < 0) {
            start = start - 1;
          }
        }
        last = start;
        for (int j = start; j >= skip; j--) {
          bitSet.set(j);
          last--;
        }
        startIndex = last;
        if (startIndex <= 0) {
          break;
        }
      }
    }
    return bitSet;
  }
}
