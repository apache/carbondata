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
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

public class RowLevelRangeLessThanEqualFilterExecuterImpl extends RowLevelFilterExecuterImpl {
  protected byte[][] filterRangeValues;

  /**
   * flag to check whether default values is present in the filter value list
   */
  private boolean isDefaultValuePresentInFilter;

  public RowLevelRangeLessThanEqualFilterExecuterImpl(
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, byte[][] filterRangeValues,
      SegmentProperties segmentProperties) {
    super(dimColEvaluatorInfoList, msrColEvalutorInfoList, exp, tableIdentifier, segmentProperties,
        null);
    this.filterRangeValues = filterRangeValues;
    ifDefaultValueMatchesFilter();
  }

  /**
   * This method will check whether default value is present in the given filter values
   */
  private void ifDefaultValueMatchesFilter() {
    if (!this.isDimensionPresentInCurrentBlock[0]) {
      CarbonDimension dimension = this.dimColEvaluatorInfoList.get(0).getDimension();
      byte[] defaultValue = dimension.getDefaultValue();
      if (null != defaultValue) {
        for (int k = 0; k < filterRangeValues.length; k++) {
          int maxCompare =
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterRangeValues[k], defaultValue);
          if (maxCompare >= 0) {
            isDefaultValuePresentInFilter = true;
            break;
          }
        }
      }
    }
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    boolean isScanRequired =
        isScanRequired(blockMinValue[dimensionBlocksIndex[0]], filterRangeValues);
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  private boolean isScanRequired(byte[] blockMinValue, byte[][] filterValues) {
    boolean isScanRequired = false;
    if (isDimensionPresentInCurrentBlock[0]) {
      for (int k = 0; k < filterValues.length; k++) {
        // and filter-min should be positive
        int minCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blockMinValue);

        // if any filter applied is not in range of min and max of block
        // then since its a less than equal to fiter validate whether the block
        // min range is less than equal to applied filter member
        if (minCompare >= 0) {
          isScanRequired = true;
          break;
        }
      }
    } else {
      isScanRequired = isDefaultValuePresentInFilter;
    }
    return isScanRequired;
  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock[0]) {
      int numberOfRows = blockChunkHolder.getDataBlock().nodeSize();
      return FilterUtil
          .createBitSetGroupWithDefaultValue(blockChunkHolder.getDataBlock().numberOfPages(),
              numberOfRows, true);
    }
    if (!dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY)) {
      return super.applyFilter(blockChunkHolder);
    }
    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimensionBlocksIndex[0]);
    if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }
    DimensionRawColumnChunk rawColumnChunk =
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
    BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
    for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
      if (rawColumnChunk.getMinValues() != null) {
        if (isScanRequired(rawColumnChunk.getMinValues()[i], this.filterRangeValues)) {
          BitSet bitSet = getFilteredIndexes(rawColumnChunk.convertToDimColDataChunk(i),
              rawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      } else {
        BitSet bitSet = getFilteredIndexes(rawColumnChunk.convertToDimColDataChunk(i),
            rawColumnChunk.getRowCount()[i]);
        bitSetGroup.setBitSet(bitSet, i);
      }
    }
    return bitSetGroup;
  }

  private BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    byte[] defaultValue = null;
    if (dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(
              dimColEvaluatorInfoList.get(0).getDimension().getDataType());
      int key = directDictionaryGenerator.generateDirectSurrogateKey(null) + 1;
      CarbonDimension currentBlockDimension =
          segmentProperties.getDimensions().get(dimensionBlocksIndex[0]);
      defaultValue = FilterUtil.getMaskKey(key, currentBlockDimension,
          this.segmentProperties.getSortColumnsGenerator());
    }
    if (dimensionColumnDataChunk.isExplicitSorted()
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
    int start = 0;
    int last = 0;
    int skip = 0;
    int startIndex = 0;
    byte[][] filterValues = this.filterRangeValues;
    //find the number of default values to skip the null value in case of direct dictionary
    if (null != defaultValue) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              defaultValue, true);
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
              filterValues[i], true);
      if (start < 0) {
        start = -(start + 1);
        if (start >= numerOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its >= filter then from that
        // index the bitset will be considered for filtering process.
        if (ByteUtil.compare(filterValues[i],
            dimensionColumnDataChunk.getChunkData(dimensionColumnDataChunk.getInvertedIndex(start)))
            <= 0) {
          start = start - 1;
        }
      }
      last = start;
      for (int j = start; j >= skip; j--) {
        bitSet.set(dimensionColumnDataChunk.getInvertedIndex(j));
        last--;
      }
      startIndex = last;
      if (startIndex <= 0) {
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
   * @param defaultValue
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows, byte[] defaultValue) {
    BitSet bitSet = new BitSet(numerOfRows);
    if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      int start = 0;
      int last = 0;
      int startIndex = 0;
      byte[][] filterValues = this.filterRangeValues;
      int skip = 0;
      //find the number of default values to skip the null value in case of direct dictionary
      if (null != defaultValue) {
        start = CarbonUtil.getFirstIndexUsingBinarySearch(
            (FixedLengthDimensionDataChunk) dimensionColumnDataChunk, startIndex, numerOfRows - 1,
            defaultValue, true);
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
            filterValues[k], true);
        if (start < 0) {
          start = -(start + 1);
          if (start >= numerOfRows) {
            start = start - 1;
          }
          // Method will compare the tentative index value after binary search, this tentative
          // index needs to be compared by the filter member if its <= filter then from that
          // index the bitset will be considered for filtering process.
          if (ByteUtil.compare(filterValues[k], dimensionColumnDataChunk.getChunkData(start))
              <= 0) {
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

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    if (isDimensionPresentInCurrentBlock[0]) {
      if (!dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY)) {
        super.readBlocks(blockChunkHolder);
      }
      int blockIndex = dimensionBlocksIndex[0];
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    }
  }
}
