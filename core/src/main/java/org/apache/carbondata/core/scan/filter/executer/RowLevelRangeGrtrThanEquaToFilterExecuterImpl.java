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
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class RowLevelRangeGrtrThanEquaToFilterExecuterImpl extends RowLevelFilterExecuterImpl {

  protected byte[][] filterRangeValues;
  private Object[] msrFilterRangeValues;
  private SerializableComparator comparator;
  /**
   * flag to check whether default values is present in the filter value list
   */
  private boolean isDefaultValuePresentInFilter;
  private int lastDimensionColOrdinal = 0;

  public RowLevelRangeGrtrThanEquaToFilterExecuterImpl(
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, byte[][] filterRangeValues,
      Object[] msrFilterRangeValues, SegmentProperties segmentProperties) {
    super(dimColEvaluatorInfoList, msrColEvalutorInfoList, exp, tableIdentifier, segmentProperties,
        null);
    this.filterRangeValues = filterRangeValues;
    this.msrFilterRangeValues = msrFilterRangeValues;
    lastDimensionColOrdinal = segmentProperties.getLastDimensionColOrdinal();
    if (isMeasurePresentInCurrentBlock[0]) {
      CarbonMeasure measure = this.msrColEvalutorInfoList.get(0).getMeasure();
      comparator = Comparator.getComparatorByDataTypeForMeasure(measure.getDataType());
    }
    if (isDimensionPresentInCurrentBlock[0] == true) {
      isNaturalSorted = dimColEvaluatorInfoList.get(0).getDimension().isUseInvertedIndex()
          && dimColEvaluatorInfoList.get(0).getDimension().isSortColumn();
    }
    ifDefaultValueMatchesFilter();
  }

  /**
   * This method will check whether default value is present in the given filter values
   */
  private void ifDefaultValueMatchesFilter() {
    if (!dimColEvaluatorInfoList.isEmpty() && !isDimensionPresentInCurrentBlock[0]) {
      CarbonDimension dimension = this.dimColEvaluatorInfoList.get(0).getDimension();
      byte[] defaultValue = dimension.getDefaultValue();
      if (null != defaultValue) {
        for (int k = 0; k < filterRangeValues.length; k++) {
          int maxCompare =
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterRangeValues[k], defaultValue);
          if (maxCompare <= 0) {
            isDefaultValuePresentInFilter = true;
            break;
          }
        }
      }
    } else if (!msrColEvalutorInfoList.isEmpty() && !isMeasurePresentInCurrentBlock[0]) {
      CarbonMeasure measure = this.msrColEvalutorInfoList.get(0).getMeasure();
      byte[] defaultValue = measure.getDefaultValue();
      if (null != defaultValue) {
        for (int k = 0; k < msrFilterRangeValues.length; k++) {
          int maxCompare = comparator.compare(msrFilterRangeValues[k],
              DataTypeUtil.getMeasureObjectFromDataType(defaultValue, measure.getDataType()));
          if (maxCompare <= 0) {
            isDefaultValuePresentInFilter = true;
            break;
          }
        }
      }
    }
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    boolean isScanRequired = false;
    byte[] maxValue = null;
    if (isMeasurePresentInCurrentBlock[0] || isDimensionPresentInCurrentBlock[0]) {
      if (isMeasurePresentInCurrentBlock[0]) {
        maxValue = blockMaxValue[measureBlocksIndex[0] + lastDimensionColOrdinal];
        isScanRequired =
            isScanRequired(maxValue, msrFilterRangeValues, msrColEvalutorInfoList.get(0).getType());
      } else {
        maxValue = blockMaxValue[dimensionBlocksIndex[0]];
        isScanRequired = isScanRequired(maxValue, filterRangeValues);
      }
    } else {
      isScanRequired = isDefaultValuePresentInFilter;
    }

    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  private boolean isScanRequired(byte[] blockMaxValue, byte[][] filterValues) {
    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      int maxCompare = ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[k], blockMaxValue);
      // if any filter value is in range than this block needs to be
      // scanned less than equal to max range.
      if (maxCompare <= 0) {
        isScanRequired = true;
        break;
      }
    }
    return isScanRequired;
  }

  private boolean isScanRequired(byte[] maxValue, Object[] filterValue,
      DataType dataType) {
    Object value = DataTypeUtil.getMeasureObjectFromDataType(maxValue, dataType);
    for (int i = 0; i < filterValue.length; i++) {
      // TODO handle min and max for null values.
      if (filterValue[i] == null) {
        return true;
      }
      if (comparator.compare(filterValue[i], value) <= 0) {
        return true;
      }
    }
    return false;
  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock[0] && !isMeasurePresentInCurrentBlock[0]) {
      int numberOfRows = blockChunkHolder.getDataBlock().nodeSize();
      return FilterUtil
          .createBitSetGroupWithDefaultValue(blockChunkHolder.getDataBlock().numberOfPages(),
              numberOfRows, true);
    }

    if (isDimensionPresentInCurrentBlock[0]) {
      int blockIndex =
          segmentProperties.getDimensionOrdinalToBlockMapping().get(dimensionBlocksIndex[0]);
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
      DimensionRawColumnChunk rawColumnChunk =
          blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(rawColumnChunk.getMaxValues()[i], this.filterRangeValues)) {
            int compare = ByteUtil.UnsafeComparer.INSTANCE
                .compareTo(filterRangeValues[0], rawColumnChunk.getMinValues()[i]);
            if (compare <= 0) {
              BitSet bitSet = new BitSet(rawColumnChunk.getRowCount()[i]);
              bitSet.flip(0, rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            } else {
              BitSet bitSet = getFilteredIndexes(rawColumnChunk.convertToDimColDataChunk(i),
                  rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            }
          }
        } else {
          BitSet bitSet = getFilteredIndexes(rawColumnChunk.convertToDimColDataChunk(i),
              rawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    } else if (isMeasurePresentInCurrentBlock[0]) {
      int blockIndex =
          segmentProperties.getMeasuresOrdinalToBlockMapping().get(measureBlocksIndex[0]);
      if (null == blockChunkHolder.getMeasureRawDataChunk()[blockIndex]) {
        blockChunkHolder.getMeasureRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getMeasureChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
      MeasureRawColumnChunk rawColumnChunk = blockChunkHolder.getMeasureRawDataChunk()[blockIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(rawColumnChunk.getMaxValues()[i], this.msrFilterRangeValues,
              msrColEvalutorInfoList.get(0).getType())) {
            int compare = comparator.compare(msrFilterRangeValues[0], DataTypeUtil
                .getMeasureObjectFromDataType(rawColumnChunk.getMinValues()[i],
                    msrColEvalutorInfoList.get(0).getType()));
            ColumnPage columnPage =
                rawColumnChunk.convertToColumnPage(i);
            if (compare <= 0 && columnPage.getNullBits().isEmpty()) {
              BitSet bitSet = new BitSet(rawColumnChunk.getRowCount()[i]);
              bitSet.flip(0, rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            } else {
              BitSet bitSet =
                  getFilteredIndexesForMeasures(rawColumnChunk.convertToColumnPage(i),
                      rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            }
          }
        } else {
          BitSet bitSet =
              getFilteredIndexesForMeasures(rawColumnChunk.convertToColumnPage(i),
                  rawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    }
    return null;
  }

  private BitSet getFilteredIndexesForMeasures(ColumnPage columnPage,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    Object[] filterValues = this.msrFilterRangeValues;
    DataType msrType = msrColEvalutorInfoList.get(0).getType();
    SerializableComparator comparator = Comparator.getComparatorByDataTypeForMeasure(msrType);
    BitSet nullBitSet = columnPage.getNullBits();
    for (int i = 0; i < filterValues.length; i++) {
      if (filterValues[i] == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.set(j);
        }
        continue;
      }
      for (int startIndex = 0; startIndex < numerOfRows; startIndex++) {
        if (!nullBitSet.get(startIndex)) {
          Object msrValue = DataTypeUtil
              .getMeasureObjectBasedOnDataType(columnPage, startIndex,
                  msrType, msrColEvalutorInfoList.get(0).getMeasure());

          if (comparator.compare(msrValue, filterValues[i]) >= 0) {
            // This is a match.
            bitSet.set(startIndex);
          }
        }
      }
    }
    return bitSet;
  }


  private BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = null;
    if (dimensionColumnDataChunk.isExplicitSorted()) {
      bitSet = setFilterdIndexToBitSetWithColumnIndex(dimensionColumnDataChunk, numerOfRows);
    } else {
      bitSet = setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows);
    }
    if (dimensionColumnDataChunk.isNoDicitionaryColumn()) {
      FilterUtil.removeNullValues(dimensionColumnDataChunk, bitSet);
    }
    return bitSet;
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
      DimensionColumnDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
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
        if (ByteUtil.compare(filterValues[i],
            dimensionColumnDataChunk.getChunkData(dimensionColumnDataChunk.getInvertedIndex(start)))
            > 0) {
          start = start + 1;
        }
      }
      last = start;
      for (int j = start; j < numerOfRows; j++) {
        bitSet.set(dimensionColumnDataChunk.getInvertedIndex(j));
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
    byte[][] filterValues = this.filterRangeValues;
    // binary search can only be applied if column is sorted
    if (isNaturalSorted && !dimensionColumnDataChunk.isNoDicitionaryColumn()) {
      int start = 0;
      int last = 0;
      int startIndex = 0;
      for (int k = 0; k < filterValues.length; k++) {
        start = CarbonUtil
            .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex,
                numerOfRows - 1, filterValues[k], false);
        if (start < 0) {
          start = -(start + 1);
          if (start == numerOfRows) {
            start = start - 1;
          }
          // Method will compare the tentative index value after binary search, this tentative
          // index needs to be compared by the filter member if its >= filter then from that
          // index the bitset will be considered for filtering process.
          if (ByteUtil.compare(filterValues[k], dimensionColumnDataChunk.getChunkData(start)) > 0) {
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
    } else {
      for (int k = 0; k < filterValues.length; k++) {
        for (int i = 0; i < numerOfRows; i++) {
          if (ByteUtil.compare(dimensionColumnDataChunk.getChunkData(i), filterValues[k]) >= 0) {
            bitSet.set(i);
          }
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
    } else if (isMeasurePresentInCurrentBlock[0]) {
      int blockIndex = measureBlocksIndex[0];
      if (null == blockChunkHolder.getMeasureRawDataChunk()[blockIndex]) {
        blockChunkHolder.getMeasureRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getMeasureChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    }
  }
}
