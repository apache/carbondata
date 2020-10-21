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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class RowLevelRangeGreaterThanEqualFilterExecutorImpl extends RowLevelFilterExecutorImpl {

  private byte[][] filterRangeValues;
  private Object[] msrFilterRangeValues;
  private SerializableComparator comparator;
  private Object[] dimFilterRangeValues;
  /**
   * flag to check whether default values is present in the filter value list
   */
  private boolean isDefaultValuePresentInFilter;

  RowLevelRangeGreaterThanEqualFilterExecutorImpl(
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, byte[][] filterRangeValues,
      Object[] msrFilterRangeValues, SegmentProperties segmentProperties) {
    super(dimColEvaluatorInfoList, msrColEvalutorInfoList, exp, tableIdentifier, segmentProperties,
        null);
    this.filterRangeValues = filterRangeValues;
    this.msrFilterRangeValues = msrFilterRangeValues;
    this.dimFilterRangeValues = filterRangeValues;
    if (!msrColEvalutorInfoList.isEmpty()) {
      CarbonMeasure measure = this.msrColEvalutorInfoList.get(0).getMeasure();
      comparator = Comparator.getComparatorByDataTypeForMeasure(measure.getDataType());
    }
    if (isDimensionPresentInCurrentBlock[0]) {
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
        for (int k = 0; k < dimFilterRangeValues.length; k++) {
          int maxCompare = FilterUtil
              .compareValues((byte[]) dimFilterRangeValues[k], defaultValue, dimension, false);
          if (maxCompare <= 0) {
            isDefaultValuePresentInFilter = true;
            break;
          }
        }
      }
    } else if (!msrColEvalutorInfoList.isEmpty() && !isMeasurePresentInCurrentBlock[0]) {
      CarbonMeasure measure = this.msrColEvalutorInfoList.get(0).getMeasure();
      byte[] defaultValue = measure.getDefaultValue();
      SerializableComparator comparatorTmp =
          Comparator.getComparatorByDataTypeForMeasure(measure.getDataType());
      if (null != defaultValue) {
        for (int k = 0; k < msrFilterRangeValues.length; k++) {
          int maxCompare = comparatorTmp.compare(msrFilterRangeValues[k],
              RestructureUtil.getMeasureDefaultValue(measure.getColumnSchema(),
                  measure.getDefaultValue()));
          if (maxCompare <= 0) {
            isDefaultValuePresentInFilter = true;
            break;
          }
        }
      }
    }
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    boolean isScanRequired = false;
    byte[] maxValue = null;
    if (isMeasurePresentInCurrentBlock[0] || isDimensionPresentInCurrentBlock[0]) {
      if (isMeasurePresentInCurrentBlock[0]) {
        if (isMinMaxSet[measureChunkIndex[0]]) {
          maxValue = blockMaxValue[measureChunkIndex[0]];
          isScanRequired = isScanRequired(maxValue, msrFilterRangeValues,
              msrColEvalutorInfoList.get(0).getType());
        } else {
          isScanRequired = true;
        }
      } else {
        maxValue = blockMaxValue[dimensionChunkIndex[0]];
        DataType dataType = dimColEvaluatorInfoList.get(0).getDimension().getDataType();
        // for no dictionary measure column comparison can be done
        // on the original data as like measure column
        if (DataTypeUtil.isPrimitiveColumn(dataType) &&
            dimColEvaluatorInfoList.get(0).getDimension().getDataType() != DataTypes.DATE) {
          isScanRequired = isScanRequired(maxValue, filterRangeValues, dataType);
        } else {
          isScanRequired =
            isScanRequired(maxValue, filterRangeValues, isMinMaxSet[dimensionChunkIndex[0]]);
        }
      }
    } else {
      isScanRequired = isDefaultValuePresentInFilter;
    }

    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  private boolean isScanRequired(byte[] blockMaxValue, byte[][] filterValues, boolean isMinMaxSet) {
    if (!isMinMaxSet) {
      // scan complete data if min max is not written for a given column
      return true;
    }
    boolean isScanRequired = false;
    for (int k = 0; k < filterValues.length; k++) {
      // filter value should be in range of max and min value i.e
      // max>filterValue>min
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

  private boolean isScanRequired(byte[] blockMaxValue, byte[][] filterValues, DataType dataType) {
    boolean isScanRequired = false;
    Object maxValue =
        DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(blockMaxValue, dataType);
    for (int k = 0; k < filterValues.length; k++) {
      if (ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(filterValues[k], CarbonCommonConstants.EMPTY_BYTE_ARRAY) == 0) {
        return true;
      }
      // filter value should be in range of max and min value i.e
      // max>filterValue>min
      // so filter-max should be negative
      Object data =
          DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(filterValues[k], dataType);
      SerializableComparator comparator = Comparator.getComparator(dataType);
      int maxCompare = comparator.compare(data, maxValue);
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
    for (Object o : filterValue) {
      // TODO handle min and max for null values.
      if (o == null) {
        return true;
      }
      if (comparator.compare(o, value) <= 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws IOException {
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock[0] && !isMeasurePresentInCurrentBlock[0]) {
      int numberOfRows = rawBlockletColumnChunks.getDataBlock().numRows();
      return FilterUtil
          .createBitSetGroupWithDefaultValue(rawBlockletColumnChunks.getDataBlock().numberOfPages(),
              numberOfRows, true);
    }

    if (isDimensionPresentInCurrentBlock[0]) {
      int chunkIndex =
          segmentProperties.getDimensionOrdinalToChunkMapping().get(dimensionChunkIndex[0]);
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      DimensionRawColumnChunk rawColumnChunk =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
      FilterExecutor filterExecutor = null;
      if (rawColumnChunk.isAdaptiveForDictionary() && dimColEvaluatorInfoList.get(0).getDimension()
          .hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        dimFilterRangeValues = FilterUtil
            .updateFiltersForDimColumns(filterRangeValues, rawColumnChunk,
                dimColEvaluatorInfoList.get(0).getDimension());
      }
      boolean isExclude = false;
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(rawColumnChunk, i)) {
            int compare = ByteUtil.UnsafeComparer.INSTANCE
                .compareTo(filterRangeValues[0], rawColumnChunk.getMinValues()[i]);
            if (compare <= 0) {
              BitSet bitSet = new BitSet(rawColumnChunk.getRowCount()[i]);
              bitSet.flip(0, rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            } else {
              DimensionColumnPage dimensionColumnPage = rawColumnChunk.decodeColumnPage(i);
              BitSet bitSet = null;
              if (null != rawColumnChunk.getLocalDictionary()) {
                if (null == filterExecutor) {
                  filterExecutor = FilterUtil
                      .getFilterExecutorForRangeFilters(dimensionColumnPage.isAdaptiveEncoded(),
                          rawColumnChunk, exp, isNaturalSorted);
                  if (filterExecutor instanceof ExcludeFilterExecutorImpl) {
                    isExclude = true;
                  }
                }
                if (!isExclude) {
                  bitSet = ((IncludeFilterExecutorImpl) filterExecutor)
                      .getFilteredIndexes(dimensionColumnPage,
                          rawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                          rawBlockletColumnChunks.getBitSetGroup(), i);
                } else {
                  bitSet = ((ExcludeFilterExecutorImpl) filterExecutor)
                      .getFilteredIndexes(dimensionColumnPage,
                          rawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                          rawBlockletColumnChunks.getBitSetGroup(), i);
                }
              } else {
                bitSet = getFilteredIndexes(dimensionColumnPage,
                    rawColumnChunk.getRowCount()[i]);
              }
              bitSetGroup.setBitSet(bitSet, i);
            }
          }
        } else {
          BitSet bitSet = getFilteredIndexes(rawColumnChunk.decodeColumnPage(i),
              rawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    } else {
      int chunkIndex = segmentProperties.getMeasuresOrdinalToChunkMapping()
          .get(msrColEvalutorInfoList.get(0).getColumnIndex());
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readMeasureChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      MeasureRawColumnChunk rawColumnChunk =
          rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(rawColumnChunk.getMaxValues()[i], this.msrFilterRangeValues,
              msrColEvalutorInfoList.get(0).getType())) {
            int compare = comparator.compare(msrFilterRangeValues[0], DataTypeUtil
                .getMeasureObjectFromDataType(rawColumnChunk.getMinValues()[i],
                    msrColEvalutorInfoList.get(0).getType()));
            ColumnPage columnPage =
                rawColumnChunk.decodeColumnPage(i);
            if (compare <= 0 && columnPage.getNullBits().isEmpty()) {
              BitSet bitSet = new BitSet(rawColumnChunk.getRowCount()[i]);
              bitSet.flip(0, rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            } else {
              BitSet bitSet =
                  getFilteredIndexesForMeasures(rawColumnChunk.decodeColumnPage(i),
                      rawColumnChunk.getRowCount()[i]);
              bitSetGroup.setBitSet(bitSet, i);
            }
          }
        } else {
          BitSet bitSet =
              getFilteredIndexesForMeasures(rawColumnChunk.decodeColumnPage(i),
                  rawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    }
  }

  private boolean isScanRequired(DimensionRawColumnChunk rawColumnChunk, int columnIndex) {
    boolean scanRequired;
    DataType dataType = dimColEvaluatorInfoList.get(0).getDimension().getDataType();
    // for no dictionary measure column comparison can be done
    // on the original data as like measure column
    if (DataTypeUtil.isPrimitiveColumn(dataType) &&
        dimColEvaluatorInfoList.get(0).getDimension().getDataType() != DataTypes.DATE) {
      scanRequired =
          isScanRequired(rawColumnChunk.getMaxValues()[columnIndex], this.filterRangeValues,
              dataType);
    } else {
      scanRequired =
          isScanRequired(rawColumnChunk.getMaxValues()[columnIndex], this.filterRangeValues,
              rawColumnChunk.getMinMaxFlagArray()[columnIndex]);
    }
    return scanRequired;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws IOException {
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock[0] && !isMeasurePresentInCurrentBlock[0]) {
      int numberOfPages = rawBlockletColumnChunks.getDataBlock().numberOfPages();
      BitSet bitSet = new BitSet(numberOfPages);
      bitSet.set(0, numberOfPages);
      return bitSet;
    }

    if (isDimensionPresentInCurrentBlock[0]) {
      int chunkIndex =
          segmentProperties.getDimensionOrdinalToChunkMapping().get(dimensionChunkIndex[0]);
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock()
                .readDimensionChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      DimensionRawColumnChunk rawColumnChunk =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
      BitSet bitSet = new BitSet(rawColumnChunk.getPagesCount());
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(rawColumnChunk, i)) {
            bitSet.set(i);
          }
        } else {
          bitSet.set(i);
        }
      }
      return bitSet;
    } else {
      int chunkIndex = segmentProperties.getMeasuresOrdinalToChunkMapping()
          .get(msrColEvalutorInfoList.get(0).getColumnIndex());
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock()
                .readMeasureChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      MeasureRawColumnChunk rawColumnChunk =
          rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex];
      BitSet bitSet = new BitSet(rawColumnChunk.getPagesCount());
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(rawColumnChunk.getMaxValues()[i], this.msrFilterRangeValues,
              msrColEvalutorInfoList.get(0).getType())) {
            bitSet.set(i);
          }
        } else {
          bitSet.set(i);
        }
      }
      return bitSet;
    }

  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    if (isDimensionPresentInCurrentBlock[0]) {
      byte[] col =
          (byte[]) value.getVal(dimColEvaluatorInfoList.get(0).getDimension().getOrdinal());
      return ByteUtil.compare(filterRangeValues[0], col) <= 0;
    }

    if (isMeasurePresentInCurrentBlock[0]) {
      Object col =
          value.getVal(msrColEvalutorInfoList.get(0).getMeasure().getOrdinal() + dimOrdinalMax);
      return comparator.compare(msrFilterRangeValues[0], col) <= 0;
    }
    return false;
  }

  private BitSet getFilteredIndexesForMeasures(ColumnPage columnPage,
      int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
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
      for (int startIndex = 0; startIndex < numberOfRows; startIndex++) {
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

  private BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numberOfRows) {
    BitSet bitSet = null;
    if (dimensionColumnPage.isExplicitSorted()) {
      bitSet = setFilteredIndexToBitSetWithColumnIndex(dimensionColumnPage, numberOfRows);
    } else {
      bitSet = setFilteredIndexToBitSet(dimensionColumnPage, numberOfRows);
    }
    byte[] defaultValue = null;
    if (dimColEvaluatorInfoList.get(0).getDimension().getDataType() == DataTypes.STRING) {
      defaultValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    } else if (dimColEvaluatorInfoList.get(0).getDimension().getDataType() == DataTypes.DATE) {
      defaultValue = FilterUtil
          .getDefaultNullValue(dimColEvaluatorInfoList.get(0).getDimension());
    } else if (!dimensionColumnPage.isAdaptiveEncoded()) {
      defaultValue = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    }
    if (dimensionColumnPage.isNoDictionaryColumn() ||
        dimColEvaluatorInfoList.get(0).getDimension().getDataType() == DataTypes.DATE) {
      FilterUtil.removeNullValues(dimensionColumnPage, bitSet, defaultValue);
    }
    return bitSet;
  }

  /**
   * Method will scan the block and finds the range start index from which all members
   * will be considered for applying range filters. this method will be called if the
   * column is not supported by default so column index mapping  will be present for
   * accessing the members from the block.
   *
   * @param dimensionColumnPage
   * @param numberOfRows
   * @return BitSet.
   */
  private BitSet setFilteredIndexToBitSetWithColumnIndex(
      DimensionColumnPage dimensionColumnPage, int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
    int start = 0;
    int last = 0;
    int startIndex = 0;
    for (int i = 0; i < dimFilterRangeValues.length; i++) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numberOfRows - 1,
              dimFilterRangeValues[i], false);
      if (start < 0) {
        start = -(start + 1);
        if (start == numberOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its >= filter then from that
        // index the bitset will be considered for filtering process.
        if (dimensionColumnPage.compareTo(start, dimFilterRangeValues[i]) < 0) {
          start = start + 1;
        }
      }
      last = start;
      for (int j = start; j < numberOfRows; j++) {
        bitSet.set(dimensionColumnPage.getInvertedIndex(j));
        last++;
      }
      startIndex = last;
      if (startIndex >= numberOfRows) {
        break;
      }
    }
    return bitSet;
  }

  /**
   * Method will scan the block and finds the range start index from which all
   * members will be considered for applying range filters. this method will
   * be called if the column is sorted default so column index
   * mapping will be present for accessing the members from the block.
   *
   * @param dimensionColumnPage
   * @param numberOfRows
   * @return BitSet.
   */
  private BitSet setFilteredIndexToBitSet(DimensionColumnPage dimensionColumnPage,
      int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
    // binary search can only be applied if column is sorted
    if (isNaturalSorted && dimensionColumnPage.isExplicitSorted()) {
      int start = 0;
      int last = 0;
      int startIndex = 0;
      for (int k = 0; k < dimFilterRangeValues.length; k++) {
        start = CarbonUtil
            .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex,
                numberOfRows - 1, dimFilterRangeValues[k], false);
        if (start < 0) {
          start = -(start + 1);
          if (start == numberOfRows) {
            start = start - 1;
          }
          // Method will compare the tentative index value after binary search, this tentative
          // index needs to be compared by the filter member if its >= filter then from that
          // index the bitset will be considered for filtering process.
          if (dimensionColumnPage.compareTo(start, dimFilterRangeValues[k]) < 0) {
            start = start + 1;
          }
        }

        last = start;
        for (int j = start; j < numberOfRows; j++) {
          bitSet.set(j);
          last++;
        }
        startIndex = last;
        if (startIndex >= numberOfRows) {
          break;
        }
      }
    } else {
      for (int k = 0; k < dimFilterRangeValues.length; k++) {
        for (int i = 0; i < numberOfRows; i++) {
          if (dimensionColumnPage.compareTo(i, dimFilterRangeValues[k]) >= 0) {
            bitSet.set(i);
          }
        }
      }
    }
    return bitSet;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    if (isDimensionPresentInCurrentBlock[0]) {
      if (dimColEvaluatorInfoList.get(0).getDimension().getDataType() != DataTypes.DATE) {
        super.readColumnChunks(rawBlockletColumnChunks);
      }
      int chunkIndex = dimensionChunkIndex[0];
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
    } else if (isMeasurePresentInCurrentBlock[0]) {
      int chunkIndex = msrColEvalutorInfoList.get(0).getColumnIndex();
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readMeasureChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
    }
  }
}
