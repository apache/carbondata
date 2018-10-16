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
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
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

public class RowLevelRangeLessThanEqualFilterExecuterImpl extends RowLevelFilterExecuterImpl {
  protected byte[][] filterRangeValues;
  protected Object[] msrFilterRangeValues;
  protected SerializableComparator comparator;

  /**
   * flag to check whether default values is present in the filter value list
   */
  private boolean isDefaultValuePresentInFilter;
  public RowLevelRangeLessThanEqualFilterExecuterImpl(
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvalutorInfoList, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, byte[][] filterRangeValues,
      Object[] msrFilterRangeValues, SegmentProperties segmentProperties) {
    super(dimColEvaluatorInfoList, msrColEvalutorInfoList, exp, tableIdentifier, segmentProperties,
        null);
    this.filterRangeValues = filterRangeValues;
    this.msrFilterRangeValues = msrFilterRangeValues;
    if (!msrColEvalutorInfoList.isEmpty()) {
      CarbonMeasure measure = this.msrColEvalutorInfoList.get(0).getMeasure();
      comparator = Comparator.getComparatorByDataTypeForMeasure(measure.getDataType());
    }
    ifDefaultValueMatchesFilter();
    if (isDimensionPresentInCurrentBlock[0]) {
      isNaturalSorted = dimColEvaluatorInfoList.get(0).getDimension().isUseInvertedIndex()
          && dimColEvaluatorInfoList.get(0).getDimension().isSortColumn();
    }
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
          if (maxCompare >= 0) {
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

          if (maxCompare >= 0) {
            isDefaultValuePresentInFilter = true;
            break;
          }
        }
      }
    }
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    byte[] minValue = null;
    boolean isScanRequired = false;
    if (isMeasurePresentInCurrentBlock[0] || isDimensionPresentInCurrentBlock[0]) {
      if (isMeasurePresentInCurrentBlock[0]) {
        minValue = blockMinValue[measureChunkIndex[0]];
        isScanRequired =
            isScanRequired(minValue, msrFilterRangeValues, msrColEvalutorInfoList.get(0).getType());
      } else {
        minValue = blockMinValue[dimensionChunkIndex[0]];
        DataType dataType = dimColEvaluatorInfoList.get(0).getDimension().getDataType();
        // for no dictionary measure column comparison can be done
        // on the original data as like measure column
        if (DataTypeUtil.isPrimitiveColumn(dataType) && !dimColEvaluatorInfoList.get(0)
            .getDimension().hasEncoding(Encoding.DICTIONARY)) {
          isScanRequired = isScanRequired(minValue, filterRangeValues, dataType);
        } else {
          isScanRequired =
            isScanRequired(minValue, filterRangeValues, isMinMaxSet[dimensionChunkIndex[0]]);
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

  private boolean isScanRequired(byte[] blockMinValue, byte[][] filterValues, boolean isMinMaxSet) {
    if (!isMinMaxSet) {
      // scan complete data if min max is not written for a given column
      return true;
    }
    boolean isScanRequired = false;
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
    return isScanRequired;
  }

  private boolean isScanRequired(byte[] blockMinValue, byte[][] filterValues, DataType dataType) {
    boolean isScanRequired = false;
    Object minValue =
        DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(blockMinValue, dataType);
    for (int k = 0; k < filterValues.length; k++) {
      if (ByteUtil.UnsafeComparer.INSTANCE
          .compareTo(filterValues[k], CarbonCommonConstants.EMPTY_BYTE_ARRAY) == 0) {
        return true;
      }
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      Object data =
          DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(filterValues[k], dataType);
      SerializableComparator comparator = Comparator.getComparator(dataType);
      int minCompare = comparator.compare(data, minValue);
      // if any filter value is in range than this block needs to be
      // scanned less than equal to max range.
      if (minCompare >= 0) {
        isScanRequired = true;
        break;
      }
    }
    return isScanRequired;
  }

  private boolean isScanRequired(byte[] minValue, Object[] filterValue,
      DataType dataType) {
    Object value =
        DataTypeUtil.getMeasureObjectFromDataType(minValue, dataType);
    for (int i = 0; i < filterValue.length; i++) {
      // TODO handle min and max for null values.
      if (filterValue[i] == null) {
        return true;
      }
      if (comparator.compare(filterValue[i], value) >= 0) {
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
      FilterExecuter filterExecuter = null;
      boolean isExclude = false;
      for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
        if (rawColumnChunk.getMinValues() != null) {
          if (isScanRequired(rawColumnChunk, i)) {
            BitSet bitSet;
            DimensionColumnPage dimensionColumnPage = rawColumnChunk.decodeColumnPage(i);
            if (null != rawColumnChunk.getLocalDictionary()) {
              if (null == filterExecuter) {
                filterExecuter = FilterUtil
                    .getFilterExecutorForRangeFilters(rawColumnChunk, exp, isNaturalSorted);
                if (filterExecuter instanceof ExcludeFilterExecuterImpl) {
                  isExclude = true;
                }
              }
              if (!isExclude) {
                bitSet = ((IncludeFilterExecuterImpl) filterExecuter)
                    .getFilteredIndexes(dimensionColumnPage,
                        rawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                        rawBlockletColumnChunks.getBitSetGroup(), i);
              } else {
                bitSet = ((ExcludeFilterExecuterImpl) filterExecuter)
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
        } else {
          BitSet bitSet = getFilteredIndexes(rawColumnChunk.decodeColumnPage(i),
              rawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    } else if (isMeasurePresentInCurrentBlock[0]) {
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
        if (rawColumnChunk.getMinValues() != null) {
          if (isScanRequired(rawColumnChunk.getMinValues()[i], this.msrFilterRangeValues,
              msrColEvalutorInfoList.get(0).getType())) {
            BitSet bitSet =
                getFilteredIndexesForMeasures(rawColumnChunk.decodeColumnPage(i),
                    rawColumnChunk.getRowCount()[i]);
            bitSetGroup.setBitSet(bitSet, i);
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
    return null;
  }

  private boolean isScanRequired(DimensionRawColumnChunk rawColumnChunk, int i) {
    boolean scanRequired;
    DataType dataType = dimColEvaluatorInfoList.get(0).getDimension().getDataType();
    // for no dictionary measure column comparison can be done
    // on the original data as like measure column
    if (DataTypeUtil.isPrimitiveColumn(dataType) && !dimColEvaluatorInfoList.get(0)
        .getDimension().hasEncoding(Encoding.DICTIONARY)) {
      scanRequired =
          isScanRequired(rawColumnChunk.getMinValues()[i], this.filterRangeValues, dataType);
    } else {
      scanRequired = isScanRequired(rawColumnChunk.getMinValues()[i], this.filterRangeValues,
        rawColumnChunk.getMinMaxFlagArray()[i]);
    }
    return scanRequired;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws FilterUnsupportedException, IOException {
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
        if (rawColumnChunk.getMinValues() != null) {
          if (isScanRequired(rawColumnChunk, i)) {
            bitSet.set(i);
          }
        } else {
          bitSet.set(i);
        }
      }
      return bitSet;
    } else if (isMeasurePresentInCurrentBlock[0]) {
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
        if (rawColumnChunk.getMinValues() != null) {
          if (isScanRequired(rawColumnChunk.getMinValues()[i], this.msrFilterRangeValues,
              msrColEvalutorInfoList.get(0).getType())) {
            bitSet.set(i);
          }
        } else {
          bitSet.set(i);
        }
      }
      return bitSet;
    }
    return null;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    if (isDimensionPresentInCurrentBlock[0]) {
      byte[] col =
          (byte[]) value.getVal(dimColEvaluatorInfoList.get(0).getDimension().getOrdinal());
      return ByteUtil.compare(filterRangeValues[0], col) >= 0;
    }

    if (isMeasurePresentInCurrentBlock[0]) {
      Object col =
          value.getVal(msrColEvalutorInfoList.get(0).getMeasure().getOrdinal() + dimOrdinalMax);
      return comparator.compare(msrFilterRangeValues[0], col) >= 0;
    }
    return false;
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

          if (comparator.compare(msrValue, filterValues[i]) <= 0) {
            // This is a match.
            bitSet.set(startIndex);
          }
        }
      }
    }
    return bitSet;
  }

  private BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numerOfRows) {
    byte[] defaultValue = null;
    if (dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
      DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
          .getDirectDictionaryGenerator(
              dimColEvaluatorInfoList.get(0).getDimension().getDataType());
      int key = directDictionaryGenerator.generateDirectSurrogateKey(null);
      CarbonDimension currentBlockDimension =
          segmentProperties.getDimensions().get(dimensionChunkIndex[0]);
      if (currentBlockDimension.isSortColumn()) {
        defaultValue = FilterUtil.getMaskKey(key, currentBlockDimension,
            this.segmentProperties.getSortColumnsGenerator());
      } else {
        defaultValue = ByteUtil.toXorBytes(key);
      }
    } else if (dimColEvaluatorInfoList.get(0).getDimension().getDataType() != DataTypes.STRING) {
      defaultValue = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
    }
    BitSet bitSet = null;
    if (dimensionColumnPage.isExplicitSorted()) {
      bitSet = setFilterdIndexToBitSetWithColumnIndex(dimensionColumnPage, numerOfRows,
          dimensionColumnPage.isAdaptiveEncoded() ? null : defaultValue);
    } else {
      bitSet = setFilterdIndexToBitSet(dimensionColumnPage, numerOfRows,
          dimensionColumnPage.isAdaptiveEncoded() ? null : defaultValue);
    }
    if (dimColEvaluatorInfoList.get(0).getDimension().getDataType() == DataTypes.STRING) {
      defaultValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
    }
    if (dimensionColumnPage.isNoDicitionaryColumn()) {
      FilterUtil.removeNullValues(dimensionColumnPage, bitSet, defaultValue);
    }
    return bitSet;
  }

  /**
   * Method will scan the block and finds the range start index from which all members
   * will be considered for applying range filters. this method will be called if the
   * column is not supported by default so column index mapping  will be present for
   * accesing the members from the block.
   *
   * @param dimensionColumnPage
   * @param numerOfRows
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      DimensionColumnPage dimensionColumnPage, int numerOfRows,
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
          .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
              defaultValue, true);
      if (start < 0) {
        skip = -(start + 1);
        // end of block
        if (skip == numerOfRows) {
          return bitSet;
        }
      } else {
        // as start will be last index of null value inclusive
        // so adding 1 to skip last null value
        skip = start + 1;
      }
      startIndex = skip;
    }
    for (int i = 0; i < filterValues.length; i++) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
              filterValues[i], true);
      if (start < 0) {
        start = -(start + 1);
        if (start >= numerOfRows) {
          start = start - 1;
        }
        // When negative value of start is returned from getFirstIndexUsingBinarySearch the Start
        // will be pointing to the next consecutive position. So compare it again and point to the
        // previous value returned from getFirstIndexUsingBinarySearch.
        if (ByteUtil.compare(filterValues[i],
            dimensionColumnPage.getChunkData(dimensionColumnPage.getInvertedIndex(start)))
            < 0) {
          start = start - 1;
        }
      }
      last = start;
      for (int j = start; j >= skip; j--) {
        bitSet.set(dimensionColumnPage.getInvertedIndex(j));
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
   * @param dimensionColumnPage
   * @param numerOfRows
   * @param defaultValue
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSet(DimensionColumnPage dimensionColumnPage,
      int numerOfRows, byte[] defaultValue) {
    BitSet bitSet = new BitSet(numerOfRows);
    byte[][] filterValues = this.filterRangeValues;
    // binary search can only be applied if column is sorted
    if (isNaturalSorted && dimensionColumnPage.isExplicitSorted()) {
      int start = 0;
      int last = 0;
      int startIndex = 0;
      int skip = 0;
      //find the number of default values to skip the null value in case of direct dictionary
      if (null != defaultValue) {
        start = CarbonUtil
            .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex,
                numerOfRows - 1, defaultValue, true);
        if (start < 0) {
          skip = -(start + 1);
          // end of block
          if (skip == numerOfRows) {
            return bitSet;
          }
        } else {
          // as start will be last index of null value inclusive
          // so adding 1 to skip last null value
          skip = start + 1;
        }
        startIndex = skip;
      }
      for (int k = 0; k < filterValues.length; k++) {
        start = CarbonUtil
            .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex,
                numerOfRows - 1, filterValues[k], true);
        if (start < 0) {
          start = -(start + 1);
          if (start >= numerOfRows) {
            start = start - 1;
          }
          // When negative value of start is returned from getFirstIndexUsingBinarySearch the Start
          // will be pointing to the next consecutive position. So compare it again and point to the
          // previous value returned from getFirstIndexUsingBinarySearch.
          if (ByteUtil.compare(filterValues[k], dimensionColumnPage.getChunkData(start)) < 0) {
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
    } else {
      for (int k = 0; k < filterValues.length; k++) {
        for (int i = 0; i < numerOfRows; i++) {
          if (ByteUtil.compare(dimensionColumnPage.getChunkData(i), filterValues[k]) <= 0) {
            bitSet.set(i);
          }
        }
      }
    }
    return bitSet;
  }

  @Override public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws IOException {
    if (isDimensionPresentInCurrentBlock[0]) {
      if (!dimColEvaluatorInfoList.get(0).getDimension().hasEncoding(Encoding.DICTIONARY)) {
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
