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
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
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

public class IncludeFilterExecuterImpl implements FilterExecuter {

  protected DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;
  DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  private MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo;
  private MeasureColumnExecuterFilterInfo msrColumnExecutorInfo;
  protected SegmentProperties segmentProperties;
  private boolean isDimensionPresentInCurrentBlock = false;
  private boolean isMeasurePresentInCurrentBlock = false;
  protected SerializableComparator comparator;
  /**
   * is dimension column data is natural sorted
   */
  private boolean isNaturalSorted = false;

  public IncludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo, SegmentProperties segmentProperties,
      boolean isMeasure) {

    this.segmentProperties = segmentProperties;
    if (!isMeasure) {
      this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
      dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();
      FilterUtil
          .prepareKeysFromSurrogates(dimColumnEvaluatorInfo.getFilterValues(), segmentProperties,
              dimColumnEvaluatorInfo.getDimension(), dimColumnExecuterInfo, null, null);
      isDimensionPresentInCurrentBlock = true;
      isNaturalSorted =
          dimColumnEvaluatorInfo.getDimension().isUseInvertedIndex() && dimColumnEvaluatorInfo
              .getDimension().isSortColumn();

    } else {
      this.msrColumnEvaluatorInfo = msrColumnEvaluatorInfo;
      msrColumnExecutorInfo = new MeasureColumnExecuterFilterInfo();
      comparator =
          Comparator.getComparatorByDataTypeForMeasure(getMeasureDataType(msrColumnEvaluatorInfo));
      FilterUtil
          .prepareKeysFromSurrogates(msrColumnEvaluatorInfo.getFilterValues(), segmentProperties,
              null, null, msrColumnEvaluatorInfo.getMeasure(), msrColumnExecutorInfo);
      isMeasurePresentInCurrentBlock = true;

      DataType msrType = getMeasureDataType(msrColumnEvaluatorInfo);
      comparator = Comparator.getComparatorByDataTypeForMeasure(msrType);
    }

  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws IOException {
    if (isDimensionPresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
          .get(dimColumnEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      DimensionRawColumnChunk dimensionRawColumnChunk =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
      for (int i = 0; i < dimensionRawColumnChunk.getPagesCount(); i++) {
        if (dimensionRawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(dimensionRawColumnChunk.getMaxValues()[i],
              dimensionRawColumnChunk.getMinValues()[i], dimColumnExecuterInfo.getFilterKeys())) {
            BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.decodeColumnPage(i),
                dimensionRawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                rawBlockletColumnChunks.getBitSetGroup(), i);
            bitSetGroup.setBitSet(bitSet, i);
          }
        } else {
          BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.decodeColumnPage(i),
              dimensionRawColumnChunk.getRowCount()[i], useBitsetPipeLine,
              rawBlockletColumnChunks.getBitSetGroup(), i);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    } else if (isMeasurePresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getMeasuresOrdinalToChunkMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readMeasureChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      MeasureRawColumnChunk measureRawColumnChunk =
          rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(measureRawColumnChunk.getPagesCount());
      DataType msrType = getMeasureDataType(msrColumnEvaluatorInfo);
      for (int i = 0; i < measureRawColumnChunk.getPagesCount(); i++) {
        if (measureRawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(measureRawColumnChunk.getMaxValues()[i],
              measureRawColumnChunk.getMinValues()[i], msrColumnExecutorInfo.getFilterKeys(),
              msrColumnEvaluatorInfo.getType())) {
            BitSet bitSet =
                getFilteredIndexesForMeasure(measureRawColumnChunk.decodeColumnPage(i),
                    measureRawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                    rawBlockletColumnChunks.getBitSetGroup(), i, msrType);
            bitSetGroup.setBitSet(bitSet, i);
          }
        } else {
          BitSet bitSet =
              getFilteredIndexesForMeasure(measureRawColumnChunk.decodeColumnPage(i),
                  measureRawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                  rawBlockletColumnChunks.getBitSetGroup(), i, msrType);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    }
    return null;
  }

  @Override public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    if (isDimensionPresentInCurrentBlock) {
      byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
      byte[] col = (byte[])value.getVal(dimColumnEvaluatorInfo.getDimension().getOrdinal());
      for (int i = 0; i < filterValues.length; i++) {
        if (0 == ByteUtil.UnsafeComparer.INSTANCE.compareTo(col, 0, col.length,
            filterValues[i], 0, filterValues[i].length)) {
          return true;
        }
      }
    } else if (isMeasurePresentInCurrentBlock) {
      Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
      Object col = value.getVal(msrColumnEvaluatorInfo.getMeasure().getOrdinal() + dimOrdinalMax);
      for (int i = 0; i < filterValues.length; i++) {
        if (filterValues[i] == null) {
          if (null == col) {
            return true;
          }
          continue;
        }
        if (comparator.compare(col, filterValues[i]) == 0) {
          return true;
        }
      }
    }
    return false;
  }

  private DataType getMeasureDataType(MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo) {
    if (msrColumnEvaluatorInfo.getType() == DataTypes.BOOLEAN) {
      return DataTypes.BOOLEAN;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.SHORT) {
      return DataTypes.SHORT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.INT) {
      return DataTypes.INT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.LONG) {
      return DataTypes.LONG;
    } else if (DataTypes.isDecimal(msrColumnEvaluatorInfo.getType())) {
      return DataTypes.createDefaultDecimalType();
    } else {
      return DataTypes.DOUBLE;
    }
  }

  private BitSet getFilteredIndexesForMeasures(ColumnPage columnPage,
      int rowsInPage, DataType msrType) {
    // Here the algorithm is
    // Get the measure values from the chunk. compare sequentially with the
    // the filter values. The one that matches sets it Bitset.
    BitSet bitSet = new BitSet(rowsInPage);
    Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();

    SerializableComparator comparator = Comparator.getComparatorByDataTypeForMeasure(msrType);
    BitSet nullBitSet = columnPage.getNullBits();
    for (int i = 0; i < filterValues.length; i++) {
      if (filterValues[i] == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.set(j);
        }
        continue;
      }
      for (int startIndex = 0; startIndex < rowsInPage; startIndex++) {
        if (!nullBitSet.get(startIndex)) {
          // Check if filterValue[i] matches with measure Values.
          Object msrValue = DataTypeUtil
              .getMeasureObjectBasedOnDataType(columnPage, startIndex,
                  msrType, msrColumnEvaluatorInfo.getMeasure());

          if (comparator.compare(msrValue, filterValues[i]) == 0) {
            // This is a match.
            bitSet.set(startIndex);
          }
        }
      }
    }
    return bitSet;
  }

  /**
   * Below method will be used to apply filter on measure column
   * @param measureColumnPage
   * @param numberOfRows
   * @param useBitsetPipeLine
   * @param prvBitSetGroup
   * @param pageNumber
   * @param msrDataType
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexesForMeasure(ColumnPage measureColumnPage, int numberOfRows,
      boolean useBitsetPipeLine, BitSetGroup prvBitSetGroup, int pageNumber, DataType msrDataType) {
    // check whether previous indexes can be optimal to apply filter on measure column
    if (CarbonUtil.usePreviousFilterBitsetGroup(useBitsetPipeLine, prvBitSetGroup, pageNumber,
        msrColumnExecutorInfo.getFilterKeys().length)) {
      return getFilteredIndexesForMsrUsingPrvBitSet(measureColumnPage, prvBitSetGroup, pageNumber,
          numberOfRows, msrDataType);
    } else {
      return getFilteredIndexesForMeasures(measureColumnPage, numberOfRows, msrDataType);
    }
  }

  /**
   * Below method will be used to apply filter on measure column based on previous filtered indexes
   * @param measureColumnPage
   * @param prvBitSetGroup
   * @param pageNumber
   * @param numberOfRows
   * @param msrDataType
   * @return filtred indexes bitset
   */
  private BitSet getFilteredIndexesForMsrUsingPrvBitSet(ColumnPage measureColumnPage,
      BitSetGroup prvBitSetGroup, int pageNumber, int numberOfRows, DataType msrDataType) {
    BitSet bitSet = new BitSet(numberOfRows);
    Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
    BitSet nullBitSet = measureColumnPage.getNullBits();
    BitSet prvPageBitSet = prvBitSetGroup.getBitSet(pageNumber);
    SerializableComparator comparator = Comparator.getComparatorByDataTypeForMeasure(msrDataType);
    for (int i = 0; i < filterValues.length; i++) {
      if (filterValues[i] == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.set(j);
        }
        continue;
      }
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        if (!nullBitSet.get(index)) {
          // Check if filterValue[i] matches with measure Values.
          Object msrValue = DataTypeUtil
              .getMeasureObjectBasedOnDataType(measureColumnPage, index,
                  msrDataType, msrColumnEvaluatorInfo.getMeasure());

          if (comparator.compare(msrValue, filterValues[i]) == 0) {
            // This is a match.
            bitSet.set(index);
          }
        }
      }
    }
    return bitSet;
  }

  /**
   * Below method will be used to apply filter on dimension column
   * @param dimensionColumnPage
   * @param numberOfRows
   * @param useBitsetPipeLine
   * @param prvBitSetGroup
   * @param pageNumber
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numberOfRows, boolean useBitsetPipeLine, BitSetGroup prvBitSetGroup, int pageNumber) {
    // check whether previous indexes can be optimal to apply filter on dimension column
    if (CarbonUtil.usePreviousFilterBitsetGroup(useBitsetPipeLine, prvBitSetGroup, pageNumber,
        dimColumnExecuterInfo.getFilterKeys().length)) {
      return getFilteredIndexesUisngPrvBitset(dimensionColumnPage, prvBitSetGroup, pageNumber,
          numberOfRows);
    } else {
      return getFilteredIndexes(dimensionColumnPage, numberOfRows);
    }
  }

  private BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numberOfRows) {
    if (dimensionColumnPage.isExplicitSorted()) {
      return setFilterdIndexToBitSetWithColumnIndex(dimensionColumnPage, numberOfRows);
    }
    return setFilterdIndexToBitSet(dimensionColumnPage, numberOfRows);
  }

  /**
   * Below method will be used to apply filter on dimension
   * column based on previous filtered indexes
   * @param dimensionColumnPage
   * @param prvBitSetGroup
   * @param pageNumber
   * @param numberOfRows
   * @return filtered bitset
   */
  private BitSet getFilteredIndexesUisngPrvBitset(DimensionColumnPage dimensionColumnPage,
      BitSetGroup prvBitSetGroup, int pageNumber, int numberOfRows) {
    BitSet prvPageBitSet = prvBitSetGroup.getBitSet(pageNumber);
    BitSet bitSet = new BitSet(numberOfRows);
    byte[][] filterKeys = dimColumnExecuterInfo.getFilterKeys();
    int compareResult = 0;
    // if dimension data was natural sorted then get the index from previous bitset
    // and use the same in next column data, otherwise use the inverted index reverse
    if (!dimensionColumnPage.isExplicitSorted()) {
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        compareResult = CarbonUtil
            .isFilterPresent(filterKeys, dimensionColumnPage, 0, filterKeys.length - 1, index);
        if (compareResult == 0) {
          bitSet.set(index);
        }
      }
    } else {
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        compareResult = CarbonUtil
            .isFilterPresent(filterKeys, dimensionColumnPage, 0, filterKeys.length - 1,
                dimensionColumnPage.getInvertedReverseIndex(index));
        if (compareResult == 0) {
          bitSet.set(index);
        }
      }
    }
    return bitSet;
  }
  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      DimensionColumnPage dimensionColumnPage, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    int startIndex = 0;
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      if (startIndex >= numerOfRows) {
        break;
      }
      int[] rangeIndex = CarbonUtil
          .getRangeIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
              filterValues[i]);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
        bitSet.set(dimensionColumnPage.getInvertedIndex(j));
      }
      if (rangeIndex[1] >= 0) {
        startIndex = rangeIndex[1] + 1;
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(DimensionColumnPage dimensionColumnPage,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    // binary search can only be applied if column is sorted and
    // inverted index exists for that column
    if (isNaturalSorted) {
      int startIndex = 0;
      for (int i = 0; i < filterValues.length; i++) {
        if (startIndex >= numerOfRows) {
          break;
        }
        int[] rangeIndex = CarbonUtil
            .getRangeIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
                filterValues[i]);
        for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
          bitSet.set(j);
        }
        if (rangeIndex[1] >= 0) {
          startIndex = rangeIndex[1] + 1;
        }
      }
    } else {
      if (filterValues.length > 1) {
        for (int i = 0; i < numerOfRows; i++) {
          int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
              dimensionColumnPage.getChunkData(i));
          if (index >= 0) {
            bitSet.set(i);
          }
        }
      } else {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnPage.compareTo(j, filterValues[0]) == 0) {
            bitSet.set(j);
          }
        }
      }
    }
    return bitSet;
  }

  @Override
  public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = null;
    int columnIndex = 0;
    int chunkIndex = 0;
    boolean isScanRequired = false;

    if (isDimensionPresentInCurrentBlock) {
      filterValues = dimColumnExecuterInfo.getFilterKeys();
      columnIndex = dimColumnEvaluatorInfo.getColumnIndex();
      chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping().get(columnIndex);
      isScanRequired = isScanRequired(blkMaxVal[chunkIndex], blkMinVal[chunkIndex], filterValues);
    } else if (isMeasurePresentInCurrentBlock) {
      columnIndex = msrColumnEvaluatorInfo.getColumnIndex();
      chunkIndex =
          segmentProperties.getMeasuresOrdinalToChunkMapping().get(columnIndex) +
              segmentProperties.getLastDimensionColOrdinal();
      isScanRequired = isScanRequired(blkMaxVal[chunkIndex], blkMinVal[chunkIndex],
          msrColumnExecutorInfo.getFilterKeys(),
          msrColumnEvaluatorInfo.getType());
    }

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

  private boolean isScanRequired(byte[] maxValue, byte[] minValue, Object[] filterValue,
      DataType dataType) {
    Object maxObject = DataTypeUtil.getMeasureObjectFromDataType(maxValue, dataType);
    Object minObject = DataTypeUtil.getMeasureObjectFromDataType(minValue, dataType);
    for (int i = 0; i < filterValue.length; i++) {
      // TODO handle min and max for null values.
      if (filterValue[i] == null) {
        return true;
      }
      if (comparator.compare(filterValue[i], maxObject) <= 0
          && comparator.compare(filterValue[i], minObject) >= 0) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    if (isDimensionPresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
          .get(dimColumnEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
    } else if (isMeasurePresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getMeasuresOrdinalToChunkMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readMeasureChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
    }
  }
}
