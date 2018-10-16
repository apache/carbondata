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

public class ExcludeFilterExecuterImpl implements FilterExecuter {

  private DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  private DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  private MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo;
  private MeasureColumnExecuterFilterInfo msrColumnExecutorInfo;
  protected SegmentProperties segmentProperties;
  private boolean isDimensionPresentInCurrentBlock = false;
  private boolean isMeasurePresentInCurrentBlock = false;
  private SerializableComparator comparator;
  /**
   * is dimension column data is natural sorted
   */
  private boolean isNaturalSorted = false;

  private byte[][] filterValues;

  public ExcludeFilterExecuterImpl(byte[][] filterValues, boolean isNaturalSorted) {
    this.filterValues = filterValues;
    this.isNaturalSorted = isNaturalSorted;
  }
  public ExcludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo, SegmentProperties segmentProperties,
      boolean isMeasure) {
    this.segmentProperties = segmentProperties;
    if (!isMeasure) {
      this.dimColEvaluatorInfo = dimColEvaluatorInfo;
      dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();

      FilterUtil.prepareKeysFromSurrogates(dimColEvaluatorInfo.getFilterValues(), segmentProperties,
          dimColEvaluatorInfo.getDimension(), dimColumnExecuterInfo, null, null);
      isDimensionPresentInCurrentBlock = true;
      isNaturalSorted =
          dimColEvaluatorInfo.getDimension().isUseInvertedIndex() && dimColEvaluatorInfo
              .getDimension().isSortColumn();
    } else {
      this.msrColumnEvaluatorInfo = msrColumnEvaluatorInfo;
      msrColumnExecutorInfo = new MeasureColumnExecuterFilterInfo();
      FilterUtil
          .prepareKeysFromSurrogates(msrColumnEvaluatorInfo.getFilterValues(), segmentProperties,
              null, null, msrColumnEvaluatorInfo.getMeasure(), msrColumnExecutorInfo);
      isMeasurePresentInCurrentBlock = true;

      DataType msrType = FilterUtil.getMeasureDataType(msrColumnEvaluatorInfo);
      comparator = Comparator.getComparatorByDataTypeForMeasure(msrType);
    }

  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws IOException {
    if (isDimensionPresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
          .get(dimColEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      DimensionRawColumnChunk dimensionRawColumnChunk =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
      DimensionColumnPage[] dimensionColumnPages =
          dimensionRawColumnChunk.decodeAllColumnPages();
      filterValues = FilterUtil
          .getEncodedFilterValues(dimensionRawColumnChunk.getLocalDictionary(),
              dimColumnExecuterInfo.filterKeysForExclude);
      BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
      for (int i = 0; i < dimensionColumnPages.length; i++) {
        BitSet bitSet = getFilteredIndexes(dimensionColumnPages[i],
            dimensionRawColumnChunk.getRowCount()[i], useBitsetPipeLine,
            rawBlockletColumnChunks.getBitSetGroup(), i);
        bitSetGroup.setBitSet(bitSet, i);
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
      ColumnPage[] ColumnPages =
          measureRawColumnChunk.decodeAllColumnPages();
      BitSetGroup bitSetGroup = new BitSetGroup(measureRawColumnChunk.getPagesCount());
      DataType msrType = FilterUtil.getMeasureDataType(msrColumnEvaluatorInfo);
      for (int i = 0; i < ColumnPages.length; i++) {
        BitSet bitSet =
            getFilteredIndexesForMeasure(
                measureRawColumnChunk.decodeColumnPage(i),
                measureRawColumnChunk.getRowCount()[i],
                useBitsetPipeLine,
                rawBlockletColumnChunks.getBitSetGroup(),
                i,
                msrType);
        bitSetGroup.setBitSet(bitSet, i);
      }
      return bitSetGroup;
    }
    return null;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws FilterUnsupportedException, IOException {
    if (isDimensionPresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
          .get(dimColEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock()
                .readDimensionChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      DimensionRawColumnChunk dimensionRawColumnChunk =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
      BitSet bitSet = new BitSet(dimensionRawColumnChunk.getPagesCount());
      bitSet.set(0, dimensionRawColumnChunk.getPagesCount());
      return bitSet;
    } else if (isMeasurePresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getMeasuresOrdinalToChunkMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex] =
            rawBlockletColumnChunks.getDataBlock()
                .readMeasureChunk(rawBlockletColumnChunks.getFileReader(), chunkIndex);
      }
      MeasureRawColumnChunk measureRawColumnChunk =
          rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex];

      BitSet bitSet = new BitSet(measureRawColumnChunk.getPagesCount());
      bitSet.set(0, measureRawColumnChunk.getPagesCount());
      return bitSet;
    }
    return null;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    if (isDimensionPresentInCurrentBlock) {
      byte[][] filterValues = dimColumnExecuterInfo.getExcludeFilterKeys();
      byte[] col = (byte[])value.getVal(dimColEvaluatorInfo.getDimension().getOrdinal());
      for (int i = 0; i < filterValues.length; i++) {
        if (0 == ByteUtil.UnsafeComparer.INSTANCE.compareTo(col, 0, col.length,
            filterValues[i], 0, filterValues[i].length)) {
          return false;
        }
      }
    } else if (isMeasurePresentInCurrentBlock) {
      Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
      Object col = value.getVal(msrColumnEvaluatorInfo.getMeasure().getOrdinal() + dimOrdinalMax);
      for (int i = 0; i < filterValues.length; i++) {
        if (filterValues[i] == null) {
          if (null == col) {
            return false;
          }
          continue;
        }
        if (comparator.compare(col, filterValues[i]) == 0) {
          return false;
        }
      }
    }
    return true;
  }

  private BitSet getFilteredIndexes(ColumnPage columnPage, int numerOfRows, DataType msrType) {
    // Here the algorithm is
    // Get the measure values from the chunk. compare sequentially with the
    // the filter values. The one that matches sets it Bitset.
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
    SerializableComparator comparator = Comparator.getComparatorByDataTypeForMeasure(msrType);
    for (int i = 0; i < filterValues.length; i++) {
      BitSet nullBitSet = columnPage.getNullBits();
      if (filterValues[i] == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.flip(j);
        }
        continue;
      }
      for (int startIndex = 0; startIndex < numerOfRows; startIndex++) {
        if (!nullBitSet.get(startIndex)) {
          // Check if filterValue[i] matches with measure Values.
          Object msrValue = DataTypeUtil
              .getMeasureObjectBasedOnDataType(columnPage, startIndex,
                  msrType, msrColumnEvaluatorInfo.getMeasure());

          if (comparator.compare(msrValue, filterValues[i]) == 0) {
            // This is a match.
            bitSet.flip(startIndex);
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
      return getFilteredIndexes(measureColumnPage, numberOfRows, msrDataType);
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
    bitSet.flip(0, numberOfRows);
    Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
    BitSet nullBitSet = measureColumnPage.getNullBits();
    BitSet prvPageBitSet = prvBitSetGroup.getBitSet(pageNumber);
    SerializableComparator comparator = Comparator.getComparatorByDataTypeForMeasure(msrDataType);
    for (int i = 0; i < filterValues.length; i++) {
      if (filterValues[i] == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.flip(j);
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
            bitSet.flip(index);
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
  protected BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numberOfRows, boolean useBitsetPipeLine, BitSetGroup prvBitSetGroup, int pageNumber) {
    // check whether applying filtered based on previous bitset will be optimal
    if (filterValues.length > 0 && CarbonUtil
        .usePreviousFilterBitsetGroup(useBitsetPipeLine, prvBitSetGroup, pageNumber,
            filterValues.length)) {
      return getFilteredIndexesUisngPrvBitset(dimensionColumnPage, prvBitSetGroup, pageNumber);
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
   * Below method will be used to apply filter based on previous filtered bitset
   * @param dimensionColumnPage
   * @param prvBitSetGroup
   * @param pageNumber
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexesUisngPrvBitset(DimensionColumnPage dimensionColumnPage,
      BitSetGroup prvBitSetGroup, int pageNumber) {
    BitSet prvPageBitSet = prvBitSetGroup.getBitSet(pageNumber);
    if (prvPageBitSet == null || prvPageBitSet.isEmpty()) {
      return prvPageBitSet;
    }
    BitSet bitSet = new BitSet();
    bitSet.or(prvPageBitSet);
    int compareResult = 0;
    // if dimension data was natural sorted then get the index from previous bitset
    // and use the same in next column data, otherwise use the inverted index reverse
    if (!dimensionColumnPage.isExplicitSorted()) {
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        compareResult = CarbonUtil
            .isFilterPresent(filterValues, dimensionColumnPage, 0, filterValues.length - 1, index);
        if (compareResult != 0) {
          bitSet.set(index);
        } else {
          if (bitSet.get(index)) {
            bitSet.flip(index);
          }
        }
      }
    } else {
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        compareResult = CarbonUtil
            .isFilterPresent(filterValues, dimensionColumnPage, 0, filterValues.length - 1,
                dimensionColumnPage.getInvertedReverseIndex(index));
        if (compareResult != 0) {
          bitSet.set(index);
        } else {
          if (bitSet.get(index)) {
            bitSet.flip(index);
          }
        }
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      DimensionColumnPage dimensionColumnPage, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    if (filterValues.length == 0) {
      return bitSet;
    }
    int startIndex = 0;
    for (int i = 0; i < filterValues.length; i++) {
      if (startIndex >= numerOfRows) {
        break;
      }
      int[] rangeIndex = CarbonUtil
          .getRangeIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
              filterValues[i]);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
        bitSet.flip(dimensionColumnPage.getInvertedIndex(j));
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
    bitSet.flip(0, numerOfRows);
    // filterValues can be null when the dictionary chunk and surrogate size both are one
    if (filterValues.length == 0) {
      return bitSet;
    }
    // binary search can only be applied if column is sorted
    if (isNaturalSorted && dimensionColumnPage.isExplicitSorted()) {
      int startIndex = 0;
      for (int i = 0; i < filterValues.length; i++) {
        if (startIndex >= numerOfRows) {
          break;
        }
        int[] rangeIndex = CarbonUtil
            .getRangeIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
                filterValues[i]);
        for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
          bitSet.flip(j);
        }
        if (rangeIndex[1] >= 0) {
          startIndex = rangeIndex[1] + 1;
        }
      }
    } else {
      if (filterValues.length > 1) {
        for (int i = 0; i < numerOfRows; i++) {
          int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
              dimensionColumnPage, i);
          if (index >= 0) {
            bitSet.flip(i);
          }
        }
      } else {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnPage.compareTo(j, filterValues[0]) == 0) {
            bitSet.flip(j);
          }
        }
      }
    }
    return bitSet;
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    bitSet.flip(0, 1);
    return bitSet;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    if (isDimensionPresentInCurrentBlock) {
      int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
          .get(dimColEvaluatorInfo.getColumnIndex());
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
