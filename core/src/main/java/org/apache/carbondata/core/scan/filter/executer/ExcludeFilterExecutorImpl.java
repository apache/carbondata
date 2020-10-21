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
import org.apache.carbondata.core.scan.filter.FilterExecutorUtil;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.intf.FilterExecutorType;
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

public class ExcludeFilterExecutorImpl implements FilterExecutor {

  private DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  private DimColumnExecutorFilterInfo dimColumnExecutorInfo;
  private MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo;
  private MeasureColumnExecutorFilterInfo msrColumnExecutorInfo;
  protected SegmentProperties segmentProperties;
  private boolean isDimensionPresentInCurrentBlock = false;
  private boolean isMeasurePresentInCurrentBlock = false;
  private SerializableComparator comparator;
  /**
   * is dimension column data is natural sorted
   */
  private boolean isNaturalSorted = false;

  private Object[] dimensionFilterValues;

  private FilterBitSetUpdater filterBitSetUpdater;

  public ExcludeFilterExecutorImpl(Object[] filterValues, boolean isNaturalSorted) {
    this.dimensionFilterValues = filterValues;
    this.isNaturalSorted = isNaturalSorted;
    this.filterBitSetUpdater =
        BitSetUpdaterFactory.INSTANCE.getBitSetUpdater(FilterExecutorType.EXCLUDE);
  }

  public ExcludeFilterExecutorImpl(DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo, SegmentProperties segmentProperties,
      boolean isMeasure) {
    this.filterBitSetUpdater =
        BitSetUpdaterFactory.INSTANCE.getBitSetUpdater(FilterExecutorType.EXCLUDE);
    this.segmentProperties = segmentProperties;
    if (!isMeasure) {
      this.dimColEvaluatorInfo = dimColEvaluatorInfo;
      dimColumnExecutorInfo = new DimColumnExecutorFilterInfo();

      FilterUtil.prepareKeysFromSurrogates(dimColEvaluatorInfo.getFilterValues(), segmentProperties,
          dimColEvaluatorInfo.getDimension(), dimColumnExecutorInfo, null, null);
      isDimensionPresentInCurrentBlock = true;
      isNaturalSorted =
          dimColEvaluatorInfo.getDimension().isUseInvertedIndex() && dimColEvaluatorInfo
              .getDimension().isSortColumn();
    } else {
      this.msrColumnEvaluatorInfo = msrColumnEvaluatorInfo;
      msrColumnExecutorInfo = new MeasureColumnExecutorFilterInfo();
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
      BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
      this.dimensionFilterValues = FilterUtil
          .updateFiltersForDimColumns(dimColumnExecutorInfo.filterKeysForExclude,
              dimensionRawColumnChunk, dimColEvaluatorInfo.getDimension());
      // get the encoded filter values for local dictionary generated column page
      if (null != dimensionRawColumnChunk.getLocalDictionary()) {
        dimensionFilterValues = FilterUtil
            .getEncodedFilterValuesForLocalDict(dimensionRawColumnChunk.isAdaptiveForDictionary(),
                dimensionRawColumnChunk.getLocalDictionary(),
                dimColumnExecutorInfo.filterKeysForExclude);
      }
      DimensionColumnPage[] dimensionColumnPages =
          dimensionRawColumnChunk.decodeAllColumnPages();
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
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks) {
    int numberOfPages = rawBlockletColumnChunks.getDataBlock().numberOfPages();
    BitSet bitSet = new BitSet(numberOfPages);
    bitSet.set(0, numberOfPages);
    return bitSet;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    if (isDimensionPresentInCurrentBlock) {
      byte[][] filterValues = dimColumnExecutorInfo.getExcludeFilterKeys();
      byte[] col = (byte[])value.getVal(dimColEvaluatorInfo.getDimension().getOrdinal());
      for (byte[] filterValue : filterValues) {
        if (0 == ByteUtil.UnsafeComparer.INSTANCE.compareTo(
            col, 0, col.length, filterValue, 0, filterValue.length)) {
          return false;
        }
      }
    } else if (isMeasurePresentInCurrentBlock) {
      Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
      Object col = value.getVal(msrColumnEvaluatorInfo.getMeasure().getOrdinal() + dimOrdinalMax);
      for (Object filterValue : filterValues) {
        if (filterValue == null) {
          if (null == col) {
            return false;
          }
          continue;
        }
        if (comparator.compare(col, filterValue) == 0) {
          return false;
        }
      }
    }
    return true;
  }

  private BitSet getFilteredIndexes(ColumnPage columnPage, int numberOfRows, DataType msrType) {
    // Here the algorithm is
    // Get the measure values from the chunk. compare sequentially with the
    // the filter values. The one that matches sets it Bitset.
    BitSet bitSet = new BitSet(numberOfRows);
    bitSet.flip(0, numberOfRows);
    FilterExecutorUtil.executeIncludeExcludeFilterForMeasure(columnPage, bitSet,
        msrColumnExecutorInfo, msrColumnEvaluatorInfo, filterBitSetUpdater);
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
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexesForMsrUsingPrvBitSet(ColumnPage measureColumnPage,
      BitSetGroup prvBitSetGroup, int pageNumber, int numberOfRows, DataType msrDataType) {
    BitSet bitSet = new BitSet(numberOfRows);
    bitSet.flip(0, numberOfRows);
    Object[] filterValues = msrColumnExecutorInfo.getFilterKeys();
    BitSet nullBitSet = measureColumnPage.getPresenceMeta().getBitSet();
    BitSet prvPageBitSet = prvBitSetGroup.getBitSet(pageNumber);
    SerializableComparator comparator = Comparator.getComparatorByDataTypeForMeasure(msrDataType);
    for (Object filterValue : filterValues) {
      if (filterValue == null) {
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.flip(j);
        }
        continue;
      }
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        if (!nullBitSet.get(index)) {
          // Check if filterValue[i] matches with measure Values.
          Object msrValue = DataTypeUtil.getMeasureObjectBasedOnDataType(measureColumnPage, index,
              msrDataType, msrColumnEvaluatorInfo.getMeasure());

          if (comparator.compare(msrValue, filterValue) == 0) {
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
    if (dimensionFilterValues.length > 0 && CarbonUtil
        .usePreviousFilterBitsetGroup(useBitsetPipeLine, prvBitSetGroup, pageNumber,
            dimensionFilterValues.length)) {
      return getFilteredIndexesUsingPrvBitset(dimensionColumnPage, prvBitSetGroup, pageNumber);
    } else {
      return getFilteredIndexes(dimensionColumnPage, numberOfRows);
    }
  }

  private BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numberOfRows) {
    if (dimensionColumnPage.isExplicitSorted()) {
      return setFilteredIndexToBitSetWithColumnIndex(dimensionColumnPage, numberOfRows);
    }
    return setFilteredIndexToBitSet(dimensionColumnPage, numberOfRows);
  }

  /**
   * Below method will be used to apply filter based on previous filtered bitset
   * @param dimensionColumnPage
   * @param prvBitSetGroup
   * @param pageNumber
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexesUsingPrvBitset(DimensionColumnPage dimensionColumnPage,
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
        compareResult = CarbonUtil.isFilterPresent(dimensionFilterValues, dimensionColumnPage, 0,
            dimensionFilterValues.length - 1, index);
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
        compareResult = CarbonUtil.isFilterPresent(dimensionFilterValues, dimensionColumnPage, 0,
            dimensionFilterValues.length - 1, dimensionColumnPage.getInvertedReverseIndex(index));
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

  private BitSet setFilteredIndexToBitSetWithColumnIndex(
      DimensionColumnPage dimensionColumnPage, int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
    bitSet.flip(0, numberOfRows);
    if (dimensionFilterValues.length == 0) {
      return bitSet;
    }
    int startIndex = 0;
    for (Object dimensionFilterValue : dimensionFilterValues) {
      if (startIndex >= numberOfRows) {
        break;
      }
      int[] rangeIndex = CarbonUtil
          .getRangeIndexUsingBinarySearch(dimensionColumnPage, startIndex, numberOfRows - 1,
              dimensionFilterValue);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
        bitSet.flip(dimensionColumnPage.getInvertedIndex(j));
      }
      if (rangeIndex[1] >= 0) {
        startIndex = rangeIndex[1] + 1;
      }
    }
    return bitSet;
  }

  private BitSet setFilteredIndexToBitSet(DimensionColumnPage dimensionColumnPage,
      int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
    bitSet.flip(0, numberOfRows);
    // filterValues can be null when the dictionary chunk and surrogate size both are one
    if (dimensionFilterValues.length == 0) {
      return bitSet;
    }
    // binary search can only be applied if column is sorted
    if (isNaturalSorted && dimensionColumnPage.isExplicitSorted()) {
      int startIndex = 0;
      for (Object dimensionFilterValue : dimensionFilterValues) {
        if (startIndex >= numberOfRows) {
          break;
        }
        int[] rangeIndex = CarbonUtil
            .getRangeIndexUsingBinarySearch(dimensionColumnPage, startIndex, numberOfRows - 1,
                dimensionFilterValue);
        for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
          bitSet.flip(j);
        }
        if (rangeIndex[1] >= 0) {
          startIndex = rangeIndex[1] + 1;
        }
      }
    } else {
      if (dimensionFilterValues.length > 1) {
        for (int i = 0; i < numberOfRows; i++) {
          int index = CarbonUtil
              .binarySearch(dimensionFilterValues, 0, dimensionFilterValues.length - 1,
                  dimensionColumnPage, i);
          if (index >= 0) {
            bitSet.flip(i);
          }
        }
      } else {
        for (int j = 0; j < numberOfRows; j++) {
          if (dimensionColumnPage.compareTo(j, dimensionFilterValues[0]) == 0) {
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
