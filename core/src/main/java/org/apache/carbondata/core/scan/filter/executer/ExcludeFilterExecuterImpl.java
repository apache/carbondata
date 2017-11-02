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
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class ExcludeFilterExecuterImpl implements FilterExecuter {

  protected DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  protected DimColumnExecuterFilterInfo dimColumnExecuterInfo;
  protected MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo;
  protected MeasureColumnExecuterFilterInfo msrColumnExecutorInfo;
  protected SegmentProperties segmentProperties;
  protected boolean isDimensionPresentInCurrentBlock = false;
  protected boolean isMeasurePresentInCurrentBlock = false;
  /**
   * is dimension column data is natural sorted
   */
  private boolean isNaturalSorted = false;

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
    }

  }

  @Override
  public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder, boolean useBitsetPipeLine)
      throws IOException {
    if (isDimensionPresentInCurrentBlock) {
      int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimColEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
      DimensionRawColumnChunk dimensionRawColumnChunk =
          blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
      DimensionColumnDataChunk[] dimensionColumnDataChunks =
          dimensionRawColumnChunk.convertToDimColDataChunks();
      BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
      for (int i = 0; i < dimensionColumnDataChunks.length; i++) {
        BitSet bitSet = getFilteredIndexes(dimensionColumnDataChunks[i],
            dimensionRawColumnChunk.getRowCount()[i], useBitsetPipeLine,
            blockChunkHolder.getBitSetGroup(), i);
        bitSetGroup.setBitSet(bitSet, i);
      }

      return bitSetGroup;
    } else if (isMeasurePresentInCurrentBlock) {
      int blockIndex = segmentProperties.getMeasuresOrdinalToBlockMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getMeasureRawDataChunk()[blockIndex]) {
        blockChunkHolder.getMeasureRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getMeasureChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
      MeasureRawColumnChunk measureRawColumnChunk =
          blockChunkHolder.getMeasureRawDataChunk()[blockIndex];
      ColumnPage[] ColumnPages =
          measureRawColumnChunk.convertToColumnPage();
      BitSetGroup bitSetGroup = new BitSetGroup(measureRawColumnChunk.getPagesCount());
      DataType msrType = getMeasureDataType(msrColumnEvaluatorInfo);
      for (int i = 0; i < ColumnPages.length; i++) {
        BitSet bitSet =
            getFilteredIndexesForMeasure(
                measureRawColumnChunk.convertToColumnPage(i),
                measureRawColumnChunk.getRowCount()[i],
                useBitsetPipeLine,
                blockChunkHolder.getBitSetGroup(),
                i,
                msrType);
        bitSetGroup.setBitSet(bitSet, i);
      }
      return bitSetGroup;
    }
    return null;
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
   * @param dimensionColumnDataChunk
   * @param numberOfRows
   * @param useBitsetPipeLine
   * @param prvBitSetGroup
   * @param pageNumber
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numberOfRows, boolean useBitsetPipeLine, BitSetGroup prvBitSetGroup, int pageNumber) {
    // check whether applying filtered based on previous bitset will be optimal
    if (CarbonUtil.usePreviousFilterBitsetGroup(useBitsetPipeLine, prvBitSetGroup, pageNumber,
        dimColumnExecuterInfo.getFilterKeys().length)) {
      return getFilteredIndexesUisngPrvBitset(dimensionColumnDataChunk, prvBitSetGroup, pageNumber,
          numberOfRows);
    } else {
      return getFilteredIndexes(dimensionColumnDataChunk, numberOfRows);
    }
  }

  private BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numberOfRows) {
    if (dimensionColumnDataChunk.isExplicitSorted()) {
      return setFilterdIndexToBitSetWithColumnIndex(dimensionColumnDataChunk, numberOfRows);
    }
    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numberOfRows);
  }

  /**
   * Below method will be used to apply filter based on previous filtered bitset
   * @param dimensionColumnDataChunk
   * @param prvBitSetGroup
   * @param pageNumber
   * @param numberOfRows
   * @return filtered indexes bitset
   */
  private BitSet getFilteredIndexesUisngPrvBitset(DimensionColumnDataChunk dimensionColumnDataChunk,
      BitSetGroup prvBitSetGroup, int pageNumber, int numberOfRows) {
    BitSet prvPageBitSet = prvBitSetGroup.getBitSet(pageNumber);
    BitSet bitSet = new BitSet(numberOfRows);
    byte[][] filterKeys = dimColumnExecuterInfo.getFilterKeys();
    int compareResult = 0;
    // if dimension data was natural sorted then get the index from previous bitset
    // and use the same in next column data, otherwise use the inverted index reverse
    if (!dimensionColumnDataChunk.isExplicitSorted()) {
      for (int index = prvPageBitSet.nextSetBit(0);
           index >= 0; index = prvPageBitSet.nextSetBit(index + 1)) {
        compareResult = CarbonUtil
            .isFilterPresent(filterKeys, dimensionColumnDataChunk, 0, filterKeys.length - 1, index);
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
            .isFilterPresent(filterKeys, dimensionColumnDataChunk, 0, filterKeys.length - 1,
                dimensionColumnDataChunk.getInvertedReverseIndex(index));
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
      DimensionColumnDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    int startIndex = 0;
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    for (int i = 0; i < filterValues.length; i++) {
      if (startIndex >= numerOfRows) {
        break;
      }
      int[] rangeIndex = CarbonUtil
          .getRangeIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[i]);
      for (int j = rangeIndex[0]; j <= rangeIndex[1]; j++) {
        bitSet.flip(dimensionColumnDataChunk.getInvertedIndex(j));
      }
      if (rangeIndex[1] >= 0) {
        startIndex = rangeIndex[1] + 1;
      }
    }
    return bitSet;
  }

  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    bitSet.flip(0, numerOfRows);
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    // binary search can only be applied if column is sorted
    if (isNaturalSorted) {
      int startIndex = 0;
      for (int i = 0; i < filterValues.length; i++) {
        if (startIndex >= numerOfRows) {
          break;
        }
        int[] rangeIndex = CarbonUtil
            .getRangeIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
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
              dimensionColumnDataChunk.getChunkData(i));
          if (index >= 0) {
            bitSet.flip(i);
          }
        }
      } else {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnDataChunk.compareTo(j, filterValues[0]) == 0) {
            bitSet.flip(j);
          }
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

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    if (isDimensionPresentInCurrentBlock) {
      int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimColEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    } else if (isMeasurePresentInCurrentBlock) {
      int blockIndex = segmentProperties.getMeasuresOrdinalToBlockMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getMeasureRawDataChunk()[blockIndex]) {
        blockChunkHolder.getMeasureRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getMeasureChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    }
  }
}
