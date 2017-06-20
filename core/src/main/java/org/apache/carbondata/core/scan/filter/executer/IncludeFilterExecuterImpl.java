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
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Comparator;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.partition.PartitionFilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

public class IncludeFilterExecuterImpl implements FilterExecuter {

  protected DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;
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

  public IncludeFilterExecuterImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo, SegmentProperties segmentProperties,
      boolean isMeasure) {

    this.segmentProperties = segmentProperties;
    if (isMeasure == false) {
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
      FilterUtil
          .prepareKeysFromSurrogates(msrColumnEvaluatorInfo.getFilterValues(), segmentProperties,
              null, null, msrColumnEvaluatorInfo.getMeasure(), msrColumnExecutorInfo);
      isMeasurePresentInCurrentBlock = true;

    }

  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder) throws IOException {
    if (isDimensionPresentInCurrentBlock == true) {
      int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimColumnEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
      DimensionRawColumnChunk dimensionRawColumnChunk =
          blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(dimensionRawColumnChunk.getPagesCount());
      for (int i = 0; i < dimensionRawColumnChunk.getPagesCount(); i++) {
        if (dimensionRawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(dimensionRawColumnChunk.getMaxValues()[i],
              dimensionRawColumnChunk.getMinValues()[i], dimColumnExecuterInfo.getFilterKeys())) {
            BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.convertToDimColDataChunk(i),
                dimensionRawColumnChunk.getRowCount()[i]);
            bitSetGroup.setBitSet(bitSet, i);
          }
        } else {
          BitSet bitSet = getFilteredIndexes(dimensionRawColumnChunk.convertToDimColDataChunk(i),
              dimensionRawColumnChunk.getRowCount()[i]);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    } else if (isMeasurePresentInCurrentBlock == true) {
      int blockIndex = segmentProperties.getMeasuresOrdinalToBlockMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getMeasureRawDataChunk()[blockIndex]) {
        blockChunkHolder.getMeasureRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getMeasureChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
      MeasureRawColumnChunk measureRawColumnChunk =
          blockChunkHolder.getMeasureRawDataChunk()[blockIndex];
      BitSetGroup bitSetGroup = new BitSetGroup(measureRawColumnChunk.getPagesCount());
      DataType msrType = getMeasureDataType(msrColumnEvaluatorInfo);
      for (int i = 0; i < measureRawColumnChunk.getPagesCount(); i++) {
        if (measureRawColumnChunk.getMaxValues() != null) {
          if (isScanRequired(measureRawColumnChunk.getMaxValues()[i],
              measureRawColumnChunk.getMinValues()[i], msrColumnExecutorInfo.getFilterKeys(),
              msrColumnEvaluatorInfo.getType())) {
            BitSet bitSet =
                getFilteredIndexesForMeasures(measureRawColumnChunk.convertToMeasureColDataChunk(i),
                    measureRawColumnChunk.getRowCount()[i], msrType);
            bitSetGroup.setBitSet(bitSet, i);
          }
        } else {
          BitSet bitSet =
              getFilteredIndexesForMeasures(measureRawColumnChunk.convertToMeasureColDataChunk(i),
                  measureRawColumnChunk.getRowCount()[i], msrType);
          bitSetGroup.setBitSet(bitSet, i);
        }
      }
      return bitSetGroup;
    }
    return null;
  }

  private DataType getMeasureDataType(MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo) {
    switch (msrColumnEvaluatorInfo.getType()) {
      case SHORT:
        return DataType.SHORT;
      case INT:
        return DataType.INT;
      case LONG:
        return DataType.LONG;
      case DECIMAL:
        return DataType.DECIMAL;
      default:
        return DataType.DOUBLE;
    }
  }

  private BitSet getFilteredIndexesForMeasures(MeasureColumnDataChunk measureColumnDataChunk,
      int rowsInPage, DataType msrType) {
    // Here the algorithm is
    // Get the measure values from the chunk. compare sequentially with the
    // the filter values. The one that matches sets it Bitset.
    BitSet bitSet = new BitSet(rowsInPage);
    byte[][] filterValues = msrColumnExecutorInfo.getFilterKeys();

    Comparator comparator = PartitionFilterUtil.getComparatorByDataTypeForMeasure(msrType);
    for (int i = 0; i < filterValues.length; i++) {
      if (filterValues[i].length == 0) {
        BitSet nullBitSet = measureColumnDataChunk.getNullValueIndexHolder().getBitSet();
        for (int j = nullBitSet.nextSetBit(0); j >= 0; j = nullBitSet.nextSetBit(j + 1)) {
          bitSet.set(j);
        }
        continue;
      }
      Object filter = DataTypeUtil.getMeasureObjectFromDataType(filterValues[i], msrType);
      for (int startIndex = 0; startIndex < rowsInPage; startIndex++) {
        // Check if filterValue[i] matches with measure Values.
        Object msrValue = DataTypeUtil
            .getMeasureObjectBasedOnDataType(measureColumnDataChunk.getColumnPage(), startIndex,
                msrType, msrColumnEvaluatorInfo.getMeasure());

        if (comparator.compare(msrValue, filter) == 0) {
          // This is a match.
          bitSet.set(startIndex);
        }
      }
    }
    return bitSet;
  }

  protected BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    if (dimensionColumnDataChunk.isExplicitSorted()) {
      return setFilterdIndexToBitSetWithColumnIndex(dimensionColumnDataChunk, numerOfRows);
    }
    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows);
  }

  private BitSet setFilterdIndexToBitSetWithColumnIndex(
      DimensionColumnDataChunk dimensionColumnDataChunk, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
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
        bitSet.set(dimensionColumnDataChunk.getInvertedIndex(j));
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
            .getRangeIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
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
              dimensionColumnDataChunk.getChunkData(i));
          if (index >= 0) {
            bitSet.set(i);
          }
        }
      } else {
        for (int j = 0; j < numerOfRows; j++) {
          if (dimensionColumnDataChunk.compareTo(j, filterValues[0]) == 0) {
            bitSet.set(j);
          }
        }
      }
    }
    return bitSet;
  }

  public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = null;
    int columnIndex = 0;
    int blockIndex = 0;
    boolean isScanRequired = false;

    if (isDimensionPresentInCurrentBlock == true) {
      filterValues = dimColumnExecuterInfo.getFilterKeys();
      columnIndex = dimColumnEvaluatorInfo.getColumnIndex();
      blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping().get(columnIndex);
      isScanRequired =
          isScanRequired(blkMaxVal[blockIndex], blkMinVal[blockIndex], filterValues);

    } else if (isMeasurePresentInCurrentBlock) {
      filterValues = msrColumnExecutorInfo.getFilterKeys();
      columnIndex = msrColumnEvaluatorInfo.getColumnIndex();
      // blockIndex =
      // segmentProperties.getDimensionOrdinalToBlockMapping().get(columnIndex) + segmentProperties
      //         .getLastDimensionColOrdinal();
      blockIndex =
          segmentProperties.getMeasuresOrdinalToBlockMapping().get(columnIndex) + segmentProperties
              .getLastDimensionColOrdinal();
      isScanRequired = isScanRequired(blkMaxVal[blockIndex], blkMinVal[blockIndex], filterValues,
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

  private boolean isScanRequired(byte[] maxValue, byte[] minValue, byte[][] filterValue,
      DataType dataType) {
    for (int i = 0; i < filterValue.length; i++) {
      if (filterValue[i].length == 0 || maxValue.length == 0 || minValue.length == 0) {
        return isScanRequired(maxValue, minValue, filterValue);
      } else {
        switch (dataType) {
          case DOUBLE:
            double maxValueDouble = ByteBuffer.wrap(maxValue).getDouble();
            double minValueDouble = ByteBuffer.wrap(minValue).getDouble();
            double filterValueDouble = ByteBuffer.wrap(filterValue[i]).getDouble();
            if (filterValueDouble <= maxValueDouble && filterValueDouble >= minValueDouble) {
              return true;
            }
            break;
          case INT:
          case SHORT:
          case LONG:
            long maxValueLong = ByteBuffer.wrap(maxValue).getLong();
            long minValueLong = ByteBuffer.wrap(minValue).getLong();
            long filterValueLong = ByteBuffer.wrap(filterValue[i]).getLong();
            if (filterValueLong <= maxValueLong && filterValueLong >= minValueLong) {
              return true;
            }
            break;
          case DECIMAL:
            BigDecimal maxDecimal = DataTypeUtil.byteToBigDecimal(maxValue);
            BigDecimal minDecimal = DataTypeUtil.byteToBigDecimal(minValue);
            BigDecimal filterDecimal = DataTypeUtil.byteToBigDecimal(filterValue[i]);
            if (filterDecimal.compareTo(maxDecimal) <= 0
                && filterDecimal.compareTo(minDecimal) >= 0) {
              return true;
            }
        }
      }
    }
    return false;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    if (isDimensionPresentInCurrentBlock == true) {
      int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimColumnEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    } else if (isMeasurePresentInCurrentBlock == true) {
      int blockIndex = segmentProperties.getMeasuresOrdinalToBlockMapping()
          .get(msrColumnEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getMeasureRawDataChunk()[blockIndex]) {
        blockChunkHolder.getMeasureRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getMeasureChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    }
  }
}
