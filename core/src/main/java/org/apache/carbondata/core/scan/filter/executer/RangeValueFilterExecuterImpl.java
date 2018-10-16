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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * This Class get a Min And Max value of a RANGE expression. Finds out if scan is required
 * for this Range. Also search the data block and set the required bitsets which falls within
 * the Range of the RANGE Expression.
 */
public class RangeValueFilterExecuterImpl implements FilterExecuter {

  private DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  private Expression exp;
  private byte[][] filterRangesValues;
  private SegmentProperties segmentProperties;
  private boolean isDefaultValuePresentInFilter;
  /**
   * it has index at which given dimension is stored in file
   */
  private int dimensionChunkIndex;

  /**
   * flag to check whether the filter dimension is present in current block list of dimensions.
   * Applicable for restructure scenarios
   */
  private boolean isDimensionPresentInCurrentBlock;
  private boolean lessThanExp;
  private boolean lessThanEqualExp;
  private boolean greaterThanExp;
  private boolean greaterThanEqualExp;
  private boolean startBlockMinIsDefaultStart;
  private boolean endBlockMaxisDefaultEnd;
  private boolean isRangeFullyCoverBlock;
  private boolean isNaturalSorted;

  public RangeValueFilterExecuterImpl(DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      Expression exp, byte[][] filterRangeValues, SegmentProperties segmentProperties) {

    this.dimColEvaluatorInfo = dimColEvaluatorInfo;
    this.exp = exp;
    this.segmentProperties = segmentProperties;
    this.filterRangesValues = filterRangeValues;
    this.lessThanExp = isLessThan();
    this.lessThanEqualExp = isLessThanEqualTo();
    this.greaterThanExp = isGreaterThan();
    this.greaterThanEqualExp = isGreaterThanEqualTo();
    startBlockMinIsDefaultStart = false;
    endBlockMaxisDefaultEnd = false;
    isRangeFullyCoverBlock = false;
    initDimensionChunkIndexes();
    ifDefaultValueMatchesFilter();
    if (isDimensionPresentInCurrentBlock) {
      isNaturalSorted = dimColEvaluatorInfo.getDimension().isUseInvertedIndex()
          && dimColEvaluatorInfo.getDimension().isSortColumn();
    }
  }

  /**
   * This method will initialize the dimension info for the current block to be
   * used for filtering the data
   */
  private void initDimensionChunkIndexes() {
    // find the dimension in the current block dimensions list
    CarbonDimension dimensionFromCurrentBlock =
        segmentProperties.getDimensionFromCurrentBlock(dimColEvaluatorInfo.getDimension());
    if (null != dimensionFromCurrentBlock) {
      dimColEvaluatorInfo.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
      this.dimensionChunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
          .get(dimensionFromCurrentBlock.getOrdinal());
      isDimensionPresentInCurrentBlock = true;
    }
  }

  /**
   * This method will check whether default value is present in the given filter values
   */
  private void ifDefaultValueMatchesFilter() {
    isDefaultValuePresentInFilter = false;
    if (!this.isDimensionPresentInCurrentBlock && null != filterRangesValues) {
      CarbonDimension dimension = this.dimColEvaluatorInfo.getDimension();
      byte[] defaultValue = dimension.getDefaultValue();
      if (null != defaultValue) {
        int minCompare =
            FilterUtil.compareValues(filterRangesValues[0], defaultValue, dimension, true);
        int maxCompare =
            FilterUtil.compareValues(filterRangesValues[1], defaultValue, dimension, false);

        if (((greaterThanExp && maxCompare > 0) || (greaterThanEqualExp && maxCompare >= 0)) && (
            (lessThanExp && minCompare > 0) || (lessThanEqualExp && minCompare >= 0))) {
          isDefaultValuePresentInFilter = true;
        }
      }
    }
  }

  /**
   * Method to apply the filter.
   * @param rawBlockletColumnChunks
   * @return
   * @throws FilterUnsupportedException
   * @throws IOException
   */
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
    return applyNoAndDirectFilter(rawBlockletColumnChunks, useBitsetPipeLine);
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    // In case of Alter Table Add and Delete Columns the isDimensionPresentInCurrentBlock can be
    // false, in that scenario the default values of the column should be shown.
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock) {
      int i = blockChunkHolder.getDataBlock().numberOfPages();
      BitSet bitSet = new BitSet();
      bitSet.set(0, i);
      return bitSet;
    }

    int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());

    if (null == blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex]) {
      blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex] = blockChunkHolder.getDataBlock()
          .readDimensionChunk(blockChunkHolder.getFileReader(), chunkIndex);
    }

    DimensionRawColumnChunk rawColumnChunk =
        blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex];
    BitSet bitSet = new BitSet(rawColumnChunk.getPagesCount());
    for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
      if (rawColumnChunk.getMaxValues() != null) {
        if (isScanRequired(rawColumnChunk.getMinValues()[i], rawColumnChunk.getMaxValues()[i],
            this.filterRangesValues, rawColumnChunk.getMinMaxFlagArray()[i])) {
          bitSet.set(i);
        }
      } else {
        bitSet.set(i);
      }
    }
    return bitSet;

  }

  /**
   * apply range filter on a row
   */
  public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {

    byte[] col = (byte[]) value.getVal(dimColEvaluatorInfo.getDimension().getOrdinal());
    byte[][] filterValues = this.filterRangesValues;

    if (isDimensionPresentInCurrentBlock) {
      boolean result;
      if (greaterThanExp) {
        result = ByteUtil.compare(filterValues[0], col) < 0;
      } else {
        result = ByteUtil.compare(filterValues[0], col) <= 0;
      }

      if (result) {
        if (lessThanExp) {
          return ByteUtil.compare(filterValues[1], col) > 0;
        } else {
          return ByteUtil.compare(filterValues[1], col) >= 0;
        }
      }
    }
    return false;
  }

  /**
   * Method to find presence of LessThan Expression.
   * @return
   */
  private boolean isLessThan() {
    for (Expression result : this.exp.getChildren()) {
      if (result instanceof LessThanExpression) {
        return true;
      }
    }
    return false;
  }

  /**
   * Method to find presence of LessThanEqualTo Expression.
   * @return
   */
  private boolean isLessThanEqualTo() {
    for (Expression result : this.exp.getChildren()) {
      if (result instanceof LessThanEqualToExpression) {
        return true;
      }
    }
    return false;
  }

  /**
   * Method to find presence of GreaterThan Expression.
   * @return
   */
  private boolean isGreaterThan() {
    for (Expression result : this.exp.getChildren()) {
      if (result instanceof GreaterThanExpression) {
        return true;
      }
    }
    return false;
  }

  /**
   * Method to find presence of GreaterThanEqual Expression.
   * @return
   */
  private boolean isGreaterThanEqualTo() {
    for (Expression result : this.exp.getChildren()) {
      if (result instanceof GreaterThanEqualToExpression) {
        return true;
      }
    }
    return false;
  }



  /**
   * Method to identify if scanning of Data Block required or not by comparing the Block Min and Max
   * values and comparing them with filter min and max value.
   * @param blockMinValue
   * @param blockMaxValue
   * @param filterValues
   * @return
   */
  public boolean isScanRequired(byte[] blockMinValue, byte[] blockMaxValue, byte[][] filterValues,
      boolean isMinMaxSet) {
    if (!isMinMaxSet) {
      // scan complete data if min max is not written for a given column
      return true;
    }
    boolean isScanRequired = true;
    isRangeFullyCoverBlock = false;
    startBlockMinIsDefaultStart = false;
    endBlockMaxisDefaultEnd = false;

    /*
    For Undertsanding the below logic kept the value evaluation code intact.
    int filterMinlessThanBlockMin =
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockMinValue, filterValues[0]);
    int filterMaxLessThanBlockMin =
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockMinValue, filterValues[1]);

    int filterMinGreaterThanBlockMax =
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[0], blockMaxValue);
    int filterMaxGreaterThanBlockMax =
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[1], blockMaxValue);
     */

    // if any filter value is in range than this block needs to be
    // scanned less than equal to max range.
    // There are 4 cases which has been explicitly handled.
    // case A: Outside Boundary > No Scan required
    //                                                Block Min <---------->  Block Max
    //        Filter Min <----------> Filter Max
    // case B: Filter Overlaps Upper Part Block then Block Start Scan Becomes StartIndex of Block.
    //                         Block Min <-----------------> Block Max
    //   Filter Min <-------------------------> Filter Min
    // Case C: Filter Overlaps Lower Part of Block then Block End Scan becomes Number Of Rows - 1
    //         Block Min <-------------------------> Block Max
    //                          Filter Min <--------------------------> Filter Max
    // Case D: Filter Values Completely overlaps Block Min and Max then all bits are set.
    //                       Block Min <-----------------------> Block Max
    //         Filter Min <-----------------------------------------------> Filter Max
    // for no dictionary measure column comparison can be done
    // on the original data as like measure column

    if (isDimensionPresentInCurrentBlock) {
      CarbonDimension carbonDimension = dimColEvaluatorInfo.getDimension();
      if (((lessThanExp) && (
          FilterUtil.compareValues(filterValues[1], blockMinValue, carbonDimension, true) >= 0))
          || ((lessThanEqualExp) && (
          FilterUtil.compareValues(filterValues[1], blockMinValue, carbonDimension, true) > 0)) || (
          (greaterThanExp) && (
              FilterUtil.compareValues(filterValues[0], blockMaxValue, carbonDimension, false)
                  >= 0)) || ((greaterThanEqualExp) && (
          FilterUtil.compareValues(filterValues[0], blockMaxValue, carbonDimension, false) > 0))) {
        // completely out of block boundary
        isScanRequired = false;
      } else {
        if (((greaterThanExp) && (
            FilterUtil.compareValues(filterValues[0], blockMinValue, carbonDimension, true) > 0))
            || ((greaterThanEqualExp) && (
            FilterUtil.compareValues(filterValues[0], blockMinValue, carbonDimension, true)
                >= 0))) {
          startBlockMinIsDefaultStart = true;
        }

        if (((lessThanExp) && (
            FilterUtil.compareValues(filterValues[1], blockMaxValue, carbonDimension, false) > 0))
            || ((lessThanEqualExp) && (
            FilterUtil.compareValues(filterValues[1], blockMaxValue, carbonDimension, false)
                >= 0))) {
          endBlockMaxisDefaultEnd = true;
        }

        if (startBlockMinIsDefaultStart && endBlockMaxisDefaultEnd) {
          isRangeFullyCoverBlock = true;
        }
      }
      return isScanRequired;
    } else {
      return isDefaultValuePresentInFilter;
    }
  }

  /**
   * Method checks is the scan lies within the range values or not.
   * @param blockMaxValue
   * @param blockMinValue
   * @return
   */
  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = this.filterRangesValues;
    int columnIndex = this.dimColEvaluatorInfo.getColumnIndexInMinMaxByteArray();
    boolean isScanRequired =
        columnIndex >= blockMinValue.length || isScanRequired(blockMinValue[columnIndex],
            blockMaxValue[columnIndex], filterValues, isMinMaxSet[columnIndex]);
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  /**
   * Method to apply the Range Filter.
   * @param blockChunkHolder
   * @return
   * @throws IOException
   */
  private BitSetGroup applyNoAndDirectFilter(RawBlockletColumnChunks blockChunkHolder,
      boolean useBitsetPipeLine) throws IOException {

    // In case of Alter Table Add and Delete Columns the isDimensionPresentInCurrentBlock can be
    // false, in that scenario the default values of the column should be shown.
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock) {
      int numberOfRows = blockChunkHolder.getDataBlock().numRows();
      return FilterUtil.createBitSetGroupWithDefaultValue(
          blockChunkHolder.getDataBlock().numberOfPages(), numberOfRows, true);
    }

    int chunkIndex = segmentProperties.getDimensionOrdinalToChunkMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());

    if (null == blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex]) {
      blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex] =
          blockChunkHolder.getDataBlock().readDimensionChunk(
              blockChunkHolder.getFileReader(), chunkIndex);
    }

    DimensionRawColumnChunk rawColumnChunk =
        blockChunkHolder.getDimensionRawColumnChunks()[chunkIndex];
    BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
    FilterExecuter filterExecuter = null;
    boolean isExclude = false;
    for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
      if (rawColumnChunk.getMaxValues() != null) {
        if (isScanRequired(rawColumnChunk.getMinValues()[i], rawColumnChunk.getMaxValues()[i],
            this.filterRangesValues, rawColumnChunk.getMinMaxFlagArray()[i])) {
          if (isRangeFullyCoverBlock) {
            // Set all the bits in this case as filter Min Max values cover the whole block.
            BitSet bitSet = new BitSet(rawColumnChunk.getRowCount()[i]);
            bitSet.flip(0, rawColumnChunk.getRowCount()[i]);
            bitSetGroup.setBitSet(bitSet, i);
          } else {
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
                        blockChunkHolder.getBitSetGroup(), i);
              } else {
                bitSet = ((ExcludeFilterExecuterImpl) filterExecuter)
                    .getFilteredIndexes(dimensionColumnPage,
                        rawColumnChunk.getRowCount()[i], useBitsetPipeLine,
                        blockChunkHolder.getBitSetGroup(), i);
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
  }

  private BitSet getFilteredIndexes(DimensionColumnPage dimensionColumnPage,
      int numerOfRows) {
    if (dimensionColumnPage.isExplicitSorted()) {
      return setFilterdIndexToBitSetWithColumnIndex(dimensionColumnPage, numerOfRows);
    }
    return setFilterdIndexToBitSet(dimensionColumnPage, numerOfRows);
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
      DimensionColumnPage dimensionColumnPage, int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    int start = 0;
    int startIndex = 0;
    int startMin = 0;
    int endMax = 0;
    byte[][] filterValues = this.filterRangesValues;

    // For Range expression we expect two values. The First is the Min Value and Second is the
    // Max value.
    // Get the Min Value
    if (!startBlockMinIsDefaultStart) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
              filterValues[0], greaterThanExp);
      if (greaterThanExp && start >= 0) {
        start = CarbonUtil
            .nextGreaterValueToTarget(start, dimensionColumnPage, filterValues[0],
                numerOfRows);
      }

      if (start < 0) {
        start = -(start + 1);
        if (start == numerOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its >= filter then from that
        // index the bitset will be considered for filtering process.
        if ((ByteUtil.compare(filterValues[0], dimensionColumnPage
            .getChunkData(dimensionColumnPage.getInvertedIndex(start)))) > 0) {
          start = start + 1;
        }
      }

      startMin = start;
    } else {
      startMin = startIndex;
    }

    // Get the Max value
    if (!endBlockMaxisDefaultEnd) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
              filterValues[1], lessThanEqualExp);

      if (lessThanExp && start >= 0) {
        start =
            CarbonUtil.nextLesserValueToTarget(start, dimensionColumnPage, filterValues[1]);
      }

      if (start < 0) {
        start = -(start + 1);
        if (start == numerOfRows) {
          start = start - 1;
        }
        // In case the start is less than 0, then positive value of start is pointing to the next
        // value of the searched key. So move to the previous one.
        if ((ByteUtil.compare(filterValues[1],
            dimensionColumnPage.getChunkData(dimensionColumnPage.getInvertedIndex(start)))
            < 0)) {
          start = start - 1;
        }
      }
      endMax = start;
    } else {
      endMax = numerOfRows - 1;
    }

    for (int j = startMin; j <= endMax; j++) {
      bitSet.set(dimensionColumnPage.getInvertedIndex(j));
    }

    // Binary Search cannot be done on '@NU#LL$!", so need to check and compare for null on
    // matching row.
    if (dimensionColumnPage.isNoDicitionaryColumn()) {
      updateForNoDictionaryColumn(startMin, endMax, dimensionColumnPage, bitSet);
    }
    return bitSet;
  }

  private void updateForNoDictionaryColumn(int start, int end, DimensionColumnPage dataChunk,
      BitSet bitset) {
    for (int j = start; j <= end; j++) {
      if (dataChunk.compareTo(j, CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY) == 0
          || dataChunk.compareTo(j, CarbonCommonConstants.EMPTY_BYTE_ARRAY) == 0) {
        bitset.flip(j);
      }
    }
  }

  /**
   * Method will scan the block and finds the range start index from which all
   * members will be considered for applying range filters. this method will
   * be called if the column is sorted default so column index
   * mapping will be present for accesaing the members from the block.
   *
   * @param dimensionColumnPage
   * @param numerOfRows
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSet(DimensionColumnPage dimensionColumnPage,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    // if (dimensionColumnPage instanceof FixedLengthDimensionColumnPage) {
    byte[][] filterValues = this.filterRangesValues;
    if (dimensionColumnPage.isExplicitSorted()) {
      int start = 0;
      int startMin = 0;
      int endMax = 0;
      int startIndex = 0;
      // For Range expression we expect two values. The First is the Min Value and Second is the
      // Max value.
      if (!startBlockMinIsDefaultStart) {

        start = CarbonUtil
            .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
                filterValues[0], greaterThanExp);

        if (greaterThanExp && start >= 0) {
          start = CarbonUtil
              .nextGreaterValueToTarget(start, dimensionColumnPage, filterValues[0],
                  numerOfRows);
        }

        if (start < 0) {
          start = -(start + 1);
          if (start == numerOfRows) {
            start = start - 1;
          }
          // Method will compare the tentative index value after binary search, this tentative
          // index needs to be compared by the filter member if its >= filter then from that
          // index the bitset will be considered for filtering process.
          if ((ByteUtil.compare(filterValues[0], dimensionColumnPage.getChunkData(start)))
              > 0) {
            start = start + 1;
          }
        }
        startMin = start;
      } else {
        startMin = startIndex;
      }

      if (!endBlockMaxisDefaultEnd) {
        start = CarbonUtil
            .getFirstIndexUsingBinarySearch(dimensionColumnPage, startIndex, numerOfRows - 1,
                filterValues[1], lessThanEqualExp);

        if (lessThanExp && start >= 0) {
          start =
              CarbonUtil.nextLesserValueToTarget(start, dimensionColumnPage, filterValues[1]);
        }

        if (start < 0) {
          start = -(start + 1);
          if (start == numerOfRows) {
            start = start - 1;
          }
          // In case the start is less than 0, then positive value of start is pointing to the next
          // value of the searched key. So move to the previous one.
          if ((ByteUtil.compare(filterValues[1], dimensionColumnPage.getChunkData(start))
              < 0)) {
            start = start - 1;
          }
        }
        endMax = start;
      } else {
        endMax = numerOfRows - 1;
      }

      for (int j = startMin; j <= endMax; j++) {
        bitSet.set(j);
      }

      // Binary Search cannot be done on '@NU#LL$!", so need to check and compare for null on
      // matching row.
      if (dimensionColumnPage.isNoDicitionaryColumn()) {
        updateForNoDictionaryColumn(startMin, endMax, dimensionColumnPage, bitSet);
      }
    } else {
      byte[] defaultValue = null;
      if (dimColEvaluatorInfo.getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(dimColEvaluatorInfo.getDimension().getDataType());
        int key = directDictionaryGenerator.generateDirectSurrogateKey(null);
        CarbonDimension currentBlockDimension =
            segmentProperties.getDimensions().get(dimensionChunkIndex);
        if (currentBlockDimension.isSortColumn()) {
          defaultValue = FilterUtil.getMaskKey(key, currentBlockDimension,
              this.segmentProperties.getSortColumnsGenerator());
        } else {
          defaultValue = ByteUtil.toXorBytes(key);
        }
      } else {
        if (dimColEvaluatorInfo.getDimension().getDataType() == DataTypes.STRING) {
          defaultValue = CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY;
        } else if (!dimensionColumnPage.isAdaptiveEncoded()) {
          defaultValue = CarbonCommonConstants.EMPTY_BYTE_ARRAY;
        }
      }
      // evaluate result for lower range value first and then perform and operation in the
      // upper range value in order to compute the final result
      bitSet = evaluateGreaterThanFilterForUnsortedColumn(dimensionColumnPage, filterValues[0],
          numerOfRows);
      BitSet upperRangeBitSet =
          evaluateLessThanFilterForUnsortedColumn(dimensionColumnPage, filterValues[1],
              numerOfRows);
      bitSet.and(upperRangeBitSet);
      FilterUtil.removeNullValues(dimensionColumnPage, bitSet, defaultValue);
    }
    return bitSet;
  }

  /**
   * This method will evaluate the result for filter column based on the lower range value
   *
   * @param dimensionColumnPage
   * @param filterValue
   * @param numberOfRows
   * @return
   */
  private BitSet evaluateGreaterThanFilterForUnsortedColumn(
      DimensionColumnPage dimensionColumnPage, byte[] filterValue, int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
    if (greaterThanExp) {
      for (int i = 0; i < numberOfRows; i++) {
        if ((ByteUtil.compare(dimensionColumnPage.getChunkData(i), filterValue) > 0)) {
          bitSet.set(i);
        }
      }
    } else if (greaterThanEqualExp) {
      for (int i = 0; i < numberOfRows; i++) {
        if ((ByteUtil.compare(dimensionColumnPage.getChunkData(i), filterValue) >= 0)) {
          bitSet.set(i);
        }
      }
    }
    return bitSet;
  }

  /**
   * This method will evaluate the result for filter column based on the upper range value
   *
   * @param dimensionColumnPage
   * @param filterValue
   * @param numberOfRows
   * @return
   */
  private BitSet evaluateLessThanFilterForUnsortedColumn(
      DimensionColumnPage dimensionColumnPage, byte[] filterValue, int numberOfRows) {
    BitSet bitSet = new BitSet(numberOfRows);
    if (lessThanExp) {
      for (int i = 0; i < numberOfRows; i++) {
        if ((ByteUtil.compare(dimensionColumnPage.getChunkData(i), filterValue) < 0)) {
          bitSet.set(i);
        }
      }
    } else if (lessThanEqualExp) {
      for (int i = 0; i < numberOfRows; i++) {
        if ((ByteUtil.compare(dimensionColumnPage.getChunkData(i), filterValue) <= 0)) {
          bitSet.set(i);
        }
      }
    }
    return bitSet;
  }

  /**
   * Method to read the blocks.
   * @param rawBlockletColumnChunks
   * @throws IOException
   */
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
    }
  }
}
