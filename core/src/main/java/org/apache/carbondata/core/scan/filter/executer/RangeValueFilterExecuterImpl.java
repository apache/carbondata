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
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.GreaterThanExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanEqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * This Class get a Min And Max value of a RANGE expression. Finds out if scan is required
 * for this Range. Also search the data block and set the required bitsets which falls within
 * the Range of the RANGE Expression.
 */
public class RangeValueFilterExecuterImpl extends ValueBasedFilterExecuterImpl {

  private DimColumnResolvedFilterInfo dimColEvaluatorInfo;
  private MeasureColumnResolvedFilterInfo msrColEvalutorInfo;
  private AbsoluteTableIdentifier tableIdentifier;
  private Expression exp;
  private byte[][] filterRangesValues;
  private SegmentProperties segmentProperties;
  private boolean isDefaultValuePresentInFilter;
  /**
   * it has index at which given dimension is stored in file
   */
  protected int dimensionBlocksIndex;

  /**
   * flag to check whether the filter dimension is present in current block list of dimensions.
   * Applicable for restructure scenarios
   */
  protected boolean isDimensionPresentInCurrentBlock;
  boolean lessThanExp;
  boolean lessThanEqualExp;
  boolean greaterThanExp;
  boolean greaterThanEqualExp;
  boolean startBlockMinIsDefaultStart;
  boolean endBlockMaxisDefaultEnd;
  boolean isRangeFullyCoverBlock;

  public RangeValueFilterExecuterImpl(DimColumnResolvedFilterInfo dimColEvaluatorInfo,
      MeasureColumnResolvedFilterInfo msrColEvaluatorInfo, Expression exp,
      AbsoluteTableIdentifier tableIdentifier, byte[][] filterRangeValues,
      SegmentProperties segmentProperties) {

    this.dimColEvaluatorInfo = dimColEvaluatorInfo;
    this.msrColEvalutorInfo = msrColEvaluatorInfo;
    this.exp = exp;
    this.segmentProperties = segmentProperties;
    this.tableIdentifier = tableIdentifier;
    this.filterRangesValues = filterRangeValues;
    this.lessThanExp = isLessThan();
    this.lessThanEqualExp = isLessThanEqualTo();
    this.greaterThanExp = isGreaterThan();
    this.greaterThanEqualExp = isGreaterThanEqualTo();
    startBlockMinIsDefaultStart = false;
    endBlockMaxisDefaultEnd = false;
    isRangeFullyCoverBlock = false;
    initDimensionBlockIndexes();
    ifDefaultValueMatchesFilter();

  }

  /**
   * This method will initialize the dimension info for the current block to be
   * used for filtering the data
   */
  private void initDimensionBlockIndexes() {
    // find the dimension in the current block dimensions list
    CarbonDimension dimensionFromCurrentBlock =
        segmentProperties.getDimensionFromCurrentBlock(dimColEvaluatorInfo.getDimension());
    if (null != dimensionFromCurrentBlock) {
      dimColEvaluatorInfo.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
      this.dimensionBlocksIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimensionFromCurrentBlock.getOrdinal());
      isDimensionPresentInCurrentBlock = true;
    }
  }

  /**
   * This method will check whether default value is present in the given filter values
   */
  private void ifDefaultValueMatchesFilter() {
    isDefaultValuePresentInFilter = false;
    if (this.isDimensionPresentInCurrentBlock) {
      CarbonDimension dimension = this.dimColEvaluatorInfo.getDimension();
      byte[] defaultValue = dimension.getDefaultValue();
      if (null != defaultValue) {
        int maxCompare =
            ByteUtil.UnsafeComparer.INSTANCE.compareTo(defaultValue, filterRangesValues[0]);
        int minCompare =
            ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterRangesValues[1], defaultValue);

        if (((greaterThanExp && maxCompare > 0) || (greaterThanEqualExp && maxCompare >= 0))
            && ((lessThanExp && minCompare > 0) || (lessThanEqualExp && minCompare >= 0))) {
          isDefaultValuePresentInFilter = true;
        }
      }
    }
  }

  /**
   * Method to apply the filter.
   * @param blockChunkHolder
   * @return
   * @throws FilterUnsupportedException
   * @throws IOException
   */
  public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {
    return applyNoAndDirectFilter(blockChunkHolder);
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
  public boolean isScanRequired(byte[] blockMinValue, byte[] blockMaxValue, byte[][] filterValues) {
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

    if (isDimensionPresentInCurrentBlock == true) {
      if (((lessThanExp == true) && (
          ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockMinValue, filterValues[1]) >= 0)) || (
          (lessThanEqualExp == true) && (
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockMinValue, filterValues[1]) > 0)) || (
          (greaterThanExp == true) && (
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[0], blockMaxValue) >= 0)) || (
          (greaterThanEqualExp == true) && (
              ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[0], blockMaxValue) > 0))) {
        // completely out of block boundary
        isScanRequired = false;
      } else {
        if (((greaterThanExp == true) && (
            ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockMinValue, filterValues[0]) > 0)) || (
            (greaterThanEqualExp == true) && (
                ByteUtil.UnsafeComparer.INSTANCE.compareTo(blockMinValue, filterValues[0]) >= 0))) {
          startBlockMinIsDefaultStart = true;
        }

        if (((lessThanExp == true) && (
            ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[1], blockMaxValue) > 0)) || (
            (lessThanEqualExp == true) && (
                ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValues[1], blockMaxValue) >= 0))) {
          endBlockMaxisDefaultEnd = true;
        }

        if (startBlockMinIsDefaultStart == true && endBlockMaxisDefaultEnd == true) {
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
  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    byte[][] filterValues = this.filterRangesValues;
    int columnIndex = this.dimColEvaluatorInfo.getColumnIndex();
    boolean isScanRequired =
        isScanRequired(blockMinValue[columnIndex], blockMaxValue[columnIndex], filterValues);
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  /**
   * Method to apply the Range Filter.
   * @param blockChunkHolder
   * @return
   * @throws FilterUnsupportedException
   * @throws IOException
   */
  public BitSetGroup applyNoAndDirectFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException, IOException {

    // In case of Alter Table Add and Delete Columns the isDimensionPresentInCurrentBlock can be
    // false, in that scenario the default values of the column should be shown.
    // select all rows if dimension does not exists in the current block
    if (!isDimensionPresentInCurrentBlock) {
      int numberOfRows = blockChunkHolder.getDataBlock().nodeSize();
      return FilterUtil
          .createBitSetGroupWithDefaultValue(blockChunkHolder.getDataBlock().numberOfPages(),
              numberOfRows, true);
    }

    int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
        .get(dimColEvaluatorInfo.getColumnIndex());

    if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
      blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
          .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
    }

    DimensionRawColumnChunk rawColumnChunk =
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex];
    BitSetGroup bitSetGroup = new BitSetGroup(rawColumnChunk.getPagesCount());
    for (int i = 0; i < rawColumnChunk.getPagesCount(); i++) {
      if (rawColumnChunk.getMaxValues() != null) {
        if (isScanRequired(rawColumnChunk.getMinValues()[i], rawColumnChunk.getMaxValues()[i],
            this.filterRangesValues)) {
          if (isRangeFullyCoverBlock == true) {
            // Set all the bits in this case as filter Min Max values cover the whole block.
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
  }

  private BitSet getFilteredIndexes(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    if (dimensionColumnDataChunk.isExplicitSorted()
        && dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
      return setFilterdIndexToBitSetWithColumnIndex(dimensionColumnDataChunk, numerOfRows);
    }
    return setFilterdIndexToBitSet(dimensionColumnDataChunk, numerOfRows);
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
    int startIndex = 0;
    int startMin = 0;
    int endMax = 0;
    byte[][] filterValues = this.filterRangesValues;

    // For Range expression we expect two values. The First is the Min Value and Second is the
    // Max value.
    // Get the Min Value
    if (startBlockMinIsDefaultStart == false) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[0], greaterThanExp);
      if (greaterThanExp == true && start >= 0) {
        start = CarbonUtil
            .nextGreaterValueToTarget(start, dimensionColumnDataChunk, filterValues[0],
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
        if ((greaterThanExp == true) && (ByteUtil.compare(filterValues[0], dimensionColumnDataChunk
            .getChunkData(dimensionColumnDataChunk.getInvertedIndex(start)))) > 0) {
          start = start + 1;
        } else if ((greaterThanEqualExp == true) && (ByteUtil.compare(filterValues[0],
            dimensionColumnDataChunk
                .getChunkData(dimensionColumnDataChunk.getInvertedIndex(start)))) >= 0) {
          start = start + 1;
        }
      }

      startMin = start;
    } else {
      startMin = startIndex;
    }

    // Get the Max value
    if (endBlockMaxisDefaultEnd == false) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[1], lessThanEqualExp);
      if (lessThanExp == true && start >= 0) {
        start =
            CarbonUtil.nextLesserValueToTarget(start, dimensionColumnDataChunk, filterValues[1]);
      }

      if (start < 0) {
        start = -(start + 1);
        if (start == numerOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its >= filter then from that
        // index the bitset will be considered for filtering process.
        if ((lessThanExp == true) && (ByteUtil.compare(filterValues[1],
            dimensionColumnDataChunk.getChunkData(dimensionColumnDataChunk.getInvertedIndex(start)))
            < 0)) {
          start = start - 1;
        } else if ((lessThanEqualExp == true) && (ByteUtil.compare(filterValues[1],
            dimensionColumnDataChunk.getChunkData(dimensionColumnDataChunk.getInvertedIndex(start)))
            <= 0)) {
          start = start - 1;
        }
      }
      endMax = start;
    } else {
      endMax = numerOfRows - 1;
    }

    for (int j = startMin; j <= endMax; j++) {
      bitSet.set(dimensionColumnDataChunk.getInvertedIndex(j));
    }

    // Binary Search cannot be done on '@NU#LL$!", so need to check and compare for null on
    // matching row.
    if (dimensionColumnDataChunk.isNoDicitionaryColumn()) {
      updateForNoDictionaryColumn(startMin, endMax, dimensionColumnDataChunk, bitSet);
    }
    return bitSet;
  }

  private void updateForNoDictionaryColumn(int start, int end, DimensionColumnDataChunk dataChunk,
      BitSet bitset) {
    for (int j = start; j <= end; j++) {
      if (dataChunk.compareTo(j, CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY) == 0) {
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
   * @param dimensionColumnDataChunk
   * @param numerOfRows
   * @return BitSet.
   */
  private BitSet setFilterdIndexToBitSet(DimensionColumnDataChunk dimensionColumnDataChunk,
      int numerOfRows) {
    BitSet bitSet = new BitSet(numerOfRows);
    // if (dimensionColumnDataChunk instanceof FixedLengthDimensionDataChunk) {
    int start = 0;
    int startMin = 0;
    int endMax = 0;
    int startIndex = 0;
    byte[][] filterValues = this.filterRangesValues;
    // For Range expression we expect two values. The First is the Min Value and Second is the
    // Max value.
    if (startBlockMinIsDefaultStart == false) {

      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk,
              startIndex, numerOfRows - 1, filterValues[0], greaterThanExp);

      if (greaterThanExp == true && start >= 0) {
        start = CarbonUtil
            .nextGreaterValueToTarget(start, dimensionColumnDataChunk, filterValues[0],
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
        if ((greaterThanExp == true)
            && (ByteUtil.compare(filterValues[0], dimensionColumnDataChunk.getChunkData(start)))
            > 0) {
          start = start + 1;
        } else if ((greaterThanEqualExp == true)
            && (ByteUtil.compare(filterValues[0], dimensionColumnDataChunk.getChunkData(start)))
            >= 0) {
          start = start + 1;
        }
      }
      startMin = start;
    } else {
      startMin = startIndex;
    }

    if (endBlockMaxisDefaultEnd == false) {
      start = CarbonUtil
          .getFirstIndexUsingBinarySearch(dimensionColumnDataChunk, startIndex, numerOfRows - 1,
              filterValues[1], lessThanEqualExp);

      if (lessThanExp == true && start >= 0) {
        start =
            CarbonUtil.nextLesserValueToTarget(start, dimensionColumnDataChunk, filterValues[1]);
      }

      if (start < 0) {
        start = -(start + 1);
        if (start == numerOfRows) {
          start = start - 1;
        }
        // Method will compare the tentative index value after binary search, this tentative
        // index needs to be compared by the filter member if its >= filter then from that
        // index the bitset will be considered for filtering process.
        if ((lessThanExp == true) && (
            ByteUtil.compare(filterValues[1], dimensionColumnDataChunk.getChunkData(start)) < 0)) {
          start = start - 1;
        } else if ((lessThanEqualExp == true) && (
            ByteUtil.compare(filterValues[1], dimensionColumnDataChunk.getChunkData(start)) <= 0)) {
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
    if (dimensionColumnDataChunk.isNoDicitionaryColumn()) {
      updateForNoDictionaryColumn(startMin, endMax, dimensionColumnDataChunk, bitSet);
    }

    return bitSet;
  }

  /**
   * Method to read the blocks.
   * @param blockChunkHolder
   * @throws IOException
   */
  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    if (isDimensionPresentInCurrentBlock == true) {
      int blockIndex = segmentProperties.getDimensionOrdinalToBlockMapping()
          .get(dimColEvaluatorInfo.getColumnIndex());
      if (null == blockChunkHolder.getDimensionRawDataChunk()[blockIndex]) {
        blockChunkHolder.getDimensionRawDataChunk()[blockIndex] = blockChunkHolder.getDataBlock()
            .getDimensionChunk(blockChunkHolder.getFileReader(), blockIndex);
      }
    }
  }
}
