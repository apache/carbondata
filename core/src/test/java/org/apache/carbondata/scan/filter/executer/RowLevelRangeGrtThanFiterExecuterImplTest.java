/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.filter.executer;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockletBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.datastorage.store.impl.FileHolderImpl;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RowLevelRangeGrtThanFiterExecuterImplTest {

  private RowLevelRangeGrtThanFiterExecuterImpl rowLevelRangeGrtThanFiterExecuter;
  @Rule public FilterExecutorTestRule testRule = new FilterExecutorTestRule();

  @Before public void init() {

    rowLevelRangeGrtThanFiterExecuter = new RowLevelRangeGrtThanFiterExecuterImpl(
        Arrays.asList(testRule.dimColumnResolvedFilterInfo), Arrays.asList(testRule.filterInfo),
        testRule.greaterThanEqualsTo, testRule.tableIdentifier, new byte[][] { { 0, 1, 2, 3 } },
        testRule.segmentProperties);
  }

  @Test public void testIsScanRequiredFalse() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return 1;
      }
    };

    BitSet result = rowLevelRangeGrtThanFiterExecuter
        .isScanRequired(new byte[][] { { 1, 2, 3 } }, new byte[][] { { 50, 51, 52 } });

    BitSet expectedResult = new BitSet(1);
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testIsScanRequiredTrue() {

    new MockUp<ByteUtil.UnsafeComparer>() {
      @Mock public int compareTo(byte[] buffer1, byte[] buffer2) {
        return -1;
      }
    };

    BitSet result = rowLevelRangeGrtThanFiterExecuter
        .isScanRequired(new byte[][] { { 1, 2, 3 } }, new byte[][] { { 50, 51, 52 } });

    BitSet expectedResult = new BitSet(1);
    expectedResult.flip(0);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testApplyFilterWithoutEncodingList() throws FilterUnsupportedException {

    testRule.carbonDimension = new CarbonDimension(testRule.columnSchema1, 1, 1, 1, -1);

    testRule.dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    testRule.dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(testRule.carbonDimension, testRule.dimColumnFilterInfo);
    testRule.dimColumnResolvedFilterInfo.setDimension(testRule.carbonDimension);
    testRule.dimColumnResolvedFilterInfo.setColumnIndex(0);
    testRule.dimColumnResolvedFilterInfo.setFilterValues(testRule.dimColumnFilterInfo);

    rowLevelRangeGrtThanFiterExecuter = new RowLevelRangeGrtThanFiterExecuterImpl(
        Arrays.asList(testRule.dimColumnResolvedFilterInfo), Arrays.asList(testRule.filterInfo),
        testRule.greaterThanEqualsTo, testRule.tableIdentifier, new byte[][] { { 0, 1, 2, 3 } },
        testRule.segmentProperties);

    List<DataFileFooter> footerList = testRule.getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { null, null });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = rowLevelRangeGrtThanFiterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet(1);
    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testApplyFilterEncodingList() throws FilterUnsupportedException {

    new MockUp<CarbonUtil>() {

      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit) {
        return -1;
      }
    };

    List<DataFileFooter> footerList = testRule.getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { null, null });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = rowLevelRangeGrtThanFiterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testApplyFilterEncodingListFixedIndex() throws FilterUnsupportedException {

    new MockUp<CarbonUtil>() {

      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit) {
        return 0;
      }
    };

    new MockUp<ByteUtil>() {
      @Mock public int compare(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };

    List<DataFileFooter> footerList = testRule.getDataFileFooterList();
    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 }, attributes);

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = rowLevelRangeGrtThanFiterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(1);
    expectedResult.flip(2);
    expectedResult.flip(3);
    expectedResult.flip(4);
    expectedResult.flip(5);
    expectedResult.flip(6);
    expectedResult.flip(7);
    expectedResult.flip(8);
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testApplyFilterEncodingListFixedIndexII() throws FilterUnsupportedException {

    new MockUp<CarbonUtil>() {

      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit) {
        return -2;
      }
    };

    new MockUp<ByteUtil>() {
      @Mock public int compare(byte[] buffer1, byte[] buffer2) {
        return 0;
      }
    };

    List<DataFileFooter> footerList = testRule.getDataFileFooterList();
    footerList.get(0).setNumberOfRows(1);

    BTreeBuilderInfo bTreeBuilderInfo = new BTreeBuilderInfo(footerList, new int[] { 1, 1 });
    bTreeBuilderInfo.getFooterList().get(0).getBlockletList().get(0).setNumberOfRows(1);

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(0);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
    attributes.setInvertedIndexesReverse(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 }, attributes);

    BlockletBTreeLeafNode blockletBTreeLeafNode = new BlockletBTreeLeafNode(bTreeBuilderInfo, 0, 0);
    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    BitSet result = rowLevelRangeGrtThanFiterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(0);

    assertThat(result, is(equalTo(expectedResult)));
  }
}
