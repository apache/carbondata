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
import java.util.Collections;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.VariableLengthDimensionDataChunk;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.datastorage.store.impl.FileHolderImpl;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import mockit.Mock;
import mockit.MockUp;
import org.junit.Rule;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class ExcludeFilterExecuterImplTest {

  private ExcludeFilterExecuterImpl excludeFilterExecuter;
  @Rule public FilterExecutorTestRule testRule = new FilterExecutorTestRule();

  @Test public void testApplyFilter() {

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { null, null });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(Arrays.asList(1, 2));

    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);

    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testIsScanRequired() {
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, new DimColumnFilterInfo());
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);

    BitSet result = excludeFilterExecuter
        .isScanRequired(new byte[][] { { 96, 11 } }, new byte[][] { { 64, 2 } });

    BitSet expectedResult = new BitSet(1);
    expectedResult.flip(0, 1);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testApplyFilterWithVariableLengthDimensionI() {

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);

    byte[] data1 = { 99, 104, 105, 110, 97 };
    byte[] data2 = { 117, 115, 97 };

    VariableLengthDimensionDataChunk dataChunk = new VariableLengthDimensionDataChunk(
        Arrays.asList(data1, data1, data1, data1, data1, data1, data1, data1, data1, data2),
        attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(null);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(Arrays
        .asList(new byte[][] { { 64, 78, 85, 35, 76, 76, 36, 33 }, { 99, 104, 105, 110, 97 } }));

    testRule.columnSchema3.setEncodingList(Collections.<Encoding>emptyList());
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema3, 0, -1, -1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);
    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();
    expectedResult.flip(9);

    assertThat(result, is(equalTo(expectedResult)));
  }

  @Test public void testApplyFilterWithVariableLengthDimensionII() {

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0 });
    attributes.setInvertedIndexesReverse(new int[] { 0 });

    byte[] data1 = { 99, 104, 105, 110, 97 };
    byte[] data2 = { 117, 115, 97 };

    VariableLengthDimensionDataChunk dataChunk = new VariableLengthDimensionDataChunk(
        Arrays.asList(data1, data1, data1, data1, data1, data1, data1, data1, data1, data2),
        attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(null);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(Arrays
        .asList(new byte[][] { { 64, 78, 85, 35, 76, 76, 36, 33 }, { 99, 104, 105, 110, 97 } }));

    testRule.columnSchema3.setEncodingList(Collections.<Encoding>emptyList());
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema3, 0, -1, -1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);
    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

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

  @Test public void testApplyFilterWithVariableLengthDimensionIII() {

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0 });

    byte[] data1 = { 99, 104, 105, 110, 97 };
    byte[] data2 = { 117, 115, 97 };

    VariableLengthDimensionDataChunk dataChunk = new VariableLengthDimensionDataChunk(
        Arrays.asList(data1, data1, data1, data1, data1, data1, data1, data1, data1, data2),
        attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(null);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(Arrays
        .asList(new byte[][] { { 64, 78, 85, 35, 76, 76, 36, 33 }, { 99, 104, 105, 110, 97 } }));

    testRule.columnSchema3.setEncodingList(Collections.<Encoding>emptyList());
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema3, 0, -1, -1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);
    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

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

  @Test public void testApplyFilterWithFixedLengthDimensionI() {

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 2, 2, 2, 2, 2, 2, 2, 2, 2, 3 }, attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(null);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(
        Arrays.asList(new byte[][] { { 64, 78, 85, 35, 76, 76, 36, 33 } }));

    testRule.columnSchema4.setEncodingList(Collections.<Encoding>emptyList());
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema4, 0, -1, -1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);
    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

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

  @Test public void testApplyFilterWithFixedLengthDimensionII() {

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 99, 104, 105, 110, 97 }, attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(null);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(
        Arrays.asList(new byte[][] { { 64, 78, 85, 35, 76, 76, 36, 33 } }));

    testRule.columnSchema4.setEncodingList(Collections.<Encoding>emptyList());
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);
    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

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

  @Test public void testApplyFilterWithFixedLengthDimensionIII() {

    new MockUp<CarbonUtil>() {

      @Mock
      public int getFirstIndexUsingBinarySearch(FixedLengthDimensionDataChunk dimColumnDataChunk,
          int low, int high, byte[] compareValue, boolean matchUpLimit) {
        return 0;
      }
    };

    new MockUp<ByteUtil.UnsafeComparer>() {

      @Mock
      public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
          int length2) {
        return 0;
      }
    };

    DimensionChunkAttributes attributes = new DimensionChunkAttributes();
    attributes.setNoDictionary(true);
    attributes.setEachRowSize(-1);
    attributes.setInvertedIndexes(new int[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });

    FixedLengthDimensionDataChunk dataChunk =
        new FixedLengthDimensionDataChunk(new byte[] { 99, 104, 105, 110, 97 }, attributes);

    BlocksChunkHolder blocksChunkHolder = new BlocksChunkHolder(0, 0);
    blocksChunkHolder.setDataBlock(testRule.blockletBTreeLeafNode);
    blocksChunkHolder.setMeasureDataChunk(new MeasureColumnDataChunk[] { null, null });
    blocksChunkHolder.setDimensionDataChunk(new DimensionColumnDataChunk[] { dataChunk });
    blocksChunkHolder.setFileReader(new FileHolderImpl());

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setIncludeFilter(false);
    dimColumnFilterInfo.setFilterList(null);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(
        Arrays.asList(new byte[][] { { 99, 104, 105, 110, 97 } }));

    testRule.columnSchema4.setEncodingList(Collections.<Encoding>emptyList());
    CarbonDimension carbonDimension = new CarbonDimension(testRule.columnSchema4, 1, 1, 1, -1);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo
        .addDimensionResolvedFilterInstance(carbonDimension, dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setColumnIndex(0);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);

    excludeFilterExecuter =
        new ExcludeFilterExecuterImpl(dimColumnResolvedFilterInfo, testRule.segmentProperties);
    final BitSet result = excludeFilterExecuter.applyFilter(blocksChunkHolder);

    BitSet expectedResult = new BitSet();

    assertThat(result, is(equalTo(expectedResult)));
  }
}
