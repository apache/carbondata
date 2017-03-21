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
package org.apache.carbondata.core.scan.result.impl;


public class NonFilterQueryScannedResultTest {

//  private static NonFilterQueryScannedResult filterQueryScannedResult;
//
//  @BeforeClass public static void setUp() {
//    BlockExecutionInfo blockExecutionInfo = new BlockExecutionInfo();
//    blockExecutionInfo.setFixedLengthKeySize(2);
//    blockExecutionInfo.setNoDictionaryBlockIndexes(new int[] { 0, 1 });
//    blockExecutionInfo.setDictionaryColumnBlockIndex(new int[] { 0, 1 });
//    Map<Integer, KeyStructureInfo> columnGourpToKeyInfo = new HashMap<>();
//    columnGourpToKeyInfo.put(1, new KeyStructureInfo());
//    blockExecutionInfo.setColumnGroupToKeyStructureInfo(columnGourpToKeyInfo);
//    Map<Integer, GenericQueryType> genericQueryType = new HashMap<>();
//    genericQueryType.put(1, new ArrayQueryType("Query1", "Parent", 1));
//    blockExecutionInfo.setComplexDimensionInfoMap(genericQueryType);
//    blockExecutionInfo.setComplexColumnParentBlockIndexes(new int[] { 1 });
//    QueryDimension[] queryDimensions = { new QueryDimension("Col1"), new QueryDimension("Col2") };
//    blockExecutionInfo.setQueryDimensions(queryDimensions);
//    filterQueryScannedResult = new NonFilterQueryScannedResult(blockExecutionInfo);
//    DimensionChunkAttributes dimensionChunkAttributes = new DimensionChunkAttributes();
//    dimensionChunkAttributes.setEachRowSize(0);
//    ColumnGroupDimensionDataChunk[] columnGroupDimensionDataChunks =
//        { new ColumnGroupDimensionDataChunk(new byte[] { 1, 2 }, dimensionChunkAttributes),
//            new ColumnGroupDimensionDataChunk(new byte[] { 2, 3 }, dimensionChunkAttributes) };
//    filterQueryScannedResult.setDimensionChunks(columnGroupDimensionDataChunks);
//    MeasureColumnDataChunk measureColumnDataChunk = new MeasureColumnDataChunk();
//    filterQueryScannedResult
//        .setMeasureChunks(new MeasureColumnDataChunk[] { measureColumnDataChunk });
//  }
//
//  @Test public void testToGetDictionaryKeyArray() {
//    new MockUp<ColumnGroupDimensionDataChunk>() {
//      @Mock @SuppressWarnings("unused") public int fillChunkData(byte[] data, int offset, int rowId,
//          KeyStructureInfo restructuringInfo) {
//        return 1;
//      }
//    };
//    byte[] keyArray = filterQueryScannedResult.getDictionaryKeyArray();
//    byte[] expectedResult = { 0, 0 };
//    assertThat(expectedResult, is(equalTo(keyArray)));
//  }
//
//  @Test public void testToGetDictionaryKeyIntegerArray() {
//    new MockUp<ColumnGroupDimensionDataChunk>() {
//      @Mock @SuppressWarnings("unused")
//      public int fillConvertedChunkData(int rowId, int columnIndex, int[] row,
//          KeyStructureInfo info) {
//        return 1;
//      }
//    };
//    int[] keyArray = filterQueryScannedResult.getDictionaryKeyIntegerArray();
//    int[] expectedResult = { 0, 0 };
//    assertThat(expectedResult, is(equalTo(keyArray)));
//  }
//
//  @Test public void testToGetComplexTypeKeyArray() {
//    new MockUp<ByteArrayOutputStream>() {
//      @Mock @SuppressWarnings("unused") public synchronized byte toByteArray()[] {
//        return new byte[] { 1, 2, 3 };
//      }
//    };
//    new MockUp<ArrayQueryType>() {
//      @Mock @SuppressWarnings("unused") public void parseBlocksAndReturnComplexColumnByteArray(
//          DimensionColumnDataChunk[] dimensionColumnDataChunks, int rowNumber,
//          DataOutputStream dataOutputStream) throws IOException {
//      }
//    };
//    filterQueryScannedResult.incrementCounter();
//    byte[][] keyArray = filterQueryScannedResult.getComplexTypeKeyArray();
//    byte[][] expectedResult = { { 1, 2, 3 } };
//    assertThat(expectedResult, is(equalTo(keyArray)));
//  }
//
//  @Test public void testToGetNoDictionaryKeyArray() {
//    new MockUp<ColumnGroupDimensionDataChunk>() {
//      @Mock @SuppressWarnings("unused") public byte[] getChunkData(int rowId) {
//        return new byte[] { 1, 2, 3 };
//      }
//    };
//    byte[][] dictionaryKeyArray = filterQueryScannedResult.getNoDictionaryKeyArray();
//    byte[][] expectedResult = { { 1, 2, 3 }, { 1, 2, 3 } };
//    assertThat(expectedResult, is(equalTo(dictionaryKeyArray)));
//  }
//
//  @Test public void testToGetNoDictionaryKeyStringArray() {
//    new MockUp<ColumnGroupDimensionDataChunk>() {
//      @Mock @SuppressWarnings("unused") public byte[] getChunkData(int rowId) {
//        return "1".getBytes();
//      }
//    };
//    filterQueryScannedResult.incrementCounter();
//    String[] dictionaryKeyStringArray = filterQueryScannedResult.getNoDictionaryKeyStringArray();
//    String[] expectedResult = { "1", "1" };
//    assertThat(expectedResult, is(equalTo(dictionaryKeyStringArray)));
//  }
//
//  @Test public void testToGetCurrentRowId() {
//    int rowId = filterQueryScannedResult.getCurrentRowId();
//    int expectedResult = 2;
//    assertThat(expectedResult, is(equalTo(rowId)));
//  }
//
//  @Test public void testToGetDimensionKey() {
//    new MockUp<ColumnGroupDimensionDataChunk>() {
//      @Mock @SuppressWarnings("unused") public byte[] getChunkData(int rowId) {
//        return "1".getBytes();
//      }
//    };
//    byte[] dictionaryKeyStringArray = filterQueryScannedResult.getDimensionKey(0);
//    byte[] expectedResult = "1".getBytes();
//    assertThat(expectedResult, is(equalTo(dictionaryKeyStringArray)));
//  }
//
//  @Test public void testToGetIsNullMeasureValue() {
//    new MockUp<MeasureColumnDataChunk>() {
//      @Mock @SuppressWarnings("unused") public PresenceMeta getNullValueIndexHolder() {
//        return new PresenceMeta();
//
//      }
//    };
//    new MockUp<PresenceMeta>() {
//      @Mock @SuppressWarnings("unused") public BitSet getBitSet() {
//        return new BitSet();
//      }
//    };
//    new MockUp<BitSet>() {
//      @Mock @SuppressWarnings("unused") public boolean get(int bitIndex) {
//        return false;
//      }
//    };
//
//    boolean nullMeasureValue = filterQueryScannedResult.isNullMeasureValue(0);
//    assertThat(false, is(equalTo(nullMeasureValue)));
//  }
//
//  @Test public void testToGetLongMeasureValue() {
//    new MockUp<MeasureColumnDataChunk>() {
//      @Mock @SuppressWarnings("unused") public CarbonReadDataHolder getMeasureDataHolder() {
//        return new CarbonReadDataHolder();
//
//      }
//    };
//    new MockUp<CarbonReadDataHolder>() {
//      @Mock @SuppressWarnings("unused") public long getReadableLongValueByIndex(int index) {
//        return 2L;
//      }
//    };
//    long longMeasureValue = filterQueryScannedResult.getLongMeasureValue(0);
//    long expectedResult = 2L;
//    assertThat(expectedResult, is(equalTo(longMeasureValue)));
//  }
//
//  @Test public void testToGetDoubleMeasureValue() {
//    new MockUp<MeasureColumnDataChunk>() {
//      @Mock @SuppressWarnings("unused") public CarbonReadDataHolder getMeasureDataHolder() {
//        return new CarbonReadDataHolder();
//
//      }
//    };
//    new MockUp<CarbonReadDataHolder>() {
//      @Mock @SuppressWarnings("unused") public double getReadableDoubleValueByIndex(int index) {
//        return 2.0;
//      }
//    };
//    double longMeasureValue = filterQueryScannedResult.getDoubleMeasureValue(0);
//    double expectedResult = 2.0;
//    assertThat(expectedResult, is(equalTo(longMeasureValue)));
//  }
//
//  @Test public void testToGetBigDecimalMeasureValue() {
//    new MockUp<MeasureColumnDataChunk>() {
//      @Mock @SuppressWarnings("unused") public CarbonReadDataHolder getMeasureDataHolder() {
//        return new CarbonReadDataHolder();
//
//      }
//    };
//    new MockUp<CarbonReadDataHolder>() {
//      @Mock @SuppressWarnings("unused")
//      public BigDecimal getReadableBigDecimalValueByIndex(int index) {
//        return new BigDecimal(2);
//      }
//    };
//    BigDecimal longMeasureValue = filterQueryScannedResult.getBigDecimalMeasureValue(0);
//    BigDecimal expectedResult = new BigDecimal(2);
//    assertThat(expectedResult, is(equalTo(longMeasureValue)));
//  }

}
