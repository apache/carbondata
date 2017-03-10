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
package org.apache.carbondata.core.scan.collector.impl;


public class DictionaryBasedResultCollectorTest {

//  private static DictionaryBasedResultCollector dictionaryBasedResultCollector;
//  private static BlockExecutionInfo blockExecutionInfo;
//
//  @BeforeClass public static void setUp() {
//    blockExecutionInfo = new BlockExecutionInfo();
//    KeyStructureInfo keyStructureInfo = new KeyStructureInfo();
//    blockExecutionInfo.setKeyStructureInfo(keyStructureInfo);
//    MeasureInfo aggregatorInfo = new MeasureInfo();
//    aggregatorInfo.setMeasureOrdinals(new int[] { 10, 20, 30, 40 });
//    aggregatorInfo.setMeasureExists(new boolean[] { true, false, false, false });
//    aggregatorInfo.setDefaultValues(new Object[] { 1, 2, 3, 4 });
//    aggregatorInfo.setMeasureDataTypes(
//        new DataType[] { DataType.INT, DataType.TIMESTAMP, DataType.INT, DataType.INT });
//    blockExecutionInfo.setMeasureInfo(aggregatorInfo);
//    QueryDimension queryDimension1 = new QueryDimension("QDCol1");
//    queryDimension1.setQueryOrder(1);
//    ColumnSchema columnSchema = new ColumnSchema();
//    queryDimension1.setDimension(new CarbonDimension(columnSchema, 0, 0, 0, 0));
//    QueryDimension queryDimension2 = new QueryDimension("QDCol2");
//    queryDimension2.setQueryOrder(2);
//    queryDimension2.setDimension(new CarbonDimension(columnSchema, 1, 1, 1, 1));
//    QueryDimension queryDimension3 = new QueryDimension("QDCol3");
//    queryDimension3.setQueryOrder(3);
//    queryDimension3.setDimension(new CarbonDimension(columnSchema, 2, 0, 0, 0));
//    QueryDimension queryDimension4 = new QueryDimension("QDCol4");
//    queryDimension4.setQueryOrder(4);
//    queryDimension4.setDimension(new CarbonDimension(columnSchema, 3, 0, 0, 0));
//    blockExecutionInfo.setQueryDimensions(
//        new QueryDimension[] { queryDimension1, queryDimension2, queryDimension3,
//            queryDimension4 });
//    QueryMeasure queryMeasure1 = new QueryMeasure("QMCol1");
//    queryMeasure1.setQueryOrder(1);
//    QueryMeasure queryMeasure2 = new QueryMeasure("QMCol2");
//    queryMeasure1.setQueryOrder(2);
//    QueryMeasure queryMeasure3 = new QueryMeasure("QMCol3");
//    queryMeasure1.setQueryOrder(3);
//    QueryMeasure queryMeasure4 = new QueryMeasure("QMCol4");
//    queryMeasure1.setQueryOrder(4);
//    blockExecutionInfo.setQueryMeasures(
//        new QueryMeasure[] { queryMeasure1, queryMeasure2, queryMeasure3, queryMeasure4 });
//    Map<Integer, GenericQueryType> complexDimensionInfoMap = new HashMap<>();
//    complexDimensionInfoMap.put(1, new ArrayQueryType("name1", "parent1", 1));
//    complexDimensionInfoMap.put(2, new ArrayQueryType("name2", "parent2", 2));
//    complexDimensionInfoMap.put(3, new ArrayQueryType("name3", "parent3", 3));
//    complexDimensionInfoMap.put(4, new ArrayQueryType("name4", "parent4", 4));
//    blockExecutionInfo.setComplexDimensionInfoMap(complexDimensionInfoMap);
//    dictionaryBasedResultCollector = new DictionaryBasedResultCollector(blockExecutionInfo);
//  }
//
//  @Test public void testToCollectData() {
//    new MockUp<CarbonUtil>() {
//      @SuppressWarnings("unused") @Mock boolean[] getDictionaryEncodingArray(
//          QueryDimension[] queryDimensions) {
//        return new boolean[] { true, false, true, true };
//      }
//
//      @SuppressWarnings("unused") @Mock boolean[] getDirectDictionaryEncodingArray(
//          QueryDimension[] queryDimensions) {
//        return new boolean[] { true, true, false, false };
//      }
//
//      @SuppressWarnings("unused") @Mock boolean[] getComplexDataTypeArray(
//          QueryDimension[] queryDimensions) {
//        return new boolean[] { false, false, true, false };
//      }
//    };
//    new MockUp<DataTypeUtil>() {
//      @SuppressWarnings("unused") @Mock Object getDataBasedOnDataType(String data,
//          DataType actualDataType) {
//        return 1;
//      }
//    };
//
//    new MockUp<NonFilterQueryScannedResult>() {
//      @SuppressWarnings("unused") @Mock int[] getDictionaryKeyIntegerArray() {
//        this.getMockInstance().incrementCounter();
//        System.out.println("Mocked");
//        return new int[] { 1, 2 };
//      }
//
//      @SuppressWarnings("unused") @Mock String[] getNoDictionaryKeyStringArray() {
//        return new String[] { "1", "2" };
//      }
//
//      @SuppressWarnings("unused") @Mock byte[][] getComplexTypeKeyArray() {
//        return new byte[][] { { 1, 2 }, { 1, 2 } };
//      }
//
//      @SuppressWarnings("unused") @Mock public MeasureColumnDataChunk getMeasureChunk(int ordinal) {
//        MeasureColumnDataChunk measureColumnDataChunk = new MeasureColumnDataChunk();
//        PresenceMeta presenceMeta = new PresenceMeta();
//        BitSet bitSet = new BitSet();
//        bitSet.set(1);
//        presenceMeta.setBitSet(bitSet);
//        measureColumnDataChunk.setNullValueIndexHolder(presenceMeta);
//        CarbonReadDataHolder carbonReadDataHolder = new CarbonReadDataHolder();
//        carbonReadDataHolder.setReadableLongValues(new long[] { 1 });
//        measureColumnDataChunk.setMeasureDataHolder(carbonReadDataHolder);
//        return measureColumnDataChunk;
//      }
//    };
//
//    new MockUp<DirectDictionaryKeyGeneratorFactory>() {
//      @SuppressWarnings("unused") @Mock DirectDictionaryGenerator getDirectDictionaryGenerator(
//          DataType dataType) {
//        if (dataType == DataType.TIMESTAMP) return new TimeStampDirectDictionaryGenerator(
//            CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
//        else return null;
//      }
//    };
//    new MockUp<TimeStampDirectDictionaryGenerator>() {
//      @SuppressWarnings("unused") @Mock Object getValueFromSurrogate(int key) {
//        return 100L;
//      }
//    };
//
//    new MockUp<ArrayQueryType>() {
//      @SuppressWarnings("unused") @Mock Object getDataBasedOnDataTypeFromSurrogates(
//          ByteBuffer surrogateData) {
//        return ByteBuffer.wrap("1".getBytes());
//      }
//    };
//
//    AbstractScannedResult abstractScannedResult =
//        new NonFilterQueryScannedResult(blockExecutionInfo);
//    abstractScannedResult.setNumberOfRows(2);
//    List<Object[]> result = dictionaryBasedResultCollector.collectData(abstractScannedResult, 2);
//    int expectedResult = 2;
//    assertThat(result.size(), is(equalTo(expectedResult)));
//  }
}