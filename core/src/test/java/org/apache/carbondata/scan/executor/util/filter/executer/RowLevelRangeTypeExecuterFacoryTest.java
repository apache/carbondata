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

package org.apache.carbondata.scan.executor.util.filter.executer;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.CarbonTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.BTreeBuilderInfo;
import org.apache.carbondata.core.carbon.datastore.IndexKey;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.datastore.chunk.DimensionChunkAttributes;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BlockBTreeLeafNode;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.metadata.blocklet.datachunk.PresenceMeta;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex;
import org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex;
import org.apache.carbondata.core.carbon.metadata.datatype.DataType;
import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.expression.conditional.LessThanExpression;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.filter.executer.RowLevelFilterExecuterImpl;
import org.apache.carbondata.scan.filter.executer.RowLevelRangeTypeExecuterFacory;
import org.apache.carbondata.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.scan.filter.intf.RowImpl;
import org.apache.carbondata.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertNotNull;

public class RowLevelRangeTypeExecuterFacoryTest {
  static BTreeBuilderInfo bTreeBuilderInfo;
  static BlockBTreeLeafNode blockBTreeLeafNode;
  static DimensionChunkAttributes dimensionChunkAttributes;
  static CarbonDimension carbonDimension;
  static DimColumnFilterInfo dimColumnFilterInfo;
  static ColumnSchema columnSchema;
  static SegmentProperties segmentProperties;
  static RowLevelRangeFilterResolverImpl rowLevelRangeFilterResolver;
  static BitSet bitSet;
  static List<DimColumnResolvedFilterInfo> dimColumnResolvedFilterInfoList;

  @BeforeClass public static void setUp() {
    byte[] dictKeys = { 1, 2, 3, 4, 5, 6 };
    byte[] noDictKeys = { 1, 2, 3, 4, 5, 6 };
    final byte[] bytArr = { 1, 2, 3, 4, 5, 6 };
    byte[] bytArr1 = { 8, 8, 9, 6, 5, 6 };
    byte[] startKey = { 1, 2, 3, 4, 5, 6 };
    byte[] endKey = { 1, 2, 3, 4, 5, 6 };
    final int[] intArr = { 1, 2, 3, 4, 5, 6 };
    final byte[][] byteArr = { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 4, 5 }, { 5, 6 }, { 6, 7 } };
    long[] longArr = { 1, 2, 3, 4, 5, 6 };
    double[] doubleArr = { 1, 2, 3, 4, 5, 6 };
    List<Integer> intList = new ArrayList<>();
    intList.add(new Integer("1"));
    intList.add(new Integer("2"));
    intList.add(new Integer("2"));
    intList.add(new Integer("2"));
    intList.add(new Integer("2"));
    intList.add(new Integer("2"));
    List<byte[]> bytArrList = new ArrayList<>();
    bytArrList.add(bytArr);
    bytArrList.add(bytArr);
    bytArrList.add(bytArr);
    bytArrList.add(bytArr);

    dimColumnFilterInfo = new DimColumnFilterInfo();
    dimColumnFilterInfo.setFilterList(intList);
    dimColumnFilterInfo.setIncludeFilter(true);
    dimColumnFilterInfo.setFilterListForNoDictionaryCols(bytArrList);

    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(Encoding.INVERTED_INDEX);
    encodingList.add(Encoding.BIT_PACKED);
    encodingList.add(Encoding.BIT_PACKED);
    encodingList.add(Encoding.DIRECT_DICTIONARY);
    encodingList.add(Encoding.DICTIONARY);

    Map<String, String> columnProperties = new HashMap<>();
    columnProperties.put("key", "value");
    columnProperties.put("key1", "value1");
    columnProperties.put("key2", "value2");
    columnProperties.put("key3", "value3");
    columnProperties.put("key3", "value3");
    columnProperties.put("key3", "value3");

    columnSchema = new ColumnSchema();
    columnSchema.setEncodingList(encodingList);
    columnSchema.setColumnar(true);
    columnSchema.setDefaultValue(bytArr);
    columnSchema.setColumnName("column");
    columnSchema.setAggregateFunction("aggFunc");
    columnSchema.setColumnUniqueId("id");
    columnSchema.setDimensionColumn(true);
    columnSchema.setInvisible(true);
    columnSchema.setColumnGroup(5);
    columnSchema.setColumnReferenceId("refId");
    columnSchema.setNumberOfChild(0);
    columnSchema.setPrecision(5);
    columnSchema.setScale(5);
    columnSchema.setDataType(DataType.STRING);
    columnSchema.setUseInvertedIndex(true);
    columnSchema.setColumnProperties(columnProperties);

    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    columnSchemaList.add(columnSchema);
    columnSchemaList.add(columnSchema);
    columnSchemaList.add(columnSchema);
    columnSchemaList.add(columnSchema);
    columnSchemaList.add(columnSchema);
    columnSchemaList.add(columnSchema);

    final int[] cardinality = { 0, 0, 0, 0, 0, 0 };

    segmentProperties = new SegmentProperties(columnSchemaList, cardinality);

    IndexKey indexKey = new IndexKey(dictKeys, noDictKeys);

    carbonDimension = new CarbonDimension(columnSchema, 1, 1, 1, 1);
    carbonDimension.hasEncoding(Encoding.DICTIONARY);
    carbonDimension.hasEncoding(Encoding.BIT_PACKED);
    carbonDimension.hasEncoding(Encoding.DELTA);
    carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY);
    carbonDimension.setDefaultValue(bytArr);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo.setColumnIndex(5);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setDefaultValue("default");
    dimColumnResolvedFilterInfo.setDimensionExistsInCurrentSilce(true);
    dimColumnResolvedFilterInfo.setNeedCompressedData(true);
    dimColumnResolvedFilterInfo.setRowIndex(5);
    dimColumnResolvedFilterInfo.setRsSurrogates(1);
    dimColumnResolvedFilterInfo.setEndIndexKey(indexKey);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    final Map<Integer, Integer> dimensionOrdinalMap = new HashMap<>();
    dimensionOrdinalMap.put(new Integer("1"), new Integer("2"));
    dimensionOrdinalMap.put(new Integer("5"), new Integer("4"));
    dimensionOrdinalMap.put(new Integer("2"), new Integer("3"));
    dimensionOrdinalMap.put(new Integer("6"), new Integer("7"));

    dimColumnResolvedFilterInfoList = new ArrayList<>();
    dimColumnResolvedFilterInfoList.add(dimColumnResolvedFilterInfo);
    dimColumnResolvedFilterInfoList.add(dimColumnResolvedFilterInfo);
    dimColumnResolvedFilterInfoList.add(dimColumnResolvedFilterInfo);

    new RowImpl();
    ColumnExpression columnExpression = new ColumnExpression("column1", DataType.STRING);
    columnExpression.setColIndex(5);

    CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier("testDatabase", "testTable", "testId");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("storePath", carbonTableIdentifier);

    ArrayQueryType arrayQueryType = new ArrayQueryType("name", "parentName", 1);
    ArrayQueryType arrayQueryType2 = new ArrayQueryType("name", "parentName", 2);
    Map<Integer, GenericQueryType> complexDimensionInfoMap = new HashMap<>();
    complexDimensionInfoMap.put(new Integer("1"), arrayQueryType);
    complexDimensionInfoMap.put(new Integer("2"), arrayQueryType2);
    complexDimensionInfoMap.put(new Integer("2"), arrayQueryType2);

    LessThanExpression lessThanExpression =
        new LessThanExpression(columnExpression, columnExpression);

    rowLevelRangeFilterResolver =
        new RowLevelRangeFilterResolverImpl(lessThanExpression, true, true,
            absoluteTableIdentifier);

    dimensionChunkAttributes = new DimensionChunkAttributes();
    dimensionChunkAttributes.setEachRowSize(0);
    dimensionChunkAttributes.setInvertedIndexes(intArr);
    dimensionChunkAttributes.setInvertedIndexesReverse(intArr);
    dimensionChunkAttributes.setNoDictionary(true);

    new MockUp<RowLevelRangeFilterResolverImpl>() {
      @SuppressWarnings("unused") @Mock
      public byte[][] getFilterRangeValues(SegmentProperties segmentProperties) {
        return byteArr;
      }
    };

    BigDecimal[] bigDecimals = new BigDecimal[6];
    bigDecimals[0] = new BigDecimal("1111");
    bigDecimals[1] = new BigDecimal("2222");
    bigDecimals[2] = new BigDecimal("3333");
    bigDecimals[3] = new BigDecimal("4444");
    bigDecimals[4] = new BigDecimal("5555");
    bigDecimals[5] = new BigDecimal("6666");
    CarbonReadDataHolder carbonReadDataHolder = new CarbonReadDataHolder();
    carbonReadDataHolder.setReadableDoubleValues(doubleArr);
    carbonReadDataHolder.setReadableBigDecimalValues(bigDecimals);
    carbonReadDataHolder.setReadableByteValues(byteArr);
    carbonReadDataHolder.setReadableLongValues(longArr);

    bitSet = new BitSet();
    bitSet.flip(0);
    bitSet.flip(1);
    bitSet.flip(2);
    bitSet.flip(3);
    bitSet.flip(4);
    bitSet.flip(5);

    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(true);
    presenceMeta.setBitSet(bitSet);

    List<ByteBuffer> minValues = new ArrayList<>();
    minValues.add(ByteBuffer.wrap(bytArr));
    minValues.add(ByteBuffer.wrap(bytArr));

    List<ByteBuffer> maxValues = new ArrayList<>();
    maxValues.add(ByteBuffer.wrap(bytArr1));
    maxValues.add(ByteBuffer.wrap(bytArr1));

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex(minValues, maxValues);
    BlockletBTreeIndex blockletBTreeIndex = new BlockletBTreeIndex(startKey, endKey);
    BlockletIndex blockletIndex = new BlockletIndex(blockletBTreeIndex, blockletMinMaxIndex);

    DataFileFooter dataFileFooter = new DataFileFooter();
    dataFileFooter.setBlockletIndex(blockletIndex);
    final List<DataFileFooter> dataFileFooterList = new ArrayList<>();
    dataFileFooterList.add(dataFileFooter);
    dataFileFooterList.add(dataFileFooter);
    bTreeBuilderInfo = new BTreeBuilderInfo(dataFileFooterList, intArr);
    new MockUp<BTreeBuilderInfo>() {
      @SuppressWarnings("unused") @Mock public List<DataFileFooter> getFooterList() {
        return dataFileFooterList;
      }

    };
    blockBTreeLeafNode = new BlockBTreeLeafNode(bTreeBuilderInfo, 1, 1);

  }

  @Test public void testGetRowLevelRangeTypeExecuterForLessThan() throws Exception {
    RowLevelFilterExecuterImpl result = RowLevelRangeTypeExecuterFacory
        .getRowLevelRangeTypeExecuter(FilterExecuterType.ROWLEVEL_LESSTHAN,
            rowLevelRangeFilterResolver, segmentProperties);
    assertNotNull(result);

  }

  @Test public void testGetRowLevelRangeTypeExecuterForGreaterThan() throws Exception {
    RowLevelFilterExecuterImpl result = RowLevelRangeTypeExecuterFacory
        .getRowLevelRangeTypeExecuter(FilterExecuterType.ROWLEVEL_GREATERTHAN,
            rowLevelRangeFilterResolver, segmentProperties);
    assertNotNull(result);
  }

  @Test public void testGetRowLevelRangeTypeExecuterForGreaterThanEqualTo() throws Exception {
    RowLevelFilterExecuterImpl result = RowLevelRangeTypeExecuterFacory
        .getRowLevelRangeTypeExecuter(FilterExecuterType.ROWLEVEL_GREATERTHAN_EQUALTO,
            rowLevelRangeFilterResolver, segmentProperties);
    assertNotNull(result);
  }

  @Test public void testGetRowLevelRangeTypeExecuterForLessThanEqualTo() throws Exception {
    RowLevelFilterExecuterImpl result = RowLevelRangeTypeExecuterFacory
        .getRowLevelRangeTypeExecuter(FilterExecuterType.ROWLEVEL_LESSTHAN_EQUALTO,
            rowLevelRangeFilterResolver, segmentProperties);
    assertNotNull(result);
  }

}
