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
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.ColumnGroupDimensionDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.impl.FixedLengthDimensionDataChunk;
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
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;
import org.apache.carbondata.core.datastorage.store.impl.FileHolderImpl;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.directdictionary.timestamp.TimeStampDirectDictionaryGenerator;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.scan.complextypes.ArrayQueryType;
import org.apache.carbondata.scan.expression.ColumnExpression;
import org.apache.carbondata.scan.filter.DimColumnFilterInfo;
import org.apache.carbondata.scan.filter.FilterUtil;
import org.apache.carbondata.scan.filter.GenericQueryType;
import org.apache.carbondata.scan.filter.executer.RowLevelRangeLessThanEqualFilterExecuterImpl;
import org.apache.carbondata.scan.filter.executer.RowLevelRangeLessThanFiterExecuterImpl;
import org.apache.carbondata.scan.filter.intf.RowImpl;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

import mockit.Mock;
import mockit.MockUp;
import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertFalse;

public class RowLevelRangeLessThanFiterExecuterImplTest {
  static RowLevelRangeLessThanFiterExecuterImpl rowLevelRangeLessThanFiterExecuterImpl;
  static BlocksChunkHolder blocksChunkHolder;
  static BTreeBuilderInfo bTreeBuilderInfo;
  static ColumnGroupDimensionDataChunk[] columnGroupDimensionDataChunks;
  static MeasureColumnDataChunk[] measureColumnDataChunks;
  static BlockBTreeLeafNode blockBTreeLeafNode;
  static DimensionChunkAttributes dimensionChunkAttributes;
  static FixedLengthDimensionDataChunk[] fixedLengthDimensionDataChunk;

  @BeforeClass public static void setUp() {
    byte[] dictKeys = { 1, 2, 3, 4, 5, 6 };
    byte[] noDictKeys = { 1, 2, 3, 4, 5, 6 };
    final byte[] bytArr = { 1, 2, 3, 4, 5, 6 };
    byte[] bytArr1 = { 8, 8, 9, 6, 5, 6 };
    byte[] startKey = { 1, 2, 3, 4, 5, 6 };
    byte[] endKey = { 1, 2, 3, 4, 5, 6 };
    int[] intArr = { 1, 2, 3, 4, 5, 6 };
    byte[][] byteArr = { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 4, 5 }, { 5, 6 }, { 6, 7 } };
    long[] longArr = { 1, 2, 3, 4, 5, 6 };
    double[] doubleArr = { 1, 2, 3, 4, 5, 6 };

    DimColumnFilterInfo dimColumnFilterInfo = new DimColumnFilterInfo();

    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(Encoding.INVERTED_INDEX);
    encodingList.add(Encoding.BIT_PACKED);
    encodingList.add(Encoding.BIT_PACKED);
    encodingList.add(Encoding.DICTIONARY);
    encodingList.add(Encoding.DIRECT_DICTIONARY);

    ColumnSchema columnSchema = new ColumnSchema();
    columnSchema.setEncodingList(encodingList);

    IndexKey indexKey = new IndexKey(dictKeys, noDictKeys);

    CarbonDimension carbonDimension = new CarbonDimension(columnSchema, 1, 1, 1, 1);
    carbonDimension.hasEncoding(Encoding.DICTIONARY);
    carbonDimension.hasEncoding(Encoding.BIT_PACKED);
    carbonDimension.hasEncoding(Encoding.DELTA);
    carbonDimension.hasEncoding(Encoding.DIRECT_DICTIONARY);

    DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo = new DimColumnResolvedFilterInfo();
    dimColumnResolvedFilterInfo.setColumnIndex(1);
    dimColumnResolvedFilterInfo.setFilterValues(dimColumnFilterInfo);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);
    dimColumnResolvedFilterInfo.setDefaultValue("default");
    dimColumnResolvedFilterInfo.setDimensionExistsInCurrentSilce(true);
    dimColumnResolvedFilterInfo.setNeedCompressedData(true);
    dimColumnResolvedFilterInfo.setRowIndex(5);
    dimColumnResolvedFilterInfo.setRsSurrogates(1);
    dimColumnResolvedFilterInfo.setEndIndexKey(indexKey);
    dimColumnResolvedFilterInfo.setEndIndexKey(indexKey);
    dimColumnResolvedFilterInfo.setDimension(carbonDimension);

    final Map<Integer, Integer> dimensionOrdinalMap = new HashMap<>();
    dimensionOrdinalMap.put(new Integer("1"), new Integer("2"));
    dimensionOrdinalMap.put(new Integer("5"), new Integer("4"));
    dimensionOrdinalMap.put(new Integer("2"), new Integer("3"));
    dimensionOrdinalMap.put(new Integer("6"), new Integer("7"));

    final TimeStampDirectDictionaryGenerator timeStampDirectDictionaryGenerator =
        new TimeStampDirectDictionaryGenerator("yyyy-MM-dd HH:mm:ss");

    final TimeStampDirectDictionaryGenerator timeStampDirectDictionaryGenerator1 =
        new TimeStampDirectDictionaryGenerator(CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP,
                CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT));

    new MockUp<DirectDictionaryKeyGeneratorFactory>() {
      @SuppressWarnings("unused") @Mock
      public DirectDictionaryGenerator getDirectDictionaryGenerator(DataType dataType,
          String dateFormat) {
        return timeStampDirectDictionaryGenerator;
      }

      @SuppressWarnings("unused") @Mock
      public DirectDictionaryGenerator getDirectDictionaryGenerator(DataType dataType) {
        return timeStampDirectDictionaryGenerator1;
      }
    };

    new MockUp<SegmentProperties>() {
      @SuppressWarnings("unused") @Mock
      public Map<Integer, Integer> getDimensionOrdinalToBlockMapping() {
        return dimensionOrdinalMap;
      }
    };

    new MockUp<CarbonReadDataHolder>() {
      @SuppressWarnings("unused") @Mock public double getReadableDoubleValueByIndex(int index) {
        return 6;
      }
    };

    List<DimColumnResolvedFilterInfo> dimColumnResolvedFilterInfoList = new ArrayList<>();
    dimColumnResolvedFilterInfoList.add(dimColumnResolvedFilterInfo);
    dimColumnResolvedFilterInfoList.add(dimColumnResolvedFilterInfo);
    dimColumnResolvedFilterInfoList.add(dimColumnResolvedFilterInfo);

    MeasureColumnResolvedFilterInfo measureColumnResolvedFilterInfo =
        new MeasureColumnResolvedFilterInfo();
    measureColumnResolvedFilterInfo.setColumnIndex(5);
    measureColumnResolvedFilterInfo.setType(DataType.DECIMAL);
    measureColumnResolvedFilterInfo.setType(DataType.LONG);
    measureColumnResolvedFilterInfo.setType(DataType.INT);
    measureColumnResolvedFilterInfo.setType(DataType.DOUBLE);
    measureColumnResolvedFilterInfo.setDefaultValue(6);
    measureColumnResolvedFilterInfo.setAggregator("aggregator");
    measureColumnResolvedFilterInfo.setUniqueValue(new Integer("5"));
    measureColumnResolvedFilterInfo.setMeasureExistsInCurrentSlice(true);
    measureColumnResolvedFilterInfo.setRowIndex(5);

    List<MeasureColumnResolvedFilterInfo> measureColumnResolvedFilterInfoList = new ArrayList<>();
    measureColumnResolvedFilterInfoList.add(measureColumnResolvedFilterInfo);
    measureColumnResolvedFilterInfoList.add(measureColumnResolvedFilterInfo);
    measureColumnResolvedFilterInfoList.add(measureColumnResolvedFilterInfo);

    new RowImpl();
    ColumnExpression columnExpression = new ColumnExpression("column1", DataType.STRING);
    columnExpression.setColIndex(5);

    CarbonTableIdentifier carbonTableIdentifier =
        new CarbonTableIdentifier("testDatabase", "testTable", "testId");
    AbsoluteTableIdentifier absoluteTableIdentifier =
        new AbsoluteTableIdentifier("storePath", carbonTableIdentifier);

    List<ColumnSchema> columnSchemaList = new ArrayList<>();
    columnSchemaList.add(columnSchema);
    int[] cardinality = { 1, 2, 3, 4, 5, 6 };

    SegmentProperties segmentProperties = new SegmentProperties(columnSchemaList, cardinality);

    ArrayQueryType arrayQueryType = new ArrayQueryType("name", "parentName", 1);
    ArrayQueryType arrayQueryType2 = new ArrayQueryType("name", "parentName", 2);
    Map<Integer, GenericQueryType> complexDimensionInfoMap = new HashMap<>();
    complexDimensionInfoMap.put(new Integer("1"), arrayQueryType);
    complexDimensionInfoMap.put(new Integer("2"), arrayQueryType2);

    rowLevelRangeLessThanFiterExecuterImpl =
        new RowLevelRangeLessThanFiterExecuterImpl(dimColumnResolvedFilterInfoList,
            measureColumnResolvedFilterInfoList, columnExpression, absoluteTableIdentifier, byteArr,
            segmentProperties);
    dimensionChunkAttributes = new DimensionChunkAttributes();
    dimensionChunkAttributes.setEachRowSize(0);
    dimensionChunkAttributes.setInvertedIndexes(intArr);
    dimensionChunkAttributes.setInvertedIndexesReverse(intArr);
    dimensionChunkAttributes.setNoDictionary(true);

    new MockUp<ColumnGroupDimensionDataChunk>() {
      @SuppressWarnings("unused") @Mock public byte[] getChunkData(int rowId) {
        return bytArr;
      }
    };

    new MockUp<CarbonUtil>() {
      @SuppressWarnings("unused") @Mock public int getSurrogateKey(byte[] data, ByteBuffer buffer) {
        return 6;
      }

    };

    new MockUp<FilterUtil>() {
      @SuppressWarnings("unused") @Mock
      public byte[] getMaskKey(int surrogate, CarbonDimension carbonDimension,
          KeyGenerator blockLevelKeyGenerator) {
        return bytArr;
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

    BitSet bitSet = new BitSet();
    bitSet.flip(0);
    bitSet.flip(1);
    bitSet.flip(2);
    bitSet.flip(3);
    bitSet.flip(4);
    bitSet.flip(5);

    PresenceMeta presenceMeta = new PresenceMeta();
    presenceMeta.setRepresentNullValues(true);
    presenceMeta.setBitSet(bitSet);

    MeasureColumnDataChunk measureColumnDataChunk = new MeasureColumnDataChunk();
    measureColumnDataChunk.setMeasureDataHolder(carbonReadDataHolder);
    measureColumnDataChunk.setNullValueIndexHolder(presenceMeta);

    measureColumnDataChunks = new MeasureColumnDataChunk[6];
    measureColumnDataChunks[0] = measureColumnDataChunk;
    measureColumnDataChunks[1] = measureColumnDataChunk;
    measureColumnDataChunks[2] = measureColumnDataChunk;
    measureColumnDataChunks[3] = measureColumnDataChunk;
    measureColumnDataChunks[4] = measureColumnDataChunk;
    measureColumnDataChunks[5] = measureColumnDataChunk;

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

  @Test public void testIsScanRequired() {
    byte[][] maxValue = { { 32, 22 }, { 12, 13 }, { 13, 14 }, { 14, 15 }, { 15, 16 }, { 61, 71 } };
    byte[][] minValue = { { 1, 2 }, { 2, 3 }, { 3, 4 }, { 4, 5 }, { 5, 6 }, { 6, 7 } };
    BitSet expected = new BitSet();
    expected.flip(0);
    BitSet result = rowLevelRangeLessThanFiterExecuterImpl.isScanRequired(maxValue, minValue);
    assertEquals(result, expected);
  }

  @Test public void testApplyFilter() throws Exception {
    byte[] bytArr = { 1, 2, 3, 4, 5, 6 };
    fixedLengthDimensionDataChunk = new FixedLengthDimensionDataChunk[6];
    fixedLengthDimensionDataChunk[0] =
        new FixedLengthDimensionDataChunk(bytArr, dimensionChunkAttributes);
    fixedLengthDimensionDataChunk[1] =
        new FixedLengthDimensionDataChunk(bytArr, dimensionChunkAttributes);
    fixedLengthDimensionDataChunk[2] =
        new FixedLengthDimensionDataChunk(bytArr, dimensionChunkAttributes);
    fixedLengthDimensionDataChunk[3] =
        new FixedLengthDimensionDataChunk(bytArr, dimensionChunkAttributes);
    fixedLengthDimensionDataChunk[4] =
        new FixedLengthDimensionDataChunk(bytArr, dimensionChunkAttributes);
    fixedLengthDimensionDataChunk[5] =
        new FixedLengthDimensionDataChunk(bytArr, dimensionChunkAttributes);
    FileHolderImpl fileHolder = new FileHolderImpl();
    blocksChunkHolder = new BlocksChunkHolder(6, 6);
    blocksChunkHolder.setDimensionDataChunk(fixedLengthDimensionDataChunk);
    blocksChunkHolder.setMeasureDataChunk(measureColumnDataChunks);
    blocksChunkHolder.setDataBlock(blockBTreeLeafNode);
    blocksChunkHolder.setFileReader(fileHolder);
    BitSet result = rowLevelRangeLessThanFiterExecuterImpl.applyFilter(blocksChunkHolder);
    int expected = 4;
    assertEquals(result.length(), expected);
  }

  @Test public void testApplyFilterForBitSet() throws Exception {

    byte[] bytArr = { 1, 2, 3, 4, 5, 6 };
    dimensionChunkAttributes = new DimensionChunkAttributes();

    columnGroupDimensionDataChunks = new ColumnGroupDimensionDataChunk[6];
    columnGroupDimensionDataChunks[0] =
        new ColumnGroupDimensionDataChunk(bytArr, dimensionChunkAttributes);
    columnGroupDimensionDataChunks[1] =
        new ColumnGroupDimensionDataChunk(bytArr, dimensionChunkAttributes);
    columnGroupDimensionDataChunks[2] =
        new ColumnGroupDimensionDataChunk(bytArr, dimensionChunkAttributes);
    columnGroupDimensionDataChunks[3] =
        new ColumnGroupDimensionDataChunk(bytArr, dimensionChunkAttributes);
    columnGroupDimensionDataChunks[4] =
        new ColumnGroupDimensionDataChunk(bytArr, dimensionChunkAttributes);
    columnGroupDimensionDataChunks[5] =
        new ColumnGroupDimensionDataChunk(bytArr, dimensionChunkAttributes);

    FileHolderImpl fileHolder = new FileHolderImpl();
    blocksChunkHolder = new BlocksChunkHolder(6, 6);
    blocksChunkHolder.setDimensionDataChunk(columnGroupDimensionDataChunks);
    blocksChunkHolder.setMeasureDataChunk(measureColumnDataChunks);
    blocksChunkHolder.setDataBlock(blockBTreeLeafNode);
    blocksChunkHolder.setFileReader(fileHolder);
    BitSet result = rowLevelRangeLessThanFiterExecuterImpl.applyFilter(blocksChunkHolder);
    int expected = 0;
    assertEquals(result.length(), expected);
  }

}
