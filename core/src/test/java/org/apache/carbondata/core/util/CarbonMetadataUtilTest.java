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

package org.apache.carbondata.core.util;

import mockit.Mock;
import mockit.MockUp;

import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.metadata.index.BlockIndexInfo;
import org.apache.carbondata.core.datastorage.store.compression.WriterCompressModel;
import org.apache.carbondata.core.metadata.BlockletInfoColumnar;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.format.*;
import org.apache.carbondata.format.BlockletMinMaxIndex;
import org.apache.carbondata.format.ColumnSchema;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.*;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.getIndexHeader;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.convertFileFooter;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.getBlockIndexInfo;

public class CarbonMetadataUtilTest {
  static List<ByteBuffer> byteBufferList;
  static byte[] byteArr;
  static List<ColumnSchema> columnSchemas;
  static List<BlockletInfo> blockletInfoList;
  static List<ColumnSchema> columnSchemaList;
  static Long[] objMaxArr;
  static Long[] objMinArr;

  @BeforeClass public static void setUp() {
    Long lngObj = new Long("11221");
    byte byt = 1;
    objMaxArr = new Long[6];
    objMaxArr[0] = new Long("111111");
    objMaxArr[1] = new Long("121111");
    objMaxArr[2] = new Long("131111");
    objMaxArr[3] = new Long("141111");
    objMaxArr[4] = new Long("151111");
    objMaxArr[5] = new Long("161111");

    objMinArr = new Long[6];
    objMinArr[0] = new Long("119");
    objMinArr[1] = new Long("121");
    objMinArr[2] = new Long("131");
    objMinArr[3] = new Long("141");
    objMinArr[4] = new Long("151");
    objMinArr[5] = new Long("161");

    columnSchemaList = new ArrayList<>();
    List<Encoding> encodingList = new ArrayList<>();
    encodingList.add(Encoding.BIT_PACKED);
    encodingList.add(Encoding.DELTA);
    encodingList.add(Encoding.INVERTED_INDEX);
    encodingList.add(Encoding.DIRECT_DICTIONARY);

    byteArr = "412111".getBytes();
    byte[] byteArr1 = "321".getBytes();
    byte[] byteArr2 = "356".getBytes();

    byteBufferList = new ArrayList<>();
    ByteBuffer bb = ByteBuffer.allocate(byteArr.length);
    bb.put(byteArr);
    ByteBuffer bb1 = ByteBuffer.allocate(byteArr1.length);
    bb1.put(byteArr1);
    ByteBuffer bb2 = ByteBuffer.allocate(byteArr2.length);
    bb2.put(byteArr2);
    byteBufferList.add(bb);
    byteBufferList.add(bb1);
    byteBufferList.add(bb2);

    DataChunk dataChunk = new DataChunk();
    dataChunk.setEncoders(encodingList);
    dataChunk.setEncoder_meta(byteBufferList);

    List<DataChunk> dataChunkList = new ArrayList<>();
    dataChunkList.add(dataChunk);
    dataChunkList.add(dataChunk);

    BlockletInfo blockletInfo = new BlockletInfo();
    blockletInfo.setColumn_data_chunks(dataChunkList);
    blockletInfoList = new ArrayList<>();
    blockletInfoList.add(blockletInfo);
    blockletInfoList.add(blockletInfo);

    ValueEncoderMeta valueEncoderMeta = new ValueEncoderMeta();
    valueEncoderMeta.setMantissa(5);
    valueEncoderMeta.setMinValue(objMinArr);
    valueEncoderMeta.setMaxValue(objMaxArr);
    valueEncoderMeta.setUniqueValue(lngObj);
    valueEncoderMeta.setType('a');
    valueEncoderMeta.setDataTypeSelected(byt);

    List<Encoding> encoders = new ArrayList<>();
    encoders.add(Encoding.INVERTED_INDEX);
    encoders.add(Encoding.BIT_PACKED);
    encoders.add(Encoding.DELTA);
    encoders.add(Encoding.DICTIONARY);
    encoders.add(Encoding.DIRECT_DICTIONARY);
    encoders.add(Encoding.RLE);

    ColumnSchema columnSchema = new ColumnSchema(DataType.INT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema1 =
        new ColumnSchema(DataType.ARRAY, "column", "3", true, encoders, true);
    ColumnSchema columnSchema2 =
        new ColumnSchema(DataType.DECIMAL, "column", "3", true, encoders, true);
    ColumnSchema columnSchema3 =
        new ColumnSchema(DataType.DOUBLE, "column", "3", true, encoders, true);
    ColumnSchema columnSchema4 =
        new ColumnSchema(DataType.LONG, "column", "3", true, encoders, true);
    ColumnSchema columnSchema5 =
        new ColumnSchema(DataType.SHORT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema6 =
        new ColumnSchema(DataType.STRUCT, "column", "3", true, encoders, true);
    ColumnSchema columnSchema7 =
        new ColumnSchema(DataType.STRING, "column", "3", true, encoders, true);
    columnSchemas = new ArrayList<>();
    columnSchemas.add(columnSchema);
    columnSchemas.add(columnSchema1);
    columnSchemas.add(columnSchema2);
    columnSchemas.add(columnSchema3);
    columnSchemas.add(columnSchema4);
    columnSchemas.add(columnSchema5);
    columnSchemas.add(columnSchema6);
    columnSchemas.add(columnSchema7);
  }

  @Test public void testGetIndexHeader() {
    int[] columnCardinality = { 1, 2, 3, 4 };
    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNum_cols(0);
    segmentInfo.setColumn_cardinalities(CarbonUtil.convertToIntegerList(columnCardinality));
    IndexHeader indexHeader = new IndexHeader();
    indexHeader.setVersion(2);
    indexHeader.setSegment_info(segmentInfo);
    indexHeader.setTable_columns(columnSchemaList);
    indexHeader.setBucket_id(0);
    IndexHeader indexheaderResult = getIndexHeader(columnCardinality, columnSchemaList, 0);
    assertEquals(indexHeader, indexheaderResult);
  }

  @Test public void testConvertFileFooter() throws Exception {
    int[] intArr = { 1, 2, 3, 4, 5 };
    boolean[] boolArr = { true, true, true, true, true };
    long[] longArr = { 1, 2, 3, 4, 5 };
    byte[][] maxByteArr = { { 1, 2 }, { 3, 4 }, { 5, 6 }, { 2, 4 }, { 1, 2 } };
    int[] cardinality = { 1, 2, 3, 4, 5 };
    char[] charArr = { 'a', 's', 'd', 'g', 'h' };

    org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema colSchema =
        new org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema();
    org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema colSchema1 =
        new org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema();
    List<org.apache.carbondata.core.carbon.metadata.schema.table.column.ColumnSchema>
        columnSchemaList = new ArrayList<>();
    columnSchemaList.add(colSchema);
    columnSchemaList.add(colSchema1);

    SegmentProperties segmentProperties = new SegmentProperties(columnSchemaList, cardinality);

    final List<Integer> integerList = new ArrayList<>();
    integerList.add(new Integer("1"));
    integerList.add(new Integer("2"));

    WriterCompressModel writerCompressModel = new WriterCompressModel();
    writerCompressModel.setMaxValue(objMaxArr);
    writerCompressModel.setMinValue(objMinArr);
    writerCompressModel.setDataTypeSelected(byteArr);
    writerCompressModel.setMantissa(intArr);
    writerCompressModel.setType(charArr);
    writerCompressModel.setUniqueValue(objMinArr);

    BlockletInfoColumnar blockletInfoColumnar = new BlockletInfoColumnar();

    BitSet[] bitSetArr = new BitSet[6];
    bitSetArr[0] = new BitSet();
    bitSetArr[1] = new BitSet();
    bitSetArr[2] = new BitSet();
    bitSetArr[3] = new BitSet();
    bitSetArr[4] = new BitSet();
    bitSetArr[5] = new BitSet();
    blockletInfoColumnar.setColumnMaxData(maxByteArr);
    blockletInfoColumnar.setColumnMinData(maxByteArr);
    blockletInfoColumnar.setKeyLengths(intArr);
    blockletInfoColumnar.setColGrpBlocks(boolArr);
    blockletInfoColumnar.setKeyOffSets(longArr);
    blockletInfoColumnar.setDataIndexMapOffsets(longArr);
    blockletInfoColumnar.setAggKeyBlock(boolArr);
    blockletInfoColumnar.setDataIndexMapLength(intArr);
    blockletInfoColumnar.setIsSortedKeyColumn(boolArr);
    blockletInfoColumnar.setKeyOffSets(longArr);
    blockletInfoColumnar.setMeasureLength(intArr);
    blockletInfoColumnar.setMeasureOffset(longArr);
    blockletInfoColumnar.setMeasureNullValueIndex(bitSetArr);
    blockletInfoColumnar.setCompressionModel(writerCompressModel);

    BlockletInfoColumnar blockletInfoColumnar1 = new BlockletInfoColumnar();
    blockletInfoColumnar1.setColumnMaxData(maxByteArr);
    blockletInfoColumnar1.setColumnMinData(maxByteArr);
    blockletInfoColumnar1.setKeyLengths(intArr);
    blockletInfoColumnar1.setKeyOffSets(longArr);
    blockletInfoColumnar1.setDataIndexMapOffsets(longArr);
    blockletInfoColumnar1.setAggKeyBlock(boolArr);
    blockletInfoColumnar1.setDataIndexMapLength(intArr);
    blockletInfoColumnar1.setIsSortedKeyColumn(boolArr);
    blockletInfoColumnar1.setColGrpBlocks(boolArr);
    blockletInfoColumnar1.setKeyOffSets(longArr);
    blockletInfoColumnar1.setMeasureLength(intArr);
    blockletInfoColumnar1.setMeasureOffset(longArr);
    blockletInfoColumnar1.setMeasureNullValueIndex(bitSetArr);
    blockletInfoColumnar1.setCompressionModel(writerCompressModel);
    blockletInfoColumnar1.setColGrpBlocks(boolArr);

    List<BlockletInfoColumnar> blockletInfoColumnarList = new ArrayList<>();
    blockletInfoColumnarList.add(blockletInfoColumnar);
    blockletInfoColumnarList.add(blockletInfoColumnar1);

    new MockUp<CarbonUtil>() {
      @SuppressWarnings("unused") @Mock public List<Integer> convertToIntegerList(int[] array) {
        return integerList;
      }
    };

    final Set<Integer> integerSet = new HashSet<>();
    integerSet.add(new Integer("1"));
    integerSet.add(new Integer("2"));
    new MockUp<SegmentProperties>() {
      @SuppressWarnings("unused") @Mock
      public Set<Integer> getDimensionOrdinalForBlock(int blockIndex) {
        return integerSet;
      }
    };

    SegmentInfo segmentInfo = new SegmentInfo();
    segmentInfo.setNum_cols(4);
    segmentInfo.setColumn_cardinalities(integerList);

    FileFooter fileFooter = new FileFooter();
    fileFooter.setNum_rows(4);
    fileFooter.setSegment_info(segmentInfo);

    byte[] byteMaxArr = "1".getBytes();
    byte[] byteMinArr = "2".getBytes();

    BlockletMinMaxIndex blockletMinMaxIndex = new BlockletMinMaxIndex();
    blockletMinMaxIndex.addToMax_values(ByteBuffer.wrap(byteMaxArr));
    blockletMinMaxIndex.addToMin_values(ByteBuffer.wrap(byteMinArr));
    FileFooter result = convertFileFooter(blockletInfoColumnarList, 4, cardinality, columnSchemas,
        segmentProperties);
    assertEquals(result.getTable_columns(), columnSchemas);

  }

  @Test public void testGetBlockIndexInfo() throws Exception {
    byte[] startKey = { 1, 2, 3, 4, 5 };
    byte[] endKey = { 9, 3, 5, 5, 5 };
    byte[] byteArr = { 1, 2, 3, 4, 5 };
    List<ByteBuffer> minList = new ArrayList<>();
    minList.add(ByteBuffer.wrap(byteArr));

    byte[] byteArr1 = { 9, 9, 8, 6, 7 };
    List<ByteBuffer> maxList = new ArrayList<>();
    maxList.add(ByteBuffer.wrap(byteArr1));

    org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex
        blockletMinMaxIndex =
        new org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletMinMaxIndex(minList,
            maxList);
    org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex
        blockletBTreeIndex =
        new org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletBTreeIndex(startKey,
            endKey);
    org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex blockletIndex =
        new org.apache.carbondata.core.carbon.metadata.blocklet.index.BlockletIndex(
            blockletBTreeIndex, blockletMinMaxIndex);

    BlockIndexInfo blockIndexInfo = new BlockIndexInfo(1, "file", 1, blockletIndex);

    List<BlockIndexInfo> blockIndexInfoList = new ArrayList<>();
    blockIndexInfoList.add(blockIndexInfo);
    List<BlockIndex> result = getBlockIndexInfo(blockIndexInfoList);
    String expected = "file";
    assertEquals(result.get(0).file_name, expected);
  }

}
