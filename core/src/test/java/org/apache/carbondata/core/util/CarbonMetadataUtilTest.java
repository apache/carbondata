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

package org.apache.carbondata.core.util;

import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.index.BlockIndexInfo;
import org.apache.carbondata.format.BlockIndex;
import org.apache.carbondata.format.BlockletInfo;
import org.apache.carbondata.format.ColumnSchema;
import org.apache.carbondata.format.DataChunk;
import org.apache.carbondata.format.DataType;
import org.apache.carbondata.format.Encoding;
import org.apache.carbondata.format.IndexHeader;
import org.apache.carbondata.format.SegmentInfo;

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.getBlockIndexInfo;
import static org.apache.carbondata.core.util.CarbonMetadataUtil.getIndexHeader;

public class CarbonMetadataUtilTest {
  static List<ByteBuffer> byteBufferList;
  static byte[] byteArr;
  static List<ColumnSchema> columnSchemas;
  static List<BlockletInfo> blockletInfoList;
  static List<ColumnSchema> columnSchemaList;
  static Long[] objMaxArr;
  static Long[] objMinArr;
  static int[] objDecimal;

  @BeforeClass public static void setUp() {
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

    objDecimal = new int[] { 0, 0, 0, 0, 0, 0 };

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

    ValueEncoderMeta meta = CarbonTestUtil.createValueEncoderMeta();
    meta.setDecimal(5);
    meta.setMinValue(objMinArr);
    meta.setMaxValue(objMaxArr);
    meta.setType(org.apache.carbondata.core.metadata.datatype.DataType.DOUBLE_MEASURE_CHAR);

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
    indexHeader.setVersion(3);
    indexHeader.setSegment_info(segmentInfo);
    indexHeader.setTable_columns(columnSchemaList);
    indexHeader.setBucket_id(0);
    indexHeader.setSchema_time_stamp(0L);
    IndexHeader indexheaderResult = getIndexHeader(columnCardinality, columnSchemaList, 0, 0L);
    assertEquals(indexHeader, indexheaderResult);
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

    List<Boolean> isMinMaxSet = new ArrayList<>();
    isMinMaxSet.add(true);

    org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex
        blockletMinMaxIndex =
        new org.apache.carbondata.core.metadata.blocklet.index.BlockletMinMaxIndex(minList,
            maxList, isMinMaxSet);
    org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex
        blockletBTreeIndex =
        new org.apache.carbondata.core.metadata.blocklet.index.BlockletBTreeIndex(startKey,
            endKey);
    org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex blockletIndex =
        new org.apache.carbondata.core.metadata.blocklet.index.BlockletIndex(
            blockletBTreeIndex, blockletMinMaxIndex);

    BlockIndexInfo blockIndexInfo = new BlockIndexInfo(1, "file", 1, blockletIndex);

    List<BlockIndexInfo> blockIndexInfoList = new ArrayList<>();
    blockIndexInfoList.add(blockIndexInfo);
    List<BlockIndex> result = getBlockIndexInfo(blockIndexInfoList);
    String expected = "file";
    assertEquals(result.get(0).file_name, expected);
  }

  @Test public void testGetBlockletIndex() throws Exception {

    long left = Long.MAX_VALUE;
    long right = 100;
    ByteBuffer buffer = ByteBuffer.allocate(8);
    buffer.putLong(left);
    buffer.flip();
    byte[] l = buffer.array().clone();

    buffer.rewind();
    buffer.putLong(right);
    buffer.flip();
    byte[] r = buffer.array().clone();

    Method method = CarbonMetadataUtil.class
        .getDeclaredMethod("compareMeasureData", l.getClass(), r.getClass(),
            org.apache.carbondata.core.metadata.datatype.DataType.class);
    method.setAccessible(true);
    int out = (int)method.invoke(method, l, r, DataTypes.LONG);
    assertEquals(1, out);

  }

}
