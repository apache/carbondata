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
package org.apache.carbondata.core.datastore.chunk.reader.measure;


public class CompressedMeasureChunkFileBasedReaderTest {

//  static CompressedMeasureChunkFileBasedReaderV1 compressedMeasureChunkFileBasedReader;
//  static CarbonWriteDataHolder[] dataHolder = new CarbonWriteDataHolder[1];
//
//  static WriterCompressModel writerCompressModel;
//  @BeforeClass public static void setup() {
//    List<DataChunk> dataChunkList = new ArrayList<>();
//    DataChunk dataChunk = new DataChunk();
//    dataChunkList.add(dataChunk);
//    dataChunk.setDataPageLength(10);
//    writerCompressModel = new WriterCompressModel();
//    Object maxValue[] = new Object[]{new Long[]{8L, 0L}};
//    Object minValue[] = new Object[]{new Long[]{1L,0L}};
//    byte[] dataTypeSelected = new byte[1];
//    char[] aggType = new char[]{'b'};
//    MeasureMetaDataModel measureMDMdl =
//                new MeasureMetaDataModel(minValue, maxValue, new int[]{1}, maxValue.length, null,
//                    aggType, dataTypeSelected);
//    writerCompressModel = ValueCompressionUtil.getWriterCompressModel(measureMDMdl);
//    
//
//    ValueEncoderMeta meta = new ValueEncoderMeta();
//    meta.setMaxValue(new Long[]{8L,0L});
//    meta.setMinValue(new Long[]{1L,0L});
//    meta.setMantissa(1);
//    meta.setType('b');
//    List<ValueEncoderMeta> valueEncoderMetaList = new ArrayList<>();
//    valueEncoderMetaList.add(meta);
//    dataChunkList.get(0).setValueEncoderMeta(valueEncoderMetaList);
//    BlockletInfo info = new BlockletInfo();
//    info.setMeasureColumnChunk(dataChunkList);
//    compressedMeasureChunkFileBasedReader =
//        new CompressedMeasureChunkFileBasedReaderV1(info, "filePath");
//  }
//
//  @Test public void readMeasureChunkTest() throws IOException {
//    FileHolder fileHolder = new MockUp<FileHolder>() {
//      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
//        byte mockedValue[] = { 1, 5, 4, 8, 7 };
//        return mockedValue;
//      }
//    }.getMockInstance();
//
//    MeasureColumnDataChunk measureColumnDataChunks =
//        compressedMeasureChunkFileBasedReader.readMeasureChunk(fileHolder, 0);
//
//    BigDecimal bigD = new BigDecimal("2.1");
//    assertEquals(bigD,
//        measureColumnDataChunks.getMeasureDataHolder().getReadableBigDecimalValueByIndex(0));
//      
//  }
//
//  @Test public void readMeasureChunksTest() throws IOException {
//    FileHolder fileHolder = new MockUp<FileHolder>() {
//      @Mock public byte[] readByteArray(String filePath, long offset, int length) {
//        byte mockedValue[] = { 1, 5, 4, 8, 7 };
//        return mockedValue;
//      }
//    }.getMockInstance();
//
//    int[][] blockIndexes = {{0,0}};
//    MeasureColumnDataChunk measureColumnDataChunks[] =
//        compressedMeasureChunkFileBasedReader.readMeasureChunks(fileHolder, blockIndexes);
//
//    BigDecimal bigD = new BigDecimal("2.1");
//    assertEquals(bigD,
//        measureColumnDataChunks[0].getMeasureDataHolder().getReadableBigDecimalValueByIndex(0));
//
//  }
}