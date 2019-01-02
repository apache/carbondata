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
package org.apache.carbondata.core.datastore.page.encoding;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLECodec;
import org.apache.carbondata.core.datastore.page.encoding.rle.RLEEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RLECodecTest {

  static class TestData {
    private byte[] inputByteData;
    private ColumnPage inputBytePage;
    private byte[] expectedEncodedByteData;

    TestData(byte[] inputByteData, byte[] expectedEncodedByteData) throws IOException, MemoryException {
      this.inputByteData = inputByteData;
      inputBytePage = ColumnPage.newPage(
          new ColumnPageEncoderMeta(
              TableSpec.ColumnSpec.newInstance("test", DataTypes.BYTE, ColumnType.MEASURE),
              DataTypes.BYTE, "snappy"),
          inputByteData.length);
      inputBytePage.setStatsCollector(PrimitivePageStatsCollector.newInstance(DataTypes.BYTE));
      for (int i = 0; i < inputByteData.length; i++) {
        inputBytePage.putData(i, inputByteData[i]);
      }
      this.expectedEncodedByteData = expectedEncodedByteData;
    }

  }

  private static TestData data1;
  private static TestData data2;
  private static TestData data3;

  @BeforeClass public static void setUp() throws IOException, MemoryException {
    setUpData1();
    setUpData2();
    setUpData3();
  }

  private static void setUpData1() throws IOException, MemoryException {
    byte[] inputData = new byte[]{10, 10, 3, 4, 5, 6, 7, 7, 7, 7};
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(bao);
    stream.writeShort(2);
    stream.writeByte(10);
    stream.writeShort(5|0x8000);
    stream.writeByte(3);
    stream.writeByte(4);
    stream.writeByte(5);
    stream.writeByte(6);
    stream.writeByte(7);
    stream.writeShort(3);
    stream.writeByte(7);
    byte[] expectedEncodedByteData = bao.toByteArray();
    data1 = new TestData(
        inputData,
        expectedEncodedByteData);
  }

  private static void setUpData2() throws IOException, MemoryException {
    byte[] inputData = new byte[]{1, 2, 3, 4, 5, 6};
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(bao);
    stream.writeShort(6|0x8000);
    stream.writeByte(1);
    stream.writeByte(2);
    stream.writeByte(3);
    stream.writeByte(4);
    stream.writeByte(5);
    stream.writeByte(6);
    byte[] expectedEncodedByteData = bao.toByteArray();
    data2 = new TestData(
        inputData,
        expectedEncodedByteData);
  }

  private static void setUpData3() throws IOException, MemoryException {
    byte[] inputData = new byte[]{10, 10, 10, 10, 10, 10};
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    DataOutputStream stream = new DataOutputStream(bao);
    stream.writeShort(6);
    stream.writeByte(10);
    byte[] expectedEncodedByteData = bao.toByteArray();
    data3 = new TestData(
        inputData,
        expectedEncodedByteData);
  }

  private void testBytePageEncode(ColumnPage inputPage, byte[] expectedEncodedBytes)
      throws IOException, MemoryException {
    RLECodec codec = new RLECodec();
    ColumnPageEncoder encoder = codec.createEncoder(null);
    EncodedColumnPage result = encoder.encode(inputPage);
    byte[] encoded = result.getEncodedData().array();
    assertEquals(expectedEncodedBytes.length, encoded.length);
    for (int i = 0; i < encoded.length; i++) {
      assertEquals(expectedEncodedBytes[i], encoded[i]);
    }
  }

  private void testBytePageDecode(byte[] inputBytes, byte[] expectedDecodedBytes) throws IOException, MemoryException {
    RLECodec codec = new RLECodec();
    RLEEncoderMeta meta = new RLEEncoderMeta(
        TableSpec.ColumnSpec.newInstance("test", DataTypes.BYTE, ColumnType.MEASURE),
        DataTypes.BYTE, expectedDecodedBytes.length, null, "snappy");
    ColumnPageDecoder decoder = codec.createDecoder(meta);
    ColumnPage page = decoder.decode(inputBytes, 0, inputBytes.length);
    byte[] decoded = page.getBytePage();
    assertEquals(expectedDecodedBytes.length, decoded.length);
    for (int i = 0; i < decoded.length; i++) {
      assertEquals(expectedDecodedBytes[i], decoded[i]);
    }
  }

  @Test public void testBytePageEncode() throws MemoryException, IOException {
    testBytePageEncode(data1.inputBytePage, data1.expectedEncodedByteData);
    testBytePageEncode(data2.inputBytePage, data2.expectedEncodedByteData);
    testBytePageEncode(data3.inputBytePage, data3.expectedEncodedByteData);
  }

  @Test public void testBytePageDecode() throws IOException, MemoryException {
    testBytePageDecode(data1.expectedEncodedByteData, data1.inputByteData);
    testBytePageDecode(data2.expectedEncodedByteData, data2.inputByteData);
    testBytePageDecode(data3.expectedEncodedByteData, data3.inputByteData);
  }

  @AfterClass public static void tearDown() {
  }
}
