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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.compression.Compressor;
import org.apache.carbondata.core.datastore.compression.CompressorFactory;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.UnsafeDecimalColumnPage;
import org.apache.carbondata.core.datastore.page.UnsafeFixLengthColumnPage;
import org.apache.carbondata.core.datastore.page.UnsafeVarLengthColumnPage;
import org.apache.carbondata.core.datastore.page.statistics.LVShortStringStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;

public class GetPageInUnsafeFixLengthColumnPageTest<T> {

  static class TestData<T> {
    private ColumnPage inputBytePage;
    TestData(T[] inputByteData, DataType dataType, ColumnType columnType) throws Exception {
      inputBytePage = ColumnPage.newPage(
          new ColumnPageEncoderMeta(
              TableSpec.ColumnSpec.newInstance("test", dataType, columnType),
              dataType, "snappy"),
          inputByteData.length);
      inputBytePage.setStatsCollector(PrimitivePageStatsCollector.newInstance(dataType));
      for (int i = 0; i < inputByteData.length; i++) {
        inputBytePage.putData(i, inputByteData[i]);
      }
    }
  }

  private static TestData bytePage;
  private static TestData booleanPage;
  private static TestData shortPage;
  private static TestData integerPage;
  private static TestData longPage;
  private static TestData floatPage;
  private static TestData doublePage;
  private static TestData decimalPage;
  private static TestData timestampPage;

  @BeforeClass public static void setUp() throws Exception {
    // Make Sure the ENABLE_UNSAFE_COLUMN_PAGE is set to "true".
    CarbonProperties.getInstance().removeProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true");
    assertEquals(CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE), "true");

    setUpBytePage();
    setUpBooleanPage();
    setUpShortIntegerPage();
    setUpIntegerPage();
    setUpLongPage();
    setUpDecimalPage();
    setUpFloatPage();
    setUpDoublePage();
    setUpTimeStampPage();
  }

  /**
   * we compare two methods to compress columnpage, the first method is to compress bytearray which is the origin one,
   * another method is to compress bytebuffer which is the optimized way. we make sure that two methods generate
   * same results.
   */
  private static void testGetPage(TestData testData, Compressor compressor, Class clazz) throws Exception {
    ColumnPage inputBytePage = testData.inputBytePage;
    // 1. Make Sure that the columnpage is in type of specified COLUMNPAGE class
    assertTrue(clazz.isInstance(inputBytePage));

    // 2. Try to compress bytearray in the columnpage
    byte[] compressedOriginPageByteArray = inputBytePage.compress(compressor);

    // 3. Try to compress bytebuffer in the columnpage
    ByteBuffer newPageByteBuffer = inputBytePage.getPage();
    byte[] compressedNewPageByteArray = ByteUtil.byteBufferToBytes(compressor.compressByte(newPageByteBuffer));

    // 4. Make Sure that the compared results are same.
    assertEquals(0, ByteUtil.compare(compressedOriginPageByteArray, compressedNewPageByteArray));
  }

  private static void setUpBytePage() throws Exception {
    Byte[] inputData = new Byte[]{10, 10, 3, 4, 5, 6, 7, 7, 7, 7};
    bytePage = new TestData(inputData, DataTypes.BYTE, ColumnType.MEASURE);
  }

  private static void setUpBooleanPage() throws Exception {
    Boolean[] inputData = new Boolean[]{true, false, false, true, false, true, false};
    booleanPage = new TestData(inputData, DataTypes.BOOLEAN, ColumnType.MEASURE);
  }

  private static void setUpShortIntegerPage() throws Exception {
    Short[] inputData = new Short[]{1, 2, 3, 4, 5, 6};
    shortPage = new TestData(inputData, DataTypes.SHORT, ColumnType.MEASURE);
  }

  private static void setUpIntegerPage() throws Exception {
    Integer[] inputData = new Integer[]{1, 2, 3, 4, 5, 6};
    integerPage = new TestData(inputData, DataTypes.INT, ColumnType.MEASURE);
  }

  private static void setUpLongPage() throws Exception {
    Long[] inputData = new Long[]{1l, 2l, 3l, 4l, 5l, 6l};
    longPage = new TestData(inputData, DataTypes.LONG, ColumnType.MEASURE);
  }

  private static void setUpFloatPage() throws Exception {
    Float[] inputData = new Float[]{0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f};
    floatPage = new TestData(inputData, DataTypes.FLOAT, ColumnType.MEASURE);
  }

  private static void setUpDoublePage() throws Exception {
    Double[] inputData = new Double[]{0.1, 0.2, 0.3, 0.4, 0.5, 0.6};
    doublePage = new TestData(inputData, DataTypes.DOUBLE, ColumnType.MEASURE);
  }

  private static void setUpTimeStampPage() throws Exception {
    Long[] inputData = new Long[]{System.currentTimeMillis(),
        System.currentTimeMillis(),
        System.currentTimeMillis()};
    timestampPage = new TestData(inputData, DataTypes.TIMESTAMP, ColumnType.DIRECT_DICTIONARY);
  }

  private static void setUpDecimalPage() throws Exception {
    BigDecimal[] inputData = new BigDecimal[]{new BigDecimal("0.1"),
        new BigDecimal("0.2"), new BigDecimal("0.3"),
        new BigDecimal("0.4"), new BigDecimal("0.5")};
    decimalPage = new TestData(inputData,
        DataTypes.createDecimalType(19,2), ColumnType.MEASURE);
  }

  private static void testGetPageForFixLengthString(TestData testData, Compressor compressor, Class clazz) throws Exception {
    ColumnPage inputBytePage = testData.inputBytePage;
    // 1. Make Sure that the columnpage is in type of specified COLUMNPAGE class
    assertTrue(clazz.isInstance(inputBytePage));

    // 2. Try to compress bytearray in the columnpage
    byte[][] originPageByteArray = inputBytePage.getByteArrayPage();

    // 3. Try to compress bytebuffer in the columnpage
    ByteBuffer[] newPageByteBuffer = inputBytePage.getByteBufferArrayPage(false);
    assertEquals(originPageByteArray.length, newPageByteBuffer.length);
    byte[][] newPageByteArray = new byte[newPageByteBuffer.length][];
    for (int i = 0 ; i < originPageByteArray.length; i++) {
      assertEquals(originPageByteArray[i].length, newPageByteBuffer[i].remaining());
      newPageByteArray[i] = ByteUtil.byteBufferToBytes(newPageByteBuffer[i]);
      assertEquals(0, ByteUtil.compare(originPageByteArray[i], newPageByteArray[i]));
    }
    // 4. Make Sure that the compared results are same.
  }

  @Test
  public void testGetPage() throws Exception {
    Compressor snappyCompressor = CompressorFactory.getInstance().getCompressor("SNAPPY");
    Compressor zstdCompressor = CompressorFactory.getInstance().getCompressor("ZSTD");
    Compressor gzipCompressor = CompressorFactory.getInstance().getCompressor("GZIP");

    testGetPage(bytePage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(bytePage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(bytePage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(booleanPage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(booleanPage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(booleanPage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(shortPage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(shortPage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(shortPage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(integerPage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(integerPage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(integerPage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(longPage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(longPage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(longPage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(decimalPage, snappyCompressor, UnsafeDecimalColumnPage.class);
    testGetPage(decimalPage, zstdCompressor, UnsafeDecimalColumnPage.class);
    testGetPage(decimalPage, gzipCompressor, UnsafeDecimalColumnPage.class);

    testGetPage(floatPage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(floatPage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(floatPage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(doublePage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(doublePage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(doublePage, gzipCompressor, UnsafeFixLengthColumnPage.class);

    testGetPage(timestampPage, snappyCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(timestampPage, zstdCompressor, UnsafeFixLengthColumnPage.class);
    testGetPage(timestampPage, gzipCompressor, UnsafeFixLengthColumnPage.class);
  }

  @AfterClass public static void tearDown() {
    CarbonProperties.getInstance().removeProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
  }
}
