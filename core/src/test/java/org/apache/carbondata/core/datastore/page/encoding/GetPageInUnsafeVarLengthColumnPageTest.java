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
import org.apache.carbondata.core.datastore.page.SafeDecimalColumnPage;
import org.apache.carbondata.core.datastore.page.SafeFixLengthColumnPage;
import org.apache.carbondata.core.datastore.page.SafeVarLengthColumnPage;
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

public class GetPageInUnsafeVarLengthColumnPageTest<T> {

  static class TestData<T> {
    private ColumnPage inputBytePage;
    TestData(T[] inputByteData, DataType dataType, ColumnType columnType) throws Exception {
      inputBytePage = ColumnPage.newPage(
          new ColumnPageEncoderMeta(
              TableSpec.ColumnSpec.newInstance("test", dataType, columnType),
              dataType, "snappy"),
          inputByteData.length);
      inputBytePage.setStatsCollector(LVShortStringStatsCollector.newInstance());
      for (int i = 0; i < inputByteData.length; i++) {
        inputBytePage.putData(i, inputByteData[i]);
      }
    }
  }

  private static TestData stringPage;
  private static TestData varcharPage;

  @BeforeClass public static void setUp() throws Exception {
    // Make Sure the ENABLE_UNSAFE_COLUMN_PAGE is set to "true".
    CarbonProperties.getInstance().removeProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true");
    assertEquals(CarbonProperties.getInstance().getProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE), "true");

    setUpStringPage();
    setUpVarcharPage();
  }

  private static void setUpStringPage() throws Exception {
    byte[] inputRow = "row".getBytes();
    byte[][] inputData = new byte[][]{inputRow, inputRow, inputRow};
    stringPage = new TestData(inputData, DataTypes.STRING, ColumnType.PLAIN_VALUE);
  }

  private static void setUpVarcharPage() throws Exception {
    byte[] inputRow = "row".getBytes();
    byte[][] inputData = new byte[][]{inputRow, inputRow, inputRow};
    varcharPage = new TestData(inputData, DataTypes.VARCHAR, ColumnType.PLAIN_VALUE);
  }

  /**
   * we compare two methods to compress columnpage, the first method is to compress bytearray which is the origin one,
   * another method is to compress bytebuffer which is the optimized way. we make sure that two methods generate
   * same results.
   */
  private static void testGetPage(TestData testData, Class clazz) throws Exception {
    ColumnPage inputBytePage = testData.inputBytePage;
    // 1. Make Sure that the columnpage is in type of specified COLUMNPAGE class
    assertTrue(clazz.isInstance(inputBytePage));

    // 2. Get the data in the columnpage using getByteArrayPage()
    byte[][] originPageByteArray = inputBytePage.getByteArrayPage();

    // 3. Get the data in the columnpage using getByteBufferArrayPage(false)
    ByteBuffer[] newPageByteBuffer = inputBytePage.getByteBufferArrayPage(false);
    assertEquals(originPageByteArray.length, newPageByteBuffer.length);
    byte[][] newPageByteArray = new byte[newPageByteBuffer.length][];

    // 4. Make the return value of getByteArrayPage() and getByteBufferArrayPage(false) is same
    for (int i = 0 ; i < originPageByteArray.length; i++) {
      assertEquals(originPageByteArray[i].length, newPageByteBuffer[i].remaining());
      newPageByteArray[i] = ByteUtil.byteBufferToBytes(newPageByteBuffer[i]);
      assertEquals(0, ByteUtil.compare(originPageByteArray[i], newPageByteArray[i]));
    }

    // 5. Make the return value of getByteArrayPage() and getByteBufferArrayPage(true) is same
    newPageByteBuffer = inputBytePage.getByteBufferArrayPage(true);
    assertEquals(0, ByteUtil.compare(ByteUtil.flatten(originPageByteArray),
        ByteUtil.byteBufferToBytes(newPageByteBuffer[0])));
  }

  @Test
  public void testGetPage() throws Exception {
    testGetPage(stringPage, UnsafeVarLengthColumnPage.class);
    testGetPage(varcharPage, UnsafeVarLengthColumnPage.class);
  }

  @AfterClass public static void tearDown() {
    CarbonProperties.getInstance().removeProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE);
  }
}
