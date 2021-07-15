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

package org.apache.carbondata.processing.util;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.pentaho.di.core.util.Assert.assertNull;

public class RemoveDictionaryUtilTest {
  private static RemoveDictionaryUtil removeDictionaryUtil;
  private static ByteBuffer byteBuffer;
  private static ByteBuffer byteBufferArray[];
  private static byte byteBufferArrayTwoDimension[][];

  @BeforeClass public static void setUp() {
    removeDictionaryUtil = new RemoveDictionaryUtil();
    java.lang.Integer[] integerArray = new java.lang.Integer[] { 1, 23, 3 };
    Object[] row = new Object[] { integerArray };
    RemoveDictionaryUtil.setDimension(1, 1, row);
  }

  @Test public void testPackByteBufferIntoSingleByteArray() {
    byte[] expected = new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byteBuffer = ByteBuffer.allocate(10);
    byteBufferArray = new ByteBuffer[] { byteBuffer };
    byte[] actual = RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArray);
    assertThat(actual, is(equalTo(expected)));
    byteBuffer = ByteBuffer.allocate(2);
    byteBufferArray = new ByteBuffer[] { byteBuffer, byteBuffer };
    actual = RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArray);
    expected = new byte[] { 0, 8, 0, 6, 0, 8, 0, 0, 0, 0 };

    assertThat(actual, is(equalTo(expected)));

  }

  @Test public void testPackByteBufferIntoSingleByteArrayWithEmptyArray() {
    byteBufferArray = new ByteBuffer[] {};
    byte[] actual = RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArray);
    assertNull(actual);
  }

  @Test public void testPackByteBufferIntoSingleByteArrayWithTwoDimension() {
    byte[] byteArray = new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byte[] expected = new byte[] { 0, 16, 0, 4, 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byteBufferArrayTwoDimension = new byte[][] { byteArray };
    byte[] actual =
        RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArrayTwoDimension);
    assertThat(actual, is(equalTo(expected)));
    byteBufferArrayTwoDimension =
        new byte[][] { byteArray, new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 } };
    actual = RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArrayTwoDimension);
    expected =
        new byte[] { 0, 32, 0, 6, 0, 20, 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 0, 4, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0 };
    assertThat(actual, is(equalTo(expected)));

  }

  @Test public void testPackByteBufferIntoSingleByteArrayWithTwoDimensionForNull() {

    byte[] expected = new byte[] { 0, 16, 0, 4, 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byteBuffer = ByteBuffer.allocate(10);
    byteBufferArray = new ByteBuffer[] { byteBuffer };
    byteBufferArrayTwoDimension = new byte[][] {};
    byte[] actual =
        RemoveDictionaryUtil.packByteBufferIntoSingleByteArray(byteBufferArrayTwoDimension);
    assertNull(actual);

  }

  @Test public void testExtractNoDictionaryCount() {
    int expected = 1;
    int actual = RemoveDictionaryUtil.extractNoDictionaryCount("1");
    assertThat(actual, is(equalTo(expected)));

  }

  @Test public void testExtractDimColsDataTypeValues() {
    Map<String, String> expected = new HashMap<>();
    expected.put("dee", "pak");
    expected.put("anu", "bhav");
    Map<String, String> actual = RemoveDictionaryUtil
        .extractDimColsDataTypeValues("Anu,#!:COMA:!#,bhav&#!@:AMPER:@!#&Dee,#!:COMA:!#,pak");
    assertThat(actual, is(equalTo(expected)));

  }

  @Test public void testConvertBooleanArrToString() {
    Boolean[] booleenArray = new Boolean[] { true, true, false, true };
    String expected = "true,#!:COMA:!#,true,#!:COMA:!#,false,#!:COMA:!#,true";
    String actual = RemoveDictionaryUtil.convertBooleanArrToString(booleenArray);
    assertThat(actual, is(equalTo(expected)));
  }

  @Test public void testConvertStringToBooleanArr() {
    boolean expected[] = new boolean[] { false, false, false };
    boolean[] actual = RemoveDictionaryUtil
        .convertStringToBooleanArr("Anu,#!:COMA:!#,bhav&#!@:AMPER:@!#&Dee,#!:COMA:!#,pak");
    assertThat(actual, is(equalTo(expected)));
  }

  @Test public void convertListByteArrToSingleArr() {
    byte[] expected = new byte[] { 0, 16, 0, 4, 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byte[] noDictionaryValKeyList = new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byte[] actual =
        RemoveDictionaryUtil.convertListByteArrToSingleArr(Arrays.asList(noDictionaryValKeyList));
    assertThat(actual, is(equalTo(expected)));

  }

  @Test public void testSplitNoDictionaryKey() {
    byte[] noDictionaryArr = new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    String expectedResult = "[[0, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]]";
    byte[][] actual = RemoveDictionaryUtil.splitNoDictionaryKey(noDictionaryArr, 1);
    String actualResult = Arrays.deepToString(actual);
    assertThat(actualResult, is(equalTo(expectedResult)));

  }

  @Test public void testGetCompleteDimensions() {
    java.lang.Integer[] integerArray = new java.lang.Integer[] { 1, 23, 3 };
    Object[] row = new Object[] { integerArray };

    java.lang.Integer[] actualIntegerArray = new java.lang.Integer[] { 1, 23, 3 };
    RemoveDictionaryUtil.getCompleteDimensions(row);
    assertThat(actualIntegerArray, is(equalTo(integerArray)));

  }

  @Test public void testGetByteArrayForNoDictionaryCols() {
    byte[] byteArray = new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    byte[] expectedArray = new byte[] { 0, 12, 0, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 };
    Object[] row = new Object[] { byteArray, expectedArray };
    byte[] actualArray = RemoveDictionaryUtil.getByteArrayForNoDictionaryCols(row);
    assertThat(actualArray, is(equalTo(expectedArray)));

  }

  @Test public void testCheckAllValuesForNull() {
    java.lang.Integer[] integerArray = new java.lang.Integer[] { 1, 23, 3 };
    Object[] row = new Object[] { integerArray };
    boolean expected = false;
    boolean actual = RemoveDictionaryUtil.checkAllValuesForNull(row);
    assertThat(actual, is(equalTo(expected)));
  }

  @Test public void testExtractDimColsDataTypeValuesForNull() {
    Map<String, String> expected =
        new HashMap<String, String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    Map<String, String> actual = RemoveDictionaryUtil.extractDimColsDataTypeValues(null);
    assertThat(actual, is(equalTo(expected)));
  }

}