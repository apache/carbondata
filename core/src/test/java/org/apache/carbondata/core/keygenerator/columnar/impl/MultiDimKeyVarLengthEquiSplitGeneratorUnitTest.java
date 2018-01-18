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

package org.apache.carbondata.core.keygenerator.columnar.impl;

import org.junit.*;

import static org.hamcrest.MatcherAssert.assertThat;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;

import java.util.Arrays;

import org.apache.carbondata.core.keygenerator.KeyGenException;

public class MultiDimKeyVarLengthEquiSplitGeneratorUnitTest {

  static MultiDimKeyVarLengthEquiSplitGenerator multiDimKeyVarLengthEquiSplitGenerator;

  @BeforeClass public static void setup() {
    int[] lens = new int[] { 32, 8, 16, 16, 16 };
    byte dimensionsToSplit = 1;
    multiDimKeyVarLengthEquiSplitGenerator =
        new MultiDimKeyVarLengthEquiSplitGenerator(lens, dimensionsToSplit);
  }

  @Test public void testSplitKeyWithNewDimensionToSplit() {
    int[] lens = new int[] { 24, 8, 16, 16, 16 };
    byte dimensionsToSplit = 3;
    MultiDimKeyVarLengthEquiSplitGenerator multiDimKeyVarLengthEquiSplitGeneratorNew =
        new MultiDimKeyVarLengthEquiSplitGenerator(lens, dimensionsToSplit);
    byte[][] result_value = new byte[][] { { 16, 8, 24, 46, 76, 64 }, { 80, 36, 72, 48 } };
    byte[] key = new byte[] { 16, 8, 24, 46, 76, 64, 80, 36, 72, 48 };
    byte[][] result = multiDimKeyVarLengthEquiSplitGeneratorNew.splitKey(key);
    Assert.assertTrue(Arrays.deepEquals(result, result_value));
  }

  @Test public void testSplitKeyWithNewDimensionToSplitValue16() {
    int[] lens = new int[] { 24, 8, 16, 16, 16 };
    byte dimensionsToSplit = 16;
    MultiDimKeyVarLengthEquiSplitGenerator multiDimKeyVarLengthEquiSplitGeneratorNew =
        new MultiDimKeyVarLengthEquiSplitGenerator(lens, dimensionsToSplit);
    byte[][] result_value = new byte[][] { { 16, 8, 24, 46, 76, 64, 80, 36, 72, 48 } };
    byte[] key = new byte[] { 16, 8, 24, 46, 76, 64, 80, 36, 72, 48 };
    byte[][] result = multiDimKeyVarLengthEquiSplitGeneratorNew.splitKey(key);
    Assert.assertTrue(Arrays.deepEquals(result, result_value));
  }

  @Test public void testGenerateAndSplitKeyAndGetKeyArrayWithActualLogic() throws KeyGenException {
    long[] keys = new long[] { 12253L, 48254L, 451245L, 52245L, 36458L, 48123L, 264L, 5852L, 42L };
    long[] expected_result = { 12253, 126, 58029, 52245, 36458 };
    byte[][] result_GenerateAndSplitKey =
        multiDimKeyVarLengthEquiSplitGenerator.generateAndSplitKey(keys);
    long[] result_GetKeyArray =
        multiDimKeyVarLengthEquiSplitGenerator.getKeyArray(result_GenerateAndSplitKey);
    assertThat(result_GetKeyArray, is(equalTo(expected_result)));
  }

  @Test public void testSplitKey() throws Exception {
    byte[][] result_value =
        new byte[][] { { 1, 102, 20, 56 }, { 64 }, { 36, 18 }, { 16, 28 }, { 98, 93 } };
    byte[] key = new byte[] { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 };
    byte[][] result = multiDimKeyVarLengthEquiSplitGenerator.splitKey(key);
    Assert.assertTrue(Arrays.deepEquals(result, result_value));
  }

  @Test public void testGetKeyArray() throws Exception {
    long[] result_value = new long[] { 23467064L, 64L, 9234L, 4124L, 25181L };
    byte[][] key = new byte[][] { { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 } };
    long[] result = multiDimKeyVarLengthEquiSplitGenerator.getKeyArray(key);
    assertThat(result, is(equalTo(result_value)));
  }

  @Test public void testKeyByteArray() throws Exception {
    byte[] result_value = new byte[] { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 };
    byte[][] key = new byte[][] { { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 } };
    byte[] result = multiDimKeyVarLengthEquiSplitGenerator.getKeyByteArray(key);
    Assert.assertTrue(Arrays.equals(result, result_value));
  }

  /*
   * In this test scenario We will send  blockIndexes { 0 }.
   * Where value of blockKeySize is {4,1,2,2,2}
   * It will add value 0f {0} indexes and will return the size which is 4.
   *
   * @throws Exception
   */

  @Test public void testGetKeySizeByBlockWithBlockIndexesLengthIsZero() throws Exception {
    int result_value = 4;
    int[] blockIndexes = new int[] { 0 };
    int result = multiDimKeyVarLengthEquiSplitGenerator.getKeySizeByBlock(blockIndexes);
    assertEquals(result_value, result);
  }

  /*
   * In this test scenario We will send  blockIndexes { 0, 1, 2 }.
   * Where value of blockKeySize is {4,1,2,2,2}
   * It will add value 0f {0, 1, 2} indexes and will return the size which is 7.
   *
   * @throws Exception
   */

  @Test public void testGetKeySizeByBlockWithBlockIndexesLengthIsGreaterThanZero()
      throws Exception {
    int result_value = 7;
    int[] blockIndexes = new int[] { 0, 1, 2 };
    int result = multiDimKeyVarLengthEquiSplitGenerator.getKeySizeByBlock(blockIndexes);
    assertEquals(result_value, result);
  }

  /*
   * In this test scenario We will send  blockIndexes {1, 2, 7} where {7} > blockKeySize.length which is 5.
   * Where value of blockKeySize is {4,1,2,2,2}
   * It will add value 0f {1, 2, 7} indexes and will return the size which is 3.
   *
   * @throws Exception
   */

  @Test public void testGetKeySizeByBlockWithBlockIndexesLengthIsBlockKeySizeLength()
      throws Exception {
    int result_value = 3;
    int[] blockIndexes = new int[] { 1, 2, 7 };
    int result = multiDimKeyVarLengthEquiSplitGenerator.getKeySizeByBlock(blockIndexes);
    assertEquals(result_value, result);
  }

  /*
   * In this test scenario We will send  blockIndexes {10} where {10} > blockKeySize.length which is 5.
   * Where value of blockKeySize is {4,1,2,2,2}
   * It will return default value 0.
   *
   * @throws Exception
   */

  @Test public void testGetKeySizeByBlockWith() throws Exception {
    int result_value = 0;
    int[] key = new int[] { 10 };
    int result = multiDimKeyVarLengthEquiSplitGenerator.getKeySizeByBlock(key);
    assertEquals(result_value, result);
  }

  @Test public void testEqualsWithAnyObject() throws Exception {
    Object obj = new Object();
    boolean result = multiDimKeyVarLengthEquiSplitGenerator.equals(obj);
    Assert.assertTrue(!result);
  }

  @Test public void testEqualsWithDifferentValueMultiDimKeyVarLengthEquiSplitGeneratorObject()
      throws Exception {
    int[] lens = new int[] { 32, 8, 16, 16, 16 };
    byte dimensionsToSplit = 2;
    boolean result = multiDimKeyVarLengthEquiSplitGenerator
        .equals(new MultiDimKeyVarLengthEquiSplitGenerator(lens, dimensionsToSplit));
    Assert.assertTrue(!result);
  }

  @Test public void testEqualsWithSameValueMultiDimKeyVarLengthEquiSplitGeneratorObject()
      throws Exception {
    int[] lens = new int[] { 32, 8, 16, 16, 16 };
    byte dimensionsToSplit = 1;
    boolean result = multiDimKeyVarLengthEquiSplitGenerator
        .equals(new MultiDimKeyVarLengthEquiSplitGenerator(lens, dimensionsToSplit));
    Assert.assertTrue(result);
  }

  /**
   * Test case for exception when Key size is less than byte key size
   */

  @Test(expected = ArrayIndexOutOfBoundsException.class) public void testSplitKeyWithException() {
    int[] lens = new int[] { 24, 8, 16, 16, 16 };
    byte dimensionsToSplit = 3;
    MultiDimKeyVarLengthEquiSplitGenerator multiDimKeyVarLengthEquiSplitGeneratorNew =
        new MultiDimKeyVarLengthEquiSplitGenerator(lens, dimensionsToSplit);
    byte[][] result_value = new byte[][] { { 16, 8, 24, 46, 76, 64 }, { 80, 36, 72, 48 } };
    byte[] key = new byte[] { 16, 8, 24, 46, 76 };
    byte[][] result = multiDimKeyVarLengthEquiSplitGeneratorNew.splitKey(key);
    Assert.assertTrue(Arrays.deepEquals(result, result_value));
  }

}
