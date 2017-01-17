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

import org.apache.carbondata.core.keygenerator.KeyGenException;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;

public class MultiDimKeyVarLengthVariableSplitGeneratorUnitTest {

  private static MultiDimKeyVarLengthVariableSplitGenerator
      multiDimKeyVarLengthVariableSplitGenerator;

  @BeforeClass public static void setup() {
    int[] lens = new int[] { 32, 8, 16, 16, 16 };
    int[] dimSplit = new int[] { 1, 1, 1, 1, 1 };
    multiDimKeyVarLengthVariableSplitGenerator =
        new MultiDimKeyVarLengthVariableSplitGenerator(lens, dimSplit);
  }

  @Test public void testWithDifferentValueInDimSplit() throws Exception {

    int[] lens = new int[] { 32, 8, 32, 32, 16 };
    int[] dimSplit = new int[] { 12, 8, 1, 8, 16 };
    MultiDimKeyVarLengthVariableSplitGenerator multiDimKeyVarLengthVariableSplitGeneratorNew =
        new MultiDimKeyVarLengthVariableSplitGenerator(lens, dimSplit);

    byte[][] result_value =
        new byte[][] { { 24, 56, 72, 48, 56, 36, 18, 24, 40, 24, 64, 24, 56, 72, 48 } };
    byte[] key = new byte[] { 24, 56, 72, 48, 56, 36, 18, 24, 40, 24, 64, 24, 56, 72, 48 };
    byte[][] result = multiDimKeyVarLengthVariableSplitGeneratorNew.splitKey(key);
    assertThat(result, is(equalTo(result_value)));
  }

  @Test public void testGenerateAndSplitKeyAndGetKeyArrayWithActualLogic() throws KeyGenException {
    long[] keys = new long[] { 12253L, 48254L, 451245L, 52245L, 36458L, 48123L, 264L, 5852L, 42L };
    long[] expected_result = { 12253, 126, 58029, 52245, 36458 };
    byte[][] result_GenerateAndSplitKey =
        multiDimKeyVarLengthVariableSplitGenerator.generateAndSplitKey(keys);
    long[] result_GetKeyArray =
        multiDimKeyVarLengthVariableSplitGenerator.getKeyArray(result_GenerateAndSplitKey);
    assertThat(result_GetKeyArray, is(equalTo(expected_result)));
  }

  @Test public void testGenerateAndSplitKeyAndGetKeyArrayWithActualLogicWithInt()
      throws KeyGenException {
    int[] keys = new int[] { 122, 254, 4512, 52, 36, 481, 264, 58, 42 };
    long[] expected_result = { 122L, 254L, 4512L, 52L, 36L };
    byte[][] result_GenerateAndSplitKey =
        multiDimKeyVarLengthVariableSplitGenerator.generateAndSplitKey(keys);
    long[] result_GetKeyArray =
        multiDimKeyVarLengthVariableSplitGenerator.getKeyArray(result_GenerateAndSplitKey);
    assertThat(result_GetKeyArray, is(equalTo(expected_result)));
  }

  @Test public void testGenerateAndSplitKeyAndGetKeyByteArrayWithActualLogicWithInt()
      throws KeyGenException {
    int[] keys = new int[] { 1220, 2554, 452, 520, 360, 48, 24, 56, 42 };
    byte[] expected_result = new byte[] { 0, 0, 4, -60, -6, 1, -60, 2, 8, 1, 104 };
    byte[][] result_GenerateAndSplitKey =
        multiDimKeyVarLengthVariableSplitGenerator.generateAndSplitKey(keys);
    byte[] result_GetKeyByteArray =
        multiDimKeyVarLengthVariableSplitGenerator.getKeyByteArray(result_GenerateAndSplitKey);
    assertThat(result_GetKeyByteArray, is(equalTo(expected_result)));
  }

  @Test public void testSplitKey() throws Exception {
    byte[][] result_value =
        new byte[][] { { 1, 102, 20, 56 }, { 64 }, { 36, 18 }, { 16, 28 }, { 98, 93 } };
    byte[] key = new byte[] { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 };
    byte[][] result = multiDimKeyVarLengthVariableSplitGenerator.splitKey(key);
    assertThat(result, is(equalTo(result_value)));
  }

  @Test public void testGetKeyArray() throws Exception {
    long[] result_value = new long[] { 23467064, 64, 9234, 4124, 25181 };
    byte[][] key = new byte[][] { { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 } };
    long[] result = multiDimKeyVarLengthVariableSplitGenerator.getKeyArray(key);
    assertThat(result, is(equalTo(result_value)));
  }

  @Test public void testKeyByteArray() throws Exception {
    byte[] result_value = new byte[] { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 };
    byte[][] key = new byte[][] { { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 } };
    byte[] result = multiDimKeyVarLengthVariableSplitGenerator.getKeyByteArray(key);
    assertThat(result, is(equalTo(result_value)));
  }

  @Test public void testGetKeySizeByBlockWithBlockIndexesInRange() throws Exception {
    int result_value = 3;
    int[] blockIndexes = new int[] { 1, 4 };
    int result = multiDimKeyVarLengthVariableSplitGenerator.getKeySizeByBlock(blockIndexes);
    assertEquals(result_value, result);
  }

  @Test public void testGetKeySizeByBlockWithBlockIndexes() throws Exception {
    int result_value = 9;
    int[] blockIndexes = new int[] { 1, 4, 2, 0 };
    int result = multiDimKeyVarLengthVariableSplitGenerator.getKeySizeByBlock(blockIndexes);
    assertEquals(result_value, result);
  }

  @Test public void equalsWithError() throws Exception {
    Object obj = new Object();
    boolean result = multiDimKeyVarLengthVariableSplitGenerator.equals(obj);
    assertEquals(false, result);
  }

  @Test public void equalsWithTrue() throws Exception {
    boolean result = multiDimKeyVarLengthVariableSplitGenerator
        .equals(multiDimKeyVarLengthVariableSplitGenerator);
    assertEquals(true, result);
  }

  /**
   * Test case for exception when Key size is less than byte key size
   */

  @Test(expected = ArrayIndexOutOfBoundsException.class) public void testSplitKeyWithException() {

    int[] lens = new int[] { 32, 8, 32, 32, 16 };
    int[] dimSplit = new int[] { 12, 8, 1, 8, 16 };
    MultiDimKeyVarLengthVariableSplitGenerator multiDimKeyVarLengthVariableSplitGeneratorNew =
        new MultiDimKeyVarLengthVariableSplitGenerator(lens, dimSplit);

    byte[] key = new byte[] { 24, 56, 72, 48, 56, 36, 18 };
    multiDimKeyVarLengthVariableSplitGeneratorNew.splitKey(key);
  }
}
