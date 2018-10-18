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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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

  @Test public void testSplitKey() throws Exception {
    byte[][] result_value =
        new byte[][] { { 1, 102, 20, 56 }, { 64 }, { 36, 18 }, { 16, 28 }, { 98, 93 } };
    byte[] key = new byte[] { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 };
    byte[][] result = multiDimKeyVarLengthEquiSplitGenerator.splitKey(key);
    Assert.assertTrue(Arrays.deepEquals(result, result_value));
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
