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

import org.junit.BeforeClass;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
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

  @Test public void testSplitKey() throws Exception {
    byte[][] result_value =
        new byte[][] { { 1, 102, 20, 56 }, { 64 }, { 36, 18 }, { 16, 28 }, { 98, 93 } };
    byte[] key = new byte[] { 1, 102, 20, 56, 64, 36, 18, 16, 28, 98, 93 };
    byte[][] result = multiDimKeyVarLengthVariableSplitGenerator.splitKey(key);
    assertThat(result, is(equalTo(result_value)));
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
