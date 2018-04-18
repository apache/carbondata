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

package org.apache.carbondata.core.keygenerator.mdkey;

import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class MultiDimKeyVarLengthGeneratorUnitTest {
  private MultiDimKeyVarLengthGenerator multiDimKeyVarLengthGenerator;

  @Before public void setup() {
    int[] lens = new int[] { 1, 2, 3 };
    multiDimKeyVarLengthGenerator = new MultiDimKeyVarLengthGenerator(lens);
  }

  @Test public void testEqualsWithSameInstance() throws Exception {
    int[] lens = new int[] { 1, 2, 3 };
    MultiDimKeyVarLengthGenerator multiDimKeyVarLengthGenerator1 =
        new MultiDimKeyVarLengthGenerator(lens);
    boolean result = multiDimKeyVarLengthGenerator.equals(multiDimKeyVarLengthGenerator1);
    assertEquals(true, result);
  }

  @Test public void testEqualsWithDifferenceInstance() throws Exception {
    boolean result = multiDimKeyVarLengthGenerator.equals(new Object());
    assertEquals(false, result);
  }

  @Test public void testCompareWithSameByteArray() throws Exception {
    byte[] keys = new byte[] { 10, 2, 10, 2, 10, 2, 10, 2, 10, 2 };
    int expected = 0;
    int result = multiDimKeyVarLengthGenerator.compare(keys, keys);
    assertThat(result, is(equalTo(expected)));
  }

  @Test public void testCompareWithByteArray1IsGreaterThanByteArray2() throws Exception {
    byte[] byteArray1 = new byte[] { 10, 2, 10 };
    byte[] byteArray2 = new byte[] { 10, 1 };
    int expected = 1;
    int result = multiDimKeyVarLengthGenerator.compare(byteArray1, byteArray2);
    assertThat(result, is(equalTo(expected)));
  }

  @Test public void testCompareWithByteArray1IsLessThanByteArray2() throws Exception {
    byte[] byteArray1 = new byte[] { 10, 2 };
    byte[] byteArray2 = new byte[] { 10, 1, 30 };
    int expected = 1;
    int result = multiDimKeyVarLengthGenerator.compare(byteArray1, byteArray2);
    assertThat(result, is(equalTo(expected)));
  }
}
