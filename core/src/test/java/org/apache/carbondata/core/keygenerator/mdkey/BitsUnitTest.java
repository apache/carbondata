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

import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

public class BitsUnitTest {
  static Bits bits;

  @BeforeClass public static void setup() {
    int[] lens = new int[] { 32, 8, 24, 64, 64 };
    bits = new Bits(lens);
  }

  @Test public void testGetWithIntKeys() throws Exception {
    long[] expected = new long[] { 0L, 0L, 86570434576L};
    int[] keys = new int[] { 20, 40, 16, 24, 80 };
    long[] result = bits.get(keys);
    assertThat(result, is(equalTo(expected)));
  }

  @Test public void testGetKeyByteOffsets() throws Exception {
    int[] lens = new int[] { 64, 64, 64, 64, 64 };
    Bits bits1 = new Bits(lens);
    int index = 2;
    int[] expected = new int[] { 16, 23 };
    int[] result = bits1.getKeyByteOffsets(index);
    assertThat(result, is(equalTo(expected)));
  }

  @Test public void testEqualsWithBitsObject() throws Exception {
    boolean result = bits.equals(bits);
    assertEquals(true, result);
  }

  @Test public void testEqualsWithOtherObject() throws Exception {
    boolean result = bits.equals(new Object());
    assertEquals(false, result);
  }
}
