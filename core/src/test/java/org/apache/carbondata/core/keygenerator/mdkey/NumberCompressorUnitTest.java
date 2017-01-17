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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import org.junit.Test;

public class NumberCompressorUnitTest {

  private NumberCompressor numberCompressor;

  @Test public void testCompress() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    byte[] expected_result = new byte[] { 2, 86, 115 };
    int[] keys = new int[] { 2, 5, 6, 7, 3 };
    byte[] result = numberCompressor.compress(keys);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testGetWithIntKeysAndSameIndexes() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    long[] expected_result = new long[] { 153203, 0 };
    int[] keys = new int[] { 2, 5, 6, 7, 3 };
    int size = 2;
    long[] result = numberCompressor.get(keys, size);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testGetWithIntKeysAndDifferentIndexes() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    long[] expected_result = new long[] { 2695178248884938548L, 0 };
    int[] keys = new int[] { 2, 5, 6, 7, 3, 2, 5, 6, 7, 3, 2, 5, 6, 7, 3, 4 };
    int size = 2;
    long[] result = numberCompressor.get(keys, size);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testGetWithIntKeysAndDifferentIndexesAndConsideredBitsLessThanBitsLength()
      throws Exception {
    int cardinality = 1000;
    numberCompressor = new NumberCompressor(cardinality);
    long[] expected_result = new long[] { 2311479113337014277L, 0 };
    int[] keys = new int[] { 2, 5, 6, 7, 3, 2, 5 };
    int size = 2;
    long[] result = numberCompressor.get(keys, size);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testUnCompress() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    int[] expected_result = new int[] { 6, 4, 0, 2 };
    byte[] keys = new byte[] { 100, 2 };
    int[] result = numberCompressor.unCompress(keys, 0, keys.length);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testUnCompressWithTenKeys() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    int[] expected_result =
        new int[] { 0, 10, 0, 2, 0, 10, 0, 2, 0, 10, 0, 2, 0, 10, 0, 2, 0, 10, 0, 2 };
    byte[] keys = new byte[] { 10, 2, 10, 2, 10, 2, 10, 2, 10, 2 };
    int[] result = numberCompressor.unCompress(keys, 0, keys.length);
    System.out.println(result);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testUnCompressWithPOSGreaterThanZero() throws Exception {
    int cardinality = 100;
    numberCompressor = new NumberCompressor(cardinality);
    int[] expected_result = new int[] { 16, 4, 10, 1, 2, 64, 32, 80, 8, 20, 11 };
    byte[] keys = new byte[] { 100, 2, 10, 2, 10, 2, 10, 2, 10, 11 };
    int[] result = numberCompressor.unCompress(keys, 0, keys.length);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testCompressWithWordsSizeFromBytesSize() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    byte[] expected_result = new byte[] { 2, 86, 115, 37, 103, 50, 86, 115, 86 };
    int[] keys = new int[] { 2, 5, 6, 7, 3, 2, 5, 6, 7, 3, 2, 5, 6, 7, 3, 5, 6 };
    byte[] result = numberCompressor.compress(keys);
    assertThat(result, is(equalTo(expected_result)));
  }

  @Test public void testCompressWithIntMaxValue() throws Exception {
    int cardinality = 10;
    numberCompressor = new NumberCompressor(cardinality);
    byte[] expected_result = new byte[] { -35, -52, -52 };
    int[] keys = new int[] { 214748364, 5456, 214748364, 214748364, 214748364 };
    byte[] result = numberCompressor.compress(keys);
    assertThat(result, is(equalTo(expected_result)));
  }
}
