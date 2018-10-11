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

package org.apache.carbondata.core.datastore.chunk.impl;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;


public class FixedLengthDimensionDataChunkTest {

  static FixedLengthDimensionColumnPage fixedLengthDimensionDataChunk;
  static byte[] data;

  @BeforeClass public static void setup() {
    data = "dummy string".getBytes();

    int invertedIndex[] = { 1, 3, 5, 7, 8 };

    int invertedIndexReverse[] = { 1, 0, 5, 7, 8 };
    fixedLengthDimensionDataChunk =
        new FixedLengthDimensionColumnPage(data, invertedIndex, invertedIndexReverse, 5, 4, null);
  }

  @Test public void fillChunkDataTest() {
    int[] maskByteRanges = { 1, 2, 4, 6, 5 };
    int res = fixedLengthDimensionDataChunk.fillRawData(0, 0, data);
    int expectedResult = 4 ;
    assertEquals(res, expectedResult);
  }

  @Test public void getChunkDataTest() {
    byte expected[] = { 121, 32, 115, 116 };
    byte res[] = fixedLengthDimensionDataChunk.getChunkData(0);
    Assert.assertTrue(Arrays.equals(res, expected));
  }

  @Test public void fillConvertedChunkDataTest() {
    int[] row = { 1, 2, 4, 6 };
    int res = fixedLengthDimensionDataChunk.fillSurrogateKey(1, 0, row);
    int expectedResult = 1;
    assertEquals(res, expectedResult);
  }
}


