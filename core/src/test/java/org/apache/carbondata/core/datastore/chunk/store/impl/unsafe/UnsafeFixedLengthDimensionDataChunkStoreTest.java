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
package org.apache.carbondata.core.datastore.chunk.store.impl.unsafe;

import java.util.Arrays;

import org.junit.BeforeClass;
import org.junit.Test;

public class UnsafeFixedLengthDimensionDataChunkStoreTest {

  static UnsafeFixedLengthDimensionDataChunkStore unsafeFixedLengthDimensionDataChunkStore;
  static UnsafeFixedLengthDimensionDataChunkStore unsafeFixedLengthDimensionDataChunkStore2;

  @BeforeClass public static void setup() {
    unsafeFixedLengthDimensionDataChunkStore =
        new UnsafeFixedLengthDimensionDataChunkStore(100, 1, false, 100);
    byte data[] = new byte[100];
    for (byte i = 0; i < 100; i++) {
      data[i] = i;
    }
    unsafeFixedLengthDimensionDataChunkStore.putArray(null, null, data);
    int[] invertedIndex = { 4, 0, 5, 2, 3, 6, 7, 8, 1, 9 };
    int[] invertedIndexReverse = { 1, 8, 3, 4, 0, 2, 5, 6, 7, 9 };
    unsafeFixedLengthDimensionDataChunkStore2 =
        new UnsafeFixedLengthDimensionDataChunkStore(100, 1, false, 100);
    unsafeFixedLengthDimensionDataChunkStore2.putArray(invertedIndex, invertedIndexReverse, data);
  }

  @Test public void getRowTest() {
    byte[] res1 = unsafeFixedLengthDimensionDataChunkStore.getRow(1);
    byte[] res2 = unsafeFixedLengthDimensionDataChunkStore2.getRow(1);
    assert ((res1[0] == 1) && (res2[0] == 1));
  }

  @Test public void getSurrogateTest() {
    int res1 = unsafeFixedLengthDimensionDataChunkStore.getSurrogate(2);
    int res2 = unsafeFixedLengthDimensionDataChunkStore2.getSurrogate(2);
    assert ((res1 == 2) && (res2 == 2));
  }

  @Test public void fillRowTest() {
    byte buffer[] = new byte[1];
    unsafeFixedLengthDimensionDataChunkStore2.fillRow(2, buffer, 0);
    byte[] expectedBuffer = { 2 };
    assert (Arrays.equals(buffer, expectedBuffer));
  }

  @Test public void fillRowTest2() {
    byte buffer[] = new byte[1];
    unsafeFixedLengthDimensionDataChunkStore2.fillRow(2, buffer, 0);
    byte[] expectedBuffer = { 2 };
    assert (Arrays.equals(buffer, expectedBuffer));
  }

  @Test public void compareToTest() {
    byte[] compareValue = { 1 };
    int res1 = unsafeFixedLengthDimensionDataChunkStore.compareTo(1, compareValue);
    int res2 = unsafeFixedLengthDimensionDataChunkStore2.compareTo(1, compareValue);
    assert ((res1 == 0) && (res2 == 0));
  }
}
