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

import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)

public class UnsafeVariableLengthDimesionDataChunkStoreTest {
  static UnsafeVariableLengthDimesionDataChunkStore unsafeVariableLengthDimesionDataChunkStore;
  static UnsafeVariableLengthDimesionDataChunkStore unsafeVariableLengthDimesionDataChunkStore1;

  @BeforeClass public static void setup() {
    unsafeVariableLengthDimesionDataChunkStore =
        new UnsafeVariableLengthDimesionDataChunkStore(42, false, 10);
    unsafeVariableLengthDimesionDataChunkStore1 =
        new UnsafeVariableLengthDimesionDataChunkStore(42, false, 10);
  }

  @Test public void putArrayTest() {
    byte[] data = { 0, 1, 11, 0, 2, 23, 25 };
    unsafeVariableLengthDimesionDataChunkStore.putArray(null, null, data);
    byte[] result = unsafeVariableLengthDimesionDataChunkStore.getRow(1);

    int[] invertedIndex = { 4, 0, 5, 2, 3, 6, 7, 8, 1, 9 };
    int[] invertedIndexReverse = { 1, 8, 3, 4, 0, 2, 5, 6, 7, 9 };
    unsafeVariableLengthDimesionDataChunkStore1.putArray(invertedIndex, invertedIndexReverse, data);
    byte[] result1 = unsafeVariableLengthDimesionDataChunkStore1.getRow(1);

    byte[] expectedResult = { 23, 25 };

    int i = 0;
    for (byte res : expectedResult) {
      assert ((res == result[i]) && (res == result1[i]));
      i++;
    }
  }

  @Test public void testGetRow() {
    byte[] data = { 0, 1, 11, 0, 2, 23, 25 };
    unsafeVariableLengthDimesionDataChunkStore.putArray(null, null, data);
    byte result[] = unsafeVariableLengthDimesionDataChunkStore.getRow(0);

    int[] invertedIndex = { 4, 0, 5, 2, 3, 6, 7, 8, 1, 9 };
    int[] invertedIndexReverse = { 1, 8, 3, 4, 0, 2, 5, 6, 7, 9 };
    unsafeVariableLengthDimesionDataChunkStore1.putArray(invertedIndex, invertedIndexReverse, data);
    byte result1[] = unsafeVariableLengthDimesionDataChunkStore1.getRow(0);

    byte[] expectedResult = { 11 };
    int i = 0;
    for (byte res : expectedResult) {
      assert ((res == result[i]) && (res == result1[i]));
      i++;
    }
  }

}
