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

@FixMethodOrder(MethodSorters.NAME_ASCENDING) public class UnsafeShortMeasureChunkStoreTest {
  static UnsafeShortMeasureChunkStore unsafeShortMeasureChunkStore;

  @BeforeClass public static void setup() {
    unsafeShortMeasureChunkStore = new UnsafeShortMeasureChunkStore(10);
  }

  @Test public void putDataTest() {
    short data[] = new short[10];
    for (short i = 0; i < 10; i++) {
      data[i] = i;
    }
    unsafeShortMeasureChunkStore.putData(data);
    assert (unsafeShortMeasureChunkStore.isMemoryOccupied);
  }

  @Test public void testGetShort() {
    short res = unsafeShortMeasureChunkStore.getShort(2);
    assert (res == 2);
  }
}
