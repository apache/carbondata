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

public class UnsafeLongMeasureChunkStoreTest {

  static UnsafeLongMeasureChunkStore unsafeLongMeasureChunkStore;

  @BeforeClass public static void setup() {
    unsafeLongMeasureChunkStore = new UnsafeLongMeasureChunkStore(10);
  }

  @Test public void putDataTest() {
    long data[] = new long[10];
    for (int i = 0; i < 10; i++) {
      data[i] = i;
    }
    unsafeLongMeasureChunkStore.putData(data);
    assert (unsafeLongMeasureChunkStore.isMemoryOccupied);
  }

  @Test public void testGetLong() {
    long res = unsafeLongMeasureChunkStore.getLong(2);
    assert (res == 2);
  }
}
