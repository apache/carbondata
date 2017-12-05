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

@FixMethodOrder(MethodSorters.NAME_ASCENDING) public class UnsafeByteMeasureChunkStoreTest {
  static UnsafeByteMeasureChunkStore unsafeByteMeasureChunkStore;

  @BeforeClass public static void setup() {
    unsafeByteMeasureChunkStore = new UnsafeByteMeasureChunkStore(7);
  }

  @Test public void putDataTest() {
    byte data[] = new byte[100];
    for (byte i = 0; i < 100; i++) {
      data[i] = i;
    }
    unsafeByteMeasureChunkStore.putData(data);
    boolean res = unsafeByteMeasureChunkStore.isMemoryOccupied;
    assert (res);
  }

  @Test public void testGetByte() {
    byte res = unsafeByteMeasureChunkStore.getByte(3);
    assert (res == 3);
  }
}
