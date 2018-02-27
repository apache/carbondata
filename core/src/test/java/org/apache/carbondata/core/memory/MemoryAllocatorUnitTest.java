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

package org.apache.carbondata.core.memory;

import org.junit.Assert;
import org.junit.Test;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

public class MemoryAllocatorUnitTest {

  @Test
  public void testHeapMemoryReuse() {
    MemoryAllocator heapMem = new HeapMemoryAllocator();
    // The size is less than 1024 * 1024,
    // allocate new memory every time.
    MemoryBlock onheap1 = heapMem.allocate(513);
    Object obj1 = onheap1.getBaseObject();
    heapMem.free(onheap1);
    MemoryBlock onheap2 = heapMem.allocate(514);
    Assert.assertNotEquals(obj1, onheap2.getBaseObject());

    // The size is greater than 1024 * 1024,
    // reuse the previous memory which has released.
    MemoryBlock onheap3 = heapMem.allocate(1024 * 1024 + 1);
    Assert.assertEquals(onheap3.size(), 1024 * 1024 + 1);
    Object obj3 = onheap3.getBaseObject();
    heapMem.free(onheap3);
    MemoryBlock onheap4 = heapMem.allocate(1024 * 1024 + 7);
    Assert.assertEquals(onheap4.size(), 1024 * 1024 + 7);
    Assert.assertEquals(obj3, onheap4.getBaseObject());
  }

  @Test
  public void testHeapMemoryNotPool() {
    // not pool
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_HEAP_MEMORY_POOLING_THRESHOLD_BYTES, "-1");

    MemoryAllocator heapMem = new HeapMemoryAllocator();
    MemoryBlock onheap1 = heapMem.allocate(513);
    Object obj1 = onheap1.getBaseObject();
    heapMem.free(onheap1);
    MemoryBlock onheap2 = heapMem.allocate(514);
    Assert.assertNotEquals(obj1, onheap2.getBaseObject());

    MemoryBlock onheap3 = heapMem.allocate(1024 * 1024 + 1);
    Assert.assertEquals(onheap3.size(), 1024 * 1024 + 1);
    Object obj3 = onheap3.getBaseObject();
    heapMem.free(onheap3);
    MemoryBlock onheap4 = heapMem.allocate(1024 * 1024 + 7);
    Assert.assertEquals(onheap4.size(), 1024 * 1024 + 7);
    Assert.assertNotEquals(obj3, onheap4.getBaseObject());
  }
}
