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

/**
 * Code ported from Apache Spark {org.apache.spark.unsafe.memory} package
 * A simple {@link MemoryAllocator} that uses {@code Unsafe} to allocate off-heap memory.
 */
public class UnsafeMemoryAllocator implements MemoryAllocator {

  @Override
  public MemoryBlock allocate(long size) throws OutOfMemoryError {
    long address = CarbonUnsafe.getUnsafe().allocateMemory(size);
    // initializing memory with zero
    CarbonUnsafe.getUnsafe().setMemory(null, address, size, (byte) 0);
    return new MemoryBlock(null, address, size, MemoryType.OFFHEAP);
  }

  @Override
  public void free(MemoryBlock memory) {
    assert (memory.obj == null) :
      "baseObject not null; are you trying to use the off-heap allocator to free on-heap memory?";
    CarbonUnsafe.getUnsafe().freeMemory(memory.offset);
    memory.setFreedStatus(true);
  }
}
