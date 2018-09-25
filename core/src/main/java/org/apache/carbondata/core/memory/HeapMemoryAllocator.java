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

import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import javax.annotation.concurrent.GuardedBy;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Code ported from Apache Spark {org.apache.spark.unsafe.memory} package
 * A simple {@link MemoryAllocator} that can allocate up to 16GB using a JVM long primitive array.
 */
public class HeapMemoryAllocator implements MemoryAllocator {

  @GuardedBy("this") private final Map<Long, LinkedList<WeakReference<long[]>>>
      bufferPoolsBySize = new HashMap<>();

  private int poolingThresholdBytes;
  private boolean shouldPooling = true;

  public HeapMemoryAllocator() {
    poolingThresholdBytes = CarbonProperties.getInstance().getHeapMemoryPoolingThresholdBytes();
    boolean isDriver = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "false"));
    // if set 'poolingThresholdBytes' to -1 or the object creation call is in driver,
    // it should not go through the pooling mechanism.
    if (poolingThresholdBytes == -1 || isDriver) {
      shouldPooling = false;
    }
  }

  /**
   * Returns true if allocations of the given size should go through the pooling mechanism and
   * false otherwise.
   */
  private boolean shouldPool(long size) {
    // Very small allocations are less likely to benefit from pooling.
    return shouldPooling && (size >= poolingThresholdBytes);
  }

  @Override public MemoryBlock allocate(long size) throws OutOfMemoryError {
    int numWords = (int) ((size + 7) / 8);
    long alignedSize = numWords * 8L;
    assert (alignedSize >= size);
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        final LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool != null) {
          while (!pool.isEmpty()) {
            final WeakReference<long[]> arrayReference = pool.pop();
            final long[] array = arrayReference.get();
            if (array != null) {
              assert (array.length * 8L >= size);
              MemoryBlock memory = new MemoryBlock(array, CarbonUnsafe.LONG_ARRAY_OFFSET, size);
              // reuse this MemoryBlock
              memory.setFreedStatus(false);
              return memory;
            }
          }
          bufferPoolsBySize.remove(alignedSize);
        }
      }
    }
    long[] array = new long[numWords];
    return new MemoryBlock(array, CarbonUnsafe.LONG_ARRAY_OFFSET, size);
  }

  @Override public void free(MemoryBlock memory) {
    final long size = memory.size();

    // As an additional layer of defense against use-after-free bugs, we mutate the
    // MemoryBlock to null out its reference to the long[] array.
    long[] array = (long[]) memory.obj;

    long alignedSize = ((size + 7) / 8) * 8;
    if (shouldPool(alignedSize)) {
      synchronized (this) {
        LinkedList<WeakReference<long[]>> pool = bufferPoolsBySize.get(alignedSize);
        if (pool == null) {
          pool = new LinkedList<>();
          bufferPoolsBySize.put(alignedSize, pool);
        }
        pool.add(new WeakReference<>(array));
      }
    }
    memory.setFreedStatus(true);
  }
}
