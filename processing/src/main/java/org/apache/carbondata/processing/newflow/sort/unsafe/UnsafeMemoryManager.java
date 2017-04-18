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

package org.apache.carbondata.processing.newflow.sort.unsafe;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.MemoryAllocator;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Manages memory for instance.
 */
public class UnsafeMemoryManager {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeMemoryManager.class.getName());

  static {
    long size;
    try {
      size = Long.parseLong(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB,
              CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT));
    } catch (Exception e) {
      size = Long.parseLong(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT);
      LOGGER.info("Wrong memory size given, "
          + "so setting default value to " + size);
    }
    if (size < 1024) {
      size = 1024;
      LOGGER.info("It is not recommended to keep unsafe memory size less than 1024MB, "
          + "so setting default value to " + size);
    }


    boolean offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
            CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));
    long takenSize = size * 1024 * 1024;
    MemoryAllocator allocator;
    if (offHeap) {
      allocator = MemoryAllocator.UNSAFE;
    } else {
      long maxMemory = Runtime.getRuntime().maxMemory() * 60 / 100;
      if (takenSize > maxMemory) {
        takenSize = maxMemory;
      }
      allocator = MemoryAllocator.HEAP;
    }
    INSTANCE = new UnsafeMemoryManager(takenSize, allocator);
  }

  public static final UnsafeMemoryManager INSTANCE;

  private long totalMemory;

  private long memoryUsed;

  private MemoryAllocator allocator;

  private long minimumMemory;

  private UnsafeMemoryManager(long totalMemory, MemoryAllocator allocator) {
    this.totalMemory = totalMemory;
    this.allocator = allocator;
    long numberOfCores = CarbonProperties.getInstance().getNumberOfCores();
    long sortMemoryChunkSize = CarbonProperties.getInstance().getSortMemoryChunkSizeInMB();
    sortMemoryChunkSize = sortMemoryChunkSize * 1024 * 1024;
    long totalWorkingMemoryForAllThreads = sortMemoryChunkSize * numberOfCores;
    if (totalWorkingMemoryForAllThreads >= totalMemory) {
      throw new RuntimeException("Working memory should be less than total memory configured, "
          + "so either reduce the loading threads or increase the memory size. "
          + "(Number of threads * number of threads) should be less than total unsafe memory");
    }
    minimumMemory = totalWorkingMemoryForAllThreads;
    LOGGER.info("Memory manager is created with size " + totalMemory + " with " + allocator
        + " and minimum reserve memory " + minimumMemory);
  }
  public synchronized MemoryBlock allocateMemory(long memoryRequested) {
    if (memoryUsed + memoryRequested <= totalMemory) {
      MemoryBlock allocate = allocator.allocate(memoryRequested);
      memoryUsed += allocate.size();
      LOGGER.info("Memory block is created with size "  + allocate.size() +
          " Total memory used " + memoryUsed + " memory left " + (getAvailableMemory()));
      return allocate;
    }
    return null;
  }

  public synchronized void freeMemory(MemoryBlock memoryBlock) {
    allocator.free(memoryBlock);
    memoryUsed -= memoryBlock.size();
    memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    LOGGER.info(
        "Memory released, memory used " + memoryUsed + " memory left " + (getAvailableMemory()));
  }

  public synchronized long getAvailableMemory() {
    return totalMemory - memoryUsed;
  }

  public boolean isMemoryAvailable() {
    return getAvailableMemory() > minimumMemory;
  }

  public long getUsableMemory() {
    return totalMemory - minimumMemory;
  }
}
