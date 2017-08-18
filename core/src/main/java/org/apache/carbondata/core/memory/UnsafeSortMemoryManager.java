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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Memory manager to keep track of
 * all memory for storing the sorted data
 */
public class UnsafeSortMemoryManager {

  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeSortMemoryManager.class.getName());

  /**
   * offheap is enabled
   */
  private static boolean offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
          CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));

  /**
   * map to keep taskid to memory blocks
   */
  private static Map<Long, Set<MemoryBlock>> taskIdToMemoryBlockMap;

  /**
   * singleton instance
   */
  public static final UnsafeSortMemoryManager INSTANCE;

  /**
   * total memory available for sort data storage
   */
  private long totalMemory;

  /**
   * current memory used
   */
  private long memoryUsed;

  /**
   * current memory allocator
   */
  private MemoryAllocator allocator;

  static {
    long size;
    try {
      size = Long.parseLong(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB,
              CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT));
    } catch (Exception e) {
      size = Long.parseLong(CarbonCommonConstants.IN_MEMORY_FOR_SORT_DATA_IN_MB_DEFAULT);
      LOGGER.info("Wrong memory size given, " + "so setting default value to " + size);
    }
    if (size < 1024) {
      size = 1024;
      LOGGER.info("It is not recommended to keep unsafe memory size less than 1024MB, "
          + "so setting default value to " + size);
    }

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
    INSTANCE = new UnsafeSortMemoryManager(takenSize, allocator);
    taskIdToMemoryBlockMap = new HashMap<>();
  }

  private UnsafeSortMemoryManager(long totalMemory, MemoryAllocator allocator) {
    this.totalMemory = totalMemory;
    this.allocator = allocator;
    LOGGER.info("Sort Memory manager is created with size " + totalMemory + " with " + allocator);
  }

  /**
   * Below method will be used to check whether memory required is
   * available or not
   *
   * @param required
   * @return if memory available
   */
  public synchronized boolean isMemoryAvailable(long required) {
    return memoryUsed + required < totalMemory;
  }

  /**
   * Below method will be used to allocate dummy memory
   * this will be used to allocate first and then used when u need
   *
   * @param size
   */
  public synchronized void allocateDummyMemory(long size) {
    memoryUsed += size;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Working Memory block (" + size + ") is created with size " + size
          + ". Total memory used " + memoryUsed + "Bytes, left " + (totalMemory - memoryUsed)
          + "Bytes");
    }
  }

  public synchronized void freeMemory(long taskId, MemoryBlock memoryBlock) {
    if (taskIdToMemoryBlockMap.containsKey(taskId)) {
      taskIdToMemoryBlockMap.get(taskId).remove(memoryBlock);
    }
    if (!memoryBlock.isFreedStatus()) {
      allocator.free(memoryBlock);
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Freeing memory of size: " + memoryBlock.size() + ": Current available memory is: " + (
                totalMemory - memoryUsed));
      }
    }
  }

  /**
   * Below method will be used to free all the
   * memory occupied for a task, this will be useful
   * when in case of task failure we need to clear all the memory occupied
   * @param taskId
   */
  public synchronized void freeMemoryAll(long taskId) {
    Set<MemoryBlock> memoryBlockSet = null;
    memoryBlockSet = taskIdToMemoryBlockMap.remove(taskId);
    long occuppiedMemory = 0;
    if (null != memoryBlockSet) {
      Iterator<MemoryBlock> iterator = memoryBlockSet.iterator();
      MemoryBlock memoryBlock = null;
      while (iterator.hasNext()) {
        memoryBlock = iterator.next();
        if (!memoryBlock.isFreedStatus()) {
          occuppiedMemory += memoryBlock.size();
          allocator.free(memoryBlock);
        }
      }
    }
    memoryUsed -= occuppiedMemory;
    memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          "Freeing memory of size: " + occuppiedMemory + ": Current available memory is: " + (
              totalMemory - memoryUsed));
    }
  }

  /**
   * Before calling this method caller should call allocateMemoryDummy
   * This method will be used to allocate the memory, this can be used
   * when caller wants to allocate memory first and used it anytime
   * @param taskId
   * @param memoryRequested
   * @return memory block
   */
  public synchronized MemoryBlock allocateMemoryLazy(long taskId, long memoryRequested) {
    MemoryBlock allocate = allocator.allocate(memoryRequested);
    Set<MemoryBlock> listOfMemoryBlock = taskIdToMemoryBlockMap.get(taskId);
    if (null == listOfMemoryBlock) {
      listOfMemoryBlock = new HashSet<>();
      taskIdToMemoryBlockMap.put(taskId, listOfMemoryBlock);
    }
    listOfMemoryBlock.add(allocate);
    return allocate;
  }

  /**
   * It tries to allocate memory of `size` bytes, keep retry until it allocates successfully.
   */
  public static MemoryBlock allocateMemoryWithRetry(long taskId, long size) throws MemoryException {
    MemoryBlock baseBlock = null;
    int tries = 0;
    while (tries < 100) {
      baseBlock = INSTANCE.allocateMemory(taskId, size);
      if (baseBlock == null) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          throw new MemoryException(e);
        }
      } else {
        break;
      }
      tries++;
    }
    if (baseBlock == null) {
      throw new MemoryException("Not enough memory");
    }
    return baseBlock;
  }

  private synchronized MemoryBlock allocateMemory(long taskId, long memoryRequested) {
    if (memoryUsed + memoryRequested <= totalMemory) {
      MemoryBlock allocate = allocator.allocate(memoryRequested);
      memoryUsed += allocate.size();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Working Memory block (" + allocate.size() + ") is created with size " + allocate.size()
                + ". Total memory used " + memoryUsed + "Bytes, left " + (totalMemory - memoryUsed)
                + "Bytes");
      }
      Set<MemoryBlock> listOfMemoryBlock = taskIdToMemoryBlockMap.get(taskId);
      if (null == listOfMemoryBlock) {
        listOfMemoryBlock = new HashSet<>();
        taskIdToMemoryBlockMap.put(taskId, listOfMemoryBlock);
      }
      listOfMemoryBlock.add(allocate);
      return allocate;
    }
    return null;
  }

  public static boolean isOffHeap() {
    return offHeap;
  }
}
