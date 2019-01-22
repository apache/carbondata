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

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

/**
 * Memory manager to keep track of
 * all memory for storing the sorted data
 */
public class UnsafeSortMemoryManager {

  /**
   * logger
   */
  private static final Logger LOGGER =
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
  private static Map<String, Set<MemoryBlock>> taskIdToMemoryBlockMap;

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
      size = Long.parseLong(CarbonProperties.getInstance().getProperty(
          CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB));
    } catch (Exception e) {
      size = CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT;
      LOGGER.info("Wrong memory size given, so setting default value to " + size);
    }
    if (size < CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT) {
      size = CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT;
      LOGGER.warn(String.format(
          "It is not recommended to set unsafe sort memory size less than %dMB,"
              + " so setting default value to %d",
          CarbonCommonConstants.CARBON_SORT_STORAGE_INMEMORY_IN_MB_DEFAULT, size));
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
   * total usable memory for sort memory manager
   * @return size in bytes
   */
  public long getUsableMemory() {
    return totalMemory;
  }

  public synchronized void freeMemory(String taskId, MemoryBlock memoryBlock) {
    if (taskIdToMemoryBlockMap.containsKey(taskId)) {
      taskIdToMemoryBlockMap.get(taskId).remove(memoryBlock);
    }
    if (!memoryBlock.isFreedStatus()) {
      allocator.free(memoryBlock);
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format(
            "Freeing sort memory block (%s) with size: %d, current available memory is: %d",
            memoryBlock.toString(), memoryBlock.size(), totalMemory - memoryUsed));
      }
    }
  }

  /**
   * Below method will be used to free all the
   * memory occupied for a task, this will be useful
   * when in case of task failure we need to clear all the memory occupied
   * @param taskId
   */
  public synchronized void freeMemoryAll(String taskId) {
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
          String.format("Freeing sort memory of size: %d, current available memory is: %d",
              occuppiedMemory, totalMemory - memoryUsed));
    }
    LOGGER.info(String.format(
        "Total sort memory used after task %s is %d. Current running tasks are: %s",
        taskId, memoryUsed, StringUtils.join(taskIdToMemoryBlockMap.keySet(), ", ")));
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

  public synchronized MemoryBlock allocateMemory(String taskId, long memoryRequested) {
    if (memoryUsed + memoryRequested <= totalMemory) {
      MemoryBlock allocate = allocator.allocate(memoryRequested);
      memoryUsed += allocate.size();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format(
            "Sort Memory block is created with size %d. Total memory used %d Bytes, left %d Bytes",
            allocate.size(), memoryUsed, totalMemory - memoryUsed));
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
