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
 * Manages memory for instance.
 */
public class UnsafeMemoryManager {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeMemoryManager.class.getName());

  private static boolean offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
          CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));
  private static Map<Long,Set<MemoryBlock>> taskIdToMemoryBlockMap;
  static {
    long size = 0L;
    String defaultWorkingMemorySize = null;
    try {
      // check if driver unsafe memory is configured and JVM process is in driver. In that case
      // initialize unsafe memory configured for driver
      boolean isDriver = Boolean.parseBoolean(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "false"));
      boolean initializedWithUnsafeDriverMemory = false;
      if (isDriver) {
        defaultWorkingMemorySize = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.UNSAFE_DRIVER_WORKING_MEMORY_IN_MB);
        if (null != defaultWorkingMemorySize) {
          size = Long.parseLong(defaultWorkingMemorySize);
          initializedWithUnsafeDriverMemory = true;
        }
      }
      if (!initializedWithUnsafeDriverMemory) {
        defaultWorkingMemorySize = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB);
        if (null != defaultWorkingMemorySize) {
          size = Long.parseLong(defaultWorkingMemorySize);
        }
      }
    } catch (Exception e) {
      LOGGER.info("Invalid memory size value: " + defaultWorkingMemorySize);
    }
    long takenSize = size;
    MemoryAllocator allocator;
    if (offHeap) {
      allocator = MemoryAllocator.UNSAFE;
      long defaultSize = Long.parseLong(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT);
      if (takenSize < defaultSize) {
        takenSize = defaultSize;
      }
      takenSize = takenSize * 1024 * 1024;
    } else {
      long maxMemory = Runtime.getRuntime().maxMemory() * 60 / 100;
      if (takenSize == 0L) {
        takenSize = maxMemory;
      } else {
        takenSize = takenSize * 1024 * 1024;
        if (takenSize > maxMemory) {
          takenSize = maxMemory;
        }
      }
      allocator = MemoryAllocator.HEAP;
    }
    INSTANCE = new UnsafeMemoryManager(takenSize, allocator);
    taskIdToMemoryBlockMap = new HashMap<>();
  }

  public static final UnsafeMemoryManager INSTANCE;

  private long totalMemory;

  private long memoryUsed;

  private MemoryAllocator allocator;

  private UnsafeMemoryManager(long totalMemory, MemoryAllocator allocator) {
    this.totalMemory = totalMemory;
    this.allocator = allocator;
    LOGGER
        .info("Working Memory manager is created with size " + totalMemory + " with " + allocator);
  }

  private synchronized MemoryBlock allocateMemory(MemoryAllocator memoryAllocator, long taskId,
      long memoryRequested) {
    if (memoryUsed + memoryRequested <= totalMemory) {
      MemoryBlock allocate = memoryAllocator.allocate(memoryRequested);
      memoryUsed += allocate.size();
      Set<MemoryBlock> listOfMemoryBlock = taskIdToMemoryBlockMap.get(taskId);
      if (null == listOfMemoryBlock) {
        listOfMemoryBlock = new HashSet<>();
        taskIdToMemoryBlockMap.put(taskId, listOfMemoryBlock);
      }
      listOfMemoryBlock.add(allocate);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Memory block (" + allocate + ") is created with size " + allocate.size()
            + ". Total memory used " + memoryUsed + "Bytes, left " + (totalMemory - memoryUsed)
            + "Bytes");
      }
      return allocate;
    }
    return null;
  }

  public synchronized void freeMemory(long taskId, MemoryBlock memoryBlock) {
    freeMemory(allocator, taskId, memoryBlock);
  }

  public synchronized void freeMemory(MemoryAllocator memoryAllocator, long taskId,
      MemoryBlock memoryBlock) {
    if (taskIdToMemoryBlockMap.containsKey(taskId)) {
      taskIdToMemoryBlockMap.get(taskId).remove(memoryBlock);
    }
    if (!memoryBlock.isFreedStatus()) {
      memoryAllocator.free(memoryBlock);
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Freeing memory of size: " + memoryBlock.size() + "available memory:  " + (totalMemory
                - memoryUsed));
      }
    }
  }

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
    LOGGER.info("Total memory used after task " + taskId + " is " + memoryUsed
        + " Current tasks running now are : " + taskIdToMemoryBlockMap.keySet());
  }

  public synchronized boolean isMemoryAvailable() {
    return memoryUsed > totalMemory;
  }

  public long getUsableMemory() {
    return totalMemory;
  }

  /**
   * It tries to allocate memory of `size` bytes, keep retry until it allocates successfully.
   */
  public static MemoryBlock allocateMemoryWithRetry(long taskId, long size)
      throws MemoryException {
    return allocateMemoryWithRetry(INSTANCE.allocator, taskId, size);
  }

  public static MemoryBlock allocateMemoryWithRetry(MemoryAllocator memoryAllocator, long taskId,
      long size) throws MemoryException {
    MemoryBlock baseBlock = null;
    int tries = 0;
    while (tries < 300) {
      baseBlock = INSTANCE.allocateMemory(memoryAllocator, taskId, size);
      if (baseBlock == null) {
        try {
          LOGGER.info("Memory is not available, retry after 500 millis");
          Thread.sleep(500);
        } catch (InterruptedException e) {
          throw new MemoryException(e);
        }
      } else {
        break;
      }
      tries++;
    }
    if (baseBlock == null) {
      INSTANCE.printCurrentMemoryUsage();
      throw new MemoryException(
          "Not enough memory. please increase carbon.unsafe.working.memory.in.mb");
    }
    return baseBlock;
  }

  public static boolean isOffHeap() {
    return offHeap;
  }

  private synchronized void printCurrentMemoryUsage() {
    LOGGER.error(
        " Memory Used : " + memoryUsed + " Tasks running : " + taskIdToMemoryBlockMap.keySet());
  }
}
