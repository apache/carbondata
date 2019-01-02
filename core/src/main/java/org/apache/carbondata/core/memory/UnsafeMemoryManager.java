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
 * Manages memory for instance.
 */
public class UnsafeMemoryManager {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(UnsafeMemoryManager.class.getName());

  private static boolean offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP_SORT,
          CarbonCommonConstants.ENABLE_OFFHEAP_SORT_DEFAULT));
  private static Map<String,Set<MemoryBlock>> taskIdToOffheapMemoryBlockMap;
  static {
    long size = 0L;
    String configuredWorkingMemorySize = null;
    try {
      // check if driver unsafe memory is configured and JVM process is in driver. In that case
      // initialize unsafe memory configured for driver
      boolean isDriver = Boolean.parseBoolean(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE,
              CarbonCommonConstants.IS_DRIVER_INSTANCE_DEFAULT));
      boolean initializedWithUnsafeDriverMemory = false;
      if (isDriver) {
        configuredWorkingMemorySize = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.UNSAFE_DRIVER_WORKING_MEMORY_IN_MB);
        if (null != configuredWorkingMemorySize) {
          size = Long.parseLong(configuredWorkingMemorySize);
          initializedWithUnsafeDriverMemory = true;
        }
      }
      if (!initializedWithUnsafeDriverMemory) {
        configuredWorkingMemorySize = CarbonProperties.getInstance()
            .getProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB);
        if (null != configuredWorkingMemorySize) {
          size = Long.parseLong(configuredWorkingMemorySize);
        }
      }
    } catch (Exception e) {
      LOGGER.info("Invalid offheap working memory size value: " + configuredWorkingMemorySize);
    }
    long takenSize = size;
    MemoryType memoryType;
    if (offHeap) {
      memoryType = MemoryType.OFFHEAP;
      long defaultSize = Long.parseLong(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT);
      if (takenSize < defaultSize) {
        takenSize = defaultSize;
        LOGGER.warn(String.format(
            "It is not recommended to set offheap working memory size less than %sMB,"
                + " so setting default value to %d",
            CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT, defaultSize));
      }
      takenSize = takenSize * 1024 * 1024;
    } else {
      // For ON-HEAP case not considering any size as it will based on max memory(Xmx) given to
      // JVM and JVM will take care of freeing the memory
      memoryType = MemoryType.ONHEAP;
    }
    INSTANCE = new UnsafeMemoryManager(takenSize, memoryType);
    taskIdToOffheapMemoryBlockMap = new HashMap<>();
  }

  public static final UnsafeMemoryManager INSTANCE;

  private long totalMemory;

  private long memoryUsed;

  private MemoryType memoryType;

  private UnsafeMemoryManager(long totalMemory, MemoryType memoryType) {
    this.totalMemory = totalMemory;
    this.memoryType = memoryType;
    LOGGER.info("Offheap Working Memory manager is created with size " + totalMemory + " with "
        + memoryType);
  }

  private synchronized MemoryBlock allocateMemory(MemoryType memoryType, String taskId,
      long memoryRequested) {
    MemoryBlock memoryBlock;
    if (memoryUsed + memoryRequested <= totalMemory && memoryType == MemoryType.OFFHEAP) {
      memoryBlock = MemoryAllocator.UNSAFE.allocate(memoryRequested);
      memoryUsed += memoryBlock.size();
      Set<MemoryBlock> listOfMemoryBlock = taskIdToOffheapMemoryBlockMap.get(taskId);
      if (null == listOfMemoryBlock) {
        listOfMemoryBlock = new HashSet<>();
        taskIdToOffheapMemoryBlockMap.put(taskId, listOfMemoryBlock);
      }
      listOfMemoryBlock.add(memoryBlock);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String.format("Creating Offheap working Memory block (%s) with size %d."
                + " Total memory used %d Bytes, left %d Bytes.",
            memoryBlock.toString(), memoryBlock.size(), memoryUsed, totalMemory - memoryUsed));
      }
    } else {
      // not adding on heap memory block to map as JVM will take care of freeing the memory
      memoryBlock = MemoryAllocator.HEAP.allocate(memoryRequested);
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(String
            .format("Creating onheap working Memory block with size: (%d)", memoryBlock.size()));
      }
    }
    return memoryBlock;
  }

  public synchronized void freeMemory(String taskId, MemoryBlock memoryBlock) {
    if (taskIdToOffheapMemoryBlockMap.containsKey(taskId)) {
      taskIdToOffheapMemoryBlockMap.get(taskId).remove(memoryBlock);
    }
    if (!memoryBlock.isFreedStatus()) {
      getMemoryAllocator(memoryBlock.getMemoryType()).free(memoryBlock);
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
      if (LOGGER.isDebugEnabled() && memoryBlock.getMemoryType() == MemoryType.OFFHEAP) {
        LOGGER.debug(String.format("Freeing offheap working memory block (%s) with size: %d, "
                + "current available memory is: %d", memoryBlock.toString(), memoryBlock.size(),
            totalMemory - memoryUsed));
      }
    }
  }

  public synchronized void freeMemoryAll(String taskId) {
    Set<MemoryBlock> memoryBlockSet;
    memoryBlockSet = taskIdToOffheapMemoryBlockMap.remove(taskId);
    long occuppiedMemory = 0;
    if (null != memoryBlockSet) {
      Iterator<MemoryBlock> iterator = memoryBlockSet.iterator();
      MemoryBlock memoryBlock = null;
      while (iterator.hasNext()) {
        memoryBlock = iterator.next();
        if (!memoryBlock.isFreedStatus()) {
          occuppiedMemory += memoryBlock.size();
          getMemoryAllocator(memoryBlock.getMemoryType()).free(memoryBlock);
        }
      }
    }
    memoryUsed -= occuppiedMemory;
    memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format(
          "Freeing offheap working memory of size %d. Current available memory is %d",
          occuppiedMemory, totalMemory - memoryUsed));
    }
    LOGGER.info(String.format(
        "Total offheap working memory used after task %s is %d. Current running tasks are %s",
        taskId, memoryUsed, StringUtils.join(taskIdToOffheapMemoryBlockMap.keySet(), ", ")));
  }

  public long getUsableMemory() {
    return totalMemory;
  }

  /**
   * It tries to allocate memory of `size` bytes, keep retry until it allocates successfully.
   */
  public static MemoryBlock allocateMemoryWithRetry(String taskId, long size)
      throws MemoryException {
    return allocateMemoryWithRetry(INSTANCE.memoryType, taskId, size);
  }

  public static MemoryBlock allocateMemoryWithRetry(MemoryType memoryType, String taskId,
      long size) {
    return INSTANCE.allocateMemory(memoryType, taskId, size);
  }

  private MemoryAllocator getMemoryAllocator(MemoryType memoryType) {
    switch (memoryType) {
      case ONHEAP:
        return MemoryAllocator.HEAP;
      default:
        return MemoryAllocator.UNSAFE;
    }
  }

  public static boolean isOffHeap() {
    return offHeap;
  }
}
