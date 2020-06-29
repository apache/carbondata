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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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
  private static Map<String, Set<MemoryBlock>> taskIdToOffheapMemoryBlockMap;
  static {
    long size = 0L;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3035
    String configuredWorkingMemorySize = null;
    try {
      // check if driver unsafe memory is configured and JVM process is in driver. In that case
      // initialize unsafe memory configured for driver
      boolean isDriver = Boolean.parseBoolean(CarbonProperties.getInstance()
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3038
          .getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE,
              CarbonCommonConstants.IS_DRIVER_INSTANCE_DEFAULT));
      boolean initializedWithUnsafeDriverMemory = false;
      if (isDriver) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3035
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3047
      LOGGER.info("Invalid offheap working memory size value: " + configuredWorkingMemorySize);
    }
    long takenSize = size;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2990
    MemoryType memoryType;
    if (offHeap) {
      memoryType = MemoryType.OFFHEAP;
      long defaultSize = Long.parseLong(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT);
      if (takenSize < defaultSize) {
        takenSize = defaultSize;
        LOGGER.warn(String.format(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3047
            "It is not recommended to set offheap working memory size less than %sMB,"
                + " so setting default value to %d",
            CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT, defaultSize));
      }
      takenSize = takenSize * 1024 * 1024;
    } else {
      // For ON-HEAP case not considering any size as it will based on max memory(Xmx) given to
      // JVM and JVM will take care of freeing the memory
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2990
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3124
    LOGGER.info("Offheap Working Memory manager is created with size " + totalMemory + " with "
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3047
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3140
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2990
      getMemoryAllocator(memoryBlock.getMemoryType()).free(memoryBlock);
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3124
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
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2990
          getMemoryAllocator(memoryBlock.getMemoryType()).free(memoryBlock);
        }
      }
    }
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1386
    memoryUsed -= occuppiedMemory;
    memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(String.format(
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3047
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
  public static MemoryBlock allocateMemoryWithRetry(String taskId, long size) {
    return allocateMemoryWithRetry(INSTANCE.memoryType, taskId, size);
  }

  public static MemoryBlock allocateMemoryWithRetry(MemoryType memoryType, String taskId,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3047
      long size) {
    return INSTANCE.allocateMemory(memoryType, taskId, size);
  }

  private MemoryAllocator getMemoryAllocator(MemoryType memoryType) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-2990
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

  /**
   * DirectByteBuffers are garbage collected by using a phantom reference and a
   * reference queue. Every once a while, the JVM checks the reference queue and
   * cleans the DirectByteBuffers. However, as this doesn't happen
   * immediately after discarding all references to a DirectByteBuffer, it's
   * easy to OutOfMemoryError yourself using DirectByteBuffers. This function
   * explicitly calls the Cleaner method of a DirectByteBuffer.
   *
   * @param toBeDestroyed The DirectByteBuffer that will be "cleaned". Utilizes reflection.
   */
  public static void destroyDirectByteBuffer(ByteBuffer toBeDestroyed) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3787
    if (!toBeDestroyed.isDirect()) {
      return;
    }
    try {
      Method cleanerMethod = toBeDestroyed.getClass().getMethod("cleaner");
      cleanerMethod.setAccessible(true);
      Object cleaner = cleanerMethod.invoke(toBeDestroyed);
      Method cleanMethod = cleaner.getClass().getMethod("clean");
      cleanMethod.setAccessible(true);
      cleanMethod.invoke(cleaner);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }
}
