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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * Memory manager singleton class
 */
public class CarbonUnsafeMemoryManager {

  public static final CarbonUnsafeMemoryManager INSTANCE = new CarbonUnsafeMemoryManager();

  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonUnsafeMemoryManager.class.getName());

  /**
   * is offheap
   */
  private static boolean offHeap = Boolean.parseBoolean(CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.ENABLE_OFFHEAP,
          CarbonCommonConstants.ENABLE_OFFHEAP_DEFAULT));

  /**
   * working memory manager instance
   */
  private UnsafeMemoryManager workingMemoryManager;

  /**
   * sort storage memory instance
   */
  private UnsafeMemoryManager sortMemoryManager;

  /**
   * Below method will be used to get unsafe working
   * memory manager instance
   *
   * @return unsafe working memory instance
   */
  public UnsafeMemoryManager getUnsafeWorkingMemoryManager() {
    if (null == workingMemoryManager) {
      synchronized (INSTANCE) {
        if (null == workingMemoryManager) {
          long size;
          try {
            size = Long.parseLong(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB,
                    CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT));
          } catch (Exception e) {
            size = Long.parseLong(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB_DEFAULT);
            LOGGER.info(
                "Wrong UNSAFE_WORKING memory size given, " + "so setting default value to " + size);
          }
          if (size < 512) {
            size = 512;
            LOGGER.info("It is not recommended to keep unsafe working memory size less than 512MB, "
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
          workingMemoryManager =
              new UnsafeMemoryManager(takenSize, allocator, offHeap, 100, 300, "UNSAFE_WORKING");
          LOGGER.info("Working Memory manager is created with size " + size + " with " + allocator);
        }
      }
    }
    return workingMemoryManager;
  }

  /**
   * Below method will be used to get sort
   * storage memory manager instace
   *
   * @return sort storage memory manager instance
   */
  public UnsafeMemoryManager getUnsafeSortStorgeManager() {
    if (null == sortMemoryManager) {
      synchronized (INSTANCE) {
        if (null == sortMemoryManager) {
          long size;
          try {
            size = Long.parseLong(CarbonProperties.getInstance()
                .getProperty(CarbonCommonConstants.IN_MEMORY_STORAGE_FOR_SORTED_DATA_IN_MB,
                    CarbonCommonConstants.IN_MEMORY_STORAGE_FOR_SORTED_DATA_IN_MB));
          } catch (Exception e) {
            size = Long.parseLong(CarbonCommonConstants.IN_MEMORY_STORAGE_FOR_SORTED_DATA_IN_MB);
            LOGGER.info(
                "Wrong UNSAFE_SORT_STORAGE memory size given, " + "so setting default value to "
                    + size);
          }
          if (size < 512) {
            size = 512;
            LOGGER.info(
                "It is not recommended to keep unsafe sort storage memory size less than 512MB, "
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
          sortMemoryManager = new UnsafeMemoryManager(takenSize, allocator, offHeap, 100, 50,
              "UNSAFE_SORT_STORAGE");
          LOGGER.info("Sort Memory manager is created with size " + size + " with " + allocator);
        }
      }
    }
    return sortMemoryManager;
  }

  /**
   * Below method will be used to free
   * memory for task id
   *
   * @param taskId
   */
  public void freeAllMemoryForTask(long taskId) {
    if (null != sortMemoryManager) {
      sortMemoryManager.freeMemoryAll(taskId);
    }
    if (null != workingMemoryManager) {
      workingMemoryManager.freeMemoryAll(taskId);
    }
  }

}
