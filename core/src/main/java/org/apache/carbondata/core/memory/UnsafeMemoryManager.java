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

/**
 * Manages memory for instance.
 */
public class UnsafeMemoryManager {

  /**
   * logger
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(UnsafeMemoryManager.class.getName());

  /**
   * map to store task id to memory block mapping
   */
  private Map<Long, Set<MemoryBlock>> taskIdToMemoryBlockMap;

  /**
   * total memory available
   */
  private long totalMemory;

  /**
   * current memory used
   */
  private long memoryUsed;

  /**
   * type of memory allocator
   */
  private MemoryAllocator allocator;

  /**
   * is store is offheap
   */
  private boolean offHeap;

  /**
   * number of try to get the memory block
   */
  private int numberOfTries;

  /**
   * delay time for getting the memory block
   */
  private int delay;

  /**
   * this will be used to print log message
   */
  private String managerType;

  UnsafeMemoryManager(long totalMemory, MemoryAllocator allocator, boolean offheap,
      int numberOfTries, int delay, String managerType) {
    this.totalMemory = totalMemory;
    this.allocator = allocator;
    taskIdToMemoryBlockMap = new HashMap<>();
    this.numberOfTries = numberOfTries;
    this.delay = delay;
    this.offHeap = offheap;
    this.managerType = managerType;
  }

  private synchronized MemoryBlock allocateMemory(long taskId, long memoryRequested) {
    if (memoryUsed + memoryRequested <= totalMemory) {
      MemoryBlock allocate = allocator.allocate(memoryRequested);
      memoryUsed += allocate.size();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(managerType +
            ":Memory block (" + allocate + ") is created with size " + allocate.size()
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

  public synchronized void freeMemory(long taskId, MemoryBlock memoryBlock) {
    Set<MemoryBlock> memoryBlocks = taskIdToMemoryBlockMap.get(taskId);
    if (null != memoryBlocks && memoryBlocks.remove(memoryBlock)) {
      allocator.free(memoryBlock);
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(managerType + ":Freeing memory of size: " + memoryBlock.size()
            + ": Current available memory is: " + (totalMemory - memoryUsed));
      }
    }
  }

  public void freeMemoryAll(long taskId) {
    Set<MemoryBlock> memoryBlockSet = null;
    synchronized (this) {
      memoryBlockSet = taskIdToMemoryBlockMap.remove(taskId);
    }
    long occuppiedMemory = 0;
    if (null != memoryBlockSet) {
      Iterator<MemoryBlock> iterator = memoryBlockSet.iterator();
      MemoryBlock memoryBlock = null;
      while (iterator.hasNext()) {
        memoryBlock = iterator.next();
        occuppiedMemory += memoryBlock.size();
        allocator.free(memoryBlock);
      }
    }
    synchronized (this) {
      memoryUsed -= occuppiedMemory;
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(managerType +
          ":Freeing memory of size: " + occuppiedMemory + ": Current available memory is: " + (
              totalMemory - memoryUsed));
    }
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
  public MemoryBlock allocateMemoryWithRetry(long taskId, long size) throws MemoryException {
    MemoryBlock baseBlock = null;
    int tries = 0;
    while (tries < numberOfTries) {
      baseBlock = allocateMemory(taskId, size);
      if (baseBlock == null) {
        try {
          Thread.sleep(delay);
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

  public boolean isOffHeap() {
    return offHeap;
  }

  /**
   * Below method will be used to allocate dummy memory
   * this will be used to allocate first and then used when u need
   *
   * @param size
   */
  public synchronized boolean allocateDummyMemory(long size) {
    if (memoryUsed + size > totalMemory) {
      return false;
    }
    memoryUsed += size;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(this.managerType + ":Memory block (" + size + ") is created with size " + size
          + ". Total memory used " + memoryUsed + "Bytes, left " + (totalMemory - memoryUsed)
          + "Bytes");
    }
    return true;
  }

  /**
   * Below method will be used to allocate the memory block from one manager to other
   * for this memory need to be allocated first
   * Caller must call {@link #allocateDummyMemory(long)}
   *
   * @param taskId
   * @param memoryBlock
   */
  public synchronized void allocateLazyWithMemoryBlock(long taskId, MemoryBlock memoryBlock) {
    Set<MemoryBlock> memoryBlocks = taskIdToMemoryBlockMap.get(taskId);
    if (null == memoryBlocks) {
      memoryBlocks = new HashSet<>();
      taskIdToMemoryBlockMap.put(taskId, memoryBlocks);
    }
    memoryBlocks.add(memoryBlock);
  }

  /**
   * Below method will be used to remove the memory block
   * and update the memory used, but it will not release the memory
   * This will be useful when from one manager memory block is copied to
   * other
   *
   * @param taskId
   * @param memoryBlock
   */
  public synchronized void removeMemoryBlockAndUpdateMemoryUsed(long taskId,
      MemoryBlock memoryBlock) {
    Set<MemoryBlock> listOfMemoryBlock = taskIdToMemoryBlockMap.get(taskId);
    if (null != listOfMemoryBlock && listOfMemoryBlock.remove(memoryBlock)) {
      memoryUsed -= memoryBlock.size();
      memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(managerType + ":Freeing memory of size: " + memoryBlock.size()
            + ": Current available memory is: " + (totalMemory - memoryUsed));
      }
    }
  }

  /**
   * Below method will be used to free the dummy allocated memory
   * when it was not used in case of sort storage memory
   *
   * @param memorySize
   */
  public synchronized void freeDummyAllocatedMemory(long memorySize) {
    memoryUsed -= memorySize;
    memoryUsed = memoryUsed < 0 ? 0 : memoryUsed;
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug(
          managerType + ":Freeing memory of size: " + memorySize + ": Current available memory is: "
              + (totalMemory - memoryUsed));
    }
  }

}
