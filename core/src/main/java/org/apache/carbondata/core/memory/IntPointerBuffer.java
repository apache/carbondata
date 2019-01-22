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

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.log4j.Logger;

/**
 * Holds the pointers for rows.
 */
public class IntPointerBuffer {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(IntPointerBuffer.class.getName());

  private int length;

  private int actualSize;

  private int[] pointerBlock;

  private MemoryBlock pointerMemoryBlock;

  private String taskId;

  public IntPointerBuffer(String taskId) {
    // TODO can be configurable, it is initial size and it can grow automatically.
    this.length = 100000;
    pointerBlock = new int[length];
    this.taskId = taskId;
  }

  public IntPointerBuffer(int length) {
    this.length = length;
    pointerBlock = new int[length];
  }

  public void set(int rowId, int value) {
    pointerBlock[rowId] = value;
  }

  public void set(int value) {
    ensureMemory();
    pointerBlock[actualSize] = value;
    actualSize++;
  }

  /**
   * Returns the value at position {@code rowId}.
   */
  public int get(int rowId) {
    assert rowId >= 0 : "rowId (" + rowId + ") should >= 0";
    assert rowId < length : "rowId (" + rowId + ") should < length (" + length + ")";
    if (pointerBlock == null) {
      return CarbonUnsafe.getUnsafe().getInt(pointerMemoryBlock.getBaseObject(),
          pointerMemoryBlock.getBaseOffset() + (rowId << 2));
    }
    return pointerBlock[rowId];
  }

  public void loadToUnsafe() {
    pointerMemoryBlock =
        UnsafeSortMemoryManager.INSTANCE.allocateMemory(this.taskId, pointerBlock.length * 4);
    // pointerMemoryBlock it means sort storage memory manager does not have space to loaf pointer
    // buffer in that case use pointerBlock
    if (null != pointerMemoryBlock) {
      for (int i = 0; i < pointerBlock.length; i++) {
        CarbonUnsafe.getUnsafe()
            .putInt(pointerMemoryBlock.getBaseObject(), pointerMemoryBlock.getBaseOffset() + i * 4,
                pointerBlock[i]);
      }
      pointerBlock = null;
    }
  }

  public int getActualSize() {
    return actualSize;
  }

  public int[] getPointerBlock() {
    return pointerBlock;
  }

  private void ensureMemory() {
    if (actualSize >= length) {
      // Expand by quarter, may be we can correct the logic later
      int localLength = length + (int) (length * (0.25));
      int[] memoryAddress = new int[localLength];
      System.arraycopy(pointerBlock, 0, memoryAddress, 0, length);
      pointerBlock = memoryAddress;
      length = localLength;
    }
  }

  public void freeMemory() {
    pointerBlock = null;
    if (pointerMemoryBlock != null) {
      UnsafeSortMemoryManager.INSTANCE.freeMemory(this.taskId, pointerMemoryBlock);
    }
  }
}