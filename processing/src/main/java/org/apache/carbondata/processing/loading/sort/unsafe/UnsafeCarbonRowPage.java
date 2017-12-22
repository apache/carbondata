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

package org.apache.carbondata.processing.loading.sort.unsafe;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.carbondata.core.memory.IntPointerBuffer;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.memory.UnsafeSortMemoryManager;
import org.apache.carbondata.processing.loading.sort.SortStepRowHandler;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

/**
 * It can keep the data of prescribed size data in offheap/onheap memory and returns it when needed
 */
public class UnsafeCarbonRowPage {

  private IntPointerBuffer buffer;

  private int lastSize;

  private long sizeToBeUsed;

  private MemoryBlock dataBlock;

  private boolean saveToDisk;

  private MemoryManagerType managerType;

  private long taskId;
  private TableFieldStat tableFieldStat;
  private SortStepRowHandler sortStepRowHandler;

  public UnsafeCarbonRowPage(TableFieldStat tableFieldStat, MemoryBlock memoryBlock,
      boolean saveToDisk, long taskId) {
    this.tableFieldStat = tableFieldStat;
    this.sortStepRowHandler = new SortStepRowHandler(tableFieldStat);
    this.saveToDisk = saveToDisk;
    this.taskId = taskId;
    buffer = new IntPointerBuffer(this.taskId);
    this.dataBlock = memoryBlock;
    // TODO Only using 98% of space for safe side.May be we can have different logic.
    sizeToBeUsed = dataBlock.size() - (dataBlock.size() * 5) / 100;
    this.managerType = MemoryManagerType.UNSAFE_MEMORY_MANAGER;
  }

  public int addRow(Object[] row) {
    int size = addRow(row, dataBlock.getBaseOffset() + lastSize);
    buffer.set(lastSize);
    lastSize = lastSize + size;
    return size;
  }

  /**
   * add row as 3 parts
   * @param row
   * @param address
   * @return
   */
  private int addRow(Object[] row, long address) {
    return sortStepRowHandler.writeRawRowAsPartedToUnsafeMemory(row,
        dataBlock.getBaseObject(), address);
  }

  /**
   * get one row from memory address
   * @param address address
   * @return one row
   */
  public Object[] getRow(long address) {
    return sortStepRowHandler.readPartedRowFromUnsafeMemory(dataBlock.getBaseObject(), address);
  }

  public void fillRow(long address, DataOutputStream stream) throws IOException {
    Object[] row = getRow(address);
    sortStepRowHandler.writePartedRowToOutputStream(row, stream);
  }

  /**
   * get all rows in this carbon row page
   * @return all rows
   */
  public Object[][] getAllRows() {
    int entryCount = buffer.getActualSize();
    Object[][] allRows = new Object[entryCount][];
    for (int rowIdx = 0; rowIdx < entryCount; rowIdx++) {
      long rowAddress = dataBlock.getBaseOffset() + buffer.get(rowIdx);
      allRows[rowIdx] = getRow(rowAddress);
    }
    return allRows;
  }

  public void freeMemory() {
    switch (managerType) {
      case UNSAFE_MEMORY_MANAGER:
        UnsafeMemoryManager.INSTANCE.freeMemory(taskId, dataBlock);
        break;
      default:
        UnsafeSortMemoryManager.INSTANCE.freeMemory(taskId, dataBlock);
        buffer.freeMemory();
    }
  }

  public boolean isSaveToDisk() {
    return saveToDisk;
  }

  public IntPointerBuffer getBuffer() {
    return buffer;
  }

  public int getUsedSize() {
    return lastSize;
  }

  public boolean canAdd() {
    return lastSize < sizeToBeUsed;
  }

  public MemoryBlock getDataBlock() {
    return dataBlock;
  }

  public boolean[] getNoDictionarySortColumnMapping() {
    return tableFieldStat.getIsSortColNoDictFlags();
  }

  public TableFieldStat getTableFieldStat() {
    return tableFieldStat;
  }

  public void setNewDataBlock(MemoryBlock newMemoryBlock) {
    this.dataBlock = newMemoryBlock;
    this.managerType = MemoryManagerType.UNSAFE_SORT_MEMORY_MANAGER;
  }

  public enum MemoryManagerType {
    UNSAFE_MEMORY_MANAGER, UNSAFE_SORT_MEMORY_MANAGER
  }
}
