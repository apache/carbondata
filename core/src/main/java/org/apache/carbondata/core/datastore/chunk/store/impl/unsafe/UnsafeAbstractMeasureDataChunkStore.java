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

package org.apache.carbondata.core.datastore.chunk.store.impl.unsafe;

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.chunk.store.MeasureDataChunkStore;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;
import org.apache.carbondata.core.memory.MemoryBlock;

/**
 * Responsibility is store the measure data in memory, memory can be on heap or
 * offheap based on the user configuration using unsafe interface
 */
public abstract class UnsafeAbstractMeasureDataChunkStore<T> implements MeasureDataChunkStore<T> {

  /**
   * memory block
   */
  protected MemoryBlock dataPageMemoryBlock;

  /**
   * number of rows
   */
  protected int numberOfRows;

  /**
   * to check memory is released or not
   */
  protected boolean isMemoryReleased;

  /**
   * to check memory is occupied or not
   */
  protected boolean isMemoryOccupied;

  public UnsafeAbstractMeasureDataChunkStore(int numberOfRows) {
    this.numberOfRows = numberOfRows;
  }

  /**
   * to get the byte value
   *
   * @param index
   * @return byte value based on index
   */
  @Override public byte getByte(int index) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * to get the short value
   *
   * @param index
   * @return short value based on index
   */
  @Override public short getShort(int index) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * to get the int value
   *
   * @param index
   * @return int value based on index
   */
  @Override public int getInt(int index) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * to get the long value
   *
   * @param index
   * @return long value based on index
   */
  @Override public long getLong(int index) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * to get the double value
   *
   * @param index
   * @return double value based on index
   */
  @Override public double getDouble(int index) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * To get the bigdecimal value
   *
   * @param index
   * @return bigdecimal value based on index
   */
  @Override public BigDecimal getBigDecimal(int index) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * To free the occupied memory
   */
  @Override public void freeMemory() {
    if (isMemoryReleased) {
      return;
    }
    MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().free(dataPageMemoryBlock);
    isMemoryReleased = true;
    this.dataPageMemoryBlock = null;
    this.isMemoryOccupied = false;
  }

}
