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

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;

/**
 * Responsible for storing Byte array data to memory. memory can be on heap or
 * offheap based on the user configuration using unsafe interface
 */
public class UnsafeByteMeasureChunkStore extends UnsafeAbstractMeasureDataChunkStore<byte[]> {

  public UnsafeByteMeasureChunkStore(int numberOfRows) {
    super(numberOfRows);
  }

  /**
   * Below method will be used to put byte array data to memory
   *
   * @param data
   */
  @Override public void putData(byte[] data) {
    assert (!this.isMemoryOccupied);
    this.dataPageMemoryBlock =
        MemoryAllocatorFactory.INSATANCE.getMemoryAllocator().allocate(data.length);
    // copy the data to memory
    CarbonUnsafe.unsafe
        .copyMemory(data, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataPageMemoryBlock.getBaseObject(),
            dataPageMemoryBlock.getBaseOffset(), dataPageMemoryBlock.size());
    this.isMemoryOccupied = true;
  }

  /**
   * to get the byte value
   *
   * @param index
   * @return byte value based on index
   */
  @Override public byte getByte(int index) {
    return CarbonUnsafe.unsafe
        .getByte(dataPageMemoryBlock.getBaseObject(), dataPageMemoryBlock.getBaseOffset() + index);
  }
}
