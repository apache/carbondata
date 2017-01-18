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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;

/**
 * Responsible for storing long array data to memory. memory can be on heap or
 * offheap based on the user configuration using unsafe interface
 */
public class UnsafeLongMeasureChunkStore extends UnsafeAbstractMeasureDataChunkStore<long[]> {

  public UnsafeLongMeasureChunkStore(int numberOfRows) {
    super(numberOfRows);
  }

  /**
   * Below method will be used to put long array data to memory
   *
   * @param data
   */
  @Override public void putData(long[] data) {
    assert (!this.isMemoryOccupied);
    this.dataPageMemoryBlock = MemoryAllocatorFactory.INSATANCE.getMemoryAllocator()
        .allocate(data.length * CarbonCommonConstants.LONG_SIZE_IN_BYTE);
    // copy the data to memory
    CarbonUnsafe.unsafe
        .copyMemory(data, CarbonUnsafe.LONG_ARRAY_OFFSET, dataPageMemoryBlock.getBaseObject(),
            dataPageMemoryBlock.getBaseOffset(), dataPageMemoryBlock.size());
    this.isMemoryOccupied = true;
  }

  /**
   * to get the long value
   *
   * @param index
   * @return long value based on index
   */
  @Override public long getLong(int index) {
    return CarbonUnsafe.unsafe.getLong(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + (index * CarbonCommonConstants.LONG_SIZE_IN_BYTE));
  }
}
