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
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.memory.MemoryAllocatorFactory;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Responsible for storing big decimal array data to memory. memory can be on heap or
 * offheap based on the user configuration using unsafe interface
 */
public class UnsafeBigDecimalMeasureChunkStore extends UnsafeAbstractMeasureDataChunkStore<byte[]> {

  /**
   * start position of data offsets
   */
  private long offsetStartPosition;

  public UnsafeBigDecimalMeasureChunkStore(int numberOfRows) {
    super(numberOfRows);
  }

  @Override public void putData(byte[] data) {
    assert (!this.isMemoryOccupied);
    this.dataPageMemoryBlock = MemoryAllocatorFactory.INSATANCE.getMemoryAllocator()
        .allocate(data.length + (numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    this.offsetStartPosition = data.length;
    // copy the data to memory
    CarbonUnsafe.unsafe
        .copyMemory(data, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataPageMemoryBlock.getBaseObject(),
            dataPageMemoryBlock.getBaseOffset(), dataPageMemoryBlock.size());
    // As data is of variable length and data format is
    // <length in short><data><length in short><data>
    // we need to store offset of each data so data can be accessed directly
    // for example:
    //data = {0,0,0,0,5,1,2,3,4,5,0,0,0,0,6,0,1,2,3,4,5,0,0,0,0,2,8,9}
    //so value stored in offset will be position of actual data
    // [5,14,24]
    // to store this value we need to get the actual data length + 4 bytes used for storing the
    // length
    // start position will be used to store the current data position
    int startOffset = 0;
    // position from where offsets will start
    long pointerOffsets = this.offsetStartPosition;
    // as first position will be start from 4 byte as data is stored first in the memory block
    // we need to skip first two bytes this is because first two bytes will be length of the data
    // which we have to skip
    CarbonUnsafe.unsafe.putInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + pointerOffsets,
        CarbonCommonConstants.INT_SIZE_IN_BYTE);
    // incrementing the pointers as first value is already filled and as we are storing as int
    // we need to increment the 4 bytes to set the position of the next value to set
    pointerOffsets += CarbonCommonConstants.INT_SIZE_IN_BYTE;
    // creating a byte buffer which will wrap the length of the row
    // using byte buffer as unsafe will return bytes in little-endian encoding
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
    // store length of data
    byte[] length = new byte[CarbonCommonConstants.INT_SIZE_IN_BYTE];
    // as first offset is already stored, we need to start from the 2nd row in data array
    for (int i = 1; i < numberOfRows; i++) {
      // first copy the length of previous row
      CarbonUnsafe.unsafe.copyMemory(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + startOffset, length, CarbonUnsafe.BYTE_ARRAY_OFFSET,
          CarbonCommonConstants.INT_SIZE_IN_BYTE);
      buffer.put(length);
      buffer.flip();
      // so current row position will be
      // previous row length + 4 bytes used for storing previous row data
      startOffset += CarbonCommonConstants.INT_SIZE_IN_BYTE + buffer.getInt();
      // as same byte buffer is used to avoid creating many byte buffer for each row
      // we need to clear the byte buffer
      buffer.clear();
      // now put the offset of current row, here we need to add 4 more bytes as current will
      // also have length part so we have to skip length
      CarbonUnsafe.unsafe.putInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + pointerOffsets,
          startOffset + CarbonCommonConstants.INT_SIZE_IN_BYTE);
      // incrementing the pointers as first value is already filled and as we are storing as int
      // we need to increment the 4 bytes to set the position of the next value to set
      pointerOffsets += CarbonCommonConstants.INT_SIZE_IN_BYTE;

      this.isMemoryOccupied = true;
    }
  }

  /**
   * to get the byte value
   *
   * @param index
   * @return byte value based on index
   */
  @Override public BigDecimal getBigDecimal(int index) {
    // now to get the row from memory block we need to do following thing
    // 1. first get the current offset
    // 2. if it's not a last row- get the next row offset
    // Subtract the current row offset + 4 bytes(to skip the data length) with next row offset
    // else subtract the current row offset
    // with complete data length get the offset of set of data
    int currentDataOffset = CarbonUnsafe.unsafe.getInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + this.offsetStartPosition + (index
            * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    int length = 0;
    // calculating the length of data
    if (index < numberOfRows - 1) {
      int OffsetOfNextdata = CarbonUnsafe.unsafe.getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.offsetStartPosition + ((index + 1)
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
      length = OffsetOfNextdata - (currentDataOffset + CarbonCommonConstants.INT_SIZE_IN_BYTE);
    } else {
      // for last record we need to subtract with data length
      length = (int) this.offsetStartPosition - currentDataOffset;
    }
    byte[] row = new byte[length];
    CarbonUnsafe.unsafe.copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + currentDataOffset, row,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return DataTypeUtil.byteToBigDecimal(row);
  }

}
