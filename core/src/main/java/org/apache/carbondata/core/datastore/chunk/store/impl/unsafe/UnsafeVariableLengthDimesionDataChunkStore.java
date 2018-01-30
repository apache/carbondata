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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Below class is responsible to store variable length dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public class UnsafeVariableLengthDimesionDataChunkStore
    extends UnsafeAbstractDimensionDataChunkStore {

  /**
   * total number of rows
   */
  private int numberOfRows;

  /**
   * pointers offsets
   */
  private long dataPointersOffsets;

  public UnsafeVariableLengthDimesionDataChunkStore(long totalSize, boolean isInvertedIdex,
      int numberOfRows) {
    super(totalSize, isInvertedIdex, numberOfRows);
    this.numberOfRows = numberOfRows;
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  @Override public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      byte[] data) {
    // first put the data, inverted index and reverse inverted index to memory
    super.putArray(invertedIndex, invertedIndexReverse, data);
    // position from where offsets will start
    this.dataPointersOffsets = this.invertedIndexReverseOffset;
    if (isExplicitSorted) {
      this.dataPointersOffsets += (long)numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }
    // As data is of variable length and data format is
    // <length in short><data><length in short><data>
    // we need to store offset of each data so data can be accessed directly
    // for example:
    //data = {0,5,1,2,3,4,5,0,6,0,1,2,3,4,5,0,2,8,9}
    //so value stored in offset will be position of actual data
    // [2,9,17]
    // to store this value we need to get the actual data length + 2 bytes used for storing the
    // length

    // start position will be used to store the current data position
    int startOffset = 0;
    // position from where offsets will start
    long pointerOffsets = this.dataPointersOffsets;
    // as first position will be start from 2 byte as data is stored first in the memory block
    // we need to skip first two bytes this is because first two bytes will be length of the data
    // which we have to skip
    CarbonUnsafe.getUnsafe().putInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + pointerOffsets,
        CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    // incrementing the pointers as first value is already filled and as we are storing as int
    // we need to increment the 4 bytes to set the position of the next value to set
    pointerOffsets += CarbonCommonConstants.INT_SIZE_IN_BYTE;
    // creating a byte buffer which will wrap the length of the row
    // using byte buffer as unsafe will return bytes in little-endian encoding
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    // store length of data
    byte[] length = new byte[CarbonCommonConstants.SHORT_SIZE_IN_BYTE];
    // as first offset is already stored, we need to start from the 2nd row in data array
    for (int i = 1; i < numberOfRows; i++) {
      // first copy the length of previous row
      CarbonUnsafe.getUnsafe().copyMemory(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + startOffset, length, CarbonUnsafe.BYTE_ARRAY_OFFSET,
          CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
      buffer.put(length);
      buffer.flip();
      // so current row position will be
      // previous row length + 2 bytes used for storing previous row data
      startOffset += CarbonCommonConstants.SHORT_SIZE_IN_BYTE + buffer.getShort();
      // as same byte buffer is used to avoid creating many byte buffer for each row
      // we need to clear the byte buffer
      buffer.clear();
      // now put the offset of current row, here we need to add 2 more bytes as current will
      // also have length part so we have to skip length
      CarbonUnsafe.getUnsafe().putInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + pointerOffsets,
          startOffset + CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
      // incrementing the pointers as first value is already filled and as we are storing as int
      // we need to increment the 4 bytes to set the position of the next value to set
      pointerOffsets += CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }

  }

  /**
   * Below method will be used to get the row based on row id passed
   *
   * @param rowId
   * @return row
   */
  @Override public byte[] getRow(int rowId) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplicitSorted) {
      rowId = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long)rowId
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    // now to get the row from memory block we need to do following thing
    // 1. first get the current offset
    // 2. if it's not a last row- get the next row offset
    // Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
    // else subtract the current row offset + 2 bytes(to skip the data length)
    // with complete data length
    int currentDataOffset = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets + (rowId
            * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    short length = 0;
    // calculating the length of data
    if (rowId < numberOfRows - 1) {
      int OffsetOfNextdata = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets + ((rowId + 1)
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
      length = (short) (OffsetOfNextdata - (currentDataOffset
          + CarbonCommonConstants.SHORT_SIZE_IN_BYTE));
    } else {
      // for last record we need to subtract with data length
      length = (short) (this.dataLength - currentDataOffset);
    }
    byte[] data = new byte[length];
    CarbonUnsafe.getUnsafe().copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + currentDataOffset, data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
    return data;
  }

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    byte[] value = getRow(rowId);
    DataType dt = vector.getType();
    if ((!(dt == DataTypes.STRING) && value.length == 0) || ByteUtil.UnsafeComparer.INSTANCE
        .equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, value)) {
      vector.putNull(vectorRow);
    } else {
      if (dt == DataTypes.STRING) {
        vector.putBytes(vectorRow, 0, value.length, value);
      } else if (dt == DataTypes.BOOLEAN) {
        vector.putBoolean(vectorRow, ByteUtil.toBoolean(value[0]));
      } else if (dt == DataTypes.SHORT) {
        vector.putShort(vectorRow, ByteUtil.toShort(value, 0, value.length));
      } else if (dt == DataTypes.INT) {
        vector.putInt(vectorRow, ByteUtil.toInt(value, 0, value.length));
      } else if (dt == DataTypes.LONG) {
        vector.putLong(vectorRow, DataTypeUtil
            .getDataBasedOnRestructuredDataType(value, vector.getBlockDataType(), 0,
                value.length));
      } else if (dt == DataTypes.TIMESTAMP) {
        vector.putLong(vectorRow, ByteUtil.toLong(value, 0, value.length) * 1000L);
      }
    }
  }

  /**
   * to compare the two byte array
   *
   * @param rowId index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  @Override public int compareTo(int rowId, byte[] compareValue) {
    // now to get the row from memory block we need to do following thing
    // 1. first get the current offset
    // 2. if it's not a last row- get the next row offset
    // Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
    // else subtract the current row offset
    // with complete data length get the offset of set of data
    int currentDataOffset = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets + ((long) rowId
            * CarbonCommonConstants.INT_SIZE_IN_BYTE * 1L));
    short length = 0;
    // calculating the length of data
    if (rowId < numberOfRows - 1) {
      int OffsetOfNextdata = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets + ((rowId + 1)
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
      length = (short) (OffsetOfNextdata - (currentDataOffset
          + CarbonCommonConstants.SHORT_SIZE_IN_BYTE));
    } else {
      // for last record we need to subtract with data length
      length = (short) (this.dataLength - currentDataOffset);
    }
    // as this class handles this variable length data, so filter value can be
    // smaller or bigger than than actual data, so we need to take the smaller length
    int compareResult;
    int compareLength = Math.min(length , compareValue.length);
    for (int i = 0; i < compareLength; i++) {
      compareResult = (CarbonUnsafe.getUnsafe().getByte(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + currentDataOffset) & 0xff) - (compareValue[i]
          & 0xff);
      // if compare result is not equal we can break
      if (compareResult != 0) {
        return compareResult;
      }
      // increment the offset by one as comparison is done byte by byte
      currentDataOffset++;
    }
    return length - compareValue.length;
  }

}
