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
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

/**
 * Below class is responsible to store variable length dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public abstract class UnsafeVariableLengthDimensionDataChunkStore
    extends UnsafeAbstractDimensionDataChunkStore {

  /**
   * total number of rows
   */
  private int numberOfRows;

  /**
   * pointers offsets
   */
  private long dataPointersOffsets;

  /**
   * Reusable data array
   * this will be useful for vector scenario, as it will be created once and filled every time
   * if new data length is bigger than exiting data length then create new data with bigger length
   * and assign to value
   */
  private byte[] value;

  public UnsafeVariableLengthDimensionDataChunkStore(long totalSize, boolean isInvertedIdex,
      int numberOfRows) {
    super(totalSize, isInvertedIdex, numberOfRows);
    this.numberOfRows = numberOfRows;
    // initials size assigning to some random value
    this.value = new byte[20];
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param data                 data to be stored
   */
  @Override
  public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      byte[] data) {
    // first put the data, inverted index and reverse inverted index to memory
    super.putArray(invertedIndex, invertedIndexReverse, data);
    // position from where offsets will start
    this.dataPointersOffsets = this.invertedIndexReverseOffset;
    if (isExplicitSorted) {
      this.dataPointersOffsets += (long) numberOfRows * CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }
    // As data is of variable length and data format is
    // <length in short><data><length in short/int><data>
    // we need to store offset of each data so data can be accessed directly
    // for example:
    //data = {0,5,1,2,3,4,5,0,6,0,1,2,3,4,5,0,2,8,9}
    //so value stored in offset will be position of actual data
    // [2,9,17]
    // to store this value we need to get the actual data length + 2/4 bytes used for storing the
    // length

    // start position will be used to store the current data position
    int startOffset = 0;
    // as first position will be start from 2/4 byte as data is stored first in the memory block
    // we need to skip first two bytes this is because first two bytes will be length of the data
    // which we have to skip
    int [] dataOffsets = new int[numberOfRows];
    dataOffsets[0] = getLengthSize();
    // creating a byte buffer which will wrap the length of the row
    ByteBuffer buffer = ByteBuffer.wrap(data);
    for (int i = 1; i < numberOfRows; i++) {
      buffer.position(startOffset);
      // so current row position will be
      // previous row length + 2/4 bytes used for storing previous row data
      startOffset += getLengthFromBuffer(buffer) + getLengthSize();
      // as same byte buffer is used to avoid creating many byte buffer for each row
      // we need to clear the byte buffer
      dataOffsets[i] = startOffset + getLengthSize();
    }
    CarbonUnsafe.getUnsafe().copyMemory(dataOffsets, CarbonUnsafe.INT_ARRAY_OFFSET,
        dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets,
        dataOffsets.length * CarbonCommonConstants.INT_SIZE_IN_BYTE);
  }

  protected abstract int getLengthSize();
  protected abstract int getLengthFromBuffer(ByteBuffer byteBuffer);

  /**
   * Below method will be used to get the row based on row id passed
   * Getting the row from unsafe works in below logic
   * 1. if inverted index is present then get the row id based on reverse inverted index
   * 2. get the current row id data offset
   * 3. if it's not a last row- get the next row offset
   * Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
   * 4. if it's last row
   * subtract the current row offset + 2 bytes(to skip the data length) with complete data length
   * @param rowId
   * @return row
   */
  @Override
  public byte[] getRow(int rowId) {
    // get the actual row id
    rowId = getRowId(rowId);
    // get offset of data in unsafe
    int currentDataOffset = getOffSet(rowId);
    // get the data length
    int length = getLength(rowId, currentDataOffset);
    // create data array
    byte[] data = new byte[length];
    // fill the row data
    fillRowInternal(length, data, currentDataOffset);
    return data;
  }

  /**
   * Returns the actual row id for data
   * if inverted index is present then get the row id based on reverse inverted index
   * otherwise return the same row id
   * @param rowId row id
   * @return actual row id
   */
  private int getRowId(int rowId) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplicitSorted) {
      rowId = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.invertedIndexReverseOffset + ((long)rowId
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
    }
    return rowId;
  }

  /**
   * get data offset based on current row id
   * @param rowId row id
   * @return data offset
   */
  private int getOffSet(int rowId) {
    return CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets + ((long)rowId
            * CarbonCommonConstants.INT_SIZE_IN_BYTE));
  }

  /**
   * To get the length of data for row id
   * if it's not a last row- get the next row offset
   * Subtract the current row offset + 2/4 bytes(to skip the data length) with next row offset
   * if it's last row
   * subtract the current row offset + 2/4 bytes(to skip the data length) with complete data length
   * @param rowId rowId
   * @param currentDataOffset current data offset
   * @return length of row
   */
  private int getLength(int rowId, int currentDataOffset) {
    int length = 0;
    // calculating the length of data
    if (rowId < numberOfRows - 1) {
      int OffsetOfNextdata = CarbonUnsafe.getUnsafe().getInt(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + this.dataPointersOffsets + ((rowId + 1)
              * CarbonCommonConstants.INT_SIZE_IN_BYTE));
      length = OffsetOfNextdata - (currentDataOffset + getLengthSize());
    } else {
      // for last record we need to subtract with data length
      length = this.dataLength - currentDataOffset;
    }
    return length;
  }

  /**
   * Return the row from unsafe
   * @param length length of the data
   * @param data data array
   * @param currentDataOffset current data offset
   */
  private void fillRowInternal(int length, byte[] data, int currentDataOffset) {
    CarbonUnsafe.getUnsafe().copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + currentDataOffset, data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, length);
  }

  /**
   *
   * Below method will be used to put the row in vector based on row id passed
   * Getting the row from unsafe works in below logic
   * 1. if inverted index is present then get the row id based on reverse inverted index
   * 2. get the current row id data offset
   * 3. if it's not a last row- get the next row offset
   * Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
   * 4. if it's last row
   * subtract the current row offset + 2 bytes(to skip the data length) with complete data length
   * @param rowId row id
   * @param vector vector to be filled
   * @param vectorRow vector row id
   *
   */
  @Override
  public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    // get the row id from reverse inverted index based on row id
    rowId = getRowId(rowId);
    // get the current row offset
    int currentDataOffset = getOffSet(rowId);
    // get the row data length
    int length = getLength(rowId, currentDataOffset);
    // check if value length is less the current data length
    // then create a new array else use the same
    if (length > value.length) {
      value = new byte[length];
    }
    // get the row from unsafe
    fillRowInternal(length, value, currentDataOffset);
    QueryUtil.putDataToVector(vector, value, vectorRow, length);
  }

  /**
   * to compare the two byte array
   *
   * @param rowId index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  @Override
  public int compareTo(int rowId, byte[] compareValue) {
    int currentDataOffset = getOffSet(rowId);;
    int length = getLength(rowId, currentDataOffset);
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
