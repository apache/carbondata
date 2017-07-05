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

package org.apache.carbondata.core.datastore.chunk.store.impl.safe;

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.util.ByteUtil;

import org.apache.spark.sql.types.BooleanType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.LongType;
import org.apache.spark.sql.types.ShortType;
import org.apache.spark.sql.types.StringType;

/**
 * Below class is responsible to store variable length dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public class SafeVariableLengthDimensionDataChunkStore extends SafeAbsractDimensionDataChunkStore {

  /**
   * total number of rows
   */
  private int numberOfRows;

  /**
   * offset of the data this will be used during search, as we can directly jump
   * to particular location
   */
  private int[] dataOffsets;

  public SafeVariableLengthDimensionDataChunkStore(boolean isInvertedIndex, int numberOfRows) {
    super(isInvertedIndex);
    this.numberOfRows = numberOfRows;
    this.dataOffsets = new int[numberOfRows];
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
    // as first position will be start from 2 byte as data is stored first in the memory block
    // we need to skip first two bytes this is because first two bytes will be length of the data
    // which we have to skip
    dataOffsets[0] = CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    // creating a byte buffer which will wrap the length of the row
    ByteBuffer buffer = ByteBuffer.wrap(data);
    for (int i = 1; i < numberOfRows; i++) {
      buffer.position(startOffset);
      // so current row position will be
      // previous row length + 2 bytes used for storing previous row data
      startOffset += buffer.getShort() + CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
      // as same byte buffer is used to avoid creating many byte buffer for each row
      // we need to clear the byte buffer
      dataOffsets[i] = startOffset + CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    }
  }

  @Override public byte[] getRow(int rowId) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplictSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    // now to get the row from memory block we need to do following thing
    // 1. first get the current offset
    // 2. if it's not a last row- get the next row offset
    // Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
    // else subtract the current row offset with complete data
    // length get the offset of set of data
    int currentDataOffset = dataOffsets[rowId];
    short length = 0;
    // calculating the length of data
    if (rowId < numberOfRows - 1) {
      length = (short) (dataOffsets[rowId + 1] - (currentDataOffset
          + CarbonCommonConstants.SHORT_SIZE_IN_BYTE));
    } else {
      // for last record
      length = (short) (this.data.length - currentDataOffset);
    }
    byte[] currentRowData = new byte[length];
    System.arraycopy(data, currentDataOffset, currentRowData, 0, length);
    return currentRowData;
  }

  @Override public void fillRow(int rowId, CarbonColumnVector vector, int vectorRow) {
    // if column was explicitly sorted we need to get the rowid based inverted index reverse
    if (isExplictSorted) {
      rowId = invertedIndexReverse[rowId];
    }
    // now to get the row from memory block we need to do following thing
    // 1. first get the current offset
    // 2. if it's not a last row- get the next row offset
    // Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
    // else subtract the current row offset with complete data
    // length get the offset of set of data
    int currentDataOffset = dataOffsets[rowId];
    short length = 0;
    // calculating the length of data
    if (rowId < numberOfRows - 1) {
      length = (short) (dataOffsets[rowId + 1] - (currentDataOffset
          + CarbonCommonConstants.SHORT_SIZE_IN_BYTE));
    } else {
      // for last record
      length = (short) (this.data.length - currentDataOffset);
    }
    if (ByteUtil.UnsafeComparer.INSTANCE.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY, 0,
        CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length, data, currentDataOffset, length)) {
      vector.putNull(vectorRow);
    } else {
      DataType dt = vector.getType();
      if (dt instanceof StringType) {
        vector.putBytes(vectorRow, currentDataOffset, length, data);
      } else if (dt instanceof BooleanType) {
        vector.putBoolean(vectorRow, ByteUtil.toBoolean(data[currentDataOffset]));
      } else if (dt instanceof ShortType) {
        vector.putShort(vectorRow, ByteUtil.toShort(data, currentDataOffset, length));
      } else if (dt instanceof IntegerType) {
        vector.putInt(vectorRow, ByteUtil.toInt(data, currentDataOffset, length));
      } else if (dt instanceof LongType) {
        vector.putLong(vectorRow, ByteUtil.toLong(data, currentDataOffset, length));
      }
    }
  }

  @Override public int compareTo(int index, byte[] compareValue) {
    // now to get the row from memory block we need to do following thing
    // 1. first get the current offset
    // 2. if it's not a last row- get the next row offset
    // Subtract the current row offset + 2 bytes(to skip the data length) with next row offset
    // else subtract the current row offset with complete data
    // length

    // get the offset of set of data
    int currentDataOffset = dataOffsets[index];
    short length = 0;
    // calculating the length of data
    if (index < numberOfRows - 1) {
      length = (short) (dataOffsets[index + 1] - (currentDataOffset
          + CarbonCommonConstants.SHORT_SIZE_IN_BYTE));
    } else {
      // for last record
      length = (short) (this.data.length - currentDataOffset);
    }
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(data, currentDataOffset, length, compareValue, 0, compareValue.length);
  }
}
