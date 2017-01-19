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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Responsibility is store the big decimal measure data in memory,
 */
public class SafeBigDecimalMeasureChunkStore extends SafeAbstractMeasureDataChunkStore<byte[]> {

  /**
   * data chunk
   */
  private byte[] dataChunk;

  /**
   * offset of actual data
   */
  private int[] dataOffsets;

  public SafeBigDecimalMeasureChunkStore(int numberOfRows) {
    super(numberOfRows);
    this.dataOffsets = new int[numberOfRows];
  }

  @Override public void putData(byte[] data) {
    this.dataChunk = data;
    // As data is of variable length and data format is
    // <length in int><data><length in int><data>
    // we need to store offset of each data so data can be accessed directly
    // for example:
    //data = {0,0,0,0,5,1,2,3,4,5,0,0,0,0,6,0,1,2,3,4,5,0,0,0,0,2,8,9}
    //so value stored in offset will be position of actual data
    // [5,14,24]
    // to store this value we need to get the actual data length + 4 bytes used for storing the
    // length

    // start position will be used to store the current data position
    int startOffset = 0;
    // as first position will be start from 4 byte as data is stored first in the memory block
    // we need to skip first two bytes this is because first two bytes will be length of the data
    // which we have to skip
    dataOffsets[0] = CarbonCommonConstants.INT_SIZE_IN_BYTE;
    // creating a byte buffer which will wrap the length of the row
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.INT_SIZE_IN_BYTE);
    for (int i = 1; i < numberOfRows; i++) {
      buffer.put(data, startOffset, CarbonCommonConstants.INT_SIZE_IN_BYTE);
      buffer.flip();
      // so current row position will be
      // previous row length + 4 bytes used for storing previous row data
      startOffset += buffer.getInt() + CarbonCommonConstants.INT_SIZE_IN_BYTE;
      // as same byte buffer is used to avoid creating many byte buffer for each row
      // we need to clear the byte buffer
      buffer.clear();
      dataOffsets[i] = startOffset + CarbonCommonConstants.INT_SIZE_IN_BYTE;
    }
  }

  /**
   * to get the byte value
   *
   * @param index
   * @return byte value based on index
   */
  @Override public BigDecimal getBigDecimal(int index) {
    int currentDataOffset = dataOffsets[index];
    int length = 0;
    // calculating the length of data
    if (index < numberOfRows - 1) {
      length = (int) (dataOffsets[index + 1] - (currentDataOffset
          + CarbonCommonConstants.INT_SIZE_IN_BYTE));
    } else {
      // for last record
      length = (int) (this.dataChunk.length - currentDataOffset);
    }
    return DataTypeUtil.byteToBigDecimal(dataChunk, currentDataOffset, length);
  }

}
