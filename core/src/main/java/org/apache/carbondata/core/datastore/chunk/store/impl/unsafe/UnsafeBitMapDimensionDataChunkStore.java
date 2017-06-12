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

import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.datastore.chunk.store.impl.safe.SafeBitMapDimensionDataChunkStore;
import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.scan.filter.executer.AbstractFilterExecuter.FilterOperator;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class is responsible to store bitmap encoded dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public class UnsafeBitMapDimensionDataChunkStore
    extends UnsafeAbstractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  public int columnValueSize;
  private SafeBitMapDimensionDataChunkStore.BitMapDataFilter bitMapFilter =
      new SafeBitMapDimensionDataChunkStore.BitMapDataFilter();
  private int baseOffSet;

  /**
   * Constructor
   *
   * @param bitmap_encoded_dictionaries
   * @param bitmap_data_pages_offset
   * @param columnValueSize value of each column
   * @param numberOfRows    total number of rows
   */
  public UnsafeBitMapDimensionDataChunkStore(List<Integer> bitmap_encoded_dictionaries,
      List<Integer> bitmap_data_pages_offset, long totalDataSize, int columnValueSize,
      int numberOfRows) {
    super(totalDataSize, false, numberOfRows);
    this.columnValueSize = columnValueSize;
    int arraySize = bitmap_encoded_dictionaries.size();
    bitMapFilter.bitmap_encoded_dictionaries = new byte[arraySize][];
    bitMapFilter.bitmap_data_pages_offset = new int[bitmap_data_pages_offset.size()];
    baseOffSet = bitmap_data_pages_offset.get(1);
    for (byte i = 0; i < arraySize; i++) {
      bitMapFilter.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
      bitMapFilter.bitmap_data_pages_offset[i] = bitmap_data_pages_offset.get(i + 1) - baseOffSet;
    }
  }

  /**
   * Below method will be used to get the row based inverted index
   *
   * @param rowId Inverted index
   */
  @Override public byte[] getRow(int rowId) {
    // creating a row
    byte[] data = new byte[columnValueSize];
    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    CarbonUnsafe.unsafe.copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + (rowId * columnValueSize), data,
        CarbonUnsafe.BYTE_ARRAY_OFFSET, columnValueSize);
    return data;
  }

  /**
   * Below method will be used to get the surrogate key of the based on the row
   * id passed
   *
   * @param index row id
   * @return surrogate key
   */
  @Override public int getSurrogate(int index) {

    // below part is to convert the byte array to surrogate value
    int startOffsetOfData = index * columnValueSize;
    int surrogate = 0;
    for (int i = 0; i < columnValueSize; i++) {
      surrogate <<= 8;
      surrogate ^= CarbonUnsafe.unsafe.getByte(dataPageMemoryBlock.getBaseObject(),
          dataPageMemoryBlock.getBaseOffset() + startOffsetOfData) & 0xFF;
      startOffsetOfData++;
    }
    return surrogate;
  }

  /**
   * Below method will be used to fill the row values to buffer array
   *
   * @param rowId  row id of the data to be filled
   * @param buffer   buffer in which data will be filled
   * @param offset off the of the buffer
   */
  @Override public void fillRow(int rowId, byte[] buffer, int offset) {

    //copy the row from memory block based on offset
    // offset position will be index * each column value length
    CarbonUnsafe.unsafe.copyMemory(dataPageMemoryBlock.getBaseObject(),
        dataPageMemoryBlock.getBaseOffset() + (rowId * columnValueSize), buffer,
        CarbonUnsafe.BYTE_ARRAY_OFFSET + offset, columnValueSize);
  }

  /**
   * @return size of each column value
   */
  @Override public int getColumnValueSize() {
    return columnValueSize;
  }

  /**
   * to compare the two byte array
   *
   * @param index        index of first byte array
   * @param compareValue value of to be compared
   * @return compare result
   */
  @Override public int compareTo(int index, byte[] compareValue) {
    // based on index we need to calculate the actual position in memory block
    index = index * columnValueSize;
    int compareResult = 0;
    for (int i = 0; i < compareValue.length; i++) {
      compareResult = (CarbonUnsafe.unsafe
          .getByte(dataPageMemoryBlock.getBaseObject(), dataPageMemoryBlock.getBaseOffset() + index)
          & 0xff) - (compareValue[i] & 0xff);
      if (compareResult != 0) {
        break;
      }
      index++;
    }
    return compareResult;
  }
  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex        inverted index to be stored
   * @param invertedIndexReverse inverted index reverse to be stored
   * @param rawData                 data to be stored
   */
  @Override public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      final byte[] rawData) {
    assert (!isMemoryOccupied);
    bitMapFilter.bitmap_data_pages_offset[bitMapFilter.bitmap_encoded_dictionaries.length]
        = rawData.length
        - baseOffSet;
    bitMapFilter.bitSets = new BitSet[bitMapFilter.bitmap_encoded_dictionaries.length];
    this.dataLength = bitMapFilter.bitmap_data_pages_offset[1];
    // copy the data to memory
    CarbonUnsafe.unsafe.copyMemory(rawData, CarbonUnsafe.BYTE_ARRAY_OFFSET,
        dataPageMemoryBlock.getBaseObject(), dataPageMemoryBlock.getBaseOffset(), this.dataLength);
    bitMapFilter.data = rawData;
    bitMapFilter.data = new byte[rawData.length - baseOffSet];
    System.arraycopy(rawData, baseOffSet, bitMapFilter.data, 0,
        bitMapFilter.data.length);

  }

  /**
   * apply Filter
   *
   * @param filterValues
   * @param operator
   * @return BitSet
   */
  @Override
  public BitSet applyFilter(byte[][] filterValues, FilterOperator operator, int numerOfRows) {
    return bitMapFilter.applyFilter(filterValues, operator, numerOfRows);
  }
}