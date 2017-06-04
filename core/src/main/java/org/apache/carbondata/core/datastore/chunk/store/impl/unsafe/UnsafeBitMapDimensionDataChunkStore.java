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

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.scan.filter.executer.AbstractFilterExecuter.FilterOperator;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class is responsible to store bitmap encoded dimension data chunk in
 * memory Memory occupied can be on heap or offheap using unsafe interface
 */
public class UnsafeBitMapDimensionDataChunkStore
    extends UnsafeAbstractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;
  private byte[][] bitmap_encoded_dictionaries;
  private int[] bitmap_data_pages_offset;
  private byte[] bitMapData;

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
    this.bitmap_encoded_dictionaries = new byte[arraySize][];
    this.bitmap_data_pages_offset = new int[bitmap_data_pages_offset.size()];
    for (int i = 0; i < arraySize; i++) {
      this.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
      this.bitmap_data_pages_offset[i] = bitmap_data_pages_offset.get(i);
    }
    this.bitmap_data_pages_offset[arraySize] = bitmap_data_pages_offset.get(arraySize);
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
    this.dataLength = bitmap_data_pages_offset[1];
    // copy the data to memory
    CarbonUnsafe.unsafe
        .copyMemory(rawData, CarbonUnsafe.BYTE_ARRAY_OFFSET, dataPageMemoryBlock.getBaseObject(),
            dataPageMemoryBlock.getBaseOffset(), this.dataLength);
    this.bitMapData = new byte[rawData.length - bitmap_data_pages_offset[1]];
    System.arraycopy(rawData, bitmap_data_pages_offset[1], bitMapData, 0, bitMapData.length);

  }

  /**
   * apply Filter
   *
   * @param filterValues
   * @param operator
   * @return BitSet
   */
  public BitSet applyFilter(byte[][] filterValues, FilterOperator operator, int numerOfRows) {

    byte[] inDicts = new byte[bitmap_encoded_dictionaries.length];
    byte inCnt = 0;
    for (byte i = 0; i < bitmap_encoded_dictionaries.length; i++) {
      int index = CarbonUtil.binarySearch(filterValues, 0, filterValues.length - 1,
          bitmap_encoded_dictionaries[i]);
      if (index >= 0) {
        inDicts[i] = 1;
        inCnt++;
      }
    }
    BitSet bitset = null;
    if (FilterOperator.NOT_IN.equals(operator)) {
      bitset = getBitSetResult(numerOfRows, inDicts, true, inCnt);
    } else {
      bitset = getBitSetResult(numerOfRows, inDicts, false, inCnt);
    }
    bitMapData = null;
    bitmap_encoded_dictionaries = null;
    bitmap_data_pages_offset = null;
    return bitset;
  }

  private BitSet getBitSetResult(int numerOfRows, byte[] inDicts, boolean notInFlg, byte inCnt) {

    if ((notInFlg && inCnt == bitmap_encoded_dictionaries.length) || (!notInFlg && inCnt == 0)) {
      return null;
    }
    if ((!notInFlg && inCnt == bitmap_encoded_dictionaries.length) || (notInFlg && inCnt == 0)) {
      if (bitmap_encoded_dictionaries.length == 1) {
        return loadBitSet(0);
      }
      BitSet resultBitSet = new BitSet(numerOfRows);
      resultBitSet.flip(0, numerOfRows);
      return resultBitSet;
    }

    if (inCnt << 2 < bitmap_encoded_dictionaries.length) {
      return bitSetOr(inDicts, notInFlg, numerOfRows, (byte) 1);
    } else {
      return bitSetOr(inDicts, !notInFlg, numerOfRows, (byte) 0);
    }
  }

  private BitSet bitSetOr(byte[] bitSetList, boolean flipFlg, int numerOfRows, byte equalValue) {
    BitSet resultBitSet = null;
    for (byte i = 0; i < bitSetList.length; i++) {
      if (bitSetList[i] == equalValue) {
        if (resultBitSet == null) {
          resultBitSet = loadBitSet(i);
        } else {
          resultBitSet.or(loadBitSet(i));
        }
      }
    }
    if (flipFlg) {
      resultBitSet.flip(0, numerOfRows);
    }
    return resultBitSet;
  }

  private BitSet loadBitSet(int index) {
    int tempIndex = index + 1;
    int pageOffSet = bitmap_data_pages_offset[tempIndex] - bitmap_data_pages_offset[1];
    int pageLength;
    if (tempIndex + 1 == bitmap_data_pages_offset.length) {
      pageLength = bitMapData.length - pageOffSet;
    } else {
      pageLength = bitmap_data_pages_offset[tempIndex + 1] - bitmap_data_pages_offset[tempIndex];
    }
    byte[] bitSetData = new byte[pageLength];
    System.arraycopy(bitMapData, pageOffSet, bitSetData, 0, pageLength);
    BitSet bitSet = BitSet.valueOf(bitSetData);
    return bitSet;
  }
}