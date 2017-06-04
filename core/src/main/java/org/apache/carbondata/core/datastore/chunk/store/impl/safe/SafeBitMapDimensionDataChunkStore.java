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

import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.scan.filter.executer.AbstractFilterExecuter.FilterOperator;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used to store bitMap dimension data
 */
public class SafeBitMapDimensionDataChunkStore extends SafeAbsractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;
  private byte[][] bitmap_encoded_dictionaries;
  private int[] bitMap_data_pages_offset;
  private byte[] bitMapData;

  public SafeBitMapDimensionDataChunkStore(List<Integer> bitMap_encoded_dictionaries,
      List<Integer> bitMap_data_pages_offset, int columnValueSize) {
    super(false);
    this.columnValueSize = columnValueSize;
    int arraySize = bitMap_encoded_dictionaries.size();
    this.bitmap_encoded_dictionaries = new byte[arraySize][];
    this.bitMap_data_pages_offset = new int[bitMap_data_pages_offset.size()];
    for (byte i = 0; i < arraySize; i++) {
      this.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitMap_encoded_dictionaries.get(i), columnValueSize);
      this.bitMap_data_pages_offset[i] = bitMap_data_pages_offset.get(i);
    }
    this.bitMap_data_pages_offset[arraySize] = bitMap_data_pages_offset.get(arraySize);
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex
   *          inverted index to be stored
   * @param invertedIndexReverse
   *          inverted index reverse to be stored
   * @param rawData
   *          data to be stored
   */
  @Override
  public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      final byte[] rawData) {
    data = new byte[bitMap_data_pages_offset[1]];
    System.arraycopy(rawData, 0, data, 0, bitMap_data_pages_offset[1]);
    this.bitMapData = new byte[rawData.length - bitMap_data_pages_offset[1]];
    System.arraycopy(rawData, bitMap_data_pages_offset[1], bitMapData, 0, bitMapData.length);
  }

  /**
   * Below method will be used to get the row data
   *
   * @param rowId
   *          Inverted index
   */
  @Override
  public byte[] getRow(int rowId) {
    byte[] data = new byte[1];
    data[0] = data[rowId];
    return data;
  }

  /**
   * Below method will be used to get the surrogate key of the based on the row
   * id passed
   *
   * @param index
   *          row id
   * @return surrogate key
   */
  @Override
  public int getSurrogate(int index) {
    // loadAllBitSets();
    return data[index];
  }

  /**
   * Below method will be used to fill the row values to buffer array
   *
   * @param rowId
   *          row id of the data to be filled
   * @param buffer
   *          buffer in which data will be filled
   * @param offset
   *          off the of the buffer
   */
  @Override
  public void fillRow(int rowId, byte[] buffer, int offset) {

    System.arraycopy(data[rowId], 0, buffer, offset, columnValueSize);
  }

  /**
   * @return size of each column value
   */
  @Override
  public int getColumnValueSize() {
    return columnValueSize;
  }

  /**
   * to compare the two byte array
   *
   * @param index
   *          index of first byte array
   * @param compareValue
   *          value of to be compared
   * @return compare result
   */
  @Override
  public int compareTo(int index, byte[] compareValue) {
    return ByteUtil.UnsafeComparer.INSTANCE.compareTo(
        this.bitmap_encoded_dictionaries[(int) data[index]], 0, columnValueSize, compareValue, 0,
        columnValueSize);
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
    bitMap_data_pages_offset = null;
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
    int pageOffSet = bitMap_data_pages_offset[tempIndex] - bitMap_data_pages_offset[1];
    int pageLength;
    if (tempIndex + 1 == bitMap_data_pages_offset.length) {
      pageLength = bitMapData.length - pageOffSet;
    } else {
      pageLength = bitMap_data_pages_offset[tempIndex + 1] - bitMap_data_pages_offset[tempIndex];
    }
    byte[] bitSetData = new byte[pageLength];
    System.arraycopy(bitMapData, pageOffSet, bitSetData, 0, pageLength);
    BitSet bitSet = BitSet.valueOf(bitSetData);
    return bitSet;
  }
}
