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
 * Below class will be used to store bitmap dimension data
 */
public class SafeBitMapDimensionDataChunkStore extends SafeAbsractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;
  private BitMapDataFilter bitMapFilter = new BitMapDataFilter();

  public SafeBitMapDimensionDataChunkStore(List<Integer> bitmap_encoded_dictionaries,
      List<Integer> bitmap_data_pages_offset, int columnValueSize) {
    super(false);
    this.columnValueSize = columnValueSize;
    int arraySize = bitmap_encoded_dictionaries.size();
    bitMapFilter.bitmap_encoded_dictionaries = new byte[arraySize][];
    bitMapFilter.bitmap_data_pages_offset = new int[bitmap_data_pages_offset.size()];
    for (byte i = 0; i < arraySize; i++) {
      bitMapFilter.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
      bitMapFilter.bitmap_data_pages_offset[i] = bitmap_data_pages_offset.get(i + 1);
    }
  }

  /**
   * Below method will be used to put the rows and its metadata in offheap
   *
   * @param invertedIndex
   *          inverted index to be stored
   * @param invertedIndexReverse
   *          inverted index reverse to be stored
   * @param data
   *          data to be stored
   */
  @Override
  public void putArray(final int[] invertedIndex, final int[] invertedIndexReverse,
      final byte[] data) {
    bitMapFilter.bitmap_data_pages_offset[bitMapFilter.bitmap_encoded_dictionaries.length]
        = data.length;
    bitMapFilter.data = data;
    // loadDictionaryDataIndex();
    bitMapFilter.bitSets = new BitSet[bitMapFilter.bitmap_encoded_dictionaries.length];
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
    data[0] = bitMapFilter.data[rowId];
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
    return bitMapFilter.data[index];
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

    System.arraycopy(bitMapFilter.data[rowId], 0, buffer,
        offset, columnValueSize);
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
        bitMapFilter.bitmap_encoded_dictionaries[(int)bitMapFilter.data[index]], 0, columnValueSize,
        compareValue, 0, columnValueSize);
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

  public static class BitMapDataFilter {
    public byte[][] bitmap_encoded_dictionaries;
    public int[] bitmap_data_pages_offset;
    public BitSet[] bitSets;
    public byte[] data;

    public BitMapDataFilter() {
    }

    public void setData(List<Integer> bitmap_encoded_dictionaries,
        List<Integer> bitmap_data_pages_offset, int columnValueSize) {
      int arraySize = bitmap_encoded_dictionaries.size();
      this.bitmap_encoded_dictionaries = new byte[arraySize][];
      this.bitmap_data_pages_offset = new int[bitmap_data_pages_offset.size()];
      for (byte i = 0; i < arraySize; i++) {
        this.bitmap_encoded_dictionaries[i] = ByteUtil
            .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
        this.bitmap_data_pages_offset[i] = bitmap_data_pages_offset.get(i + 1);
      }
    }

    /**
     * apply Filter
     *
     * @param filterValues
     * @param operator
     * @return BitSet
     */
    public BitSet applyFilter_backup(byte[][] filterValues, FilterOperator operator,
        int numerOfRows) {
      BitSet bitSet = null;
      for (byte i = 0; i < filterValues.length; i++) {
        int index = CarbonUtil.binarySearch(bitmap_encoded_dictionaries, 0,
            bitmap_encoded_dictionaries.length - 1, filterValues[i]);
        if (index >= 0) {
          if (bitSet == null) {
            bitSet = loadBitSet(index);
          } else {
            bitSet.or(loadBitSet(index));
          }
        }
      }
      if (FilterOperator.NOT_IN.equals(operator)) {
        if (bitSet == null) {
          bitSet = new BitSet(numerOfRows);
        }
        bitSet.flip(0, numerOfRows);
      }
      return bitSet;
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
      // bitMapData = null;
      // bitmap_encoded_dictionaries = null;
      // bitmap_data_pages_offset = null;
      // System.out.println("clear:" + this);
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
      BitSet bitSet = this.bitSets[index];
      if (bitSet != null) {
        return bitSet;
      }

      int pageOffSet = bitmap_data_pages_offset[index];
      int pageLength;
      pageLength = bitmap_data_pages_offset[index + 1] - bitmap_data_pages_offset[index];

      byte[] bitSetData = new byte[pageLength];
      System.arraycopy(this.data, pageOffSet, bitSetData, 0, pageLength);
      bitSet = BitSet.valueOf(bitSetData);
      bitSets[index] = bitSet;
      return bitSet;
    }
  }
}
