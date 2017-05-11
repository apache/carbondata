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
import org.apache.carbondata.core.util.BitSetGroup;
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
  private byte[][] bitmap_encoded_dictionaries;
  private int[] bitmap_data_pages_offset;
  private BitSetGroup bitSetGroup;
  private boolean isGeneratedBitSetFlg = false;
  private byte[] dictionaryData;

  public SafeBitMapDimensionDataChunkStore(List<Integer> bitmap_encoded_dictionaries,
      List<Integer> bitmap_data_pages_length, int columnValueSize) {
    super(false);
    this.columnValueSize = columnValueSize;
    int arraySize = bitmap_encoded_dictionaries.size();
    this.bitmap_encoded_dictionaries = new byte[arraySize][];
    this.bitmap_data_pages_offset = new int[bitmap_data_pages_length.size()];
    for (int i = 0; i < arraySize; i++) {
      this.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
      this.bitmap_data_pages_offset[i] = bitmap_data_pages_length.get(i);
    }
    this.bitmap_data_pages_offset[arraySize] = bitmap_data_pages_length.get(arraySize);

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
    this.data = data;
    loadDictionaryDataIndex();
    bitSetGroup = new BitSetGroup(bitmap_encoded_dictionaries.length);

    // loadAllBitSets();
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
    data[0] = dictionaryData[rowId];
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
    loadAllBitSets();
    return dictionaryData[index];
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

    System.arraycopy(dictionaryData[rowId], 0, buffer,
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
        this.bitmap_encoded_dictionaries[(int)dictionaryData[index]], 0, columnValueSize,
        compareValue, 0, columnValueSize);
  }

  /**
   * apply Filter
   *
   * @param filterValues
   * @param operator
   * @return BitSet
   */
  public BitSet applyFilter(byte[][] filterValues, FilterOperator operator, int numerOfRows) {
    BitSet bitSet = null;
    for (int i = 0; i < filterValues.length; i++) {
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
    isGeneratedBitSetFlg = true;
    if (FilterOperator.NOT_IN.equals(operator)) {
      if (bitSet == null) {
        bitSet = new BitSet(numerOfRows);
      }
      // System.out.println("bitSet.size(): " + bitSet.size());
      // System.out.println("bitSet.length(): " + bitSet.length());
      bitSet.flip(0, numerOfRows);
      // System.out.println("bitSet2: " + bitSet.cardinality());
    }
    return bitSet;
  }

  private void loadAllBitSets() {
    if (isGeneratedBitSetFlg) {
      return;
    }
    for (int i = 0; i < bitmap_encoded_dictionaries.length; i++) {
      bitSetGroup.setBitSet(loadBitSet(i), i);
    }
    isGeneratedBitSetFlg = true;
  }
  private BitSet loadBitSet(int index) {
    BitSet bitSet = this.bitSetGroup.getBitSet(index);
    if (bitSet != null) {
      return bitSet;
    }
    int pageOffSet = bitmap_data_pages_offset[index];
    int pageLength =  bitmap_data_pages_offset[index + 1] - bitmap_data_pages_offset[index];
    // System.out.println("pageLength: " + pageLength);
    byte[] bitSetData = new byte[pageLength];
    System.arraycopy(this.data, pageOffSet, bitSetData, 0, pageLength);
    bitSet = BitSet.valueOf(bitSetData);
    bitSetGroup.setBitSet(bitSet, index);
    return bitSet;
  }

  private void loadDictionaryDataIndex() {

    int pageOffSet = bitmap_data_pages_offset[bitmap_data_pages_offset.length - 1];
    int pageLength = this.data.length - pageOffSet;
    dictionaryData = new byte[pageLength];
    System.arraycopy(this.data, pageOffSet, dictionaryData, 0, pageLength);
    // System.out.println("dictIndex.toString(): " + dictIndex);
    // System.out.println("dictIndex.length(): " + dictIndex.length);
  }
}
