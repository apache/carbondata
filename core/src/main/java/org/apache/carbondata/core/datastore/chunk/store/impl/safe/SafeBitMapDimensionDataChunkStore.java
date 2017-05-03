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

import org.apache.carbondata.core.util.BitMapEncodedBitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used to store fixed length dimension data
 */
public class SafeBitMapDimensionDataChunkStore extends SafeAbsractDimensionDataChunkStore {

  /**
   * Size of each value
   */
  private int columnValueSize;
  private byte[][] bitmap_encoded_dictionaries;
  private int[] bitmap_data_pages_length;
  private BitMapEncodedBitSetGroup bitSetGroup;

  public SafeBitMapDimensionDataChunkStore(List<Integer> bitmap_encoded_dictionaries,
      List<Integer> bitmap_data_pages_length, int columnValueSize) {
    super(false);
    this.columnValueSize = columnValueSize;
    int arraySize = bitmap_encoded_dictionaries.size();
    this.bitmap_encoded_dictionaries = new byte[arraySize][];
    this.bitmap_data_pages_length = new int[arraySize];
    for (int i = 0; i < arraySize; i++) {
      this.bitmap_encoded_dictionaries[i] = ByteUtil
          .convertIntToByteArray(bitmap_encoded_dictionaries.get(i), columnValueSize);
      this.bitmap_data_pages_length[i] = bitmap_data_pages_length.get(i);
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
    this.data = data;
    bitSetGroup = new BitMapEncodedBitSetGroup(bitmap_encoded_dictionaries.length);
    int pageOffSet = 0;
    for (int i = 0; i < bitmap_encoded_dictionaries.length; i++) {
      int pageLength = bitmap_data_pages_length[i];
      // BitSet bitSet = new BitSet(pageLength);
      byte[] bitSetData = new byte[pageLength];
      // System.out.println("this.data.length: "+this.data.length);
      // System.out.println("pageOffSet: "+pageOffSet);
      // System.out.println("pageLength: "+pageLength);
      System.arraycopy(this.data, pageOffSet, bitSetData, 0, pageLength);
      BitSet bitSet = BitSet.valueOf(bitSetData);
      bitSetGroup.setBitSet(bitSet, i);
      pageOffSet += pageLength;
    }
  }

  /**
   * Below method will be used to get the row based inverted index
   *
   * @param rowId
   *          Inverted index
   */
  @Override
  public byte[] getRow(int rowId) {
    return this.bitmap_encoded_dictionaries[bitSetGroup.getBitSetIndex(rowId)];
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
    // bitSetGroup.getBitSetIndex(index);
    return ByteUtil
        .convertByteArrayToInt(this.bitmap_encoded_dictionaries[bitSetGroup.getBitSetIndex(index)]);
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

    System.arraycopy(this.bitmap_encoded_dictionaries[bitSetGroup.getBitSetIndex(rowId)], 0, buffer,
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
        this.bitmap_encoded_dictionaries[bitSetGroup.getBitSetIndex(index)], 0, columnValueSize,
        compareValue, 0, columnValueSize);
  }

  /**
   * apply Include Filter
   *
   * @param filterValues
   *          index of first byte array
   * @return BitSet
   */
  public BitSet applyIncludeFilter(byte[][] filterValues) {
    BitSet bitSet = null;
    for (int i = 0; i < filterValues.length; i++) {
      int index = CarbonUtil.binarySearch(bitmap_encoded_dictionaries, 0,
          bitmap_encoded_dictionaries.length - 1, filterValues[i]);
      if (index >= 0) {
        if (bitSet == null) {
          bitSet = this.bitSetGroup.getBitSet(index);
        } else {
          bitSet.or(this.bitSetGroup.getBitSet(index));
        }
      }
    }
    return bitSet;
  }
}
