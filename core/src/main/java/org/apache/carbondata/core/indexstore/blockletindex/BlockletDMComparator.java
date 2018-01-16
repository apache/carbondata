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
package org.apache.carbondata.core.indexstore.blockletindex;

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.carbondata.core.indexstore.row.DataMapRow;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Data map comparator
 */
public class BlockletDMComparator implements Comparator<DataMapRow> {

  /**
   * no dictionary column value is of variable length so in each column value
   * it will -1
   */
  private static final int NO_DCITIONARY_COLUMN_VALUE = -1;

  /**
   * sized of the short value in bytes
   */
  private static final short SHORT_SIZE_IN_BYTES = 2;

  private int[] eachColumnValueSize;

  /**
   * the number of no dictionary columns in SORT_COLUMNS
   */
  private int numberOfNoDictSortColumns;

  /**
   * the number of columns in SORT_COLUMNS
   */
  private int numberOfSortColumns;

  public BlockletDMComparator(int[] eachColumnValueSize, int numberOfSortColumns,
      int numberOfNoDictSortColumns) {
    this.eachColumnValueSize = eachColumnValueSize;
    this.numberOfNoDictSortColumns = numberOfNoDictSortColumns;
    this.numberOfSortColumns = numberOfSortColumns;
  }

  @Override public int compare(DataMapRow first, DataMapRow second) {
    int dictionaryKeyOffset = 0;
    int nonDictionaryKeyOffset = 0;
    int compareResult = 0;
    int processedNoDictionaryColumn = numberOfNoDictSortColumns;
    byte[][] firstBytes = splitKey(first.getByteArray(0));
    byte[][] secondBytes = splitKey(second.getByteArray(0));
    byte[] firstNoDictionaryKeys = firstBytes[1];
    ByteBuffer firstNoDictionaryKeyBuffer = ByteBuffer.wrap(firstNoDictionaryKeys);
    byte[] secondNoDictionaryKeys = secondBytes[1];
    ByteBuffer secondNoDictionaryKeyBuffer = ByteBuffer.wrap(secondNoDictionaryKeys);
    int actualOffset = 0;
    int actualOffset1 = 0;
    int firstNoDcitionaryLength = 0;
    int secondNodeDictionaryLength = 0;

    for (int i = 0; i < numberOfSortColumns; i++) {

      if (eachColumnValueSize[i] != NO_DCITIONARY_COLUMN_VALUE) {
        byte[] firstDictionaryKeys = firstBytes[0];
        byte[] secondDictionaryKeys = secondBytes[0];
        compareResult = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(firstDictionaryKeys, dictionaryKeyOffset, eachColumnValueSize[i],
                secondDictionaryKeys, dictionaryKeyOffset, eachColumnValueSize[i]);
        dictionaryKeyOffset += eachColumnValueSize[i];
      } else {
        if (processedNoDictionaryColumn > 1) {
          actualOffset = firstNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          firstNoDcitionaryLength =
              firstNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset + SHORT_SIZE_IN_BYTES)
                  - actualOffset;
          actualOffset1 = secondNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          secondNodeDictionaryLength =
              secondNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset + SHORT_SIZE_IN_BYTES)
                  - actualOffset1;
          compareResult = ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(firstNoDictionaryKeys, actualOffset, firstNoDcitionaryLength,
                  secondNoDictionaryKeys, actualOffset1, secondNodeDictionaryLength);
          nonDictionaryKeyOffset += SHORT_SIZE_IN_BYTES;
          processedNoDictionaryColumn--;
        } else {
          actualOffset = firstNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          actualOffset1 = secondNoDictionaryKeyBuffer.getShort(nonDictionaryKeyOffset);
          firstNoDcitionaryLength = firstNoDictionaryKeys.length - actualOffset;
          secondNodeDictionaryLength = secondNoDictionaryKeys.length - actualOffset1;
          compareResult = ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(firstNoDictionaryKeys, actualOffset, firstNoDcitionaryLength,
                  secondNoDictionaryKeys, actualOffset1, secondNodeDictionaryLength);
        }
      }
      if (compareResult != 0) {
        return compareResult;
      }
    }

    return 0;
  }

  /**
   * Split the index key to dictionary and no dictionary.
   * @param startKey
   * @return
   */
  private byte[][] splitKey(byte[] startKey) {
    ByteBuffer buffer = ByteBuffer.wrap(startKey);
    buffer.rewind();
    int dictonaryKeySize = buffer.getInt();
    int nonDictonaryKeySize = buffer.getInt();
    byte[] dictionaryKey = new byte[dictonaryKeySize];
    buffer.get(dictionaryKey);
    byte[] nonDictionaryKey = new byte[nonDictonaryKeySize];
    buffer.get(nonDictionaryKey);
    return new byte[][] {dictionaryKey, nonDictionaryKey};
  }
}
