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

package org.apache.carbondata.core.datastore.page.key;

import java.nio.ByteBuffer;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.NonDictionaryUtil;

import static org.apache.carbondata.core.datastore.row.WriteStepRowUtil.DICTIONARY_DIMENSION;

public class TablePageKey {
  private int pageSize;

  // MDK start key
  private int[] startKey;

  // MDK end key
  private int[] endKey;

  // startkey for no dictionary columns
  private Object[] noDictStartKey;

  // endkey for no diciotn
  private Object[] noDictEndKey;

  // startkey for no dictionary columns after packing into one column
  private byte[] packedNoDictStartKey;

  // endkey for no dictionary columns after packing into one column
  private byte[] packedNoDictEndKey;

  private SegmentProperties segmentProperties;
  private boolean hasNoDictionary;

  public TablePageKey(int pageSize, SegmentProperties segmentProperties,
                      boolean hasNoDictionary) {
    this.pageSize = pageSize;
    this.segmentProperties = segmentProperties;
    this.hasNoDictionary = hasNoDictionary;
  }

  /** update all keys based on the input row */
  public void update(int rowId, CarbonRow row) {
    if (rowId == 0) {
      startKey = ((int[])row.getData()[DICTIONARY_DIMENSION]);
      if (hasNoDictionary) {
        noDictStartKey = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      }
    }
    if (rowId == pageSize - 1) {
      endKey = ((int[])row.getData()[DICTIONARY_DIMENSION]);
      if (hasNoDictionary) {
        noDictEndKey = WriteStepRowUtil.getNoDictAndComplexDimension(row);
      }
      finalizeKeys();
    }
  }

  /** update all keys if SORT_COLUMNS option is used when creating table */
  private void finalizeKeys() {
    // If SORT_COLUMNS is used, may need to update start/end keys since the they may
    // contains dictionary columns that are not in SORT_COLUMNS, which need to be removed from
    // start/end key
    int numberOfDictSortColumns = segmentProperties.getNumberOfDictSortColumns();
    if (numberOfDictSortColumns > 0) {
      // if SORT_COLUMNS contain dictionary columns
      if (segmentProperties.getNumberOfDictionaryDimension() > numberOfDictSortColumns) {
        // if there are some dictionary columns that are not in SORT_COLUMNS, it will come to here
        int newMdkLength = 0;
        for (int i = 0; i < numberOfDictSortColumns; i++) {
          newMdkLength += ByteUtil.dateBytesSize();
        }
        int[] newStartKeyOfSortKey = new int[newMdkLength];
        int[] newEndKeyOfSortKey = new int[newMdkLength];
        System.arraycopy(startKey, 0, newStartKeyOfSortKey, 0, newMdkLength);
        System.arraycopy(endKey, 0, newEndKeyOfSortKey, 0, newMdkLength);
        startKey = newStartKeyOfSortKey;
        endKey = newEndKeyOfSortKey;
      }
    } else {
      startKey = new int[0];
      endKey = new int[0];
    }

    // Do the same update for noDictionary start/end Key
    int numberOfNoDictSortColumns = segmentProperties.getNumberOfNoDictSortColumns();
    if (numberOfNoDictSortColumns > 0) {
      // if sort_columns contain no-dictionary columns
      if (noDictStartKey.length > numberOfNoDictSortColumns) {
        Object[] newNoDictionaryStartKey = new Object[numberOfNoDictSortColumns];
        Object[] newNoDictionaryEndKey = new Object[numberOfNoDictSortColumns];
        System.arraycopy(
            noDictStartKey, 0, newNoDictionaryStartKey, 0, numberOfNoDictSortColumns);
        System.arraycopy(
            noDictEndKey, 0, newNoDictionaryEndKey, 0, numberOfNoDictSortColumns);
        noDictStartKey = newNoDictionaryStartKey;
        noDictEndKey = newNoDictionaryEndKey;
      }
      List<CarbonDimension> noDictSortColumns =
          CarbonTable.getNoDictSortColumns(segmentProperties.getDimensions());
      packedNoDictStartKey = NonDictionaryUtil.packByteBufferIntoSingleByteArray(
          convertKeys(noDictStartKey, noDictSortColumns));
      packedNoDictEndKey = NonDictionaryUtil.packByteBufferIntoSingleByteArray(
          convertKeys(noDictEndKey, noDictSortColumns));
    } else {
      noDictStartKey = new byte[0][];
      noDictEndKey = new byte[0][];
      packedNoDictStartKey = new byte[0];
      packedNoDictEndKey = new byte[0];
    }
  }

  private byte[][] convertKeys(Object[] keys, List<CarbonDimension> noDictSortColumns) {
    byte[][] finalKeys = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      if (keys[i] instanceof byte[]) {
        finalKeys[i] = (byte[]) keys[i];
      } else {
        finalKeys[i] = DataTypeUtil.getBytesDataDataTypeForNoDictionaryColumn(keys[i],
            noDictSortColumns.get(i).getDataType());
      }
    }
    return finalKeys;
  }

  public byte[] getNoDictStartKey() {
    return packedNoDictStartKey;
  }

  public byte[] getNoDictEndKey() {
    return packedNoDictEndKey;
  }

  public byte[] serializeStartKey() {
    byte[] updatedNoDictionaryStartKey = updateNoDictionaryStartAndEndKey(getNoDictStartKey());
    ByteBuffer buffer = ByteBuffer.allocate(
        CarbonCommonConstants.INT_SIZE_IN_BYTE + CarbonCommonConstants.INT_SIZE_IN_BYTE
            + startKey.length * 4 + updatedNoDictionaryStartKey.length);
    buffer.putInt(startKey.length * 4);
    buffer.putInt(updatedNoDictionaryStartKey.length);
    for (int key : startKey) {
      buffer.put(ByteUtil.toBytes(key));
    }
    buffer.put(updatedNoDictionaryStartKey);
    buffer.rewind();
    return buffer.array();
  }

  public byte[] serializeEndKey() {
    byte[] updatedNoDictionaryEndKey = updateNoDictionaryStartAndEndKey(getNoDictEndKey());
    ByteBuffer buffer = ByteBuffer.allocate(
        CarbonCommonConstants.INT_SIZE_IN_BYTE + CarbonCommonConstants.INT_SIZE_IN_BYTE
            + endKey.length * 4 + updatedNoDictionaryEndKey.length);
    buffer.putInt(endKey.length * 4);
    buffer.putInt(updatedNoDictionaryEndKey.length);
    for (int key : endKey) {
      buffer.put(ByteUtil.toBytes(key));
    }
    buffer.put(updatedNoDictionaryEndKey);
    buffer.rewind();
    return buffer.array();
  }

  /**
   * Below method will be used to update the no dictionary start and end key
   *
   * @param key key to be updated
   * @return return no dictionary key
   */
  public byte[] updateNoDictionaryStartAndEndKey(byte[] key) {
    if (key.length == 0) {
      return key;
    }
    // add key to byte buffer remove the length part of the data
    ByteBuffer buffer = ByteBuffer.wrap(key, 2, key.length - 2);
    // create a output buffer without length
    ByteBuffer output = ByteBuffer.allocate(key.length - 2);
    short numberOfByteToStorLength = 2;
    // as length part is removed, so each no dictionary value index
    // needs to be reshuffled by 2 bytes
    int NumberOfNoDictSortColumns = segmentProperties.getNumberOfNoDictSortColumns();
    for (int i = 0; i < NumberOfNoDictSortColumns; i++) {
      output.putShort((short) (buffer.getShort() - numberOfByteToStorLength));
    }
    // copy the data part
    while (buffer.hasRemaining()) {
      output.put(buffer.get());
    }
    output.rewind();
    return output.array();
  }
}