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

package org.apache.carbondata.processing.store;

import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.datastore.row.WriteStepRowUtil;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.processing.util.NonDictionaryUtil;

public class TablePageKey {
  private int pageSize;

  private byte[][] currentNoDictionaryKey;

  // MDK start key
  private byte[] startKey;

  // MDK end key
  private byte[] endKey;

  // startkey for no dictionary columns
  private byte[][] noDictStartKey;

  // endkey for no diciotn
  private byte[][] noDictEndKey;

  // startkey for no dictionary columns after packing into one column
  private byte[] packedNoDictStartKey;

  // endkey for no dictionary columns after packing into one column
  private byte[] packedNoDictEndKey;

  private CarbonFactDataHandlerModel model;

  TablePageKey(CarbonFactDataHandlerModel model, int pageSize) {
    this.model = model;
    this.pageSize = pageSize;
  }

  /** update all keys based on the input row */
  void update(int rowId, CarbonRow row) throws KeyGenException {
    byte[] currentMDKey = WriteStepRowUtil.getMdk(row, model.getMDKeyGenerator());
    if (model.getNoDictionaryCount() > 0 || model.getComplexIndexMap().size() > 0) {
      currentNoDictionaryKey = WriteStepRowUtil.getNoDictAndComplexDimension(row);
    }
    if (rowId == 0) {
      startKey = currentMDKey;
      noDictStartKey = currentNoDictionaryKey;
    }
    endKey = currentMDKey;
    noDictEndKey = currentNoDictionaryKey;
    if (rowId == pageSize - 1) {
      finalizeKeys();
    }
  }

  /** update all keys if SORT_COLUMNS option is used when creating table */
  private void finalizeKeys() {
    // If SORT_COLUMNS is used, may need to update start/end keys since the they may
    // contains dictionary columns that are not in SORT_COLUMNS, which need to be removed from
    // start/end key
    int numberOfDictSortColumns = model.getSegmentProperties().getNumberOfDictSortColumns();
    if (numberOfDictSortColumns > 0) {
      // if SORT_COLUMNS contain dictionary columns
      int[] keySize = model.getSegmentProperties().getFixedLengthKeySplitter().getBlockKeySize();
      if (keySize.length > numberOfDictSortColumns) {
        // if there are some dictionary columns that are not in SORT_COLUMNS, it will come to here
        int newMdkLength = 0;
        for (int i = 0; i < numberOfDictSortColumns; i++) {
          newMdkLength += keySize[i];
        }
        byte[] newStartKeyOfSortKey = new byte[newMdkLength];
        byte[] newEndKeyOfSortKey = new byte[newMdkLength];
        System.arraycopy(startKey, 0, newStartKeyOfSortKey, 0, newMdkLength);
        System.arraycopy(endKey, 0, newEndKeyOfSortKey, 0, newMdkLength);
        startKey = newStartKeyOfSortKey;
        endKey = newEndKeyOfSortKey;
      }
    } else {
      startKey = new byte[0];
      endKey = new byte[0];
    }

    // Do the same update for noDictionary start/end Key
    int numberOfNoDictSortColumns = model.getSegmentProperties().getNumberOfNoDictSortColumns();
    if (numberOfNoDictSortColumns > 0) {
      // if sort_columns contain no-dictionary columns
      if (noDictStartKey.length > numberOfNoDictSortColumns) {
        byte[][] newNoDictionaryStartKey = new byte[numberOfNoDictSortColumns][];
        byte[][] newNoDictionaryEndKey = new byte[numberOfNoDictSortColumns][];
        System.arraycopy(
            noDictStartKey, 0, newNoDictionaryStartKey, 0, numberOfNoDictSortColumns);
        System.arraycopy(
            noDictEndKey, 0, newNoDictionaryEndKey, 0, numberOfNoDictSortColumns);
        noDictStartKey = newNoDictionaryStartKey;
        noDictEndKey = newNoDictionaryEndKey;
      }
      packedNoDictStartKey =
          NonDictionaryUtil.packByteBufferIntoSingleByteArray(noDictStartKey);
      packedNoDictEndKey =
          NonDictionaryUtil.packByteBufferIntoSingleByteArray(noDictEndKey);
    }
  }

  public byte[] getStartKey() {
    return startKey;
  }

  public byte[] getEndKey() {
    return endKey;
  }

  public byte[] getNoDictStartKey() {
    return packedNoDictStartKey;
  }

  public byte[] getNoDictEndKey() {
    return packedNoDictEndKey;
  }

  public int getPageSize() {
    return pageSize;
  }
}