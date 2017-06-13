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
package org.apache.carbondata.core.datastore.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ByteUtil;

public class BlockIndexerStorageForShort implements IndexStorage<short[]> {

  private boolean alreadySorted;

  private short[] rowIdPage;

  private short[] rowIdRlePage;

  private byte[][] dataPage;

  private short[] dataRlePage;

  private int totalSize;

  public BlockIndexerStorageForShort(byte[][] dataPage, boolean rleOnData,
      boolean isNoDictionary, boolean isSortRequired) {
    ColumnWithShortIndex[] dataWithRowId = createColumnWithIndexArray(dataPage, isNoDictionary);
    if (isSortRequired) {
      Arrays.sort(dataWithRowId);
    }
    short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
    rleEncodeOnRowId(rowIds);
    if (rleOnData) {
      rleEncodeOnData(dataWithRowId);
    }
  }

  /**
   * Create an object with each column array and respective index
   *
   * @return
   */
  private ColumnWithShortIndex[] createColumnWithIndexArray(byte[][] keyBlock,
      boolean isNoDictionary) {
    ColumnWithShortIndex[] columnWithIndexs;
    if (isNoDictionary) {
      columnWithIndexs = new ColumnWithShortIndex[keyBlock.length];
      for (short i = 0; i < columnWithIndexs.length; i++) {
        columnWithIndexs[i] = new ColumnWithShortIndexForNoDictionay(keyBlock[i], i);
      }
    } else {
      columnWithIndexs = new ColumnWithShortIndex[keyBlock.length];
      for (short i = 0; i < columnWithIndexs.length; i++) {
        columnWithIndexs[i] = new ColumnWithShortIndex(keyBlock[i], i);
      }
    }
    return columnWithIndexs;
  }

  private short[] extractDataAndReturnRowId(ColumnWithShortIndex[] dataWithRowId,
      byte[][] keyBlock) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      keyBlock[i] = dataWithRowId[i].getColumn();
    }
    this.dataPage = keyBlock;
    return indexes;
  }

  /**
   * It compresses depends up on the sequence numbers.
   * [1,2,3,4,6,8,10,11,12,13] is translated to [1,4,6,8,10,13] and [0,6]. In
   * first array the start and end of sequential numbers and second array
   * keeps the indexes of where sequential numbers starts. If there is no
   * sequential numbers then the same array it returns with empty second
   * array.
   *
   * @param rowIds
   */
  private void rleEncodeOnRowId(short[] rowIds) {
    List<Short> list = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    List<Short> map = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    int k = 0;
    int i = 1;
    for (; i < rowIds.length; i++) {
      if (rowIds[i] - rowIds[i - 1] == 1) {
        k++;
      } else {
        if (k > 0) {
          map.add(((short) list.size()));
          list.add(rowIds[i - k - 1]);
          list.add(rowIds[i - 1]);
        } else {
          list.add(rowIds[i - 1]);
        }
        k = 0;
      }
    }
    if (k > 0) {
      map.add(((short) list.size()));
      list.add(rowIds[i - k - 1]);
      list.add(rowIds[i - 1]);
    } else {
      list.add(rowIds[i - 1]);
    }
    double compressionPercentage = (((list.size() + map.size()) * 100) / rowIds.length);
    if (compressionPercentage > 70) {
      rowIdPage = rowIds;
    } else {
      rowIdPage = convertToArray(list);
    }
    if (rowIds.length == rowIdPage.length) {
      rowIdRlePage = new short[0];
    } else {
      rowIdRlePage = convertToArray(map);
    }
    if (rowIdPage.length == 2 && rowIdRlePage.length == 1) {
      alreadySorted = true;
    }
  }

  private short[] convertToArray(List<Short> list) {
    short[] shortArray = new short[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }

  /**
   * @return the alreadySorted
   */
  public boolean isAlreadySorted() {
    return alreadySorted;
  }

  /**
   * @return the rowIdPage
   */
  public short[] getRowIdPage() {
    return rowIdPage;
  }

  /**
   * @return the rowIdRlePage
   */
  public short[] getRowIdRlePage() {
    return rowIdRlePage;
  }

  /**
   * @return the dataPage
   */
  public byte[][] getDataPage() {
    return dataPage;
  }

  private void rleEncodeOnData(ColumnWithShortIndex[] dataWithRowId) {
    byte[] prvKey = dataWithRowId[0].getColumn();
    List<ColumnWithShortIndex> list = new ArrayList<ColumnWithShortIndex>(dataWithRowId.length / 2);
    list.add(dataWithRowId[0]);
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < dataWithRowId.length; i++) {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(prvKey, dataWithRowId[i].getColumn()) != 0) {
        prvKey = dataWithRowId[i].getColumn();
        list.add(dataWithRowId[i]);
        map.add(start);
        map.add(counter);
        start += counter;
        counter = 1;
        continue;
      }
      counter++;
    }
    map.add(start);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    boolean useRle = (((list.size() + map.size()) * 100) / dataWithRowId.length) < 70;
    if (useRle) {
      this.dataPage = convertToKeyArray(list);
      dataRlePage = convertToArray(map);
    } else {
      this.dataPage = convertToKeyArray(dataWithRowId);
      dataRlePage = new short[0];
    }
  }

  private byte[][] convertToKeyArray(ColumnWithShortIndex[] indexes) {
    byte[][] shortArray = new byte[indexes.length][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = indexes[i].getColumn();
      totalSize += shortArray[i].length;
    }
    return shortArray;
  }

  private byte[][] convertToKeyArray(List<ColumnWithShortIndex> list) {
    byte[][] shortArray = new byte[list.size()][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i).getColumn();
      totalSize += shortArray[i].length;
    }
    return shortArray;
  }

  public short[] getDataRlePage() {
    return dataRlePage;
  }

  @Override public int getTotalSize() {
    return totalSize;
  }

  @Override public byte[] getMin() {
    return dataPage[0];
  }

  @Override public byte[] getMax() {
    return dataPage[dataPage.length - 1];
  }

}
