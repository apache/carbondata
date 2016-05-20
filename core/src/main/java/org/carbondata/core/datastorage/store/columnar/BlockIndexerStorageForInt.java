/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.datastorage.store.columnar;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.ByteUtil;

public class BlockIndexerStorageForInt implements IndexStorage<int[]> {
  private boolean alreadySorted;

  private int[] dataAfterComp;

  private int[] indexMap;

  private byte[][] keyBlock;

  private int[] dataIndexMap;

  private int totalSize;

  public BlockIndexerStorageForInt(byte[][] keyBlock, boolean compressData, boolean isNoDictionary,
      boolean isSortRequired) {
    ColumnWithIntIndex[] columnWithIndexs = createColumnWithIndexArray(keyBlock, isNoDictionary);
    if (isSortRequired) {
      Arrays.sort(columnWithIndexs);
    }
    compressMyOwnWay(extractDataAndReturnIndexes(columnWithIndexs, keyBlock));
    if (compressData) {
      compressDataMyOwnWay(columnWithIndexs);
    }
  }

  /**
   * Create an object with each column array and respective index
   *
   * @return
   */
  private ColumnWithIntIndex[] createColumnWithIndexArray(byte[][] keyBlock,
      boolean isNoDictionary) {
    ColumnWithIntIndex[] columnWithIndexs;
    if (isNoDictionary) {
      columnWithIndexs = new ColumnWithIntIndexForHighCard[keyBlock.length];
      for (int i = 0; i < columnWithIndexs.length; i++) {
        columnWithIndexs[i] = new ColumnWithIntIndexForHighCard(keyBlock[i], i);
      }

    } else {
      columnWithIndexs = new ColumnWithIntIndex[keyBlock.length];
      for (int i = 0; i < columnWithIndexs.length; i++) {
        columnWithIndexs[i] = new ColumnWithIntIndex(keyBlock[i], i);
      }
    }

    return columnWithIndexs;
  }

  private int[] extractDataAndReturnIndexes(ColumnWithIntIndex[] columnWithIndexs,
      byte[][] keyBlock) {
    int[] indexes = new int[columnWithIndexs.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = columnWithIndexs[i].getIndex();
      keyBlock[i] = columnWithIndexs[i].getColumn();
    }
    this.keyBlock = keyBlock;
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
   * @param indexes
   */
  public void compressMyOwnWay(int[] indexes) {
    List<Integer> list = new ArrayList<Integer>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    List<Integer> map = new ArrayList<Integer>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    int k = 0;
    int i = 1;
    for (; i < indexes.length; i++) {
      if (indexes[i] - indexes[i - 1] == 1) {
        k++;
      } else {
        if (k > 0) {
          map.add((list.size()));
          list.add(indexes[i - k - 1]);
          list.add(indexes[i - 1]);
        } else {
          list.add(indexes[i - 1]);
        }
        k = 0;
      }
    }
    if (k > 0) {
      map.add((list.size()));
      list.add(indexes[i - k - 1]);
      list.add(indexes[i - 1]);
    } else {
      list.add(indexes[i - 1]);
    }
    dataAfterComp = convertToArray(list);
    if (indexes.length == dataAfterComp.length) {
      indexMap = new int[0];
    } else {
      indexMap = convertToArray(map);
    }
    if (dataAfterComp.length == 2 && indexMap.length == 1) {
      alreadySorted = true;
    }
  }

  private int[] convertToArray(List<Integer> list) {
    int[] shortArray = new int[list.size()];
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
   * @return the dataAfterComp
   */
  public int[] getDataAfterComp() {
    return dataAfterComp;
  }

  /**
   * @return the indexMap
   */
  public int[] getIndexMap() {
    return indexMap;
  }

  /**
   * @return the keyBlock
   */
  public byte[][] getKeyBlock() {
    return keyBlock;
  }

  private void compressDataMyOwnWay(ColumnWithIntIndex[] indexes) {
    byte[] prvKey = indexes[0].getColumn();
    List<ColumnWithIntIndex> list =
        new ArrayList<ColumnWithIntIndex>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    list.add(indexes[0]);
    int counter = 1;
    int start = 0;
    List<Integer> map = new ArrayList<Integer>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < indexes.length; i++) {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(prvKey, indexes[i].getColumn()) != 0) {
        prvKey = indexes[i].getColumn();
        list.add(indexes[i]);
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
    this.keyBlock = convertToKeyArray(list);
    if (indexes.length == keyBlock.length) {
      dataIndexMap = new int[0];
    } else {
      dataIndexMap = convertToArray(map);
    }
  }

  private byte[][] convertToKeyArray(List<ColumnWithIntIndex> list) {
    byte[][] shortArray = new byte[list.size()][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i).getColumn();
      totalSize += shortArray[i].length;
    }
    return shortArray;
  }

  @Override public int[] getDataIndexMap() {
    return dataIndexMap;
  }

  @Override public int getTotalSize() {
    return totalSize;
  }

  @Override public byte[] getMin() {
    return keyBlock[0];
  }

  @Override public byte[] getMax() {
    return keyBlock[keyBlock.length - 1];
  }
}
