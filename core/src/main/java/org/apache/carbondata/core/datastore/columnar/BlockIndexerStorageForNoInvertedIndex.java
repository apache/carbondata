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
import java.util.Comparator;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ByteUtil;

public class BlockIndexerStorageForNoInvertedIndex implements IndexStorage<int[]> {
  private byte[][] keyBlock;
  private byte[][] sortedBlock;
  private int totalSize;
  private int[] dataIndexMap;

  public BlockIndexerStorageForNoInvertedIndex(byte[][] keyBlockInput, boolean compressData,
      boolean isNoDictionary) {
    // without invertedindex but can be RLE
    if (compressData) {
      // with RLE
      byte[] prvKey = keyBlockInput[0];
      List<byte[]> list = new ArrayList<byte[]>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      list.add(keyBlockInput[0]);
      int counter = 1;
      int start = 0;
      List<Integer> map = new ArrayList<Integer>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
      int length = keyBlockInput.length;
      for(int i = 1; i < length; i++) {
        if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(prvKey, keyBlockInput[i]) != 0) {
          prvKey = keyBlockInput[i];
          list.add(keyBlockInput[i]);
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
      if (keyBlockInput.length == this.keyBlock.length) {
        dataIndexMap = new int[0];
      } else {
        dataIndexMap = convertToArray(map);
      }
    } else {
      this.keyBlock = keyBlockInput;
      dataIndexMap = new int[0];
    }

    this.sortedBlock = new byte[keyBlock.length][];
    System.arraycopy(keyBlock, 0, sortedBlock, 0, keyBlock.length);
    if (isNoDictionary) {
      Arrays.sort(sortedBlock, new Comparator<byte[]>() {
        @Override
        public int compare(byte[] col1, byte[] col2) {
          return ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(col1, 2, col1.length - 2, col2, 2, col2.length - 2);
        }
      });
    } else {
      Arrays.sort(sortedBlock, new Comparator<byte[]>() {
        @Override
        public int compare(byte[] col1, byte[] col2) {
          return ByteUtil.UnsafeComparer.INSTANCE.compareTo(col1, col2);
        }
      });
    }

  }

  private int[] convertToArray(List<Integer> list) {
    int[] shortArray = new int[list.size()];
    for(int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }

  private byte[][] convertToKeyArray(List<byte[]> list) {
    byte[][] shortArray = new byte[list.size()][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
      totalSize += shortArray[i].length;
    }
    return shortArray;
  }

  @Override
  public int[] getDataIndexMap() {
    return dataIndexMap;
  }

  @Override
  public int getTotalSize() {
    return totalSize;
  }

  @Override
  public boolean isAlreadySorted() {
    return true;
  }

  /**
   * no use
   * @return
   */
  @Override
  public int[] getDataAfterComp() {
    return new int[0];
  }

  /**
   * no use
   * @return
   */
  @Override
  public int[] getIndexMap() {
    return new int[0];
  }

  /**
   * @return the keyBlock
   */
  public byte[][] getKeyBlock() {
    return keyBlock;
  }

  @Override public byte[] getMin() {
    return sortedBlock[0];
  }

  @Override public byte[] getMax() {
    return sortedBlock[sortedBlock.length - 1];
  }

}
