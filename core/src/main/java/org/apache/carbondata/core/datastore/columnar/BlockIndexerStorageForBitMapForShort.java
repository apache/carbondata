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
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class will be used to for bitmap encoded column
 */
public class BlockIndexerStorageForBitMapForShort implements IndexStorage<short[]> {

  /**
   * column data
   */
  private byte[][] keyBlock;

  /**
   * total number of rows
   */
  private int totalSize;

  private byte[] min;
  private byte[] max;
  private Map<Integer, BitSet> dictMap;
  List<Integer> dictList;
  List<Integer> bitMapPagesLengthList;

  public BlockIndexerStorageForBitMapForShort(byte[][] keyBlockInput, boolean isNoDictionary) {
    dictMap = new TreeMap<Integer, BitSet>();
    min = keyBlockInput[0];
    max = keyBlockInput[0];
    int minCompare = 0;
    int maxCompare = 0;
    BitSet dictBitSet = null;
    int sizeInBitSet = keyBlockInput.length % 8 > 0 ? (keyBlockInput.length / 8 + 1) * 8
        : keyBlockInput.length;
    // generate dictionary data page
    byte[] dictionaryDataPage = new byte[keyBlockInput.length];
    for (int i = 0; i < keyBlockInput.length; i++) {
      dictionaryDataPage[i] = keyBlockInput[i][0];
      int dictKey = dictionaryDataPage[i];
      dictBitSet = dictMap.get(dictKey);
      if (dictBitSet == null) {
        dictBitSet = new BitSet(sizeInBitSet);
        dictMap.put(dictKey, dictBitSet);
      }
      dictBitSet.set(i, true);
      minCompare = ByteUtil.compare(min, keyBlockInput[i]);
      maxCompare = ByteUtil.compare(max, keyBlockInput[i]);
      if (minCompare > 0) {
        min = keyBlockInput[i];
      }
      if (maxCompare < 0) {
        max = keyBlockInput[i];
      }
    }

    keyBlock = new byte[dictMap.size() + 1][];
    dictList = new ArrayList<Integer>(dictMap.size());
    bitMapPagesLengthList = new ArrayList<Integer>(dictMap.size());
    int index = 0;
    for (Integer dictKey : dictMap.keySet()) {
      dictBitSet = dictMap.get(dictKey);
      int byteArrayLength = dictBitSet.toByteArray().length;
      bitMapPagesLengthList.add(totalSize);
      totalSize = totalSize + byteArrayLength;
      dictList.add(dictKey);
      keyBlock[index++] = dictBitSet.toByteArray();
    }

    keyBlock[index] = dictionaryDataPage;
    bitMapPagesLengthList.add(totalSize);
    totalSize = totalSize + dictionaryDataPage.length;
  }

  @Override
  public short[] getDataIndexMap() {
    return new short[0];
  }

  @Override
  public int getTotalSize() {
    return totalSize;
  }

  @Override
  public boolean isAlreadySorted() {
    return false;
  }

  /**
   * no use
   *
   * @return
   */
  @Override
  public short[] getDataAfterComp() {
    return new short[0];
  }

  /**
   * no use
   *
   * @return
   */
  @Override
  public short[] getIndexMap() {
    return new short[0];
  }

  /**
   * @return the keyBlock
   */
  public byte[][] getKeyBlock() {
    return keyBlock;
  }

  @Override
  public byte[] getMin() {
    return min;
  }

  @Override
  public byte[] getMax() {
    return max;
  }

  public List<Integer> getDictList() {
    return dictList;
  }

  public void setDictList(List<Integer> dictList) {
    this.dictList = dictList;
  }

  public List<Integer> getBitMapPagesLengthList() {
    return bitMapPagesLengthList;
  }

  public void setBitMapPagesLengthList(List<Integer> bitMapPagesLengthList) {
    this.bitMapPagesLengthList = bitMapPagesLengthList;
  }

}
