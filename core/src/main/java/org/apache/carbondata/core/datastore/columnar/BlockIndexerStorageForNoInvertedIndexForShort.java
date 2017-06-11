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

import org.apache.carbondata.core.util.ByteUtil;

/**
 * Below class will be used to for no inverted index
 */
public class BlockIndexerStorageForNoInvertedIndexForShort implements IndexStorage<short[]> {

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

  public BlockIndexerStorageForNoInvertedIndexForShort(byte[][] keyBlockInput,
      boolean isNoDictonary) {
    this.keyBlock = keyBlockInput;
    min = keyBlock[0];
    max = keyBlock[0];
    totalSize += keyBlock[0].length;
    int minCompare = 0;
    int maxCompare = 0;
    if (!isNoDictonary) {
      for (int i = 1; i < keyBlock.length; i++) {
        totalSize += keyBlock[i].length;
        minCompare = ByteUtil.compare(min, keyBlock[i]);
        maxCompare = ByteUtil.compare(max, keyBlock[i]);
        if (minCompare > 0) {
          min = keyBlock[i];
        }
        if (maxCompare < 0) {
          max = keyBlock[i];
        }
      }
    } else {
      for (int i = 1; i < keyBlock.length; i++) {
        totalSize += keyBlock[i].length;
        minCompare = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(min, 2, min.length - 2, keyBlock[i], 2, keyBlock[i].length - 2);
        maxCompare = ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(max, 2, max.length - 2, keyBlock[i], 2, keyBlock[i].length - 2);
        if (minCompare > 0) {
          min = keyBlock[i];
        }
        if (maxCompare < 0) {
          max = keyBlock[i];
        }
      }
    }
  }

  @Override public short[] getDataIndexMap() {
    return new short[0];
  }

  @Override public int getTotalSize() {
    return totalSize;
  }

  @Override public boolean isAlreadySorted() {
    return true;
  }

  /**
   * no use
   *
   * @return
   */
  @Override public short[] getDataAfterComp() {
    return new short[0];
  }

  /**
   * no use
   *
   * @return
   */
  @Override public short[] getIndexMap() {
    return new short[0];
  }

  /**
   * @return the keyBlock
   */
  public byte[][] getKeyBlock() {
    return keyBlock;
  }

  @Override public byte[] getMin() {
    return min;
  }

  @Override public byte[] getMax() {
    return max;
  }
}
