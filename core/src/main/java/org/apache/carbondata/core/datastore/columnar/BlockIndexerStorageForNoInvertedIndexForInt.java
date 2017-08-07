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
public class BlockIndexerStorageForNoInvertedIndexForInt implements IndexStorage<int[]> {

  /**
   * column data
   */
  private byte[][] dataPage;

  /**
   * total number of rows
   */
  private int totalSize;

  private byte[] min;
  private byte[] max;

  public BlockIndexerStorageForNoInvertedIndexForInt(byte[][] dataPage) {
    this.dataPage = dataPage;
    min = this.dataPage[0];
    max = this.dataPage[0];
    totalSize += this.dataPage[0].length;
    int minCompare = 0;
    int maxCompare = 0;
    for (int i = 1; i < this.dataPage.length; i++) {
      totalSize += this.dataPage[i].length;
      minCompare = ByteUtil.compare(min, this.dataPage[i]);
      maxCompare = ByteUtil.compare(max, this.dataPage[i]);
      if (minCompare > 0) {
        min = this.dataPage[i];
      }
      if (maxCompare < 0) {
        max = this.dataPage[i];
      }
    }
  }

  public int[] getDataRlePage() {
    return new int[0];
  }

  @Override
  public int getDataRlePageLengthInBytes() {
    return 0;
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
  public int[] getRowIdPage() {
    return new int[0];
  }

  @Override
  public int getRowIdPageLengthInBytes() {
    return 0;
  }

  /**
   * no use
   *
   * @return
   */
  public int[] getRowIdRlePage() {
    return new int[0];
  }

  @Override
  public int getRowIdRlePageLengthInBytes() {
    return 0;
  }

  /**
   * @return the dataPage
   */
  public byte[][] getDataPage() {
    return dataPage;
  }

  @Override public byte[] getMin() {
    return min;
  }

  @Override public byte[] getMax() {
    return max;
  }
}