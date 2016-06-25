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
package org.carbondata.processing.store.colgroup;


/**
 * This will hold column group data.
 */
public class ColGroupDataHolder implements DataHolder {

  private int noOfRecords;

  /**
   * colGrpData[row no][data]
   */
  private byte[][] colGrpData;

  /**
   * This will have min max value of each chunk
   */
  private ColGroupMinMax colGrpMinMax;

  /**
   * each row size of this column group block
   */
  private int keyBlockSize;

  /**
   * @param colGrpModel
   * @param columnarSplitter
   * @param colGroupId
   * @param noOfRecords
   */
  public ColGroupDataHolder(int keyBlockSize,
       int noOfRecords,ColGroupMinMax colGrpMinMax) {
    this.noOfRecords = noOfRecords;
    this.keyBlockSize = keyBlockSize;
    this.colGrpMinMax = colGrpMinMax;
    colGrpData = new byte[noOfRecords][];
  }

  @Override public void addData(byte[] rowsData, int rowIndex) {
    colGrpData[rowIndex] = rowsData;
    colGrpMinMax.add(rowsData);
  }

  /**
   * this will return min of each chunk
   *
   * @return
   */
  public byte[] getMin() {
    return colGrpMinMax.getMin();
  }

  /**
   * this will return max of each chunk
   *
   * @return
   */
  public byte[] getMax() {
    return colGrpMinMax.getMax();
  }

  /**
   * Return size of this column group block
   *
   * @return
   */
  public int getKeyBlockSize() {
    return keyBlockSize;
  }

  @Override public byte[][] getData() {
    return colGrpData;
  }

  /**
   * return total size required by this block
   *
   * @return
   */
  public int getTotalSize() {
    return noOfRecords * keyBlockSize;
  }

}
