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

package org.apache.carbondata.core.datastore.page.statistics;

import java.util.BitSet;

// Statistics of dimension and measure column in a Blocklet
public class BlockletStatistics {

  // Min Key within a blocklet.
  private byte[][] blockletMinVal;

  // Max key within the blocklet.
  private byte[][] blockletMaxVal;

  // Each bits points to each column and if set shows that null values is
  // present in the column data.
  private BitSet blockletNullVal;

  public BlockletStatistics(byte[][] blockletMaxVal, byte[][] blockletMinVal,
      BitSet blockletNullVal) {
    this.blockletMinVal = blockletMinVal;
    this.blockletMaxVal = blockletMaxVal;
    this.blockletNullVal = blockletNullVal;
  }

  public byte[][] getBlockletMinVal() {
    return blockletMinVal;
  }

  public void setBlockletMinVal(byte[][] blockletMinVal) {
    this.blockletMinVal = blockletMinVal;
  }

  public byte[][] getBlockletMaxVal() {
    return blockletMaxVal;
  }

  public void setBlockletMaxVal(byte[][] blockletMaxVal) {
    this.blockletMaxVal = blockletMaxVal;
  }

  public BitSet getBlockletNullVal() {
    return blockletNullVal;
  }

  public void setBlockletNullVal(BitSet blockletNullVal) {
    this.blockletNullVal = blockletNullVal;
  }
}
