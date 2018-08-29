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

package org.apache.carbondata.datamap.minmax;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;

import org.apache.carbondata.core.metadata.schema.table.Writable;

public class MinMaxIndexHolder implements Writable, Serializable {
  private static final long serialVersionUID = 72314567865325672L;
  public static final String MINMAX_INDEX_SUFFIX = ".minmaxindex";
  public static final String MINMAX_INDEX_PREFFIX = "combine_";

  /**
   * BlockletID of the block.
   */
  private int blockletId;

  /**
   * Min values of each index column of one blocklet Bit-Packed
   */
  private byte[][] minValues;

  /**
   * Max values of each index column of one blocklet Bit-Packed
   */
  private byte[][] maxValues;

  public MinMaxIndexHolder(int colNum) {
    minValues = new byte[colNum][];
    maxValues = new byte[colNum][];
  }

  /**
   * calculate the memory size of this object
   */
  public long getSize() {
    // for blockletId
    long size = 4;
    for (int i = 0; i < minValues.length; i++) {
      if (minValues[i] != null) {
        size += minValues[i].length;
      }
      if (maxValues[i] != null) {
        size += maxValues[i].length;
      }
    }
    return size;
  }

  public byte[][] getMinValues() {
    return minValues;
  }

  public void setMinValues(byte[][] minValues) {
    this.minValues = minValues;
  }

  public byte[][] getMaxValues() {
    return maxValues;
  }

  public void setMaxValues(byte[][] maxValues) {
    this.maxValues = maxValues;
  }

  public int getBlockletId() {
    return blockletId;
  }

  public void setBlockletId(int blockletId) {
    this.blockletId = blockletId;
  }

  public byte[] getMinValueAtPos(int pos) {
    return minValues[pos];
  }

  public void setMinValueAtPos(int pos, byte[] minValue) {
    this.minValues[pos] = minValue;
  }

  public byte[] getMaxValueAtPos(int pos) {
    return maxValues[pos];
  }

  public void setMaxValueAtPos(int pos, byte[] maxValue) {
    this.maxValues[pos] = maxValue;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(blockletId);
    out.writeInt(minValues.length);
    for (int i = 0; i < minValues.length; i++) {
      out.writeInt(minValues[i].length);
      out.write(minValues[i]);
      out.writeInt(maxValues[i].length);
      out.write(maxValues[i]);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blockletId = in.readInt();
    int valueCnt = in.readInt();
    minValues = new byte[valueCnt][];
    maxValues = new byte[valueCnt][];
    for (int i = 0; i < valueCnt; i++) {
      int minValueLength = in.readInt();
      minValues[i] = new byte[minValueLength];
      in.readFully(minValues[i]);
      int maxValueLength = in.readInt();
      maxValues[i] = new byte[maxValueLength];
      in.readFully(maxValues[i]);
    }
  }

  @Override
  public String toString() {
    final StringBuffer sb = new StringBuffer("MinMaxIndexHolder{");
    sb.append("blockletId=").append(blockletId);
    sb.append(",minmaxValues={");
    for (int i = 0; i < minValues.length; i++) {
      sb.append("minValue=").append(Arrays.toString(minValues[i]))
          .append(", maxValue=").append(Arrays.toString(maxValues[i]));
    }
    sb.append("}}");
    return sb.toString();
  }
}
