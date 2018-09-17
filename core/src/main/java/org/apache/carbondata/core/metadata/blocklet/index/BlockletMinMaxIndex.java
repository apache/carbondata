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

package org.apache.carbondata.core.metadata.blocklet.index;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;

/**
 * Below class holds the information of max and min value of all the columns in a blocklet
 */
public class BlockletMinMaxIndex implements Serializable {

  /**
   * serialization version
   */
  private static final long serialVersionUID = -4311405145501302895L;

  /**
   * Min value of all columns of one blocklet Bit-Packed
   */
  private byte[][] minValues;

  /**
   * Max value of all columns of one blocklet Bit-Packed
   */
  private byte[][] maxValues;

  /**
   * flag to check whether min max is written for a column or not
   */
  private boolean[] isMinMaxSet;

  public BlockletMinMaxIndex() {
  }

  public BlockletMinMaxIndex(List<ByteBuffer> minValues, List<ByteBuffer> maxValues,
      List<Boolean> isMinMaxSet) {
    this.minValues = new byte[minValues.size()][];
    this.maxValues = new byte[maxValues.size()][];
    for (int i = 0; i < minValues.size(); i++) {
      this.minValues[i] = minValues.get(i).array();
      this.maxValues[i] = maxValues.get(i).array();
    }
    this.isMinMaxSet = ArrayUtils.toPrimitive(isMinMaxSet.toArray(new Boolean[isMinMaxSet.size()]));
  }

  /**
   * @return the minValues
   */
  public byte[][] getMinValues() {
    return minValues;
  }

  /**
   * @param minValues the minValues to set
   */
  public void setMinValues(byte[][] minValues) {
    this.minValues = minValues;
  }

  /**
   * @return the maxValues
   */
  public byte[][] getMaxValues() {
    return maxValues;
  }

  /**
   * @param maxValues the maxValues to set
   */
  public void setMaxValues(byte[][] maxValues) {
    this.maxValues = maxValues;
  }

  public boolean[] getIsMinMaxSet() {
    return isMinMaxSet;
  }

  public void setIsMinMaxSet(boolean[] isMinMaxSet) {
    this.isMinMaxSet = isMinMaxSet;
  }

}
