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

package org.apache.carbondata.core.util;

import java.util.BitSet;

/**
 * Maintains the group of bitsets.
 * Each filter executor returns BitSetGroup after filtering the data.
 */
public class BitSetGroup {

  private BitSet[] bitSets;

  public BitSetGroup(int groupSize) {
    bitSets = new BitSet[groupSize];
  }

  public void setBitSet(BitSet bitSet, int index) {
    assert index < bitSets.length;
    bitSets[index] = bitSet;
  }

  public BitSet getBitSet(int index) {
    assert index < bitSets.length;
    return bitSets[index];
  }

  public boolean isEmpty() {
    for (BitSet bitSet : bitSets) {
      if (bitSet != null && !bitSet.isEmpty()) {
        return false;
      }
    }
    return true;
  }

  public void and(BitSetGroup group) {
    int i = 0;
    for (BitSet bitSet : bitSets) {
      BitSet otherSet = group.getBitSet(i);
      if (bitSet != null && otherSet != null) {
        bitSet.and(otherSet);
      } else {
        bitSets[i] = null;
      }
      i++;
    }
  }

  public void or(BitSetGroup group) {
    int i = 0;
    for (BitSet bitSet : bitSets) {
      BitSet otherSet = group.getBitSet(i);
      if (bitSet != null && otherSet != null) {
        bitSet.or(otherSet);
      }
      // if it is null and other set is not null then replace it.
      if (bitSet == null && otherSet != null) {
        bitSets[i] = otherSet;
      }
      i++;
    }
  }

  public int getNumberOfPages() {
    return bitSets.length;
  }

  /**
   * @return return the valid pages
   */
  public int getValidPages() {
    int numberOfPages = 0;
    for (BitSet bitSet : bitSets) {
      numberOfPages += (bitSet != null && !bitSet.isEmpty()) ? 1 : 0;
    }
    return numberOfPages;
  }

  /**
   * @return return the valid pages
   */
  public int getScannedPages() {
    int numberOfPages = 0;
    for (int i = 0; i < bitSets.length; i++) {
      numberOfPages += bitSets[i] == null ? 0 : 1;
    }
    return numberOfPages;
  }
}
