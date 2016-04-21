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

package org.carbondata.query.executer.impl.comparator;

import java.util.Comparator;

import org.carbondata.query.executer.Tuple;

/**
 * Class Description : Comparator responsible for Comparing to tuple based on key
 * Version 1.0
 */
public class MaksedByteComparatorForTuple implements Comparator<Tuple> {
  /**
   * compareRange
   */
  private int[] index;

  /**
   * sortOrder
   */
  private byte sortOrder;

  /**
   * maskedKey
   */
  private byte[] maskedKey;

  /**
   * MaksedByteResultComparator Constructor
   */
  public MaksedByteComparatorForTuple(int[] compareRange, byte sortOrder, byte[] maskedKey) {
    this.index = compareRange;
    this.sortOrder = sortOrder;
    this.maskedKey = maskedKey;
  }

  /**
   * This method will be used to compare two byte array
   *
   * @param o1
   * @param o2
   */
  @Override public int compare(Tuple tuple1, Tuple tuple2) {
    int cmp = 0;
    byte[] key1 = tuple1.getKey();
    byte[] key2 = tuple2.getKey();
    for (int i = 0; i < index.length; i++) {
      int a = (key1[index[i]] & this.maskedKey[i]) & 0xff;
      int b = (key2[index[i]] & this.maskedKey[i]) & 0xff;
      cmp = a - b;
      if (cmp == 0) {
        continue;
      }
      cmp = cmp < 0 ? -1 : 1;
      break;
    }
    if (sortOrder == 1) {
      return cmp * -1;
    }
    return cmp;
  }

}
