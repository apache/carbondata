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
package org.carbondata.query.carbon.result.comparator;

import java.util.Comparator;

import org.carbondata.query.carbon.result.ListBasedResultWrapper;

/**
 * Fixed length key comparator
 */
public class FixedLengthKeyResultComparator implements Comparator<ListBasedResultWrapper> {

  /**
   * compareRange
   */
  private int[] compareRange;

  /**
   * sortOrder
   */
  private byte sortOrder;

  /**
   * maskedKey
   */
  private byte[] maskedKey;

  public FixedLengthKeyResultComparator(int[] compareRange, byte sortOrder, byte[] maskedKey) {
    this.compareRange = compareRange;
    this.sortOrder = sortOrder;
    this.maskedKey = maskedKey;
  }

  @Override public int compare(ListBasedResultWrapper listBasedResultWrapper1,
      ListBasedResultWrapper listBasedResultWrapper2) {
    int cmp = 0;
    byte[] o1 = listBasedResultWrapper1.getKey().getDictionaryKey();
    byte[] o2 = listBasedResultWrapper2.getKey().getDictionaryKey();
    for (int i = 0; i < compareRange.length; i++) {
      int a = (o1[compareRange[i]] & this.maskedKey[i]) & 0xff;
      int b = (o2[compareRange[i]] & this.maskedKey[i]) & 0xff;
      cmp = a - b;
      if (cmp != 0) {

        if (sortOrder == 1) {
          return cmp = cmp * -1;
        }
        return cmp;
      }
    }
    return 0;
  }

}
