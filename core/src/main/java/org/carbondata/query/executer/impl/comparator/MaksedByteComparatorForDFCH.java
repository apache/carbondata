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

import org.carbondata.query.executer.pagination.impl.DataFileChunkHolder;

/**
 * Class Description : Comparator responsible for Comparing to DataFileChunkHolder based on key
 * Version 1.0
 */
public class MaksedByteComparatorForDFCH implements Comparator<DataFileChunkHolder> {

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
  public MaksedByteComparatorForDFCH(int[] compareRange, byte sortOrder, byte[] maskedKey) {
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
  @Override public int compare(DataFileChunkHolder dataFileChunkHolder1,
      DataFileChunkHolder dataFileChunkHolder2) {
    int compare = 0;
    byte[] o1 = dataFileChunkHolder1.getRow();
    byte[] o2 = dataFileChunkHolder2.getRow();
    for (int i = 0; i < index.length; i++) {
      int a = (o1[index[i]] & this.maskedKey[i]) & 0xff;
      int b = (o2[index[i]] & this.maskedKey[i]) & 0xff;
      compare = a - b;
      if (compare == 0) {
        continue;
      }
      compare = compare < 0 ? -1 : 1;
      break;
    }
    if (sortOrder == 1) {
      return compare * -1;
    }
    return compare;
  }

}
