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

package org.apache.carbondata.processing.newflow.sort.unsafe.comparator;

import java.util.Comparator;

import org.apache.carbondata.core.unsafe.CarbonUnsafe;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeCarbonRow;

public class UnsafeRowComparatorForNormalDIms implements Comparator<UnsafeCarbonRow> {

  private Object baseObject;

  private int dimCount;

  public UnsafeRowComparatorForNormalDIms(int dimCount, UnsafeCarbonRowPage rowPage) {
    this.baseObject = rowPage.getDataBlock().getBaseObject();
    this.dimCount = dimCount;
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(UnsafeCarbonRow rowL, UnsafeCarbonRow rowR) {
    int diff = 0;
    long rowA = rowL.address;
    long rowB = rowR.address;
    int sizeA = 0;
    int sizeB = 0;
    for (int i = 0; i < dimCount; i++) {
      int dimFieldA = CarbonUnsafe.unsafe.getInt(baseObject, rowA + sizeA);
      sizeA += 4;
      int dimFieldB = CarbonUnsafe.unsafe.getInt(baseObject, rowB + sizeB);
      sizeB += 4;
      diff = dimFieldA - dimFieldB;
      if (diff != 0) {
        return diff;
      }
    }

    return diff;
  }
}
