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
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.processing.newflow.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.newflow.sort.unsafe.holder.UnsafeCarbonRow;

public class UnsafeRowComparator implements Comparator<UnsafeCarbonRow> {

  /**
   * noDictionaryColMaping mapping of dictionary dimensions and no dictionary dimensions.
   */
  private boolean[] noDictionaryColMaping;

  private Object baseObject;

  public UnsafeRowComparator(UnsafeCarbonRowPage rowPage) {
    this.noDictionaryColMaping = rowPage.getNoDictionaryDimensionMapping();
    this.baseObject = rowPage.getDataBlock().getBaseObject();
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
    for (boolean isNoDictionary : noDictionaryColMaping) {
      if (isNoDictionary) {
        short aShort1 = CarbonUnsafe.unsafe.getShort(baseObject, rowA + sizeA);
        byte[] byteArr1 = new byte[aShort1];
        sizeA += 2;
        CarbonUnsafe.unsafe.copyMemory(baseObject, rowA + sizeA, byteArr1,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, aShort1);
        sizeA += aShort1;

        short aShort2 = CarbonUnsafe.unsafe.getShort(baseObject, rowB + sizeB);
        byte[] byteArr2 = new byte[aShort2];
        sizeB += 2;
        CarbonUnsafe.unsafe.copyMemory(baseObject, rowB + sizeB, byteArr2,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, aShort2);
        sizeB += aShort2;

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = CarbonUnsafe.unsafe.getInt(baseObject, rowA + sizeA);
        sizeA += 4;
        int dimFieldB = CarbonUnsafe.unsafe.getInt(baseObject, rowB + sizeB);
        sizeB += 4;
        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
      }
    }

    return diff;
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(UnsafeCarbonRow rowL, Object baseObjectL, UnsafeCarbonRow rowR,
      Object baseObjectR) {
    int diff = 0;
    long rowA = rowL.address;
    long rowB = rowR.address;
    int sizeA = 0;
    int sizeB = 0;
    for (boolean isNoDictionary : noDictionaryColMaping) {
      if (isNoDictionary) {
        short aShort1 = CarbonUnsafe.unsafe.getShort(baseObjectL, rowA + sizeA);
        byte[] byteArr1 = new byte[aShort1];
        sizeA += 2;
        CarbonUnsafe.unsafe
            .copyMemory(baseObjectL, rowA + sizeA, byteArr1, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                aShort1);
        sizeA += aShort1;

        short aShort2 = CarbonUnsafe.unsafe.getShort(baseObjectR, rowB + sizeB);
        byte[] byteArr2 = new byte[aShort2];
        sizeB += 2;
        CarbonUnsafe.unsafe
            .copyMemory(baseObjectR, rowB + sizeB, byteArr2, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                aShort2);
        sizeB += aShort2;

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = CarbonUnsafe.unsafe.getInt(baseObjectL, rowA + sizeA);
        sizeA += 4;
        int dimFieldB = CarbonUnsafe.unsafe.getInt(baseObjectR, rowB + sizeB);
        sizeB += 4;
        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
      }
    }

    return diff;
  }
}
