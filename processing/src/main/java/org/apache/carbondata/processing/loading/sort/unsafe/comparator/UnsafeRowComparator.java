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

package org.apache.carbondata.processing.loading.sort.unsafe.comparator;

import java.util.Comparator;

import org.apache.carbondata.core.memory.CarbonUnsafe;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeCarbonRow;

public class UnsafeRowComparator implements Comparator<UnsafeCarbonRow> {

  /**
   * mapping of dictionary and no dictionary of sort_columns.
   */
  private boolean[] noDictionarySortColumnMaping;

  private Object baseObject;

  public UnsafeRowComparator(UnsafeCarbonRowPage rowPage) {
    this.noDictionarySortColumnMaping = rowPage.getNoDictionarySortColumnMapping();
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
    for (boolean isNoDictionary : noDictionarySortColumnMaping) {
      if (isNoDictionary) {
        short aShort1 = CarbonUnsafe.getUnsafe().getShort(baseObject, rowA + sizeA);
        byte[] byteArr1 = new byte[aShort1];
        sizeA += 2;
        CarbonUnsafe.getUnsafe().copyMemory(baseObject, rowA + sizeA, byteArr1,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, aShort1);
        sizeA += aShort1;

        short aShort2 = CarbonUnsafe.getUnsafe().getShort(baseObject, rowB + sizeB);
        byte[] byteArr2 = new byte[aShort2];
        sizeB += 2;
        CarbonUnsafe.getUnsafe().copyMemory(baseObject, rowB + sizeB, byteArr2,
            CarbonUnsafe.BYTE_ARRAY_OFFSET, aShort2);
        sizeB += aShort2;

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = CarbonUnsafe.getUnsafe().getInt(baseObject, rowA + sizeA);
        sizeA += 4;
        int dimFieldB = CarbonUnsafe.getUnsafe().getInt(baseObject, rowB + sizeB);
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
    for (boolean isNoDictionary : noDictionarySortColumnMaping) {
      if (isNoDictionary) {
        short aShort1 = CarbonUnsafe.getUnsafe().getShort(baseObjectL, rowA + sizeA);
        byte[] byteArr1 = new byte[aShort1];
        sizeA += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObjectL, rowA + sizeA, byteArr1, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                aShort1);
        sizeA += aShort1;

        short aShort2 = CarbonUnsafe.getUnsafe().getShort(baseObjectR, rowB + sizeB);
        byte[] byteArr2 = new byte[aShort2];
        sizeB += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObjectR, rowB + sizeB, byteArr2, CarbonUnsafe.BYTE_ARRAY_OFFSET,
                aShort2);
        sizeB += aShort2;

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = CarbonUnsafe.getUnsafe().getInt(baseObjectL, rowA + sizeA);
        sizeA += 4;
        int dimFieldB = CarbonUnsafe.getUnsafe().getInt(baseObjectR, rowB + sizeB);
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
