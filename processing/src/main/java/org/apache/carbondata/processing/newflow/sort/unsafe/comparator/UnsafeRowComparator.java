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

package org.apache.carbondata.processing.newflow.sort.unsafe.comparator;

import java.util.Comparator;

import org.apache.carbondata.core.memory.CarbonUnsafe;
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
        sizeA += 2;

        short aShort2 = CarbonUnsafe.unsafe.getShort(baseObject, rowB + sizeB);
        sizeB += 2;
        int minLength = (aShort1 <= aShort2) ? aShort1 : aShort2;
        int difference = UnsafeComparer.INSTANCE
            .compareUnsafeTo(baseObject, baseObject, rowA + sizeA, rowB + sizeB, aShort1, aShort2,
                minLength);

        if (difference != 0) {
          return difference;
        }
        sizeA += aShort1;
        sizeB += aShort2;
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
        sizeA += 2;

        short aShort2 = CarbonUnsafe.unsafe.getShort(baseObjectR, rowB + sizeB);
        sizeB += 2;

        int minLength = (aShort1 <= aShort2) ? aShort1 : aShort2;

        int difference = UnsafeComparer.INSTANCE
            .compareUnsafeTo(baseObjectL, baseObjectR, rowA + sizeA, rowB + sizeB, aShort1, aShort2,
                minLength);

        if (difference != 0) {
          return difference;
        }
        sizeA += aShort1;
        sizeB += aShort2;
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
