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
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

public class UnsafeRowComparator implements Comparator<UnsafeCarbonRow> {
  private Object baseObject;
  private TableFieldStat tableFieldStat;
  private int dictSizeInMemory;

  public UnsafeRowComparator(UnsafeCarbonRowPage rowPage) {
    this.baseObject = rowPage.getDataBlock().getBaseObject();
    this.tableFieldStat = rowPage.getTableFieldStat();
    this.dictSizeInMemory = (tableFieldStat.getDictSortDimCnt()
        + tableFieldStat.getDictNoSortDimCnt()) * 4;
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(UnsafeCarbonRow rowL, UnsafeCarbonRow rowR) {
    return compare(rowL, baseObject, rowR, baseObject);
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(UnsafeCarbonRow rowL, Object baseObjectL, UnsafeCarbonRow rowR,
      Object baseObjectR) {
    int diff = 0;
    long rowA = rowL.address;
    long rowB = rowR.address;
    int sizeInDictPartA = 0;

    int sizeInNonDictPartA = 0;
    int sizeInDictPartB = 0;
    int sizeInNonDictPartB = 0;
    for (boolean isNoDictionary : tableFieldStat.getIsSortColNoDictFlags()) {
      if (isNoDictionary) {
        short lengthA = CarbonUnsafe.getUnsafe().getShort(baseObjectL,
            rowA + dictSizeInMemory + sizeInNonDictPartA);
        byte[] byteArr1 = new byte[lengthA];
        sizeInNonDictPartA += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObjectL, rowA + dictSizeInMemory + sizeInNonDictPartA,
                byteArr1, CarbonUnsafe.BYTE_ARRAY_OFFSET, lengthA);
        sizeInNonDictPartA += lengthA;

        short lengthB = CarbonUnsafe.getUnsafe().getShort(baseObjectR,
            rowB + dictSizeInMemory + sizeInNonDictPartB);
        byte[] byteArr2 = new byte[lengthB];
        sizeInNonDictPartB += 2;
        CarbonUnsafe.getUnsafe()
            .copyMemory(baseObjectR, rowB + dictSizeInMemory + sizeInNonDictPartB,
                byteArr2, CarbonUnsafe.BYTE_ARRAY_OFFSET, lengthB);
        sizeInNonDictPartB += lengthB;

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = CarbonUnsafe.getUnsafe().getInt(baseObjectL, rowA + sizeInDictPartA);
        sizeInDictPartA += 4;
        int dimFieldB = CarbonUnsafe.getUnsafe().getInt(baseObjectR, rowB + sizeInDictPartB);
        sizeInDictPartB += 4;
        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
      }
    }

    return diff;
  }
}
