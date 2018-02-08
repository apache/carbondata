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

package org.apache.carbondata.processing.sort.sortdata;

import java.util.Comparator;

import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;

/**
 * This class is used as comparator for comparing intermediate sort temp row
 */
public class IntermediateSortTempRowComparator implements Comparator<IntermediateSortTempRow> {
  /**
   * isSortColumnNoDictionary whether the sort column is not dictionary or not
   */
  private boolean[] isSortColumnNoDictionary;

  /**
   * @param isSortColumnNoDictionary isSortColumnNoDictionary
   */
  public IntermediateSortTempRowComparator(boolean[] isSortColumnNoDictionary) {
    this.isSortColumnNoDictionary = isSortColumnNoDictionary;
  }

  /**
   * Below method will be used to compare two sort temp row
   */
  public int compare(IntermediateSortTempRow rowA, IntermediateSortTempRow rowB) {
    int diff = 0;
    int dictIndex = 0;
    int nonDictIndex = 0;

    for (boolean isNoDictionary : isSortColumnNoDictionary) {

      if (isNoDictionary) {
        byte[] byteArr1 = rowA.getNoDictSortDims()[nonDictIndex];
        byte[] byteArr2 = rowB.getNoDictSortDims()[nonDictIndex];
        nonDictIndex++;

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = rowA.getDictSortDims()[dictIndex];
        int dimFieldB = rowB.getDictSortDims()[dictIndex];
        dictIndex++;

        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
      }
    }
    return diff;
  }
}
