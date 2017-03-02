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

package org.apache.carbondata.processing.sortandgroupby.sortdata;

import java.util.Comparator;

import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;

public class NewRowComparator implements Comparator<Object[]> {

  /**
   * mapping of dictionary dimensions and no dictionary of sort_column.
   */
  private boolean[] noDictionarySortColumnMaping;

  /**
   * @param noDictionarySortColumnMaping
   */
  public NewRowComparator(boolean[] noDictionarySortColumnMaping) {
    this.noDictionarySortColumnMaping = noDictionarySortColumnMaping;
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;

    int index = 0;

    for (boolean isNoDictionary : noDictionarySortColumnMaping) {

      if (isNoDictionary) {
        byte[] byteArr1 = (byte[]) rowA[index];

        byte[] byteArr2 = (byte[]) rowB[index];

        int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
        if (difference != 0) {
          return difference;
        }
      } else {
        int dimFieldA = (int) rowA[index];
        int dimFieldB = (int) rowB[index];
        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
      }

      index++;
    }

    return diff;
  }
}
