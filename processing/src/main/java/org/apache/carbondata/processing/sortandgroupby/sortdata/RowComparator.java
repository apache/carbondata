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

import java.nio.ByteBuffer;
import java.util.Comparator;

import org.apache.carbondata.core.constants.IgnoreDictionary;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.processing.util.RemoveDictionaryUtil;

public class RowComparator implements Comparator<Object[]> {
  /**
   * noDictionaryCount represent number of no dictionary cols
   */
  private int noDictionaryCount;

  /**
   * noDictionaryColMaping mapping of dictionary dimensions and no dictionary dimensions.
   */
  private boolean[] noDictionaryColMaping;

  /**
   * @param noDictionaryColMaping
   * @param noDictionaryCount
   */
  public RowComparator(boolean[] noDictionaryColMaping, int noDictionaryCount) {
    this.noDictionaryCount = noDictionaryCount;
    this.noDictionaryColMaping = noDictionaryColMaping;
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;

    int normalIndex = 0;
    int noDictionaryindex = 0;

    for (boolean isNoDictionary : noDictionaryColMaping) {

      if (isNoDictionary) {
        byte[] byteArr1 = (byte[]) rowA[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()];

        ByteBuffer buff1 = ByteBuffer.wrap(byteArr1);

        // extract a high card dims from complete byte[].
        RemoveDictionaryUtil
            .extractSingleHighCardDims(byteArr1, noDictionaryindex, noDictionaryCount, buff1);

        byte[] byteArr2 = (byte[]) rowB[IgnoreDictionary.BYTE_ARRAY_INDEX_IN_ROW.getIndex()];

        ByteBuffer buff2 = ByteBuffer.wrap(byteArr2);

        // extract a high card dims from complete byte[].
        RemoveDictionaryUtil
            .extractSingleHighCardDims(byteArr2, noDictionaryindex, noDictionaryCount, buff2);

        int difference = UnsafeComparer.INSTANCE.compareTo(buff1, buff2);
        if (difference != 0) {
          return difference;
        }
        noDictionaryindex++;
      } else {
        int dimFieldA = RemoveDictionaryUtil.getDimension(normalIndex, rowA);
        int dimFieldB = RemoveDictionaryUtil.getDimension(normalIndex, rowB);
        diff = dimFieldA - dimFieldB;
        if (diff != 0) {
          return diff;
        }
        normalIndex++;
      }

    }

    return diff;
  }
}
