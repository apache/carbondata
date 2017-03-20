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

import org.apache.carbondata.processing.util.NonDictionaryUtil;

/**
 * This class is used as comparator for comparing dims which are non high cardinality dims.
 * Here the dims will be in form of int[] (surrogates) so directly comparing the integers.
 */
public class RowComparatorForNormalDims implements Comparator<Object[]> {
  /**
   * dimension count
   */
  private int dimensionCount;

  /**
   * RowComparatorForNormalDims Constructor
   *
   * @param dimensionCount
   */
  public RowComparatorForNormalDims(int dimensionCount) {
    this.dimensionCount = dimensionCount;
  }

  /**
   * Below method will be used to compare two surrogate keys
   *
   * @see Comparator#compare(Object, Object)
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;

    for (int i = 0; i < dimensionCount; i++) {

      int dimFieldA = NonDictionaryUtil.getDimension(i, rowA);
      int dimFieldB = NonDictionaryUtil.getDimension(i, rowB);

      diff = dimFieldA - dimFieldB;
      if (diff != 0) {
        return diff;
      }
    }
    return diff;
  }
}
