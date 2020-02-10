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
package org.apache.spark.sql.secondaryindex.load;

import java.util.Comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * This class is for comparing the two mdkeys in no kettle flow.
 */
public class RowComparatorWithOutKettle implements Comparator<Object[]> {

  /**
   * noDictionaryColMaping mapping of dictionary dimensions and no dictionary dimensions.
   */
  private boolean[] noDictionaryColMaping;

  private DataType[] noDicDataTypes;

  public RowComparatorWithOutKettle(boolean[] noDictionaryColMaping, DataType[] noDicDataTypes) {
    this.noDictionaryColMaping = noDictionaryColMaping;
    this.noDicDataTypes = noDicDataTypes;
  }

  /**
   * Below method will be used to compare two mdkeys
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;
    int index = 0;
    int noDictionaryIndex = 0;
    int dataTypeIdx = 0;
    int[] leftMdkArray = (int[]) rowA[0];
    int[] rightMdkArray = (int[]) rowB[0];
    Object[] leftNonDictArray = (Object[]) rowA[1];
    Object[] rightNonDictArray = (Object[]) rowB[1];
    for (boolean isNoDictionary : noDictionaryColMaping) {
      if (isNoDictionary) {
        if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[dataTypeIdx])) {
          // use data types based comparator for the no dictionary measure columns
          SerializableComparator comparator = org.apache.carbondata.core.util.comparator.Comparator
              .getComparator(noDicDataTypes[dataTypeIdx]);
          int difference = comparator
              .compare(leftNonDictArray[noDictionaryIndex], rightNonDictArray[noDictionaryIndex]);
          if (difference != 0) {
            return difference;
          }
        } else {
          diff = UnsafeComparer.INSTANCE.compareTo((byte[]) leftNonDictArray[noDictionaryIndex],
              (byte[]) rightNonDictArray[noDictionaryIndex]);
          if (diff != 0) {
            return diff;
          }
        }
        noDictionaryIndex++;
        dataTypeIdx++;
      } else {
        diff = leftMdkArray[index] - rightMdkArray[index];
        if (diff != 0) {
          return diff;
        }
        index++;
      }

    }
    return diff;
  }
}
