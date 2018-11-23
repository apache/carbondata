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

import java.io.Serializable;
import java.util.Comparator;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class NewRowComparator implements Comparator<Object[]>, Serializable {
  private static final long serialVersionUID = -1739874611112709436L;

  private DataType[] noDicDataTypes;

  private boolean[] noDicSortColumnMapping;

  public NewRowComparator(boolean[] noDicSortColumnMapping,
      DataType[] noDicDataTypes) {
    this.noDicSortColumnMapping = noDicSortColumnMapping;
    this.noDicDataTypes = noDicDataTypes;
  }

  /**
   * Below method will be used to compare two mdkey
   */
  public int compare(Object[] rowA, Object[] rowB) {
    int diff = 0;
    int index = 0;
    int dataTypeIdx = 0;
    int noDicSortIdx = 0;
    for (int i = 0; i < noDicSortColumnMapping.length; i++) {
      if (noDicSortColumnMapping[noDicSortIdx++]) {
        if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[dataTypeIdx])) {
          // use data types based comparator for the no dictionary measure columns
          SerializableComparator comparator = org.apache.carbondata.core.util.comparator.Comparator
              .getComparator(noDicDataTypes[dataTypeIdx]);
          int difference = comparator.compare(rowA[index], rowB[index]);
          if (difference != 0) {
            return difference;
          }
        } else {
          byte[] byteArr1 = (byte[]) rowA[index];
          byte[] byteArr2 = (byte[]) rowB[index];

          int difference = UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
          if (difference != 0) {
            return difference;
          }
        }
        dataTypeIdx++;
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
