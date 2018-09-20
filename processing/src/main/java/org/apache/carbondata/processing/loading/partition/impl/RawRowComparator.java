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

package org.apache.carbondata.processing.loading.partition.impl;

import java.util.Comparator;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil.UnsafeComparer;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

/**
 * comparator for the converted row. The row has not been rearranged as 3-parted yet.
 */
@InterfaceAudience.Internal
public class RawRowComparator implements Comparator<CarbonRow> {
  private int[] sortColumnIndices;
  private boolean[] isSortColumnNoDict;
  private DataType[] noDicDataTypes;

  public RawRowComparator(int[] sortColumnIndices, boolean[] isSortColumnNoDict,
      DataType[] noDicDataTypes) {
    this.sortColumnIndices = sortColumnIndices;
    this.isSortColumnNoDict = isSortColumnNoDict;
    this.noDicDataTypes = noDicDataTypes;
  }

  @Override
  public int compare(CarbonRow o1, CarbonRow o2) {
    int diff = 0;
    int i = 0;
    int noDicIdx = 0;
    for (int colIdx : sortColumnIndices) {
      if (isSortColumnNoDict[i]) {
        if (DataTypeUtil.isPrimitiveColumn(noDicDataTypes[noDicIdx])) {
          // for no dictionary numeric column get comparator based on the data type
          SerializableComparator comparator = org.apache.carbondata.core.util.comparator.Comparator
              .getComparator(noDicDataTypes[noDicIdx]);
          int difference = comparator.compare(o1.getObject(colIdx), o2.getObject(colIdx));
          if (difference != 0) {
            return difference;
          }
        } else {
          byte[] colA = (byte[]) o1.getObject(colIdx);
          byte[] colB = (byte[]) o2.getObject(colIdx);
          diff = UnsafeComparer.INSTANCE.compareTo(colA, colB);
          if (diff != 0) {
            return diff;
          }
        }
        noDicIdx++;
      } else {
        int colA = (int) o1.getObject(colIdx);
        int colB = (int) o2.getObject(colIdx);
        diff = colA - colB;
        if (diff != 0) {
          return diff;
        }
      }
      i++;
    }
    return diff;
  }
}
