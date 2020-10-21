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
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;
import org.apache.carbondata.processing.loading.row.IntermediateSortTempRow;

public class FileMergeSortComparator implements Comparator<IntermediateSortTempRow> {

  private boolean[] isSortColumnNoDictionary;

  /**
   * Datatype of all the no-dictionary columns in the table in schema order
   */
  private DataType[] noDictDataTypes;

  /**
   * Index of the no dict Sort columns in the carbonRow for final merge step of sorting.
   */
  private int[] noDictPrimitiveIndex;

  /**
   * Sort and Dictionary info of all the columns in schema order
   */
  private final Map<Integer, List<Boolean>> sortColumnSchemaOrderMap;

  /**
   * Comparator for IntermediateSortTempRow for compatibility cases where column added in old
   * version and it is sort column
   * @param isSortColumnNoDictionary isSortColumnNoDictionary
   */
  public FileMergeSortComparator(boolean[] isSortColumnNoDictionary, DataType[] noDictDataTypes,
      int[] columnIdBasedOnSchemaInRow, Map<Integer, List<Boolean>> sortColumnSchemaOrderMap) {
    this.isSortColumnNoDictionary = isSortColumnNoDictionary;
    this.noDictDataTypes = noDictDataTypes;
    this.noDictPrimitiveIndex = columnIdBasedOnSchemaInRow;
    this.sortColumnSchemaOrderMap = sortColumnSchemaOrderMap;
  }

  /**
   * In the final merging, intermediate sortTemp row will have all the data in schema order.
   * IntermediateSortTempRow#getNoDictSortDims() can return the following data:
   * 1. only no-Dictionary sort column data
   * 2. no-Dictionary sort and no-Dictionary no-sort column data
   * IntermediateSortTempRow#getDictSortDims() can return the following data:
   * 1. only Dictionary sort column data
   * 2. dictionary sort and dictionary no-sort column data
   * On this temp data row, we have to identify the sort column data and only compare those.
   * From the sortColumnSchemaOrderMap, get the sort column and dict info. If the column
   * is sort column, then perform the comparison, else increment the respective index.
   */
  @Override
  public int compare(IntermediateSortTempRow rowA, IntermediateSortTempRow rowB) {
    int diff = 0;
    int dictIndex = 0;
    int nonDictIndex = 0;
    int noDicTypeIdx = 0;
    int schemaRowIdx = 0;
    int sortIndex = 0;

    for (Map.Entry<Integer, List<Boolean>> schemaEntry : sortColumnSchemaOrderMap.entrySet()) {
      boolean isSortColumn = schemaEntry.getValue().get(0);
      boolean isDictColumn = schemaEntry.getValue().get(1);
      if (isSortColumn) {
        if (isSortColumnNoDictionary[sortIndex++]) {
          if (DataTypeUtil.isPrimitiveColumn(noDictDataTypes[noDicTypeIdx])) {
            // use data types based comparator for the no dictionary measure columns
            SerializableComparator comparator =
                org.apache.carbondata.core.util.comparator.Comparator
                    .getComparator(noDictDataTypes[noDicTypeIdx]);
            int difference = comparator
                .compare(rowA.getNoDictSortDims()[noDictPrimitiveIndex[schemaRowIdx]],
                    rowB.getNoDictSortDims()[noDictPrimitiveIndex[schemaRowIdx]]);
            schemaRowIdx++;
            if (difference != 0) {
              return difference;
            }
          } else {
            byte[] byteArr1 = (byte[]) rowA.getNoDictSortDims()[nonDictIndex];
            byte[] byteArr2 = (byte[]) rowB.getNoDictSortDims()[nonDictIndex];

            int difference = ByteUtil.UnsafeComparer.INSTANCE.compareTo(byteArr1, byteArr2);
            if (difference != 0) {
              return difference;
            }
          }
        } else {
          int dimFieldA = rowA.getDictSortDims()[dictIndex];
          int dimFieldB = rowB.getDictSortDims()[dictIndex];

          diff = dimFieldA - dimFieldB;
          if (diff != 0) {
            return diff;
          }
        }
      }
      if (isDictColumn) {
        dictIndex++;
      } else {
        nonDictIndex++;
        noDicTypeIdx++;
      }
    }
    return diff;
  }
}
