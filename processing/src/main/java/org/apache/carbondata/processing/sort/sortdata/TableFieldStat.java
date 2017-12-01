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

import java.util.Objects;

import org.apache.carbondata.core.metadata.datatype.DataType;

public class TableFieldStat {
  private int noDictDimCnt;
  private int dictDimCnt;
  private int complexDimCnt;
  // only for those simple dimension
  private int dimCnt;
  private boolean[] isDimNoDictFlags;
  // whether sort column is of dictionary type or not
  private boolean[] isSortColNoDictFlags;
  private int measureCnt;
  private DataType[] measureDataType;

  private int[] dictDimIdx;
  // indices for non dictionary dimension columns, including complex columns
  private int[] allNonDictDimIdx;
  private int[] measureIdx;

  public TableFieldStat(SortParameters sortParameters) {
    this.noDictDimCnt = sortParameters.getNoDictionaryCount();
    this.complexDimCnt = sortParameters.getComplexDimColCount();
    this.dimCnt = sortParameters.getDimColCount();
    this.dictDimCnt = dimCnt - noDictDimCnt;
    this.isDimNoDictFlags = sortParameters.getNoDictionaryDimnesionColumn();
    this.isSortColNoDictFlags = sortParameters.getNoDictionarySortColumn();
    this.measureCnt = sortParameters.getMeasureColCount();
    this.measureDataType = sortParameters.getMeasureDataType();

    int index = 0;
    int nonDictIndex = 0;
    int allCount = 0;

    // be careful that the default value is 0
    this.dictDimIdx = new int[dictDimCnt];
    this.allNonDictDimIdx = new int[noDictDimCnt + complexDimCnt];
    this.measureIdx = new int[measureCnt];

    // indices for simple dimension columns
    for (int i = 0; i < isDimNoDictFlags.length; i++) {
      if (isDimNoDictFlags[i]) {
        allNonDictDimIdx[nonDictIndex++] = i;
      } else {
        dictDimIdx[index++] = i;
      }
      allCount++;
    }

    // indices for complex dimension columns
    for (int i = 0; i < complexDimCnt; i++) {
      allNonDictDimIdx[nonDictIndex++] = allCount;
      allCount++;
    }

    // indices for measure columns
    for (int i = 0; i < measureCnt; i++) {
      measureIdx[i] = allCount;
      allCount++;
    }
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableFieldStat)) return false;
    TableFieldStat that = (TableFieldStat) o;
    return noDictDimCnt == that.noDictDimCnt && dictDimCnt == that.dictDimCnt
        && complexDimCnt == that.complexDimCnt && dimCnt == that.dimCnt
        && measureCnt == that.measureCnt;
  }

  @Override public int hashCode() {
    return Objects.hash(noDictDimCnt, dictDimCnt, complexDimCnt, dimCnt, measureCnt);
  }

  public int getNoDictDimCnt() {
    return noDictDimCnt;
  }

  public int getDictDimCnt() {
    return dictDimCnt;
  }

  public int getComplexDimCnt() {
    return complexDimCnt;
  }

  public int getDimCnt() {
    return dimCnt;
  }

  public boolean[] getIsDimNoDictFlags() {
    return isDimNoDictFlags;
  }

  public boolean[] getIsSortColNoDictFlags() {
    return isSortColNoDictFlags;
  }

  public int getMeasureCnt() {
    return measureCnt;
  }

  public DataType[] getMeasureDataType() {
    return measureDataType;
  }

  public int[] getDictDimIdx() {
    return dictDimIdx;
  }

  public int[] getAllNonDictDimIdx() {
    return allNonDictDimIdx;
  }

  public int[] getMeasureIdx() {
    return measureIdx;
  }
}
