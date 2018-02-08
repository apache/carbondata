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
import java.util.Objects;

import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * This class is used to hold field information for a table during data loading. These information
 * will be used to convert/construct/destruct row in sort process step. Because complex field is
 * processed the same as no-dict-no-sort-simple-dimension, so we treat them as the same and use
 * `no-dict-no-sort-dim` related variable to represent them in this class.
 */
public class TableFieldStat implements Serializable {
  private static final long serialVersionUID = 201712070950L;
  private int dictSortDimCnt = 0;
  private int dictNoSortDimCnt = 0;
  private int noDictSortDimCnt = 0;
  private int noDictNoSortDimCnt = 0;
  // whether sort column is of dictionary type or not
  private boolean[] isSortColNoDictFlags;
  private int measureCnt;
  private DataType[] measureDataType;

  // indices for dict & sort dimension columns
  private int[] dictSortDimIdx;
  // indices for dict & no-sort dimension columns
  private int[] dictNoSortDimIdx;
  // indices for no-dict & sort dimension columns
  private int[] noDictSortDimIdx;
  // indices for no-dict & no-sort dimension columns, including complex columns
  private int[] noDictNoSortDimIdx;
  // indices for measure columns
  private int[] measureIdx;

  public TableFieldStat(SortParameters sortParameters) {
    int noDictDimCnt = sortParameters.getNoDictionaryCount();
    int complexDimCnt = sortParameters.getComplexDimColCount();
    int dictDimCnt = sortParameters.getDimColCount() - noDictDimCnt;
    this.isSortColNoDictFlags = sortParameters.getNoDictionarySortColumn();
    int sortColCnt = isSortColNoDictFlags.length;
    for (boolean flag : isSortColNoDictFlags) {
      if (flag) {
        noDictSortDimCnt++;
      } else {
        dictSortDimCnt++;
      }
    }
    this.measureCnt = sortParameters.getMeasureColCount();
    this.measureDataType = sortParameters.getMeasureDataType();

    // be careful that the default value is 0
    this.dictSortDimIdx = new int[dictSortDimCnt];
    this.dictNoSortDimIdx = new int[dictDimCnt - dictSortDimCnt];
    this.noDictSortDimIdx = new int[noDictSortDimCnt];
    this.noDictNoSortDimIdx = new int[noDictDimCnt + complexDimCnt - noDictSortDimCnt];
    this.measureIdx = new int[measureCnt];

    int tmpNoDictSortCnt = 0;
    int tmpNoDictNoSortCnt = 0;
    int tmpDictSortCnt = 0;
    int tmpDictNoSortCnt = 0;
    boolean[] isDimNoDictFlags = sortParameters.getNoDictionaryDimnesionColumn();

    for (int i = 0; i < isDimNoDictFlags.length; i++) {
      if (isDimNoDictFlags[i]) {
        if (i < sortColCnt && isSortColNoDictFlags[i]) {
          noDictSortDimIdx[tmpNoDictSortCnt++] = i;
        } else {
          noDictNoSortDimIdx[tmpNoDictNoSortCnt++] = i;
        }
      } else {
        if (i < sortColCnt && !isSortColNoDictFlags[i]) {
          dictSortDimIdx[tmpDictSortCnt++] = i;
        } else {
          dictNoSortDimIdx[tmpDictNoSortCnt++] = i;
        }
      }
    }
    dictNoSortDimCnt = tmpDictNoSortCnt;

    int base = isDimNoDictFlags.length;
    // adding complex dimension columns
    for (int i = 0; i < complexDimCnt; i++) {
      noDictNoSortDimIdx[tmpNoDictNoSortCnt++] = base + i;
    }
    noDictNoSortDimCnt = tmpNoDictNoSortCnt;

    base += complexDimCnt;
    // indices for measure columns
    for (int i = 0; i < measureCnt; i++) {
      measureIdx[i] = base + i;
    }
  }

  public int getDictSortDimCnt() {
    return dictSortDimCnt;
  }

  public int getDictNoSortDimCnt() {
    return dictNoSortDimCnt;
  }

  public int getNoDictSortDimCnt() {
    return noDictSortDimCnt;
  }

  public int getNoDictNoSortDimCnt() {
    return noDictNoSortDimCnt;
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

  public int[] getDictSortDimIdx() {
    return dictSortDimIdx;
  }

  public int[] getDictNoSortDimIdx() {
    return dictNoSortDimIdx;
  }

  public int[] getNoDictSortDimIdx() {
    return noDictSortDimIdx;
  }

  public int[] getNoDictNoSortDimIdx() {
    return noDictNoSortDimIdx;
  }

  public int[] getMeasureIdx() {
    return measureIdx;
  }

  @Override public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableFieldStat)) return false;
    TableFieldStat that = (TableFieldStat) o;
    return dictSortDimCnt == that.dictSortDimCnt
        && dictNoSortDimCnt == that.dictNoSortDimCnt
        && noDictSortDimCnt == that.noDictSortDimCnt
        && noDictNoSortDimCnt == that.noDictNoSortDimCnt
        && measureCnt == that.measureCnt;
  }

  @Override public int hashCode() {
    return Objects.hash(dictSortDimCnt, dictNoSortDimCnt, noDictSortDimCnt,
        noDictNoSortDimCnt, measureCnt);
  }
}