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
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.processing.sort.DummyRowUpdater;
import org.apache.carbondata.processing.sort.SchemaBasedRowUpdater;
import org.apache.carbondata.processing.sort.SortTempRowUpdater;

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
  // for columns that are no_dict_dim and no_sort_dim, except complex/varchar dims
  private int noDictNoSortDimCnt = 0;
  // for columns that are complex data type
  private int complexDimCnt = 0;
  // for columns that are varchar data type
  private int varcharDimCnt = 0;
  // whether sort column is of dictionary type or not
  private boolean[] isSortColNoDictFlags;
  private boolean[] isVarcharDimFlags;
  private int measureCnt;
  private DataType[] measureDataType;
  private DataType[] noDictDataType;
  private DataType[] noDictSortDataType;
  private DataType[] noDictNoSortDataType;

  // indices for dict & sort dimension columns
  private int[] dictSortDimIdx;
  // indices for dict & no-sort dimension columns
  private int[] dictNoSortDimIdx;
  // indices for no-dict & sort dimension columns
  private int[] noDictSortDimIdx;
  // indices for no-dict & no-sort dimension columns, excluding complex/varchar columns
  private int[] noDictNoSortDimIdx;
  // indices for varchar dimension columns
  private int[] varcharDimIdx;
  // indices for varchar dimension columns
  private int [] complexDimIdx;
  // indices for measure columns
  private int[] measureIdx;

  private SortTempRowUpdater sortTempRowUpdater;

  /**
   * Index of the no dict Sort columns in the carbonRow for final merge step of sorting.
   */
  private int[] noDictSortColumnSchemaOrderMapping;

  private DataType[] noDictSchemaDataType;

  /**
   * Index of the no dict Sort columns in schema order used for final merge step of sorting.
   */
  private int[] noDictSortColIdxSchemaOrderMapping;

  /**
   * Index of the dict Sort columns in schema order for final merge step of sorting.
   */
  private int[] dictSortColIdxSchemaOrderMapping;

  private int[] changedOrderInDataField;

  public TableFieldStat(SortParameters sortParameters) {
    int noDictDimCnt = sortParameters.getNoDictionaryCount();
    int dictDimCnt = sortParameters.getDimColCount() - noDictDimCnt;
    this.complexDimCnt = sortParameters.getComplexDimColCount();
    this.isSortColNoDictFlags = sortParameters.getNoDictionarySortColumn();
    this.isVarcharDimFlags = sortParameters.getIsVarcharDimensionColumn();
    noDictSortDimCnt = sortParameters.getNoDictSortDimCnt();
    dictSortDimCnt = sortParameters.getDictSortDimCnt();
    this.measureCnt = sortParameters.getMeasureColCount();
    this.measureDataType = sortParameters.getMeasureDataType();
    this.noDictDataType = sortParameters.getNoDictDataType();
    this.noDictSortDataType = sortParameters.getNoDictSortDataType();
    this.noDictNoSortDataType = sortParameters.getNoDictNoSortDataType();
    this.noDictSchemaDataType = sortParameters.getNoDictSchemaDataType();
    this.noDictSortColIdxSchemaOrderMapping =
        sortParameters.getNoDictSortColIdxSchemaOrderMapping();
    this.dictSortColIdxSchemaOrderMapping = sortParameters.getDictSortColIdxSchemaOrderMapping();
    for (boolean flag : isVarcharDimFlags) {
      if (flag) {
        varcharDimCnt++;
      }
    }
    this.changedOrderInDataField = sortParameters.getChangedOrderInDataField();

    // be careful that the default value is 0
    this.dictSortDimIdx = new int[dictSortDimCnt];
    this.dictNoSortDimIdx = new int[dictDimCnt - dictSortDimCnt];
    this.noDictSortDimIdx = new int[noDictSortDimCnt];
    this.noDictNoSortDimIdx = new int[noDictDimCnt - noDictSortDimCnt - varcharDimCnt];
    this.complexDimIdx = new int[complexDimCnt];
    this.varcharDimIdx = new int[varcharDimCnt];
    this.measureIdx = new int[measureCnt];
    this.noDictSortColumnSchemaOrderMapping =
        sortParameters.getNoDictSortColumnSchemaOrderMapping();

    int tmpNoDictSortCnt = 0;
    int tmpNoDictNoSortCnt = 0;
    int tmpDictSortCnt = 0;
    int tmpDictNoSortCnt = 0;
    int tmpVarcharCnt = 0;
    int tmpComplexcount = 0;
    int tmpMeasureIndex = 0;

    if (sortParameters.isInsertWithoutReArrangeFlow()
        && sortParameters.getCarbonTable().getPartitionInfo() != null) {
      List<ColumnSchema> reArrangedColumnSchema =
          getReArrangedColumnSchema(sortParameters.getCarbonTable());
      for (int i = 0; i < reArrangedColumnSchema.size(); i++) {
        ColumnSchema columnSchema = reArrangedColumnSchema.get(i);
        if (columnSchema.isDimensionColumn()) {
          if (columnSchema.getDataType() == DataTypes.DATE && !columnSchema.getDataType()
              .isComplexType()) {
            if (columnSchema.isSortColumn()) {
              dictSortDimIdx[tmpDictSortCnt++] = i;
            } else {
              dictNoSortDimIdx[tmpDictNoSortCnt++] = i;
            }
          } else if (!columnSchema.getDataType().isComplexType()) {
            if (columnSchema.getDataType() == DataTypes.VARCHAR) {
              varcharDimIdx[tmpVarcharCnt++] = i;
            } else if (columnSchema.isSortColumn()) {
              noDictSortDimIdx[tmpNoDictSortCnt++] = i;
            } else {
              noDictNoSortDimIdx[tmpNoDictNoSortCnt++] = i;
            }
          } else {
            complexDimIdx[tmpComplexcount++] = i;
          }
        } else {
          measureIdx[tmpMeasureIndex++] = i;
        }
      }
    } else {
      List<CarbonDimension> allDimensions = sortParameters.getCarbonTable().getVisibleDimensions();
      List<CarbonDimension> updatedDimensions = updateDimensionsBasedOnSortColumns(allDimensions);
      for (int i = 0; i < updatedDimensions.size(); i++) {
        CarbonDimension carbonDimension = updatedDimensions.get(i);
        if (carbonDimension.getDataType() == DataTypes.DATE && !carbonDimension.isComplex()) {
          if (carbonDimension.isSortColumn()) {
            dictSortDimIdx[tmpDictSortCnt++] = i;
          } else {
            dictNoSortDimIdx[tmpDictNoSortCnt++] = i;
          }
        } else if (!carbonDimension.isComplex()) {
          if (isVarcharDimFlags[i]) {
            varcharDimIdx[tmpVarcharCnt++] = i;
          } else if (carbonDimension.isSortColumn()) {
            noDictSortDimIdx[tmpNoDictSortCnt++] = i;
          } else {
            noDictNoSortDimIdx[tmpNoDictNoSortCnt++] = i;
          }
        } else {
          complexDimIdx[tmpComplexcount++] = i;
        }
      }
      int base = updatedDimensions.size();
      // indices for measure columns
      for (int i = 0; i < measureCnt; i++) {
        measureIdx[i] = base + i;
      }
    }
    dictNoSortDimCnt = tmpDictNoSortCnt;
    noDictNoSortDimCnt = tmpNoDictNoSortCnt;
    if (sortParameters.isUpdateDictDims() || sortParameters.isUpdateNonDictDims()) {
      this.sortTempRowUpdater = new SchemaBasedRowUpdater(sortParameters.getDictDimActualPosition(),
          sortParameters.getNoDictActualPosition(), sortParameters.isUpdateDictDims(),
          sortParameters.isUpdateNonDictDims());
    } else {
      this.sortTempRowUpdater = new DummyRowUpdater();
    }
  }

  public int[] getChangedDataFieldOrder() {
    return changedOrderInDataField;
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

  public int getComplexDimCnt() {
    return complexDimCnt;
  }

  public int getVarcharDimCnt() {
    return varcharDimCnt;
  }

  public boolean[] getIsSortColNoDictFlags() {
    return isSortColNoDictFlags;
  }

  public boolean[] getIsVarcharDimFlags() {
    return isVarcharDimFlags;
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

  public int[] getComplexDimIdx() {
    return complexDimIdx;
  }

  public int[] getVarcharDimIdx() {
    return varcharDimIdx;
  }

  public int[] getMeasureIdx() {
    return measureIdx;
  }

  public int[] getNoDictSortColumnSchemaOrderMapping() {
    return noDictSortColumnSchemaOrderMapping;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TableFieldStat)) return false;
    TableFieldStat that = (TableFieldStat) o;
    return dictSortDimCnt == that.dictSortDimCnt
        && dictNoSortDimCnt == that.dictNoSortDimCnt
        && noDictSortDimCnt == that.noDictSortDimCnt
        && noDictNoSortDimCnt == that.noDictNoSortDimCnt
        && complexDimCnt == that.complexDimCnt
        && varcharDimCnt == that.varcharDimCnt
        && measureCnt == that.measureCnt;
  }

  @Override
  public int hashCode() {
    return Objects.hash(dictSortDimCnt, dictNoSortDimCnt, noDictSortDimCnt,
        noDictNoSortDimCnt, complexDimCnt, varcharDimCnt, measureCnt);
  }

  public DataType[] getNoDictSortDataType() {
    return noDictSortDataType;
  }

  public DataType[] getNoDictNoSortDataType() {
    return noDictNoSortDataType;
  }

  public DataType[] getNoDictDataType() {
    return noDictDataType;
  }

  public SortTempRowUpdater getSortTempRowUpdater() {
    return sortTempRowUpdater;
  }

  private static List<CarbonDimension> updateDimensionsBasedOnSortColumns(
      List<CarbonDimension> carbonDimensions) {
    return getCarbonDimensions(carbonDimensions);
  }

  /**
   * This method rearrange the dimensions where all the sort columns are added at first. Because
   * if the column gets added in old version like carbon1.1, it will be added at last, so if it is
   * sort column, this method will bring it to first.
   */
  private static List<CarbonDimension> getCarbonDimensions(List<CarbonDimension> carbonDimensions) {
    List<CarbonDimension> updatedDataFields = new ArrayList<>();
    List<CarbonDimension> sortFields = new ArrayList<>();
    List<CarbonDimension> nonSortFields = new ArrayList<>();
    for (CarbonDimension carbonDimension : carbonDimensions) {
      if (carbonDimension.getColumnSchema().isSortColumn()) {
        sortFields.add(carbonDimension);
      } else {
        nonSortFields.add(carbonDimension);
      }
    }
    updatedDataFields.addAll(sortFields);
    updatedDataFields.addAll(nonSortFields);
    return updatedDataFields;
  }

  private static List<ColumnSchema> getReArrangedColumnSchema(
      CarbonTable carbonTable) {
    // handle 1.1 compatibility for sort columns
    List<CarbonDimension> visibleDimensions =
        updateDimensionsBasedOnSortColumns(carbonTable.getVisibleDimensions());
    List<CarbonMeasure> visibleMeasures = carbonTable.getVisibleMeasures();
    List<ColumnSchema> otherCols = new ArrayList<>();
    if (carbonTable.getPartitionInfo() != null) {
      List<ColumnSchema> columnSchemaList = carbonTable.getPartitionInfo().getColumnSchemaList();
      for (CarbonDimension dim : visibleDimensions) {
        if (!columnSchemaList.contains(dim.getColumnSchema())) {
          otherCols.add(dim.getColumnSchema());
        } else {
          if (dim.isSortColumn()) {
            otherCols.add(dim.getColumnSchema());
          }
        }
      }
      for (CarbonMeasure measure : visibleMeasures) {
        if (!columnSchemaList.contains(measure.getColumnSchema())) {
          otherCols.add(measure.getColumnSchema());
        }
      }
      columnSchemaList.forEach(columnSchema -> {
        if (!columnSchema.isSortColumn()) {
          otherCols.add(columnSchema);
        }
      });
    }
    return otherCols;
  }

  public DataType[] getNoDictSchemaDataType() {
    return noDictSchemaDataType;
  }

  public int[] getNoDictSortColIdxSchemaOrderMapping() {
    return noDictSortColIdxSchemaOrderMapping;
  }

  public int[] getDictSortColIdxSchemaOrderMapping() {
    return dictSortColIdxSchemaOrderMapping;
  }
}