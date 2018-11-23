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

package org.apache.carbondata.core.datastore.page;

import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.statistics.DummyStatsCollector;
import org.apache.carbondata.core.datastore.page.statistics.PrimitivePageStatsCollector;
import org.apache.carbondata.core.datastore.row.ComplexColumnInfo;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * holds the complex columndata and its children data
 */
public class ComplexColumnPage {

  /**
   * number of columns
   */
  private int complexColumnIndex;

  /**
   * type of each column
   */
  private List<ComplexColumnInfo> complexColumnInfoList;

  /**
   * column page for each type
   */
  private ColumnPage[] columnPages;

  /**
   * to maintain the number of record added for each type
   */
  private int[] currentRowIdList;

  public ComplexColumnPage(List<ComplexColumnInfo> complexColumnInfoList) {
    this.complexColumnIndex = complexColumnInfoList.size();
    this.complexColumnInfoList = complexColumnInfoList;
    this.columnPages = new ColumnPage[this.complexColumnIndex];
    this.currentRowIdList = new int[complexColumnIndex];
  }

  /**
   * below method will be used to initlize the column page of complex type
   * @param columnToDictMap
   * dictionary map
   * @param pageSize
   * number of records
   * @throws MemoryException
   * if memory is not sufficient
   */
  public void initialize(Map<String, LocalDictionaryGenerator> columnToDictMap, int pageSize,
      String columnCompressor) throws MemoryException {
    DataType dataType;
    for (int i = 0; i < this.columnPages.length; i++) {
      LocalDictionaryGenerator localDictionaryGenerator =
          columnToDictMap.get(complexColumnInfoList.get(i).getColumnNames());
      TableSpec.ColumnSpec spec = getColumnSpec(i, localDictionaryGenerator);
      if (null == localDictionaryGenerator) {
        dataType = complexColumnInfoList.get(i).getColumnDataTypes();
        if (isColumnPageBasedOnDataType(i)) {
          // no dictionary primitive types need adaptive encoding,
          // hence store as actual value instead of byte array
          this.columnPages[i] = ColumnPage.newPage(
              new ColumnPageEncoderMeta(spec, dataType, columnCompressor), pageSize);
          this.columnPages[i].setStatsCollector(PrimitivePageStatsCollector.newInstance(dataType));
        } else {
          this.columnPages[i] = ColumnPage.newPage(
              new ColumnPageEncoderMeta(spec, DataTypes.BYTE_ARRAY, columnCompressor), pageSize);
          this.columnPages[i].setStatsCollector(new DummyStatsCollector());
        }
      } else {
        this.columnPages[i] = ColumnPage.newLocalDictPage(
            new ColumnPageEncoderMeta(spec, DataTypes.BYTE_ARRAY, columnCompressor), pageSize,
            localDictionaryGenerator, true);
        this.columnPages[i].setStatsCollector(new DummyStatsCollector());
      }
    }
  }

  private TableSpec.ColumnSpec getColumnSpec(int columnPageIndex,
      LocalDictionaryGenerator localDictionaryGenerator) {
    if ((localDictionaryGenerator == null) && isColumnPageBasedOnDataType(columnPageIndex)) {
      return TableSpec.ColumnSpec
          .newInstance(complexColumnInfoList.get(columnPageIndex).getColumnNames(),
              complexColumnInfoList.get(columnPageIndex).getColumnDataTypes(),
              complexColumnInfoList.get(columnPageIndex).getComplexColumnType());
    } else {
      return TableSpec.ColumnSpec
          .newInstance(complexColumnInfoList.get(columnPageIndex).getColumnNames(),
              DataTypes.BYTE_ARRAY,
              complexColumnInfoList.get(columnPageIndex).getComplexColumnType());
    }
  }

  private boolean isColumnPageBasedOnDataType(int columnPageIndex) {
    DataType dataType = complexColumnInfoList.get(columnPageIndex).getColumnDataTypes();
    if ((complexColumnInfoList.get(columnPageIndex).isNoDictionary() &&
        !((DataTypes.isStructType(dataType) ||
            DataTypes.isArrayType(dataType) ||
            DataTypes.isMapType(dataType) ||
            (dataType == DataTypes.STRING) ||
            (dataType == DataTypes.VARCHAR) ||
            (dataType == DataTypes.DATE) ||
            DataTypes.isDecimal(dataType))))) {
      // For all these above condition the ColumnPage should be Taken as BYTE_ARRAY
      // for all other cases make Column Page Based on each DataType.
      return true;
    } else {
      return false;
    }
  }

  /**
   *
   * @return complexColumnIndex
   */
  public int getComplexColumnIndex() {
    return complexColumnIndex;
  }

  /**
   * method to add complex column data
   * @param depth
   * complexColumnIndex of column
   * @param dataList
   * dataList
   */
  public void putComplexData(int depth, List<byte[]> dataList) {
    assert (depth <= this.complexColumnIndex);
    int positionNumber = currentRowIdList[depth];
    for (byte[] value : dataList) {
      if (columnPages[depth].getDataType() != DataTypes.BYTE_ARRAY) {
        if ((value == null) || (value.length == 0)) {
          columnPages[depth].putNull(positionNumber);
          columnPages[depth].statsCollector.updateNull(positionNumber);
          columnPages[depth].nullBitSet.set(positionNumber);
        } else {
          columnPages[depth].putData(positionNumber, DataTypeUtil
              .getDataBasedOnDataTypeForNoDictionaryColumn(value,
                  columnPages[depth].getColumnSpec().getSchemaDataType(), false));
        }
      } else {
        columnPages[depth].putData(positionNumber, value);
      }
      positionNumber++;
    }
    currentRowIdList[depth] = positionNumber;
  }

  /**
   * to free the used memory
   */
  public void freeMemory() {
    for (int i = 0; i < complexColumnIndex; i++) {
      columnPages[i].freeMemory();
    }
  }

  /**
   * return the column page
   * @param complexColumnIndex
   * complexColumnIndex of column
   * @return colum page
   */
  public ColumnPage getColumnPage(int complexColumnIndex) {
    assert (complexColumnIndex <= this.complexColumnIndex);
    return columnPages[complexColumnIndex];
  }
}
