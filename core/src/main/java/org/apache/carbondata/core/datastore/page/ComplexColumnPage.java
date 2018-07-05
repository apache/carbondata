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

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.statistics.DummyStatsCollector;
import org.apache.carbondata.core.localdictionary.generator.LocalDictionaryGenerator;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

/**
 * holds the complex columndata and its children data
 */
public class ComplexColumnPage {

  /**
   * number of columns
   */
  private int depth;

  /**
   * type of each column
   */
  private List<ColumnType> complexColumnType;

  /**
   * column page for each type
   */
  private ColumnPage[] columnPages;

  /**
   * to maintain the number of record added for each type
   */
  private int[] currentRowIdList;

  public ComplexColumnPage(List<ColumnType> complexColumnType) {
    this.depth = complexColumnType.size();
    this.complexColumnType = complexColumnType;
    this.columnPages = new ColumnPage[this.depth];
    this.currentRowIdList = new int[depth];
  }

  /**
   * below method will be used to initlize the column page of complex type
   * @param columnToDictMap
   * dictionary map
   * @param columnNames
   * list of columns
   * @param pageSize
   * number of records
   * @throws MemoryException
   * if memory is not sufficient
   */
  public void initialize(Map<String, LocalDictionaryGenerator> columnToDictMap,
      List<String> columnNames, int pageSize) throws MemoryException {
    for (int i = 0; i < this.columnPages.length; i++) {
      LocalDictionaryGenerator localDictionaryGenerator = columnToDictMap.get(columnNames.get(i));
      if (null == localDictionaryGenerator) {
        TableSpec.ColumnSpec spec = TableSpec.ColumnSpec
            .newInstance(columnNames.get(i), DataTypes.BYTE_ARRAY, complexColumnType.get(i));
        this.columnPages[i] = ColumnPage.newPage(spec, DataTypes.BYTE_ARRAY, pageSize);
        this.columnPages[i].setStatsCollector(new DummyStatsCollector());
      } else {
        TableSpec.ColumnSpec spec = TableSpec.ColumnSpec
            .newInstance(columnNames.get(i), DataTypes.BYTE_ARRAY, complexColumnType.get(i));
        this.columnPages[i] = ColumnPage
            .newLocalDictPage(spec, DataTypes.BYTE_ARRAY, pageSize, localDictionaryGenerator, true);
        this.columnPages[i].setStatsCollector(new DummyStatsCollector());
      }
    }
  }

  /**
   *
   * @return depth
   */
  public int getDepth() {
    return depth;
  }

  /**
   * return the type of complex column
   * @param isDepth
   * @return co plex column type
   */
  public ColumnType getComplexColumnType(int isDepth) {
    return complexColumnType.get(isDepth);
  }

  /**
   * method to add complex column data
   * @param depth
   * depth of column
   * @param dataList
   * dataList
   */
  public void putComplexData(int depth, List<byte[]> dataList) {
    assert (depth <= this.depth);
    int currentNumber = currentRowIdList[depth];
    for (int i = 0; i < dataList.size(); i++) {
      columnPages[depth].putData(currentNumber, dataList.get(i));
      currentNumber++;
    }
    currentRowIdList[depth] = currentNumber;
  }

  /**
   * to free the used memory
   */
  public void freeMemory() {
    for (int i = 0; i < depth; i++) {
      columnPages[i].freeMemory();
    }
  }

  /**
   * return the column page
   * @param depth
   * depth of column
   * @return colum page
   */
  public ColumnPage getColumnPage(int depth) {
    assert (depth <= this.depth);
    return columnPages[depth];
  }
}
