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
package org.apache.carbondata.core.datastore.columnar;

import java.util.Arrays;
import java.util.Map;

import org.apache.carbondata.core.metadata.datatype.DataType;

public class BlockIndexerStorageForNoDictionary extends BlockIndexerStorage<Object[]> {

  private short[] rowIdPage;

  private short[] rowIdRlePage;

  private Object[] dataPage;

  private DataType dataType;

  public BlockIndexerStorageForNoDictionary(Object[] dataPage, DataType dataType,
      boolean isSortRequired) {
    this.dataType = dataType;
    ColumnWithRowIdForNoDictionary<Short>[] dataWithRowId = createColumnWithRowId(dataPage);
    if (isSortRequired) {
      Arrays.sort(dataWithRowId);
    }
    short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
    Map<String, short[]> rowIdAndRleRowIdPages = rleEncodeOnRowId(rowIds);
    rowIdPage = rowIdAndRleRowIdPages.get("rowIdPage");
    rowIdRlePage = rowIdAndRleRowIdPages.get("rowRlePage");
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private ColumnWithRowIdForNoDictionary<Short>[] createColumnWithRowId(Object[] dataPage) {
    ColumnWithRowIdForNoDictionary<Short>[] columnWithIndexs =
        new ColumnWithRowIdForNoDictionary[dataPage.length];
    for (short i = 0; i < columnWithIndexs.length; i++) {
      columnWithIndexs[i] = new ColumnWithRowIdForNoDictionary<>(dataPage[i], i, dataType);
    }
    return columnWithIndexs;
  }

  private short[] extractDataAndReturnRowId(ColumnWithRowIdForNoDictionary<Short>[] dataWithRowId,
      Object[] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      dataPage[i] = dataWithRowId[i].getColumn();
    }
    this.dataPage = dataPage;
    return indexes;
  }

  /**
   * @return the rowIdPage
   */
  @Override
  public short[] getRowIdPage() {
    return rowIdPage;
  }

  @Override
  public int getRowIdPageLengthInBytes() {
    if (rowIdPage != null) {
      return rowIdPage.length * 2;
    } else {
      return 0;
    }
  }

  @Override
  public short[] getRowIdRlePage() {
    return rowIdRlePage;
  }

  @Override
  public int getRowIdRlePageLengthInBytes() {
    if (rowIdRlePage != null) {
      return rowIdRlePage.length * 2;
    } else {
      return 0;
    }
  }

  @Override public Object[] getDataPage() {
    return dataPage;
  }

  @Override public short[] getDataRlePage() {
    return new short[0];
  }

  @Override public int getDataRlePageLengthInBytes() {
    return 0;
  }

}
