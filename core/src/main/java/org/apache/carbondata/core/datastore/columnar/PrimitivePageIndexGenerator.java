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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class PrimitivePageIndexGenerator extends PageIndexGenerator<Object[]> {

  private Object[] dataPage;

  public PrimitivePageIndexGenerator(Object[] dataPage, boolean isSortRequired, DataType dataType,
      boolean applyRle) {
    ColumnDataVo<Object>[] dataWithRowId = createColumnWithRowId(dataPage);
    if (isSortRequired) {
      SerializableComparator comparator =
          org.apache.carbondata.core.util.comparator.Comparator.getComparator(dataType);
      Arrays.sort(dataWithRowId, comparator);
      short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
      rleEncodeOnRowId(rowIds);

    } else {
      this.rowIdRlePage = new short[0];
      this.invertedIndex = new short[0];
      this.dataPage = dataPage;
    }
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private PrimitiveColumnDataVO[] createColumnWithRowId(Object[] dataPage) {
    PrimitiveColumnDataVO[] columnWithIndexs = new PrimitiveColumnDataVO[dataPage.length];
    for (short i = 0; i < columnWithIndexs.length; i++) {
      columnWithIndexs[i] = new PrimitiveColumnDataVO(dataPage[i], i);
    }
    return columnWithIndexs;
  }

  private short[] extractDataAndReturnRowId(ColumnDataVo<Object>[] dataWithRowId,
      Object[] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      dataPage[i] = dataWithRowId[i].getData();
    }
    this.dataPage = dataPage;
    return indexes;
  }

  @Override public Object[] getDataPage() {
    return dataPage;
  }
}
