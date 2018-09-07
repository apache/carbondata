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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

public class PrimitivePageIndexGenerator extends PageIndexGenerator<Object[]> {

  private Object[] dataPage;

  public PrimitivePageIndexGenerator(Object[] dataPage, boolean isSortRequired,
      DataType dataType, boolean applyRle) {
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
    }
    if(applyRle) {
      rleEncodeOnData(dataWithRowId);
    }
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private PrimitiveColumnDataVO[] createColumnWithRowId(Object[] dataPage) {
    PrimitiveColumnDataVO[] columnWithIndexs =
        new PrimitiveColumnDataVO[dataPage.length];
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

  private void rleEncodeOnData(ColumnDataVo<Object>[] dataWithRowId) {
    Object prvKey = dataWithRowId[0].getData();
    List<ColumnDataVo<Object>> list = new ArrayList<>(dataWithRowId.length / 2);
    list.add(dataWithRowId[0]);
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < dataWithRowId.length; i++) {
      if (prvKey.equals(dataWithRowId[i].getData())) {
        prvKey = dataWithRowId[i].getData();
        list.add(dataWithRowId[i]);
        map.add(start);
        map.add(counter);
        start += counter;
        counter = 1;
        continue;
      }
      counter++;
    }
    map.add(start);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    boolean useRle = (((list.size() + map.size()) * 100) / dataWithRowId.length) < 70;
    if (useRle) {
      this.dataPage = convertToDataPage(list);
      dataRlePage = convertToArray(map);
    } else {
      this.dataPage = convertToDataPage(dataWithRowId);
      dataRlePage = new short[0];
    }
  }

  private Object[] convertToDataPage(ColumnDataVo<Object>[] indexes) {
    Object[] shortArray = new Object[indexes.length][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = indexes[i].getData();
    }
    return shortArray;
  }

  private Object[] convertToDataPage(List<ColumnDataVo<Object>> list) {
    Object[] shortArray = new Object[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i).getData();
    }
    return shortArray;
  }

  @Override public Object[] getDataPage() {
    return dataPage;
  }
}
