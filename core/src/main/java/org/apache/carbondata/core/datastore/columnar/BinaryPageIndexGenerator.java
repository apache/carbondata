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
import java.util.Comparator;

import org.apache.carbondata.core.util.ByteUtil;

public class BinaryPageIndexGenerator extends PageIndexGenerator<byte[][]> {

  private byte[][] dataPage;

  public BinaryPageIndexGenerator(byte[][] dataPage, boolean isSortRequired,
      final short startOffset, boolean applyRle) {
    ColumnDataVo<byte[]>[] dataWithRowId = createColumnWithRowId(dataPage);
    if (isSortRequired) {
      Arrays.sort(dataWithRowId, new Comparator<ColumnDataVo<byte[]>>() {
        @Override public int compare(ColumnDataVo<byte[]> o1, ColumnDataVo<byte[]> o2) {
          return ByteUtil.UnsafeComparer.INSTANCE
              .compareTo(o1.getData(), startOffset, o1.getData().length - startOffset, o2.getData(),
                  startOffset, o2.getData().length - startOffset);
        }
      });
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
  private ColumnDataVo<byte[]>[] createColumnWithRowId(byte[][] dataPage) {
    BinaryColumnDataVo[] columnWithIndexs = new BinaryColumnDataVo[dataPage.length];
    for (short i = 0; i < columnWithIndexs.length; i++) {
      columnWithIndexs[i] = new BinaryColumnDataVo(dataPage[i], i);
    }
    return columnWithIndexs;
  }

  private short[] extractDataAndReturnRowId(ColumnDataVo<byte[]>[] dataWithRowId,
      byte[][] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      dataPage[i] = dataWithRowId[i].getData();
    }
    this.dataPage = dataPage;
    return indexes;
  }

  /**
   * @return the dataPage
   */
  public byte[][] getDataPage() {
    return dataPage;
  }

}
