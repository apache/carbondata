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

public class BinaryPageIndexGenerator extends PageIndexGenerator<byte[][]> {

  private byte[][] dataPage;

  private int[] length;

  public BinaryPageIndexGenerator(byte[][] dataPage, boolean isSortRequired, int[] length) {
    this.dataPage = dataPage;
    this.length = length;
    if (isSortRequired) {
      ColumnDataVo<byte[]>[] dataWithRowId = createColumnWithRowId(dataPage, length);
      Arrays.sort(dataWithRowId);
      short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
      rleEncodeOnRowId(rowIds);
    } else {
      this.rowIdRlePage = new short[0];
      this.invertedIndex = new short[0];
    }
    this.dataRlePage = new short[0];
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private BinaryColumnDataVo[] createColumnWithRowId(byte[][] dataPage, int[] length) {
    BinaryColumnDataVo[] columnWithIndexe = new BinaryColumnDataVo[dataPage.length];
    for (short i = 0; i < columnWithIndexe.length; i++) {
      columnWithIndexe[i] = new BinaryColumnDataVo(dataPage[i], i, length[i]);
    }
    return columnWithIndexe;
  }

  private short[] extractDataAndReturnRowId(ColumnDataVo<byte[]>[] dataWithRowId,
      byte[][] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      dataPage[i] = dataWithRowId[i].getData();
      length[i] = dataWithRowId[i].getLength();
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

  @Override public int[] getLength() {
    return length;
  }
}
