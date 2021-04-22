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

/**
 * Support to encode on both rowId and data
 */
public class ByteArrayBlockIndexerStorage extends BlockIndexerStorage<byte[][]> {

  public ByteArrayBlockIndexerStorage(byte[][] dataPage, boolean rleOnData,
                                      boolean isNoDictionary, boolean isSortRequired) {
    ByteArrayColumnWithRowId[] dataWithRowId = createColumnWithRowId(dataPage, isNoDictionary);
    if (isSortRequired) {
      Arrays.sort(dataWithRowId);
    }
    short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
    encodeAndSetRowId(rowIds);
    if (rleOnData) {
      this.dataPage = rleEncodeOnData(dataPage);
    } else {
      this.dataPage = dataPage;
    }
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private ByteArrayColumnWithRowId[] createColumnWithRowId(byte[][] dataPage,
      boolean isNoDictionary) {
    ByteArrayColumnWithRowId[] columnWithIndexes = new ByteArrayColumnWithRowId[dataPage.length];
    for (short i = 0; i < columnWithIndexes.length; i++) {
      columnWithIndexes[i] = new ByteArrayColumnWithRowId(dataPage[i], i, isNoDictionary);
    }
    return columnWithIndexes;
  }

  private short[] extractDataAndReturnRowId(ByteArrayColumnWithRowId[] dataWithRowId,
      byte[][] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getRowId();
      dataPage[i] = dataWithRowId[i].getColumn();
    }
    this.dataPage = dataPage;
    return indexes;
  }

}
