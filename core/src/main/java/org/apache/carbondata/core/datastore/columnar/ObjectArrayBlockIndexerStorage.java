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

/**
 * Support to encode on rowId
 */
public class ObjectArrayBlockIndexerStorage extends BlockIndexerStorage<Object[]> {

  private DataType dataType;

  public ObjectArrayBlockIndexerStorage(Object[] dataPage, DataType dataType,
                                        boolean isSortRequired) {
    this.dataType = dataType;
    ObjectColumnWithRowId[] dataWithRowId = createColumnWithRowId(dataPage);
    if (isSortRequired) {
      Arrays.sort(dataWithRowId);
    }
    short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
    encodeAndSetRowId(rowIds);
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private ObjectColumnWithRowId[] createColumnWithRowId(Object[] dataPage) {
    ObjectColumnWithRowId[] columnWithIndexes =
        new ObjectColumnWithRowId[dataPage.length];
    for (short i = 0; i < columnWithIndexes.length; i++) {
      columnWithIndexes[i] = new ObjectColumnWithRowId(dataPage[i], i, dataType);
    }
    return columnWithIndexes;
  }

  private short[] extractDataAndReturnRowId(ObjectColumnWithRowId[] dataWithRowId,
      Object[] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getRowId();
      dataPage[i] = dataWithRowId[i].getColumn();
    }
    this.dataPage = dataPage;
    return indexes;
  }

  @Override
  public short[] getDataRlePage() {
    return new short[0];
  }

  @Override
  public int getDataRlePageLengthInBytes() {
    return 0;
  }

}
