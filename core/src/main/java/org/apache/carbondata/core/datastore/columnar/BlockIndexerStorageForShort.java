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
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.ByteUtil;

public class BlockIndexerStorageForShort extends BlockIndexerStorage<byte[][]> {

  private boolean alreadySorted;

  private short[] rowIdPage;

  private short[] rowIdRlePage;

  private byte[][] dataPage;

  private short[] dataRlePage;

  public BlockIndexerStorageForShort(byte[][] dataPage, boolean rleOnData,
      boolean isNoDictionary, boolean isSortRequired) {
    ColumnWithRowId<Short>[] dataWithRowId = createColumnWithRowId(dataPage, isNoDictionary);
    if (isSortRequired) {
      Arrays.sort(dataWithRowId);
    }
    short[] rowIds = extractDataAndReturnRowId(dataWithRowId, dataPage);
    Map<String, short[]> rowIdAndRleRowIdPages = rleEncodeOnRowId(rowIds);
    rowIdPage = rowIdAndRleRowIdPages.get("rowIdPage");
    rowIdRlePage = rowIdAndRleRowIdPages.get("rowRlePage");
    if (rleOnData) {
      rleEncodeOnData(dataWithRowId);
    }
  }

  /**
   * Create an object with each column array and respective rowId
   *
   * @return
   */
  private ColumnWithRowId<Short>[] createColumnWithRowId(byte[][] dataPage,
      boolean isNoDictionary) {
    ColumnWithRowId<Short>[] columnWithIndexs = new ColumnWithRowId[dataPage.length];
    if (isNoDictionary) {
      for (short i = 0; i < columnWithIndexs.length; i++) {
        columnWithIndexs[i] = new ColumnWithRowIdForHighCard<>(dataPage[i], i);
      }
    } else {
      for (short i = 0; i < columnWithIndexs.length; i++) {
        columnWithIndexs[i] = new ColumnWithRowId<>(dataPage[i], i);
      }
    }
    return columnWithIndexs;
  }

  private short[] extractDataAndReturnRowId(ColumnWithRowId<Short>[] dataWithRowId,
      byte[][] dataPage) {
    short[] indexes = new short[dataWithRowId.length];
    for (int i = 0; i < indexes.length; i++) {
      indexes[i] = dataWithRowId[i].getIndex();
      dataPage[i] = dataWithRowId[i].getColumn();
    }
    this.dataPage = dataPage;
    return indexes;
  }

  /**
   * @return the alreadySorted
   */
  public boolean isAlreadySorted() {
    return alreadySorted;
  }

  /**
   * @return the rowIdPage
   */
  public short[] getRowIdPage() {
    return rowIdPage;
  }

  public int getRowIdPageLengthInBytes() {
    if (rowIdPage != null) {
      return rowIdPage.length * 2;
    } else {
      return 0;
    }
  }

  /**
   * @return the rowIdRlePage
   */
  public short[] getRowIdRlePage() {
    return rowIdRlePage;
  }

  public int getRowIdRlePageLengthInBytes() {
    if (rowIdRlePage != null) {
      return rowIdRlePage.length * 2;
    } else {
      return 0;
    }
  }

  /**
   * @return the dataPage
   */
  public byte[][] getDataPage() {
    return dataPage;
  }

  private void rleEncodeOnData(ColumnWithRowId<Short>[] dataWithRowId) {
    byte[] prvKey = dataWithRowId[0].getColumn();
    List<ColumnWithRowId> list = new ArrayList<>(dataWithRowId.length / 2);
    list.add(dataWithRowId[0]);
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < dataWithRowId.length; i++) {
      if (ByteUtil.UnsafeComparer.INSTANCE.compareTo(prvKey, dataWithRowId[i].getColumn()) != 0) {
        prvKey = dataWithRowId[i].getColumn();
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

  private byte[][] convertToDataPage(ColumnWithRowId[] indexes) {
    byte[][] shortArray = new byte[indexes.length][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = indexes[i].getColumn();
    }
    return shortArray;
  }

  private byte[][] convertToDataPage(List<ColumnWithRowId> list) {
    byte[][] shortArray = new byte[list.size()][];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i).getColumn();
    }
    return shortArray;
  }

  @Override
  public short[] getDataRlePage() {
    return dataRlePage;
  }

  @Override
  public int getDataRlePageLengthInBytes() {
    if (dataRlePage != null) {
      return dataRlePage.length * 2;
    } else {
      return 0;
    }
  }
}
