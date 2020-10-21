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
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public abstract class PageIndexGenerator<T> {

  short[] invertedIndex;

  protected short[] rowIdRlePage;

  protected short[] dataRlePage;

  private boolean alreadySorted;

  public short[] getRowIdPage() {
    return invertedIndex;
  }

  public int getRowIdPageLengthInBytes() {
    if (invertedIndex != null) {
      return invertedIndex.length * CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    } else {
      return 0;
    }
  }

  public short[] getRowIdRlePage() {
    return rowIdRlePage;
  }

  public int getRowIdRlePageLengthInBytes() {
    if (rowIdRlePage != null) {
      return rowIdRlePage.length * CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    } else {
      return 0;
    }
  }

  public abstract T getDataPage();

  public abstract int[] getLength();

  public short[] getDataRlePage() {
    return dataRlePage;
  }

  public int getDataRlePageLengthInBytes() {
    if (dataRlePage != null) {
      return dataRlePage.length * CarbonCommonConstants.SHORT_SIZE_IN_BYTE;
    } else {
      return 0;
    }
  }

  /**
   * It compresses depends up on the sequence numbers.
   * [1,2,3,4,6,8,10,11,12,13] is translated to [1,4,6,8,10,13] and [0,6]. In
   * first array the start and end of sequential numbers and second array
   * keeps the indexes of where sequential numbers starts. If there is no
   * sequential numbers then the same array it returns with empty second
   * array.
   *
   * @param rowIds
   */
  protected void encodeAndSetRowId(short[] rowIds) {
    List<Short> list = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    List<Short> map = new ArrayList<Short>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    int k = 0;
    int i = 1;
    for (; i < rowIds.length; i++) {
      if (rowIds[i] - rowIds[i - 1] == 1) {
        k++;
      } else {
        if (k > 0) {
          map.add(((short) list.size()));
          list.add(rowIds[i - k - 1]);
          list.add(rowIds[i - 1]);
        } else {
          list.add(rowIds[i - 1]);
        }
        k = 0;
      }
    }
    if (k > 0) {
      map.add(((short) list.size()));
      list.add(rowIds[i - k - 1]);
      list.add(rowIds[i - 1]);
    } else {
      list.add(rowIds[i - 1]);
    }
    if ((((list.size() + map.size()) * 100) / rowIds.length) > 70) {
      this.invertedIndex = rowIds;
      this.rowIdRlePage = new short[0];
    } else {
      this.invertedIndex = convertToArray(list);
      this.rowIdRlePage = convertToArray(map);
    }
    if (invertedIndex.length == 2 && rowIdRlePage.length == 1) {
      alreadySorted = true;
    }
  }

  /**
   * apply RLE(run-length encoding) on byte array data page
   */
  protected Object[] rleEncodeOnData(Object[] dataPage) {
    List<Short> map = new ArrayList<>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    List<Object> list = new ArrayList<>(dataPage.length / 2);
    list.add(dataPage[0]);
    short counter = 1;
    short startIdx = 0;
    for (int i = 1; i < dataPage.length; i++) {
      if (dataPage[i - 1] != dataPage[i]) {
        list.add(dataPage[i]);
        map.add(startIdx);
        map.add(counter);
        startIdx += counter;
        counter = 1;
        continue;
      }
      counter++;
    }
    map.add(startIdx);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    if ((((list.size() + map.size()) * 100) / dataPage.length) > 70) {
      this.dataRlePage = new short[0];
      return dataPage;
    } else {
      this.dataRlePage = convertToArray(map);
      return convertToDataPage(list);
    }
  }

  private short[] convertToArray(List<Short> list) {
    short[] shortArray = new short[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }

  private Object[] convertToDataPage(List<Object> list) {
    Object[] shortArray = new Object[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }

  /**
   * @return the alreadySorted
   */
  public boolean isAlreadySorted() {
    return alreadySorted;
  }
}
