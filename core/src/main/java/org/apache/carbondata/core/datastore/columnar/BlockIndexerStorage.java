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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

public abstract class BlockIndexerStorage<T> {

  public abstract short[] getRowIdPage();

  public abstract int getRowIdPageLengthInBytes();

  public abstract short[] getRowIdRlePage();

  public abstract int getRowIdRlePageLengthInBytes();

  public abstract T getDataPage();

  public abstract short[] getDataRlePage();

  public abstract int getDataRlePageLengthInBytes();

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
  protected Map<String, short[]> rleEncodeOnRowId(short[] rowIds) {
    short[] rowIdPage;
    short[] rowIdRlePage;
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
    int compressionPercentage = (((list.size() + map.size()) * 100) / rowIds.length);
    if (compressionPercentage > 70) {
      rowIdPage = rowIds;
    } else {
      rowIdPage = convertToArray(list);
    }
    if (rowIds.length == rowIdPage.length) {
      rowIdRlePage = new short[0];
    } else {
      rowIdRlePage = convertToArray(map);
    }
    Map<String, short[]> rowIdAndRowRleIdPages = new HashMap<>(2);
    rowIdAndRowRleIdPages.put("rowIdPage", rowIdPage);
    rowIdAndRowRleIdPages.put("rowRlePage", rowIdRlePage);
    return rowIdAndRowRleIdPages;
  }

  protected short[] convertToArray(List<Short> list) {
    short[] shortArray = new short[list.size()];
    for (int i = 0; i < shortArray.length; i++) {
      shortArray[i] = list.get(i);
    }
    return shortArray;
  }
}
