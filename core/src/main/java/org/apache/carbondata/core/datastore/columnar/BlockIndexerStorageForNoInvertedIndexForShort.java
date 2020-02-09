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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Below class will be used to for no inverted index
 */
public class BlockIndexerStorageForNoInvertedIndexForShort
    extends BlockIndexerStorage<ByteBuffer[]> {

  /**
   * column data
   */
  private ByteBuffer[] dataPage;

  private short[] dataRlePage;

  public BlockIndexerStorageForNoInvertedIndexForShort(ByteBuffer[] dataPage, boolean applyRLE) {
    this.dataPage = dataPage;
    if (applyRLE) {
      List<ByteBuffer> actualDataList = Arrays.asList(dataPage);
      rleEncodeOnData(actualDataList);
    }
  }

  private void rleEncodeOnData(List<ByteBuffer> actualDataList) {
    ByteBuffer prvKey = actualDataList.get(0);
    List<ByteBuffer> list = new ArrayList<>(actualDataList.size() / 2);
    list.add(actualDataList.get(0));
    short counter = 1;
    short start = 0;
    List<Short> map = new ArrayList<>(CarbonCommonConstants.CONSTANT_SIZE_TEN);
    for (int i = 1; i < actualDataList.size(); i++) {
      if (!prvKey.equals(actualDataList.get(i))) {
        prvKey = actualDataList.get(i);
        list.add(actualDataList.get(i));
        map.add(start);
        map.add(counter);
        start += counter;
        counter = 1;
        prvKey.rewind();
        actualDataList.get(i).rewind();
        continue;
      }
      prvKey.rewind();
      actualDataList.get(i).rewind();
      counter++;
    }
    map.add(start);
    map.add(counter);
    // if rle is index size is more than 70% then rle wont give any benefit
    // so better to avoid rle index and write data as it is
    boolean useRle = (((list.size() + map.size()) * 100) / actualDataList.size()) < 70;
    if (useRle) {
      this.dataPage = convertToDataPage(list);
      dataRlePage = convertToArray(map);
    } else {
      this.dataPage = convertToDataPage(actualDataList);
      dataRlePage = new short[0];
    }
  }

  private ByteBuffer[] convertToDataPage(List<ByteBuffer> list) {
    return list.toArray(new ByteBuffer[0]);
  }

  public short[] getDataRlePage() {
    return dataRlePage;
  }

  public int getDataRlePageLengthInBytes() {
    if (dataRlePage != null) {
      return dataRlePage.length * 2;
    } else {
      return 0;
    }
  }

  /**
   * no use
   *
   * @return
   */
  public short[] getRowIdPage() {
    return new short[0];
  }

  public int getRowIdPageLengthInBytes() {
    return 0;
  }

  /**
   * no use
   *
   * @return
   */
  public short[] getRowIdRlePage() {
    return new short[0];
  }

  public int getRowIdRlePageLengthInBytes() {
    return 0;
  }

  /**
   * @return the dataPage
   */
  public ByteBuffer[] getDataPage() {
    return dataPage;
  }
}
