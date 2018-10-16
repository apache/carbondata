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

public final class UnBlockIndexer {

  private UnBlockIndexer() {

  }

  public static int[] uncompressIndex(int[] indexData, int[] indexMap) {
    int actualSize = indexData.length;
    int mapLength = indexMap.length;
    if (indexMap.length == 0) {
      return indexData;
    }
    for (int i = 0; i < mapLength; i++) {
      actualSize += indexData[indexMap[i] + 1] - indexData[indexMap[i]] - 1;
    }
    int[] indexes = new int[actualSize];
    int k = 0;
    int oldIndex = 0;
    for (int i = 0; i < indexData.length; i++) {
      int index = Arrays.binarySearch(indexMap, oldIndex, mapLength, i);
      if (index > -1) {
        oldIndex = index;
        for (int j = indexData[indexMap[index]]; j <= indexData[indexMap[index] + 1]; j++) {
          indexes[k] = j;
          k++;
        }
        i++;
      } else {
        indexes[k] = indexData[i];
        k++;
      }
    }
    return indexes;
  }

  public static byte[] uncompressData(byte[] data, int[] index, int keyLen) {
    if (index.length < 1) {
      return data;
    }
    int numberOfCopy = 0;
    int actualSize = 0;
    int srcPos = 0;
    int destPos = 0;
    for (int i = 1; i < index.length; i += 2) {
      actualSize += index[i];
    }
    byte[] uncompressedData = new byte[actualSize * keyLen];
    int picIndex = 0;
    for (int i = 0; i < data.length; i += keyLen) {
      numberOfCopy = index[picIndex * 2 + 1];
      picIndex++;
      for (int j = 0; j < numberOfCopy; j++) {
        System.arraycopy(data, srcPos, uncompressedData, destPos, keyLen);
        destPos += keyLen;
      }
      srcPos += keyLen;
    }
    return uncompressedData;
  }

}
