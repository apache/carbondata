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

package org.apache.carbondata.core.cache.dictionary;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import net.jpountz.xxhash.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

/**
 * class that implements methods specific for dictionary data look up
 */
public class ColumnReverseDictionaryInfo extends AbstractColumnDictionaryInfo {

  /**
   * Map which will maintain mapping of byte array to surrogate key
   */
  private Map<DictionaryByteArrayWrapper, Integer> dictionaryByteArrayToSurrogateKeyMap;

  /**
   * hashing algorithm to calculate hash code
   */
  private XXHash32 xxHash32;

  /**
   * check and initialize xxHash32 if enabled
   */
  public ColumnReverseDictionaryInfo() {
    boolean useXXHash = Boolean.valueOf(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.ENABLE_XXHASH,
            CarbonCommonConstants.ENABLE_XXHASH_DEFAULT));
    if (useXXHash) {
      xxHash32 = XXHashFactory.fastestInstance().hash32();
    }
  }

  /**
   * This method will find and return the surrogate key for a given dictionary value
   * Applicable scenario:
   * 1. Incremental data load : Dictionary will not be generated for existing values. For
   * that values have to be looked up in the existing dictionary cache.
   * 2. Filter scenarios where from value surrogate key has to be found.
   *
   * @param value dictionary value as byte array. It will be treated as key here
   * @return if found returns key else INVALID_SURROGATE_KEY
   */
  @Override public int getSurrogateKey(byte[] value) {
    DictionaryByteArrayWrapper dictionaryByteArrayWrapper =
        new DictionaryByteArrayWrapper(value, xxHash32);
    Integer surrogateKeyInMap =
        dictionaryByteArrayToSurrogateKeyMap.get(dictionaryByteArrayWrapper);
    if (null == surrogateKeyInMap) {
      return CarbonCommonConstants.INVALID_SURROGATE_KEY;
    }
    return surrogateKeyInMap;
  }

  /**
   * This method will add a new dictionary chunk to existing list of dictionary chunks
   *
   * @param dictionaryChunk
   */
  @Override public void addDictionaryChunk(List<byte[]> dictionaryChunk) {
    dictionaryChunks.add(dictionaryChunk);
    if (null == dictionaryByteArrayToSurrogateKeyMap) {
      createDictionaryByteArrayToSurrogateKeyMap(dictionaryChunk.size());
    }
    addDataToDictionaryMap();
  }

  /**
   * This method will add the new dictionary data to map
   */
  private void addDataToDictionaryMap() {
    int surrogateKey = dictionaryByteArrayToSurrogateKeyMap.size();
    List<byte[]> oneDictionaryChunk = dictionaryChunks.get(dictionaryChunks.size() - 1);
    for (int i = 0; i < oneDictionaryChunk.size(); i++) {
      // create a wrapper class that will calculate hash code for byte array
      DictionaryByteArrayWrapper dictionaryByteArrayWrapper =
          new DictionaryByteArrayWrapper(oneDictionaryChunk.get(i), xxHash32);
      dictionaryByteArrayToSurrogateKeyMap.put(dictionaryByteArrayWrapper, ++surrogateKey);
    }
  }

  /**
   * This method will create the dictionary map. First time it will
   * create dictionary map with capacity equal to list of byte arrays
   *
   * @param initialMapSize capacity to which map is to be instantiated
   */
  private void createDictionaryByteArrayToSurrogateKeyMap(int initialMapSize) {
    dictionaryByteArrayToSurrogateKeyMap = new ConcurrentHashMap<>(initialMapSize);
  }

  /**
   * This method will set the sort order index of a dictionary column.
   * Sort order index if the index of dictionary values after they are sorted.
   *
   * @param sortOrderIndex
   */
  @Override public void setSortOrderIndex(List<Integer> sortOrderIndex) {
  }

  /**
   * This method will set the sort reverse index of a dictionary column.
   * Sort reverse index is the index of dictionary values before they are sorted.
   *
   * @param sortReverseOrderIndex
   */
  @Override public void setSortReverseOrderIndex(List<Integer> sortReverseOrderIndex) {
  }
}
