/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.cache.dictionary;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;

/**
 * This class will be used for dictionary key and value look up
 */
public class ForwardDictionary implements Dictionary {

  /**
   * Object which will hold the information related to this dictionary column
   */
  private ColumnDictionaryInfo columnDictionaryInfo;

  /**
   * @param columnDictionaryInfo
   */
  public ForwardDictionary(ColumnDictionaryInfo columnDictionaryInfo) {
    this.columnDictionaryInfo = columnDictionaryInfo;
  }

  /**
   * This method will find and return the surrogate key for a given dictionary value
   * Applicable scenario:
   * 1. Incremental data load : Dictionary will not be generated for existing values. For
   * that values have to be looked up in the existing dictionary cache.
   * 2. Filter scenarios where from value surrogate key has to be found.
   *
   * @param value dictionary value
   * @return if found returns key else 0
   */
  @Override public int getSurrogateKey(String value) {
    return columnDictionaryInfo.getSurrogateKey(value);
  }

  /**
   * This method will find and return the surrogate key for a given dictionary value
   * Applicable scenario:
   * 1. Incremental data load : Dictionary will not be generated for existing values. For
   * that values have to be looked up in the existing dictionary cache.
   * 2. Filter scenarios where from value surrogate key has to be found.
   *
   * @param value dictionary value as byte array
   * @return if found returns key else 0
   */
  @Override public int getSurrogateKey(byte[] value) {
    return columnDictionaryInfo.getSurrogateKey(value);
  }

  /**
   * This method will find and return the dictionary value for a given surrogate key.
   * Applicable scenarios:
   * 1. Query final result preparation : While convert the final result which will
   * be surrogate key back to original dictionary values this method will be used
   *
   * @param surrogateKey a unique ID for a dictionary value
   * @return value if found else null
   */
  @Override public String getDictionaryValueForKey(int surrogateKey) {
    return columnDictionaryInfo.getDictionaryValueForKey(surrogateKey);
  }

  /**
   * This method will find and return the sort index for a given dictionary id.
   * Applicable scenarios:
   * 1. Used in case of order by queries when data sorting is required
   *
   * @param surrogateKey a unique ID for a dictionary value
   * @return if found returns key else 0
   */
  @Override public int getSortedIndex(int surrogateKey) {
    return columnDictionaryInfo.getSortedIndex(surrogateKey);
  }

  /**
   * This method will find and return the dictionary value from sorted index.
   * Applicable scenarios:
   * 1. Query final result preparation in case of order by queries:
   * While convert the final result which will
   * be surrogate key back to original dictionary values this method will be used
   *
   * @param sortedIndex sort index of dictionary value
   * @return value if found else null
   */
  @Override public String getDictionaryValueFromSortedIndex(int sortedIndex) {
    return columnDictionaryInfo.getDictionaryValueFromSortedIndex(sortedIndex);
  }

  /**
   * The method return the dictionary chunks wrapper of a column
   * The wrapper wraps the list<list<bye[]>> and provide the iterator to retrieve the chunks
   * members.
   * Applications Scenario:
   * For preparing the column Sort info while writing the sort index file.
   *
   * @return
   */
  @Override public DictionaryChunksWrapper getDictionaryChunks() {
    return columnDictionaryInfo.getDictionaryChunks();
  }

  /**
   * This method will release the objects and set default value for primitive types
   */
  @Override public void clear() {
    columnDictionaryInfo.clear();
    columnDictionaryInfo = null;
  }

  /**
   * This method will read the surrogates based on search range.
   *
   * @param surrogates
   */
  public void getSurrogateKeyByIncrementalSearch(List<String> evaluateResultList,
      List<Integer> surrogates) {
    List<byte[]> byteValuesOfFilterMembers = new ArrayList<byte[]>(evaluateResultList.size());
    byte[] keyData = null;
    for (int i = 0; i < evaluateResultList.size(); i++) {
      keyData = evaluateResultList.get(i)
          .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      byteValuesOfFilterMembers.add(keyData);
    }

    columnDictionaryInfo
        .getIncrementalSurrogateKeyFromDictionary(byteValuesOfFilterMembers, surrogates);
  }
}
