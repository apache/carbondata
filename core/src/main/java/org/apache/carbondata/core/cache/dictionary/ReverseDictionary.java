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

/**
 * This class will be used for dictionary key and value look up
 */
public class ReverseDictionary implements Dictionary {

  /**
   * Object which will hold the information related to this dictionary column
   */
  private ColumnReverseDictionaryInfo columnReverseDictionaryInfo;

  /**
   * @param columnReverseDictionaryInfo
   */
  public ReverseDictionary(ColumnReverseDictionaryInfo columnReverseDictionaryInfo) {
    this.columnReverseDictionaryInfo = columnReverseDictionaryInfo;
  }

  /**
   * This method will find and return the surrogate key for a given dictionary value
   * Applicable scenario:
   * 1. Incremental data load : Dictionary will not be generated for existing values. For
   * that values have to be looked up in the existing dictionary cache.
   * 2. Filter scenarios where from value surrogate key has to be found.
   *
   * @param value dictionary value
   * @return if found returns key else INVALID_SURROGATE_KEY
   */
  @Override public int getSurrogateKey(String value) {
    return columnReverseDictionaryInfo.getSurrogateKey(value);
  }

  /**
   * This method will find and return the surrogate key for a given dictionary value
   * Applicable scenario:
   * 1. Incremental data load : Dictionary will not be generated for existing values. For
   * that values have to be looked up in the existing dictionary cache.
   * 2. Filter scenarios where from value surrogate key has to be found.
   *
   * @param value dictionary value as byte array
   * @return if found returns key else INVALID_SURROGATE_KEY
   */
  @Override public int getSurrogateKey(byte[] value) {
    return columnReverseDictionaryInfo.getSurrogateKey(value);
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
    return columnReverseDictionaryInfo.getDictionaryValueForKey(surrogateKey);
  }

  /**
   * This method will find and return the dictionary value for a given surrogate key in bytes.
   * Applicable scenarios:
   * 1. Query final result preparation : While convert the final result which will
   * be surrogate key back to original dictionary values this method will be used
   *
   * @param surrogateKey a unique ID for a dictionary value
   * @return value if found else null
   */
  @Override public byte[] getDictionaryValueForKeyInBytes(int surrogateKey) {
    return columnReverseDictionaryInfo.getDictionaryValueForKeyInBytes(surrogateKey);
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
    return columnReverseDictionaryInfo.getSortedIndex(surrogateKey);
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
    return columnReverseDictionaryInfo.getDictionaryValueFromSortedIndex(sortedIndex);
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
    return columnReverseDictionaryInfo.getDictionaryChunks();
  }

  /**
   * This method will release the objects and set default value for primitive types
   */
  @Override public void clear() {
    if (null != columnReverseDictionaryInfo) {
      columnReverseDictionaryInfo.clear();
      columnReverseDictionaryInfo = null;
    }
  }

  /**
   * This method return the access count associated with the dictionary.
   * @return
   */
  @Override public int getAccessCount() {
    if (null != columnReverseDictionaryInfo) {
      return columnReverseDictionaryInfo.getAccessCount();
    }
    return 0;
  }
}
