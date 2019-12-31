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
 * dictionary interface which declares methods for finding surrogate key for a
 * given dictionary value and finding dictionary value from a given surrogate key
 */
public interface Dictionary {

  /**
   * This method will find and return the dictionary value for a given surrogate key.
   * Applicable scenarios:
   * 1. Query final result preparation : While convert the final result which will
   * be surrogate key back to original dictionary values this method will be used
   *
   * @param surrogateKey a unique ID for a dictionary value
   * @return value if found else null
   */
  String getDictionaryValueForKey(int surrogateKey);

  /**
   * This method will find and return the dictionary value for a given surrogate key in bytes.
   * It is as same as getDictionaryValueForKey but it does not convert bytes to String,
   * it returns bytes directly. User can convert to String by using new String(bytes).
   * Applicable scenarios:
   * 1. Query final result preparation : While convert the final result which will
   * be surrogate key back to original dictionary values this method will be used
   *
   * @param surrogateKey a unique ID for a dictionary value
   * @return value if found else null
   */
  byte[] getDictionaryValueForKeyInBytes(int surrogateKey);

  /**
   * The method return the dictionary chunks wrapper of a column
   * The wrapper wraps the list<list<bye[]>> and provide the iterator to retrieve the chunks
   * members.
   * Applications Scenario:
   * For preparing the column Sort info while writing the sort index file.
   *
   * @return
   */
  DictionaryChunksWrapper getDictionaryChunks();

  /**
   * This method will release the objects and set default value for primitive types
   */
  void clear();

}
