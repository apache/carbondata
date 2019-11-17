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

package org.apache.carbondata.core.localdictionary.dictionaryholder;

import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;

/**
 * Interface for storing the dictionary key and value.
 * Concrete implementation can be of map based or trie based.
 */
public interface DictionaryStore {

  /**
   * Below method will be used to add dictionary value to dictionary holder
   * if it is already present in the holder then it will return exiting dictionary value.
   * @param key
   * dictionary key
   * @return dictionary value
   */
  int putIfAbsent(byte[] key) throws DictionaryThresholdReachedException;

  /**
   * Below method to get the current size of dictionary
   * @return true if threshold of store reached
   */
  boolean isThresholdReached();

  /**
   * Below method will be used to get the dictionary key based on value
   * @param value
   * dictionary value
   * @return dictionary key based on value
   */
  byte[] getDictionaryKeyBasedOnValue(int value);

}
