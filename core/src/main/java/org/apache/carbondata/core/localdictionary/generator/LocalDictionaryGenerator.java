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

package org.apache.carbondata.core.localdictionary.generator;

import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;

/**
 * Interface for generating dictionary for column
 */
public interface LocalDictionaryGenerator {

  /**
   * Below method will be used to generate dictionary
   * @param data
   * data for which dictionary needs to be generated
   * @return dictionary value
   */
  int generateDictionary(byte[] data) throws DictionaryThresholdReachedException;

  /**
   * Below method will be used to check if threshold is reached
   * for dictionary for particular column
   * @return true if dictionary threshold reached for column
   */
  boolean isThresholdReached();

  /**
   * Below method will be used to get the dictionary key based on value
   * @param value
   * dictionary value
   * @return dictionary key based on value
   */
  byte[] getDictionaryKeyBasedOnValue(int value);

  /**
   * @return number of bytes used for storing the length of the dictionary value
   * for String type 2 Bytes and Long String 4 bytes
   */
  int getLVLength();
}
