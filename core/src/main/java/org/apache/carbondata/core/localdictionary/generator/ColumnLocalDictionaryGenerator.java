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

import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.localdictionary.dictionaryholder.DictionaryStore;
import org.apache.carbondata.core.localdictionary.dictionaryholder.MapBasedDictionaryStore;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;

/**
 * Class to generate local dictionary for column
 */
public class ColumnLocalDictionaryGenerator implements LocalDictionaryGenerator {

  /**
   * dictionary holder to hold dictionary values
   */
  private DictionaryStore dictionaryHolder;

  public ColumnLocalDictionaryGenerator(int threshold, int lvLength) {
    // adding 1 to threshold for null value
    int newThreshold = threshold + 1;
    this.dictionaryHolder = new MapBasedDictionaryStore(newThreshold);
    ByteBuffer byteBuffer = ByteBuffer.allocate(
        lvLength + CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);

    if (lvLength == CarbonCommonConstants.SHORT_SIZE_IN_BYTE) {
      byteBuffer.putShort((short) CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
    } else {
      byteBuffer.putInt(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY.length);
    }
    byteBuffer.put(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
    // for handling null values
    try {
      dictionaryHolder.putIfAbsent(byteBuffer.array());
    } catch (DictionaryThresholdReachedException e) {
      // do nothing
    }
  }

  /**
   * Below method will be used to generate dictionary
   * @param data
   * data for which dictionary needs to be generated
   * @return dictionary value
   */
  @Override public int generateDictionary(byte[] data) throws DictionaryThresholdReachedException {
    return this.dictionaryHolder.putIfAbsent(data);
  }

  /**
   * Below method will be used to check if threshold is reached
   * for dictionary for particular column
   * @return true if dictionary threshold reached for column
   */
  @Override public boolean isThresholdReached() {
    return this.dictionaryHolder.isThresholdReached();
  }

  /**
   * Below method will be used to get the dictionary key based on value
   * @param value
   * dictionary value
   * @return dictionary key based on value
   */
  @Override public byte[] getDictionaryKeyBasedOnValue(int value) {
    return this.dictionaryHolder.getDictionaryKeyBasedOnValue(value);
  }
}
