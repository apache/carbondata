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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.cache.dictionary.DictionaryByteArrayWrapper;
import org.apache.carbondata.core.localdictionary.exception.DictionaryThresholdReachedException;

/**
 * Map based dictionary holder class, it will use map to hold
 * the dictionary key and its value
 */
public class MapBasedDictionaryStore implements DictionaryStore {

  /**
   * use to assign dictionary value to new key
   */
  private int lastAssignValue;

  /**
   * to maintain dictionary key value
   */
  private final Map<DictionaryByteArrayWrapper, Integer> dictionary;

  /**
   * maintaining array for reverse lookup
   * otherwise iterating everytime in map for reverse lookup will be slowdown the performance
   * It will only maintain the reference
   */
  private DictionaryByteArrayWrapper[] referenceDictionaryArray;

  /**
   * dictionary threshold to check if threshold is reached
   */
  private int dictionaryThreshold;

  /**
   * for checking threshold is reached or not
   */
  private boolean isThresholdReached;

  /**
   * current datasize
   */
  private long currentSize;

  public MapBasedDictionaryStore(int dictionaryThreshold) {
    this.dictionaryThreshold = dictionaryThreshold;
    this.dictionary = new ConcurrentHashMap<>();
    this.referenceDictionaryArray = new DictionaryByteArrayWrapper[dictionaryThreshold];
  }

  /**
   * Below method will be used to add dictionary value to dictionary holder
   * if it is already present in the holder then it will return exiting dictionary value.
   *
   * @param data dictionary key
   * @return dictionary value
   */
  @Override public int putIfAbsent(byte[] data) throws DictionaryThresholdReachedException {
    // check if threshold has already reached
    checkIfThresholdReached();
    DictionaryByteArrayWrapper key = new DictionaryByteArrayWrapper(data);
    // get the dictionary value
    Integer value = dictionary.get(key);
    // if value is null then dictionary is not present in store
    if (null == value) {
      // aquire the lock
      synchronized (dictionary) {
        // check threshold
        checkIfThresholdReached();
        // get the value again as other thread might have added
        value = dictionary.get(key);
        // double chekcing
        if (null == value) {
          // increment the value
          value = ++lastAssignValue;
          currentSize += data.length;
          // if new value is greater than threshold
          if (value > dictionaryThreshold || currentSize >= Integer.MAX_VALUE) {
            // set the threshold boolean to true
            isThresholdReached = true;
            // throw exception
            checkIfThresholdReached();
          }
          // add to reference array
          // position is -1 as dictionary value starts from 1
          this.referenceDictionaryArray[value - 1] = key;
          dictionary.put(key, value);
        }
      }
    }
    return value;
  }

  private void checkIfThresholdReached() throws DictionaryThresholdReachedException {
    if (isThresholdReached) {
      if (currentSize >= Integer.MAX_VALUE) {
        throw new DictionaryThresholdReachedException(
            "Unable to generate dictionary. Dictionary Size crossed 2GB limit");
      } else {
        throw new DictionaryThresholdReachedException(
            "Unable to generate dictionary value. Dictionary threshold reached");
      }
    }
  }

  /**
   * Below method to get the current size of dictionary
   *
   * @return
   */
  @Override public boolean isThresholdReached() {
    return isThresholdReached;
  }

  /**
   * Below method will be used to get the dictionary key based on value
   *
   * @param value dictionary value
   *              Caller will take of passing proper value
   * @return dictionary key based on value
   */
  @Override public byte[] getDictionaryKeyBasedOnValue(int value) {
    assert referenceDictionaryArray != null;
    // reference array index will be -1 of the value as dictionary value starts from 1
    return referenceDictionaryArray[value - 1].getData();
  }
}
