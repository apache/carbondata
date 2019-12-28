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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * class that implements cacheable interface and methods specific to column dictionary
 */
public abstract class AbstractColumnDictionaryInfo implements DictionaryInfo {

  /**
   * list that will hold all the dictionary chunks for one column
   */
  protected List<List<byte[]>> dictionaryChunks = new CopyOnWriteArrayList<>();

  /**
   * minimum value of surrogate key, dictionary value key will start from count 1
   */
  protected static final int MINIMUM_SURROGATE_KEY = 1;

  /**
   * atomic integer to maintain the access count for a column access
   */
  protected AtomicInteger accessCount = new AtomicInteger();

  /**
   * file timestamp
   */
  protected long fileTimeStamp;

  /**
   * memory size of this object.We store it as calculation everytime is costly
   */
  protected long memorySize;

  /**
   * size of one dictionary bucket
   */
  private final int dictionaryOneChunkSize = CarbonUtil.getDictionaryChunkSize();

  /**
   * This method will return the timestamp of file based on which decision
   * the decision will be taken whether to read that file or not
   *
   * @return
   */
  @Override
  public long getFileTimeStamp() {
    return fileTimeStamp;
  }

  /**
   * This method will return the access count for a column based on which decision will be taken
   * whether to keep the object in memory
   *
   * @return
   */
  @Override
  public int getAccessCount() {
    return accessCount.get();
  }

  /**
   * This method will return the memory size of a column
   *
   * @return
   */
  @Override
  public long getMemorySize() {
    return memorySize;
  }

  /**
   * This method will decrement the access count for a column by 1
   * whenever a column usage is complete
   */
  private void decrementAccessCount() {
    if (accessCount.get() > 0) {
      accessCount.decrementAndGet();
    }
  }

  /**
   * The method return the list of dictionary chunks of a column
   * Applications Scenario.
   * For preparing the column Sort info while writing the sort index file.
   *
   * @return
   */
  @Override
  public DictionaryChunksWrapper getDictionaryChunks() {
    return new DictionaryChunksWrapper(dictionaryChunks);
  }

  /**
   * This method will release the objects and set default value for primitive types
   */
  @Override
  public void clear() {
    decrementAccessCount();
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
  @Override
  public String getDictionaryValueForKey(int surrogateKey) {
    String dictionaryValue = null;
    if (surrogateKey < MINIMUM_SURROGATE_KEY) {
      return dictionaryValue;
    }
    byte[] dictionaryValueInBytes = getDictionaryBytesFromSurrogate(surrogateKey);
    if (null != dictionaryValueInBytes) {
      dictionaryValue = new String(dictionaryValueInBytes,
          CarbonCommonConstants.DEFAULT_CHARSET_CLASS);
    }
    return dictionaryValue;
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
  @Override
  public byte[] getDictionaryValueForKeyInBytes(int surrogateKey) {
    if (surrogateKey < MINIMUM_SURROGATE_KEY) {
      return null;
    }
    return getDictionaryBytesFromSurrogate(surrogateKey);
  }

  /**
   * This method will find and return the dictionary value as byte array for a
   * given surrogate key
   *
   * @param surrogateKey
   * @return
   */
  protected byte[] getDictionaryBytesFromSurrogate(int surrogateKey) {
    byte[] dictionaryValueInBytes = null;
    // surrogate key starts from 1 and list index will start from 0, so lets say if surrogate
    // key is 10 then value will present at index 9 of the dictionary chunk list
    int actualSurrogateIndex = surrogateKey - 1;
    // lets say dictionaryOneChunkSize = 10, surrogateKey = 10, so bucket index will
    // be 0 and dictionary chunk index will be 9 to get the value
    int dictionaryBucketIndex = actualSurrogateIndex / dictionaryOneChunkSize;
    if (dictionaryChunks.size() > dictionaryBucketIndex) {
      int indexInsideBucket = actualSurrogateIndex % dictionaryOneChunkSize;
      List<byte[]> dictionaryBucketContainingSurrogateValue =
          dictionaryChunks.get(dictionaryBucketIndex);
      if (dictionaryBucketContainingSurrogateValue.size() > indexInsideBucket) {
        dictionaryValueInBytes = dictionaryBucketContainingSurrogateValue.get(indexInsideBucket);
      }
    }
    return dictionaryValueInBytes;
  }

  @Override
  public void invalidate() {

  }
}

