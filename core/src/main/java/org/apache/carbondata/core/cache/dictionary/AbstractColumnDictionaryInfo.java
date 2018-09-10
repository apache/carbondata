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

import java.nio.charset.Charset;
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
   * offset till where file is read
   */
  protected long offsetTillFileIsRead;

  /**
   * memory size of this object.We store it as calculation everytime is costly
   */
  protected long memorySize;

  /**
   * length of dictionary metadata file
   */
  private long dictionaryMetaFileLength;

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
  @Override public long getFileTimeStamp() {
    return fileTimeStamp;
  }

  /**
   * This method will return the access count for a column based on which decision will be taken
   * whether to keep the object in memory
   *
   * @return
   */
  @Override public int getAccessCount() {
    return accessCount.get();
  }

  /**
   * This method will return the memory size of a column
   *
   * @return
   */
  @Override public long getMemorySize() {
    return memorySize;
  }

  @Override public void setMemorySize(long memorySize) {
    this.memorySize = memorySize;
  }

  /**
   * This method will increment the access count for a column by 1
   * whenever a column is getting used in query or incremental data load
   */
  @Override public void incrementAccessCount() {
    accessCount.incrementAndGet();
  }

  /**
   * This method will return the size of of last dictionary chunk so that only that many
   * values are read from the dictionary reader
   *
   * @return size of last dictionary chunk
   */
  @Override public int getSizeOfLastDictionaryChunk() {
    return 0;
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
   * This method will update the end offset of file everytime a file is read
   *
   * @param offsetTillFileIsRead
   */
  @Override public void setOffsetTillFileIsRead(long offsetTillFileIsRead) {
    this.offsetTillFileIsRead = offsetTillFileIsRead;
  }

  @Override public long getOffsetTillFileIsRead() {
    return offsetTillFileIsRead;
  }

  /**
   * This method will update the timestamp of a file if a file is modified
   * like in case of incremental load
   *
   * @param fileTimeStamp
   */
  @Override public void setFileTimeStamp(long fileTimeStamp) {
    this.fileTimeStamp = fileTimeStamp;
  }

  /**
   * The method return the list of dictionary chunks of a column
   * Applications Scenario.
   * For preparing the column Sort info while writing the sort index file.
   *
   * @return
   */
  @Override public DictionaryChunksWrapper getDictionaryChunks() {
    return new DictionaryChunksWrapper(dictionaryChunks);
  }

  /**
   * This method will release the objects and set default value for primitive types
   */
  @Override public void clear() {
    decrementAccessCount();
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
    return 0;
  }

  /**
   * dictionary metadata file length which will be set whenever we reload dictionary
   * data from disk
   *
   * @param dictionaryMetaFileLength length of dictionary metadata file
   */
  @Override public void setDictionaryMetaFileLength(long dictionaryMetaFileLength) {
    this.dictionaryMetaFileLength = dictionaryMetaFileLength;
  }

  /**
   * Dictionary meta file offset which will be read to check whether length of dictionary
   * meta file has been modified
   *
   * @return
   */
  @Override public long getDictionaryMetaFileLength() {
    return dictionaryMetaFileLength;
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
    return null;
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
  @Override public byte[] getDictionaryValueForKeyInBytes(int surrogateKey) {
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
    byte[] keyData = value.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    return getSurrogateKey(keyData);
  }

  @Override public void invalidate() {

  }
}

