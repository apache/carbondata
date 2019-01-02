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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.reader.CarbonDictionaryColumnMetaChunk;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ObjectSizeCalculator;
import org.apache.carbondata.core.util.TaskMetricsMap;

import org.apache.log4j.Logger;

/**
 * This class implements methods to create dictionary cache which will hold
 * dictionary chunks for look up of surrogate keys and values
 */
public class ForwardDictionaryCache<K extends
    DictionaryColumnUniqueIdentifier, V extends Dictionary> extends AbstractDictionaryCache<K, V> {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(ForwardDictionaryCache.class.getName());

  private static final Map<DictionaryColumnUniqueIdentifier, Object> DICTIONARY_LOCK_OBJECT =
      new ConcurrentHashMap<>();

  private static final long sizeOfEmptyDictChunks =
      ObjectSizeCalculator.estimate(new ArrayList<byte[]>(CarbonUtil.getDictionaryChunkSize()), 16);

  private static final long byteArraySize = ObjectSizeCalculator.estimate(new byte[0], 16);

  /**
   * @param carbonLRUCache
   */
  public ForwardDictionaryCache(CarbonLRUCache carbonLRUCache) {
    super(carbonLRUCache);
  }

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @return dictionary
   * @throws IOException in case memory is not sufficient to load dictionary into memory
   */
  @Override public Dictionary get(DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier)
      throws IOException {
    return getDictionary(dictionaryColumnUniqueIdentifier);
  }

  /**
   * This method will return a list of values for the given list of keys.
   * For each key, this method will check and load the data if required.
   *
   * @param dictionaryColumnUniqueIdentifiers unique identifier which contains dbName,
   *                                          tableName and columnIdentifier
   * @return list of dictionary
   * @throws IOException in case memory is not sufficient to load dictionary into memory
   */
  @Override public List<Dictionary> getAll(
      List<DictionaryColumnUniqueIdentifier> dictionaryColumnUniqueIdentifiers) throws IOException {
    boolean exceptionOccurredInDictionaryLoading = false;
    String exceptionMessage = "";
    List<Dictionary> forwardDictionaryObjectList =
        new ArrayList<Dictionary>(dictionaryColumnUniqueIdentifiers.size());
    List<Future<Dictionary>> taskSubmitList =
        new ArrayList<>(dictionaryColumnUniqueIdentifiers.size());
    ExecutorService executorService = Executors.newFixedThreadPool(threadPoolSize);
    for (final DictionaryColumnUniqueIdentifier uniqueIdent : dictionaryColumnUniqueIdentifiers) {
      taskSubmitList.add(executorService.submit(new Callable<Dictionary>() {
        @Override public Dictionary call() throws IOException {
          try {
            // Register thread callback for calculating metrics
            TaskMetricsMap.getInstance().registerThreadCallback();
            // in case of multiple task for same query same executor
            // only one task should load the dictionary
            // others will wait on monitor and get the loaded dictionary values
            Object lockObject = DICTIONARY_LOCK_OBJECT.get(uniqueIdent);
            // if lock object is null
            if (null == lockObject) {
              // Acquire the lock on map
              synchronized (DICTIONARY_LOCK_OBJECT) {
                // double checking the dictionary lock object
                lockObject = DICTIONARY_LOCK_OBJECT.get(uniqueIdent);
                // if still it is null add new lock object
                if (null == lockObject) {
                  lockObject = new Object();
                  DICTIONARY_LOCK_OBJECT.put(uniqueIdent, lockObject);
                }
              }
            }
            Dictionary dictionary = null;
            synchronized (lockObject) {
              dictionary = getDictionary(uniqueIdent);
            }
            return dictionary;
          }  finally {
            // update read bytes metrics for this thread
            TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId());
          }

        }
      }));
    }
    try {
      executorService.shutdown();
      executorService.awaitTermination(2, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      LOGGER.error("Error loading the dictionary: " + e.getMessage());
    }
    for (int i = 0; i < taskSubmitList.size(); i++) {
      try {
        Dictionary columnDictionary = taskSubmitList.get(i).get();
        forwardDictionaryObjectList.add(columnDictionary);
      } catch (Throwable e) {
        exceptionOccurredInDictionaryLoading = true;
        exceptionMessage = e.getMessage();
      }
    }
    if (exceptionOccurredInDictionaryLoading) {
      clearDictionary(forwardDictionaryObjectList);
      LOGGER.error(exceptionMessage);
      throw new IOException(exceptionMessage);
    }
    return forwardDictionaryObjectList;
  }

  /**
   * This method will return the value for the given key. It will not check and load
   * the data for the given key
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @return
   */
  @Override public Dictionary getIfPresent(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    Dictionary forwardDictionary = null;
    ColumnDictionaryInfo columnDictionaryInfo = (ColumnDictionaryInfo) carbonLRUCache.get(
        getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId(),
            CacheType.FORWARD_DICTIONARY));
    if (null != columnDictionaryInfo) {
      forwardDictionary = new ForwardDictionary(columnDictionaryInfo);
      incrementDictionaryAccessCount(columnDictionaryInfo);
    }
    return forwardDictionary;
  }

  /**
   * This method will remove the cache for a given key
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   */
  @Override public void invalidate(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) {
    carbonLRUCache.remove(
        getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId(),
            CacheType.FORWARD_DICTIONARY));
  }

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * @param dictionaryColumnUniqueIdentifier unique identifier which contains dbName,
   *                                         tableName and columnIdentifier
   * @return dictionary
   * @throws IOException in case memory is not sufficient to load dictionary into memory
   */
  private Dictionary getDictionary(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier) throws IOException {
    Dictionary forwardDictionary = null;
    // dictionary is only for primitive data type
    assert (!dictionaryColumnUniqueIdentifier.getDataType().isComplexType());
    String columnIdentifier = dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId();
    ColumnDictionaryInfo columnDictionaryInfo =
        getColumnDictionaryInfo(dictionaryColumnUniqueIdentifier, columnIdentifier);
    // load sort index file in case of forward dictionary
    checkAndLoadDictionaryData(dictionaryColumnUniqueIdentifier, columnDictionaryInfo,
        getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId(),
            CacheType.FORWARD_DICTIONARY), true);
    forwardDictionary = new ForwardDictionary(columnDictionaryInfo);
    return forwardDictionary;
  }

  /**
   * This method will check and create columnDictionaryInfo object for the given column
   *
   * @param dictionaryColumnUniqueIdentifier
   * @param columnIdentifier
   * @return
   */
  private ColumnDictionaryInfo getColumnDictionaryInfo(
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, String columnIdentifier) {
    ColumnDictionaryInfo columnDictionaryInfo = (ColumnDictionaryInfo) carbonLRUCache
        .get(getLruCacheKey(columnIdentifier, CacheType.FORWARD_DICTIONARY));
    if (null == columnDictionaryInfo) {
      synchronized (dictionaryColumnUniqueIdentifier) {
        columnDictionaryInfo = (ColumnDictionaryInfo) carbonLRUCache
            .get(getLruCacheKey(columnIdentifier, CacheType.FORWARD_DICTIONARY));
        if (null == columnDictionaryInfo) {
          columnDictionaryInfo =
              new ColumnDictionaryInfo(dictionaryColumnUniqueIdentifier.getDataType());
        }
      }
    }
    return columnDictionaryInfo;
  }

  @Override public void clearAccessCount(List<DictionaryColumnUniqueIdentifier> keys) {
    for (DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier : keys) {
      Dictionary cacheable = (Dictionary) carbonLRUCache.get(
          getLruCacheKey(dictionaryColumnUniqueIdentifier.getColumnIdentifier().getColumnId(),
              CacheType.FORWARD_DICTIONARY));
      cacheable.clear();
    }
  }

  @Override protected long getEstimatedDictionarySize(DictionaryInfo dictionaryInfo,
      CarbonDictionaryColumnMetaChunk carbonDictionaryColumnMetaChunk,
      DictionaryColumnUniqueIdentifier dictionaryColumnUniqueIdentifier, boolean
      readSortIndexSize) throws IOException {
    // required size will be size total size of file - offset till file is
    // already read
    long requiredSize =
        carbonDictionaryColumnMetaChunk.getEnd_offset() -
            dictionaryInfo.getOffsetTillFileIsRead();

    long numOfRecords = dictionaryInfo.getOffsetTillFileIsRead() == 0 ?
        carbonDictionaryColumnMetaChunk.getMax_surrogate_key() :
        carbonDictionaryColumnMetaChunk.getMax_surrogate_key()
            - getNumRecordsInCarbonDictionaryColumnMetaChunk(
            dictionaryColumnUniqueIdentifier,
            dictionaryInfo.getOffsetTillFileIsRead());

    if (numOfRecords > 0) {
      long avgRecordsSize = requiredSize / numOfRecords;
      long bytesPerRecord = (long)Math.ceil(avgRecordsSize / 8.0) * 8;

      requiredSize = (bytesPerRecord + byteArraySize) * numOfRecords;
    }

    if (readSortIndexSize) {
      // every time we are loading all the sort index files.Hence memory calculation for all
      // the records
      requiredSize = requiredSize + getSortIndexSize(
          carbonDictionaryColumnMetaChunk.getMax_surrogate_key());
    }

    return requiredSize + sizeOfEmptyDictChunks;
  }
}
