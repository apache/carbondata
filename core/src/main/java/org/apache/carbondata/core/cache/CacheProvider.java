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

package org.apache.carbondata.core.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionaryCache;
import org.apache.carbondata.core.cache.dictionary.ReverseDictionaryCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BlockIndexStore;
import org.apache.carbondata.core.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexStore;
import org.apache.carbondata.core.api.CarbonProperties;

/**
 * Cache provider class which will create a cache based on given type
 */
public class CacheProvider {

  /**
   * cache provider instance
   */
  private static CacheProvider cacheProvider = new CacheProvider();

  /**
   * a map that will hold the entry for cache type to cache object mapping
   */
  private Map<CacheType, Cache> cacheTypeToCacheMap =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * object lock instance to be used in synchronization block
   */
  private final Object lock = new Object();
  /**
   * LRU cache instance
   */
  private CarbonLRUCache carbonLRUCache;

  /**
   * instance for CacheProvider LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CacheProvider.class.getName());

  /**
   * private constructor to follow singleton design pattern for this class
   */
  private CacheProvider() {

  }

  /**
   * @return cache provider instance
   */
  public static CacheProvider getInstance() {
    return cacheProvider;
  }

  /**
   * This method will check if a cache already exists for given cache type and create in case
   * it is not present in the map
   *
   * @param cacheType       type of cache
   * @param <K>
   * @param <V>
   * @return
   */
  public <K, V> Cache<K, V> createCache(CacheType cacheType) {
    //check if lru cache is null, if null create one
    //check if cache is null for given cache type, if null create one
    if (!dictionaryCacheAlreadyExists(cacheType)) {
      synchronized (lock) {
        if (!dictionaryCacheAlreadyExists(cacheType)) {
          if (null == carbonLRUCache) {
            createLRULevelCacheInstance(cacheType);
          }
          createDictionaryCacheForGivenType(cacheType);
        }
      }
    }
    return cacheTypeToCacheMap.get(cacheType);
  }

  /**
   * This method will create the cache for given cache type
   *
   * @param cacheType       type of cache
   */
  private void createDictionaryCacheForGivenType(CacheType cacheType) {
    Cache cacheObject = null;
    if (cacheType.equals(CacheType.REVERSE_DICTIONARY)) {
      cacheObject =
          new ReverseDictionaryCache<DictionaryColumnUniqueIdentifier, Dictionary>(carbonLRUCache);
    } else if (cacheType.equals(CacheType.FORWARD_DICTIONARY)) {
      cacheObject =
          new ForwardDictionaryCache<DictionaryColumnUniqueIdentifier, Dictionary>(carbonLRUCache);
    } else if (cacheType.equals(cacheType.EXECUTOR_BTREE)) {
      cacheObject = new BlockIndexStore<TableBlockUniqueIdentifier, AbstractIndex>(carbonLRUCache);
    } else if (cacheType.equals(cacheType.DRIVER_BTREE)) {
      cacheObject =
          new SegmentTaskIndexStore(carbonLRUCache);
    } else if (cacheType.equals(cacheType.DRIVER_BLOCKLET_DATAMAP)) {
      cacheObject = new BlockletDataMapIndexStore(carbonLRUCache);
    }
    cacheTypeToCacheMap.put(cacheType, cacheObject);
  }

  /**
   * This method will create the lru cache instance based on the given type
   *
   * @param cacheType
   */
  private void createLRULevelCacheInstance(CacheType cacheType) {
    boolean isDriver = CarbonProperties.IS_DRIVER_INSTANCE.getOrDefault();
    if (isDriver) {
      carbonLRUCache = new CarbonLRUCache(CarbonProperties.MAX_DRIVER_LRU_CACHE_SIZE.getOrDefault());
    } else {
      carbonLRUCache = new CarbonLRUCache(CarbonProperties.MAX_EXECUTOR_LRU_CACHE_SIZE.getOrDefault());
    }
  }

  /**
   * This method will check whether the map already has an entry for
   * given cache type
   *
   * @param cacheType
   * @return
   */
  private boolean dictionaryCacheAlreadyExists(CacheType cacheType) {
    return null != cacheTypeToCacheMap.get(cacheType);
  }

  /**
   * Below method will be used to clear the cache
   */
  public void dropAllCache() {
    if (null != carbonLRUCache) {
      carbonLRUCache.clear();
      carbonLRUCache = null;
    }
    cacheTypeToCacheMap.clear();
  }
}
