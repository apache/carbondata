/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionaryCache;
import org.apache.carbondata.core.cache.dictionary.ReverseDictionaryCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

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
   * a map that will hold the mapping of cache type to LRU cache instance
   */
  private Map<CacheType, CarbonLRUCache> cacheTypeToLRUCacheMap =
      new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  /**
   * object lock instance to be used in synchronization block
   */
  private final Object lock = new Object();

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
   * @param carbonStorePath store path
   * @param <K>
   * @param <V>
   * @return
   */
  public <K, V> Cache<K, V> createCache(CacheType cacheType, String carbonStorePath) {
    //check if lru cache is null, if null create one
    //check if cache is null for given cache type, if null create one
    if (!dictionaryCacheAlreadyExists(cacheType)) {
      synchronized (lock) {
        if (!dictionaryCacheAlreadyExists(cacheType)) {
          if (null == cacheTypeToLRUCacheMap.get(cacheType)) {
            createLRULevelCacheInstance(cacheType);
          }
          createDictionaryCacheForGivenType(cacheType, carbonStorePath);
        }
      }
    }
    return cacheTypeToCacheMap.get(cacheType);
  }

  /**
   * This method will create the cache for given cache type
   *
   * @param cacheType       type of cache
   * @param carbonStorePath store path
   */
  private void createDictionaryCacheForGivenType(CacheType cacheType, String carbonStorePath) {
    Cache cacheObject = null;
    if (cacheType.equals(CacheType.REVERSE_DICTIONARY)) {
      cacheObject =
          new ReverseDictionaryCache<DictionaryColumnUniqueIdentifier, Dictionary>(carbonStorePath,
              cacheTypeToLRUCacheMap.get(cacheType));
    } else if (cacheType.equals(CacheType.FORWARD_DICTIONARY)) {
      cacheObject =
          new ForwardDictionaryCache<DictionaryColumnUniqueIdentifier, Dictionary>(carbonStorePath,
              cacheTypeToLRUCacheMap.get(cacheType));
    }
    cacheTypeToCacheMap.put(cacheType, cacheObject);
  }

  /**
   * This method will create the lru cache instance based on the given type
   *
   * @param cacheType
   */
  private void createLRULevelCacheInstance(CacheType cacheType) {
    CarbonLRUCache carbonLRUCache = null;
    // if cache type is dictionary cache, then same lru cache instance has to be shared
    // between forward and reverse cache
    if (cacheType.equals(CacheType.REVERSE_DICTIONARY) || cacheType
        .equals(CacheType.FORWARD_DICTIONARY)) {
      carbonLRUCache = new CarbonLRUCache(CarbonCommonConstants.CARBON_MAX_LEVEL_CACHE_SIZE,
          CarbonCommonConstants.CARBON_MAX_LEVEL_CACHE_SIZE_DEFAULT);
      cacheTypeToLRUCacheMap.put(CacheType.REVERSE_DICTIONARY, carbonLRUCache);
      cacheTypeToLRUCacheMap.put(CacheType.FORWARD_DICTIONARY, carbonLRUCache);
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
    cacheTypeToLRUCacheMap.clear();
    cacheTypeToCacheMap.clear();
  }
}
