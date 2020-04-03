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

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.BlockletIndexStore;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.log4j.Logger;

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
  private static final Logger LOGGER =
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
    if (!isCacheExists(cacheType)) {
      synchronized (lock) {
        if (!isCacheExists(cacheType)) {
          if (null == carbonLRUCache) {
            createLRULevelCacheInstance();
          }
          createBlockletIndexCache(cacheType);
        }
      }
    }
    return cacheTypeToCacheMap.get(cacheType);
  }

  /**
   * This method will check if a cache already exists for given cache type and store
   * if it is not present in the map
   */
  public <K, V> Cache<K, V> createCache(CacheType cacheType, String cacheClassName)
      throws Exception {
    //check if lru cache is null, if null create one
    //check if cache is null for given cache type, if null create one
    if (!isCacheExists(cacheType)) {
      synchronized (lock) {
        if (!isCacheExists(cacheType)) {
          if (null == carbonLRUCache) {
            createLRULevelCacheInstance();
          }
          Class<?> clazz = Class.forName(cacheClassName);
          Constructor<?> constructor = clazz.getConstructors()[0];
          constructor.setAccessible(true);
          Cache cacheObject = (Cache) constructor.newInstance(carbonLRUCache);
          cacheTypeToCacheMap.put(cacheType, cacheObject);
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
  private void createBlockletIndexCache(CacheType cacheType) {
    Cache cacheObject = null;
    if (cacheType.equals(cacheType.DRIVER_BLOCKLET_INDEX)) {
      cacheObject = new BlockletIndexStore(carbonLRUCache);
    }
    cacheTypeToCacheMap.put(cacheType, cacheObject);
  }

  /**
   * This method will create the lru cache instance based on the given type
   *
   */
  private void createLRULevelCacheInstance() {
    boolean isDriver = Boolean.parseBoolean(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE,
            CarbonCommonConstants.IS_DRIVER_INSTANCE_DEFAULT));
    if (isDriver) {
      carbonLRUCache = new CarbonLRUCache(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE,
          CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT);
    } else {
      // if executor cache size is not configured then driver cache conf will be used
      String executorCacheSize = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE);
      if (null != executorCacheSize) {
        carbonLRUCache =
            new CarbonLRUCache(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE,
                CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT);
      } else {
        LOGGER.info(
            "Executor LRU cache size not configured. Initializing with driver LRU cache size.");
        carbonLRUCache = new CarbonLRUCache(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE,
            CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT);
      }
    }
  }

  /**
   * This method will check whether the map already has an entry for
   * given cache type
   *
   * @param cacheType
   * @return
   */
  private boolean isCacheExists(CacheType cacheType) {
    return null != cacheTypeToCacheMap.get(cacheType);
  }

  public CarbonLRUCache getCarbonCache() {
    return carbonLRUCache;
  }
}
