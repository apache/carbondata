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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.log4j.Logger;

/**
 * class which manages the lru cache
 */
public final class CarbonLRUCache {
  /**
   * constant for converting MB into bytes
   */
  private static final int BYTE_CONVERSION_CONSTANT = 1024 * 1024;
  /**
   * Attribute for Carbon LOGGER
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonLRUCache.class.getName());
  /**
   * Map that will contain key as table unique name and value as cache Holder
   * object
   */
  private ExpiringMap<String, Cacheable> expiringMap;
  /**
   * lruCacheSize
   */
  private long lruCacheMemorySize;
  /**
   * totalSize size of the cache
   */
  private long currentSize;

  /**
   * @param propertyName        property name to take the size configured
   * @param defaultPropertyName default property in case size is not configured
   */
  public CarbonLRUCache(String propertyName, String defaultPropertyName) {
    try {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3305
      lruCacheMemorySize = Long
          .parseLong(CarbonProperties.getInstance().getProperty(propertyName, defaultPropertyName));
    } catch (NumberFormatException e) {
      LOGGER.error(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE
          + " is not in a valid format. Falling back to default value: "
          + CarbonCommonConstants.CARBON_MAX_LRU_CACHE_SIZE_DEFAULT);
      lruCacheMemorySize = Long.parseLong(defaultPropertyName);
    }

    // if lru cache is bigger than jvm max heap then set part size of max heap (60% default)
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3281
    if (isBeyondMaxMemory()) {
      double changeSize = getPartOfXmx();
      LOGGER.warn("Configured LRU size " + lruCacheMemorySize +
              "MB exceeds the max size of JVM heap. Carbon will fallback to use " +
              changeSize + " MB instead");
      lruCacheMemorySize = (long)changeSize;
    }

    initCache();
    if (lruCacheMemorySize > 0) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-484
      LOGGER.info("Configured LRU cache size is " + lruCacheMemorySize + " MB");
      // convert in bytes
      lruCacheMemorySize = lruCacheMemorySize * BYTE_CONVERSION_CONSTANT;
    } else {
      LOGGER.info("LRU cache size not configured. Therefore default behavior will be "
              + "considered and no LRU based eviction of columns will be done");
    }
  }

  /**
   * initialize lru cache
   */
  private void initCache() {
    // Cache entries can have individual variable expiration times and policies by adding
    // variableExpiration to the map. ExpirationPolicy.ACCESSED means the expiration can occur based
    // on last access time
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
    expiringMap =
        ExpiringMap.builder().expirationPolicy(ExpirationPolicy.ACCESSED).variableExpiration()
            .build();
  }

  /**
   * This method will give the list of all the keys that can be deleted from
   * the level LRU cache
   */
  private List<String> getKeysToBeRemoved(long size) {
    List<String> toBeDeletedKeys =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    long removedSize = 0;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
    for (Entry<String, Cacheable> entry : expiringMap.entrySet()) {
      String key = entry.getKey();
      Cacheable cacheInfo = entry.getValue();
      long memorySize = cacheInfo.getMemorySize();
      if (canBeRemoved(cacheInfo)) {
        removedSize = removedSize + memorySize;
        toBeDeletedKeys.add(key);
        // check if after removing the current file size, required
        // size when added to current size is sufficient to load a
        // level or not
        if (lruCacheMemorySize >= (currentSize - memorySize + size)) {
          toBeDeletedKeys.clear();
          toBeDeletedKeys.add(key);
          removedSize = memorySize;
          break;
        }
        // check if after removing the added size/removed size,
        // required size when added to current size is sufficient to
        // load a level or not
        else if (lruCacheMemorySize >= (currentSize - removedSize + size)) {
          break;
        }
      }
    }
    // this case will come when iteration is complete over the keys but
    // still size is not sufficient for level file to be loaded, then we
    // will not delete any of the keys
    if ((currentSize - removedSize + size) > lruCacheMemorySize) {
      toBeDeletedKeys.clear();
    }
    return toBeDeletedKeys;
  }

  /**
   * @param cacheInfo
   * @return
   */
  private boolean canBeRemoved(Cacheable cacheInfo) {
    if (cacheInfo.getAccessCount() > 0) {
      return false;
    }
    return true;
  }

  /**
   * @param key
   */
  public void remove(String key) {
    synchronized (expiringMap) {
      removeKey(key);
    }
  }

  /**
   * @param keys
   */
  public void removeAll(List<String> keys) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
    synchronized (expiringMap) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3305
      for (String key : keys) {
        removeKey(key);
      }
    }
  }

  /**
   * This method will remove the key from lru cache
   *
   * @param key
   */
  private void removeKey(String key) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
    Cacheable cacheable = expiringMap.get(key);
    if (null != cacheable) {
      long memorySize = cacheable.getMemorySize();
      cacheable.invalidate();
      expiringMap.remove(key);
      currentSize = currentSize - memorySize;
      LOGGER.info("Removed entry from InMemory lru cache :: " + key);
    }
  }

  /**
   * This method will check if required size is available in the memory and then add
   * the given cacheable to object to lru cache
   *
   * @param columnIdentifier
   * @param cacheInfo
   */
  public boolean put(String columnIdentifier, Cacheable cacheInfo, long requiredSize,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
      long expiration_time) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1049
    if (LOGGER.isDebugEnabled()) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-484
      LOGGER.debug("Required size for entry " + columnIdentifier + " :: " + requiredSize
          + " Current cache size :: " + currentSize);
    }
    boolean columnKeyAddedSuccessfully = false;
    if (isLRUCacheSizeConfigured()) {
      synchronized (expiringMap) {
        if (freeMemorySizeForAddingCache(requiredSize)) {
          currentSize = currentSize + requiredSize;
          addEntryToLRUCacheMap(columnIdentifier, cacheInfo, expiration_time);
          columnKeyAddedSuccessfully = true;
        } else {
          LOGGER.error(
              "Size not available. Entry cannot be added to lru cache :: " + columnIdentifier
                  + " .Required Size = " + requiredSize + " Size available " + (lruCacheMemorySize
                  - currentSize));
        }
      }
    } else {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
      synchronized (expiringMap) {
        addEntryToLRUCacheMap(columnIdentifier, cacheInfo, expiration_time);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3337
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3306
        currentSize = currentSize + requiredSize;
      }
      columnKeyAddedSuccessfully = true;
    }
    return columnKeyAddedSuccessfully;
  }

  /**
   * The method will add the cache entry to LRU cache map
   *
   * @param columnIdentifier
   * @param cacheInfo
   */
  private void addEntryToLRUCacheMap(String columnIdentifier, Cacheable cacheInfo,
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
      long expirationTimeSeconds) {
    if (null == expiringMap.get(columnIdentifier) && expirationTimeSeconds != 0L) {
      expiringMap.put(columnIdentifier, cacheInfo, ExpirationPolicy.ACCESSED, expirationTimeSeconds,
          TimeUnit.SECONDS);
    } else expiringMap.putIfAbsent(columnIdentifier, cacheInfo);
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-1049
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Added entry to InMemory lru cache :: " + columnIdentifier);
    }
  }

  /**
   * this will check whether the LRU cache size is configured
   *
   * @return <Boolean> value
   */
  private boolean isLRUCacheSizeConfigured() {
    return lruCacheMemorySize > 0;
  }

  /**
   * This method will check a required column can be loaded into memory or not. If required
   * this method will call for eviction of existing data from memory
   *
   * @param requiredSize
   * @return
   */
  private boolean freeMemorySizeForAddingCache(long requiredSize) {
    boolean memoryAvailable = false;
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-484
    if (isSizeAvailableToLoadColumnDictionary(requiredSize)) {
      memoryAvailable = true;
    } else {
      // get the keys that can be removed from memory
      List<String> keysToBeRemoved = getKeysToBeRemoved(requiredSize);
      for (String cacheKey : keysToBeRemoved) {
        removeKey(cacheKey);
      }
      // after removing the keys check again if required size is available
      if (isSizeAvailableToLoadColumnDictionary(requiredSize)) {
        memoryAvailable = true;
      }
    }
    return memoryAvailable;
  }

  /**
   * This method will check if size is available to laod dictionary into memory
   *
   * @param requiredSize
   * @return
   */
  private boolean isSizeAvailableToLoadColumnDictionary(long requiredSize) {
    return lruCacheMemorySize >= (currentSize + requiredSize);
  }

  /**
   * @param key
   * @return
   */
  public Cacheable get(String key) {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
    synchronized (expiringMap) {
      return expiringMap.get(key);
    }
  }

  /**
   * This method will empty the level cache
   */
  public void clear() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3665
    synchronized (expiringMap) {
      for (Cacheable cachebleObj : expiringMap.values()) {
        cachebleObj.invalidate();
      }
      expiringMap.clear();
    }
  }

  public Map<String, Cacheable> getCacheMap() {
    return expiringMap;
  }

  /**
   * Check if LRU cache setting is bigger than max memory of jvm.
   * if LRU cache is bigger than max memory of jvm when query for a big segments table,
   * may cause JDBC server crash.
   * @return true LRU cache is bigger than max memory of jvm, false otherwise
   */
  private boolean isBeyondMaxMemory() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3281
    long mSize = Runtime.getRuntime().maxMemory();
    long lruSize = lruCacheMemorySize * BYTE_CONVERSION_CONSTANT;
    return lruSize >= mSize;
  }

  /**
   * when LRU cache is bigger than max heap of jvm.
   * set to part of  max heap size, use CARBON_LRU_CACHE_PERCENT_OVER_MAX_SIZE default 60%.
   * @return the LRU cache size
   */
  private double getPartOfXmx() {
    long mSizeMB = Runtime.getRuntime().maxMemory() / BYTE_CONVERSION_CONSTANT;
    return mSizeMB * CarbonCommonConstants.CARBON_LRU_CACHE_PERCENT_OVER_MAX_SIZE;
  }

  /**
   * @return current size of the cache in memory.
   */
  public long getCurrentSize() {
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3337
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3306
    return currentSize;
  }
}
