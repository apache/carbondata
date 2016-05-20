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

package org.carbondata.core.cache;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;

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
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonLRUCache.class.getName());
  /**
   * Map that will contain key as cube unique name and value as cache Holder
   * object
   */
  private Map<String, Cacheable> lruCacheMap;
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
      lruCacheMemorySize = Integer
          .parseInt(CarbonProperties.getInstance().getProperty(propertyName, defaultPropertyName));
    } catch (NumberFormatException e) {
      lruCacheMemorySize = Integer.parseInt(defaultPropertyName);
    }
    initCache();
    if (lruCacheMemorySize >= 0) {
      LOGGER.info("Configured level cahce size is " + lruCacheMemorySize + " MB");
      // convert in bytes
      lruCacheMemorySize = lruCacheMemorySize * BYTE_CONVERSION_CONSTANT;
    } else {
      LOGGER.info("Level cache size not configured. Therefore default behavior will be "
              + "considered and all levels files will be loaded in memory");
    }
  }

  /**
   * initialize lru cache
   */
  private void initCache() {
    lruCacheMap =
        new LinkedHashMap<String, Cacheable>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE, 1.0f,
            true);
  }

  /**
   * This method will give the list of all the keys that can be deleted from
   * the level LRU cache
   */
  private List<String> getKeysToBeRemoved(long size) {
    List<String> toBeDeletedKeys =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    long removedSize = 0;
    for (Entry<String, Cacheable> entry : lruCacheMap.entrySet()) {
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
    synchronized (lruCacheMap) {
      removeKey(key);
      LOGGER.info("Removed level entry from InMemory level lru cache :: " + key);
    }
  }

  /**
   * This method will remove the key from lru cache
   *
   * @param key
   */
  private void removeKey(String key) {
    Cacheable cacheable = lruCacheMap.get(key);
    if (null != cacheable) {
      currentSize = currentSize - cacheable.getMemorySize();
    }
    lruCacheMap.remove(key);
  }

  /**
   * This method will check if required size is available in the memory and then add
   * the given cacheable to object to lru cache
   *
   * @param columnIdentifier
   * @param cacheInfo
   */
  public boolean put(String columnIdentifier, Cacheable cacheInfo, long requiredSize) {
    boolean columnKeyAddedSuccessfully = false;
    if (freeMemorySizeForAddingCache(cacheInfo, requiredSize)) {
      synchronized (lruCacheMap) {
        currentSize = currentSize + requiredSize;
        if (null == lruCacheMap.get(columnIdentifier)) {
          lruCacheMap.put(columnIdentifier, cacheInfo);
        }
        columnKeyAddedSuccessfully = true;
      }
      LOGGER.debug("Added level entry to InMemory level lru cache :: " + columnIdentifier);
    } else {
      LOGGER.error("Size not available. Column cannot be added to level lru cache :: "
          + columnIdentifier + " .Required Size = " + requiredSize + " Size available "
          + (lruCacheMemorySize - currentSize));
    }
    return columnKeyAddedSuccessfully;
  }

  /**
   * This method will check a required column can be loaded into memory or not. If required
   * this method will call for eviction of existing data from memory
   *
   * @param cacheInfo
   * @param requiredSize
   * @return
   */
  private boolean freeMemorySizeForAddingCache(Cacheable cacheInfo, long requiredSize) {
    boolean memoryAvailable = false;
    if (lruCacheMemorySize > 0) {
      if (isSizeAvailableToLoadColumnDictionary(requiredSize)) {
        memoryAvailable = true;
      } else {
        synchronized (lruCacheMap) {
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
      }
    } else {
      memoryAvailable = true;
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
    synchronized (lruCacheMap) {
      return lruCacheMap.get(key);
    }
  }

  /**
   * This method will empty the level cache
   */
  public void clear() {
    synchronized (lruCacheMap) {
      lruCacheMap.clear();
    }
  }
}
