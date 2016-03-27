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

/**
 * Copyright Notice
 * =====================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Copying or reproduction without prior written approval is prohibited.
 * Copyright (c) 2013
 * =====================================
 */
package org.carbondata.query.datastorage.cache;

import java.util.*;
import java.util.Map.Entry;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.datastorage.InMemoryTableStore;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * @author m00258959
 */
public final class CarbonLRULevelCache {
    /**
     * constant for converting MB into bytes
     */
    private static final int BYTE_CONVERSION_CONSTANT = 1024 * 1024;
    /**
     * Attribute for Molap LOGGER
     */
    private static final LogService LOGGER =
            LogServiceFactory.getLogService(InMemoryTableStore.class.getName());
    /**
     * instance
     */
    private static CarbonLRULevelCache instance = new CarbonLRULevelCache();
    /**
     * Map that will contain key as cube unique name and value as cache Holder
     * object
     */
    private Map<String, LevelInfo> levelCache;
    /**
     * lruCacheSize
     */
    private long levelCacheMemorySize;
    /**
     * totalSize size of the cache
     */
    private long currentSize;

    private CarbonLRULevelCache() {
        try {
            levelCacheMemorySize = Integer.parseInt(CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.CARBON_MAX_LEVEL_CACHE_SIZE,
                            CarbonCommonConstants.CARBON_MAX_LEVEL_CACHE_SIZE_DEFAULT));
        } catch (NumberFormatException e) {
            levelCacheMemorySize =
                    Integer.parseInt(CarbonCommonConstants.CARBON_MAX_LEVEL_CACHE_SIZE_DEFAULT);
        }
        if (levelCacheMemorySize >= 0) {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Configured level cahce size is " + levelCacheMemorySize + " MB");
            // convert in bytes
            levelCacheMemorySize = levelCacheMemorySize * BYTE_CONVERSION_CONSTANT;
            initCache();
        } else {
            LOGGER.info(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Level cache size not configured. Therefore default behvior will be considered and all levels files will be laoded in memory");
        }
    }

    /**
     * @return
     */
    public static CarbonLRULevelCache getInstance() {
        return instance;
    }

    /**
     *
     *
     */
    private void initCache() {
        levelCache = Collections.synchronizedMap(
                new LinkedHashMap<String, LevelInfo>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE,
                        1.0f, true));
    }

    /**
     * This method will give the list of all the keys that can be deleted from
     * the level LRU cache
     *
     * @param sizeToBeRemoved
     * @return
     */
    public List<String> getKeysToBeremoved(long size) {
        List<String> toBeDeletedKeys =
                new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        long removedSize = 0;
        synchronized (levelCache) {
            for (Entry<String, LevelInfo> entry : levelCache.entrySet()) {
                String key = entry.getKey();
                LevelInfo levelInfo = entry.getValue();
                long fileSize = levelInfo.getFileSize();
                if (canBeRemoved(key, levelInfo)) {
                    removedSize = removedSize + fileSize;
                    toBeDeletedKeys.add(key);
                    // check if after removing the current file size, required
                    // size when added to current size is sufficient to load a
                    // level or not
                    if (levelCacheMemorySize >= (currentSize - fileSize + size)) {
                        toBeDeletedKeys.clear();
                        toBeDeletedKeys.add(key);
                        removedSize = fileSize;
                        break;
                    }
                    // check if after removing the added size/removed size,
                    // required size when added to current size is sufficient to
                    // load a level or not
                    else if (levelCacheMemorySize >= (currentSize - removedSize + size)) {
                        break;
                    }
                }
            }
            // this case will come when iteration is complete over the keys but
            // still size is not sufficient for level file to be loaded, then we
            // will nto delete any of the keys
            if ((currentSize - removedSize + size) > levelCacheMemorySize) {
                toBeDeletedKeys.clear();
            }
        }
        return toBeDeletedKeys;
    }

    /**
     * @param key
     * @param levelInfo
     * @return
     */
    private boolean canBeRemoved(String key, LevelInfo levelInfo) {
        if (!levelInfo.isLoaded() || levelInfo.getAccessCount() > 0) {
            return false;
        }
        return true;
    }

    /**
     * @param key
     * @return
     */
    public void removeAllKeysForGivenCube(final String key) {
        synchronized (levelCache) {
            Iterator<Entry<String, LevelInfo>> levelCacheItr = levelCache.entrySet().iterator();
            while (levelCacheItr.hasNext()) {
                Entry<String, LevelInfo> entry = levelCacheItr.next();
                if (entry.getKey().startsWith(key)) {
                    if (entry.getValue().isLoaded()) {
                        currentSize = currentSize - entry.getValue().getFileSize();
                    }
                    LOGGER.debug(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                            "Removed level entry from InMemory level lru cache :: " + entry
                                    .getKey());
                    levelCacheItr.remove();
                }
            }
        }
    }

    /**
     * @param key
     */
    public void remove(String key) {
        synchronized (levelCache) {
            levelCache.remove(key);
            LOGGER.info(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Removed level entry from InMemory level lru cache :: " + key);
        }
    }

    /**
     * @param cubeUniqueName
     * @param levelInfo
     */
    public void put(final String cubeUniqueName, LevelInfo levelInfo) {
        synchronized (levelCache) {
            if (levelInfo.isLoaded()) {
                currentSize = currentSize + levelInfo.getFileSize();
            }
            levelCache.put(cubeUniqueName, levelInfo);
        }
        LOGGER.debug(CarbonEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                "Added level entry to InMemory level lru cache :: " + cubeUniqueName
                        + " with loaded status :: " + levelInfo.isLoaded());
    }

    /**
     * @return
     */
    public long getLRUCacheSize() {
        return levelCacheMemorySize;
    }

    /**
     * @param key
     * @return
     */
    public LevelInfo get(String key) {
        synchronized (levelCache) {
            return levelCache.get(key);
        }
    }

    /**
     * @return Returns the currentSize.
     */
    public long getCurrentSize() {
        synchronized (levelCache) {
            return currentSize;
        }
    }

    /**
     * @param key
     */
    public void unloadLevelInCache(String key) {
        synchronized (levelCache) {
            LevelInfo levelInfo = levelCache.get(key);
            levelInfo.setLoaded(false);
            this.currentSize = this.currentSize - levelInfo.getFileSize();
        }
    }

    /**
     * @param key
     */
    public void loadLevelInCache(String key) {
        synchronized (levelCache) {
            LevelInfo levelInfo = levelCache.get(key);
            levelInfo.setLoaded(true);
            this.currentSize = this.currentSize + levelInfo.getFileSize();
        }
    }

}
