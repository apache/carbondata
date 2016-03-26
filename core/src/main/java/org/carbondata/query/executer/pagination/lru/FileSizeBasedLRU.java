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

package org.carbondata.query.executer.pagination.lru;

import java.io.File;
import java.util.*;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.constants.MolapCommonConstants;
import org.carbondata.core.util.MolapProperties;
import org.carbondata.query.util.MolapEngineLogEvent;

public class FileSizeBasedLRU {

    private static final LogService LOGGER =
            LogServiceFactory.getLogService(FileSizeBasedLRU.class.getName());
    private static FileSizeBasedLRU lru;
    /**
     * fCacheMap
     */
    private Map<LRUCacheKey, LRUCacheValue> fCacheMap;
    /**
     * fCacheSize
     */
    private int fCacheSize;
    private long size;
    private long diskSizeLimit;

    /**
     * Instantiate LRU cache.
     *
     * @param size
     * @param diskSize
     * @param hashMap
     */
    @SuppressWarnings("unchecked")
    public FileSizeBasedLRU(int intialSize, final long diskSize) {
        fCacheSize = intialSize;
        diskSizeLimit = diskSize;
        // If the cache is to be used by multiple threads,
        // the hashMap must be wrapped with code to synchronize
        fCacheMap = Collections.synchronizedMap(
                //true = use access order instead of insertion order
                new LinkedHashMap<LRUCacheKey, LRUCacheValue>(fCacheSize, .75F, true) {

                    @Override
                    public boolean removeEldestEntry(Map.Entry<LRUCacheKey, LRUCacheValue> eldest) {
                        if (size > diskSize) {
                            if (eldest.getKey().getPath() != null) {
                                size -= eldest.getKey().getSize();
                                boolean delete = new File(eldest.getKey().getPath()).delete();
                                if (!delete) {
                                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                                            "Lru cache removal is failed for the query entry "
                                                    + eldest.getKey().getPath());
                                    return false;
                                } else {
                                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                                            "Lru cache removes the query entry " + eldest.getKey()
                                                    .getPath());
                                    LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                                            "Lru cache current size " + getCurrentSize() + "MB");
                                    return true;
                                }
                            }
                        }
                        //when to remove the eldest entry
                        return false;   //size exceeded the max allowed
                    }

                    @Override
                    public LRUCacheValue put(LRUCacheKey key, LRUCacheValue value) {
                        //                    if(!key.isCompleted())
                        //                    {
                        //                        Long removeSize = super.remove(key);
                        //                        if(removeSize != null)
                        //                        {
                        //                            size -= removeSize;
                        //                        }
                        //                    }
                        LRUCacheValue removeSize = super.remove(key);
                        if (removeSize != null) {
                            size -= removeSize.getSize();
                        }

                        size += key.getSize();
                        return super.put(key, value);
                    }

                    public void clear() {
                        size = 0;
                        super.clear();
                    }
                });
    }

    /**
     * Get instance of class
     *
     * @param hashMap
     * @return
     */
    public static synchronized FileSizeBasedLRU getInstance() {
        if (lru == null) {
            long mem = 0;
            try {
                mem = Long.parseLong(MolapProperties.getInstance()
                        .getProperty(MolapCommonConstants.PAGINATED_CACHE_DISK_SIZE,
                                MolapCommonConstants.PAGINATED_CACHE_DISK_SIZE_DEFAULT.toString()));
            } catch (NumberFormatException e) {
                mem = MolapCommonConstants.PAGINATED_CACHE_DISK_SIZE_DEFAULT;
                LOGGER.error(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                        "Exception while parsing property", e);
            }
            mem = MolapProperties.getInstance()
                    .validate(mem, MolapCommonConstants.PAGINATED_CACHE_DISK_SIZE_MAX,
                            MolapCommonConstants.PAGINATED_CACHE_DISK_SIZE_MIN,
                            MolapCommonConstants.PAGINATED_CACHE_DISK_SIZE_DEFAULT);
            LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
                    "Query Lru Cache has been intilaized with limit " + mem + " MB");
            lru = new FileSizeBasedLRU(3000, mem * 1024 * 1024);
        }
        return lru;
    }

    /**
     * Put the key
     *
     * @param key
     * @param elem
     */
    public void put(LRUCacheKey key, long totalRowCount) {
        LRUCacheValue cacheValue = new LRUCacheValue();
        cacheValue.setCacheKey(key);
        cacheValue.setRowCount(totalRowCount);
        cacheValue.setSize(key.getSize());
        fCacheMap.put(key, cacheValue);
    }

    /**
     * Get the key
     *
     * @param key
     * @return
     */
    public LRUCacheValue get(LRUCacheKey key) {
        return fCacheMap.get(key);
    }

    /**
     * Get headers
     *
     * @return
     */
    public List<LRUCacheKey> getAllQueries() {
        return new ArrayList<LRUCacheKey>(fCacheMap.keySet());
    }

    /**
     * Remove key
     *
     * @param key
     * @return
     */
    public LRUCacheValue remove(LRUCacheKey key) {
        return fCacheMap.remove(key);
    }

    /**
     * To string
     */
    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return fCacheMap.toString();
    }

    //    public static void main(String[] args)
    //    {
    //      LRUCache cache = new LRUCache(1,100,null);
    //      for (long i = 0; i < 500; i++) {
    //          //cache.put(i+"", new byte[]{1,2});
    //      }
    //
    //      System.out.println(cache);
    //  }

    /**
     * Clear cache
     */
    public void clear() {
        fCacheMap.clear();
        fCacheSize = 0;
        size = 0;
    }

    /**
     * Check whether size is limits or not.
     *
     * @return
     */
    public boolean isSizeInLimits() {
        if (size > diskSizeLimit) {
            return false;
        }
        return true;
    }

    public int getCount() {
        return fCacheMap.size();
    }

    public double getCurrentSize() {
        return ((double) size / (1024 * 1024));
    }

}
