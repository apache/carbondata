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

package org.carbondata.processing.surrogatekeysgenerator.lru;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;

public final class LRUCache {
    /**
     * instance
     */
    private static final LRUCache INSTANCE = new LRUCache();

    /**
     * cache size
     */
    private int lruCacheSize;

    /**
     * cache
     */
    private Map<String, CarbonSeqGenCacheHolder> cache;

    /**
     * LRUCache constructor
     */
    private LRUCache() {
        try {
            lruCacheSize = Integer.parseInt(CarbonProperties.getInstance()
                    .getProperty(CarbonCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE,
                            CarbonCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE));
        } catch (NumberFormatException e) {
            lruCacheSize = Integer.parseInt(
                    CarbonCommonConstants.MOLAP_SEQ_GEN_INMEMORY_LRU_CACHE_SIZE_DEFAULT_VALUE);
        }
        createCache();
    }

    public static LRUCache getIntance() {
        return INSTANCE;
    }

    /**
     * below method will be used to create the cache
     */
    private void createCache() {
        cache = Collections.synchronizedMap(
                // true = use access order instead of insertion order
                new LinkedHashMap<String, CarbonSeqGenCacheHolder>(lruCacheSize + 1, 1.0f, true) {
                    //CHECKSTYLE:OFF
                    /**
                     * serialVersionUID
                     */
                    private static final long serialVersionUID = 1L;
                    //CHECKSTYLE:ON

                    @Override
                    public boolean removeEldestEntry(
                            Map.Entry<String, CarbonSeqGenCacheHolder> eldest) {
                        if (size() > lruCacheSize) {
                            cache.remove(eldest.getKey());
                            return true;
                        }
                        // when to remove the eldest entry
                        return false; // size exceeded the max allowed
                    }

                    @Override
                    public CarbonSeqGenCacheHolder get(Object key) {
                        CarbonSeqGenCacheHolder m = super.get(key);
                        if (null != m) {
                            m.setLastAccessTime(System.currentTimeMillis());
                        }
                        return m;
                    }
                });
    }

    /**
     * below method will be used to put the data into the cache
     *
     * @param key
     * @param value
     */
    public void put(String key, CarbonSeqGenCacheHolder value) {
        value.setLastAccessTime(System.currentTimeMillis());
        cache.put(key, value);
    }

    /**
     * below method will be used to get the data from the cache
     *
     * @param key
     * @return
     */
    public CarbonSeqGenCacheHolder get(String key) {
        return cache.get(key);
    }

    /**
     * below method will be used to remove the entry from the cache
     *
     * @param key
     */
    public void remove(String key) {
        cache.remove(key);
    }

    public void flush() {
        cache.clear();
    }
}
