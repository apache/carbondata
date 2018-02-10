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

import java.io.IOException;
import java.lang.reflect.Field;

import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.cache.dictionary.ForwardDictionaryCache;
import org.apache.carbondata.core.cache.dictionary.ReverseDictionaryCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.BlockIndexStore;
import org.apache.carbondata.core.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.util.CarbonProperties;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test class to test dictionary cache functionality
 */
public class CacheProviderTest {

  @Before public void setUp() throws Exception {
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE, "10");
    // enable lru cache by setting cache size
    CarbonProperties.getInstance()
        .addProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE, "20");
  }

  @Test public void getInstance() throws Exception {
    // get cache provider instance
    CacheProvider cacheProvider = CacheProvider.getInstance();
    // assert for cache provider instance
    assertTrue(cacheProvider instanceof CacheProvider);
  }

  @Test public void createCache() throws Exception {
    // get cache provider instance
    CacheProvider cacheProvider = CacheProvider.getInstance();
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> dictionaryCache =
        cacheProvider.createCache(CacheType.FORWARD_DICTIONARY);
    // assert that dictionary cache is an instance of Forward dictionary cache
    assertTrue(dictionaryCache instanceof ForwardDictionaryCache);
    assertFalse(dictionaryCache instanceof ReverseDictionaryCache);
    Cache<DictionaryColumnUniqueIdentifier, Dictionary> reverseDictionaryCache =
        cacheProvider.createCache(CacheType.REVERSE_DICTIONARY);
    // assert that dictionary cache is an instance of Reverse dictionary cache
    assertTrue(reverseDictionaryCache instanceof ReverseDictionaryCache);
    assertFalse(reverseDictionaryCache instanceof ForwardDictionaryCache);
    cacheProvider.dropAllCache();
  }

  /**
   * to test the driver and executor lru memory configuration
   *
   * @throws IOException
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  @Test public void driverExecutorCacheConfTest()
      throws IOException, NoSuchFieldException, IllegalAccessException {
    // get cache provider instance
    CacheProvider cacheProvider = CacheProvider.getInstance();
    cacheProvider.dropAllCache();
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "true");
    Cache<TableSegmentUniqueIdentifier, SegmentTaskIndexStore> driverCache =
        cacheProvider.createCache(CacheType.DRIVER_BTREE);
    Field carbonLRUCacheField = SegmentTaskIndexStore.class.getDeclaredField("lruCache");
    carbonLRUCacheField.setAccessible(true);
    CarbonLRUCache carbonLRUCache = (CarbonLRUCache) carbonLRUCacheField.get(driverCache);
    Field lruCacheMemorySizeField = CarbonLRUCache.class.getDeclaredField("lruCacheMemorySize");
    lruCacheMemorySizeField.setAccessible(true);
    long lruCacheMemorySize = (long) lruCacheMemorySizeField.get(carbonLRUCache);
    String driverCacheSize = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MAX_DRIVER_LRU_CACHE_SIZE);
    assertEquals(1024 * 1024 * Integer.parseInt(driverCacheSize), lruCacheMemorySize);
    // drop cache
    cacheProvider.dropAllCache();
    // validation test for the executor memory.
    CarbonProperties.getInstance().addProperty(CarbonCommonConstants.IS_DRIVER_INSTANCE, "false");
    Cache<TableBlockUniqueIdentifier, BlockIndexStore> executorCache =
        cacheProvider.createCache(CacheType.EXECUTOR_BTREE);
    carbonLRUCacheField = BlockIndexStore.class.getSuperclass().getDeclaredField("lruCache");
    carbonLRUCacheField.setAccessible(true);
    carbonLRUCache = (CarbonLRUCache) carbonLRUCacheField.get(executorCache);
    lruCacheMemorySizeField = CarbonLRUCache.class.getDeclaredField("lruCacheMemorySize");
    lruCacheMemorySizeField.setAccessible(true);
    lruCacheMemorySize = (long) lruCacheMemorySizeField.get(carbonLRUCache);
    String executorCacheSize = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_MAX_EXECUTOR_LRU_CACHE_SIZE);
    assertEquals(1024 * 1024 * Integer.parseInt(executorCacheSize), lruCacheMemorySize);
    cacheProvider.dropAllCache();
  }
}