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

package org.apache.carbondata.datamap.bloom;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;

import org.apache.hadoop.util.bloom.CarbonBloomFilter;

/**
 * This class is used to add cache for bloomfilter datamap to accelerate query through it.
 * The cache is implemented using carbon lru cache.
 * As for the cache, the key is a bloomindex file for a shard and the value is the bloomfilters
 * for the blocklets in this shard.
 */
@InterfaceAudience.Internal
public class BloomDataMapCache
    implements Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> {

  /**
   * CarbonLRU cache
   */
  private CarbonLRUCache lruCache;

  public BloomDataMapCache(CarbonLRUCache lruCache) {
    this.lruCache = lruCache;
  }

  @Override
  public BloomCacheKeyValue.CacheValue get(BloomCacheKeyValue.CacheKey key) {
    BloomCacheKeyValue.CacheValue cacheValue = getIfPresent(key);
    if (cacheValue == null) {
      List<CarbonBloomFilter> bloomFilters =
              BloomIndexFileStore.loadBloomFilterFromFile(key.getShardPath(), key.getIndexColumn());
      cacheValue = new BloomCacheKeyValue.CacheValue(bloomFilters);
      lruCache.put(key.toString(), cacheValue, cacheValue.getMemorySize());
    }
    return cacheValue;
  }

  @Override
  public List<BloomCacheKeyValue.CacheValue> getAll(List<BloomCacheKeyValue.CacheKey> keys) {
    List<BloomCacheKeyValue.CacheValue> cacheValues = new ArrayList<>();
    for (BloomCacheKeyValue.CacheKey key : keys) {
      BloomCacheKeyValue.CacheValue cacheValue = get(key);
      cacheValues.add(cacheValue);
    }
    return cacheValues;
  }

  @Override
  public BloomCacheKeyValue.CacheValue getIfPresent(BloomCacheKeyValue.CacheKey key) {
    return (BloomCacheKeyValue.CacheValue) lruCache.get(key.toString());
  }

  @Override
  public void invalidate(BloomCacheKeyValue.CacheKey key) {
    lruCache.remove(key.toString());
  }

  @Override
  public void put(BloomCacheKeyValue.CacheKey key, BloomCacheKeyValue.CacheValue value) {
    // No impl required.
  }

  @Override
  public void clearAccessCount(List<BloomCacheKeyValue.CacheKey> keys) {
  }
}
