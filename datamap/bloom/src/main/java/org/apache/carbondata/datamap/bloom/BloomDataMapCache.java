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

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.CarbonUtil;

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
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BloomDataMapCache.class.getName());

  /**
   * CarbonLRU cache
   */
  private CarbonLRUCache lruCache;

  public BloomDataMapCache(CarbonLRUCache lruCache) {
    this.lruCache = lruCache;
  }

  @Override
  public BloomCacheKeyValue.CacheValue get(BloomCacheKeyValue.CacheKey key)
      throws IOException {
    BloomCacheKeyValue.CacheValue cacheValue = getIfPresent(key);
    if (cacheValue == null) {
      cacheValue = loadBloomDataMapModel(key);
      lruCache.put(key.toString(), cacheValue, cacheValue.getMemorySize());
    }
    return cacheValue;
  }

  @Override
  public List<BloomCacheKeyValue.CacheValue> getAll(List<BloomCacheKeyValue.CacheKey> keys)
      throws IOException {
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
  public void put(BloomCacheKeyValue.CacheKey key, BloomCacheKeyValue.CacheValue value)
      throws IOException, MemoryException {
    // No impl required.
  }

  /**
   * load datamap from bloomindex file
   */
  private BloomCacheKeyValue.CacheValue loadBloomDataMapModel(
      BloomCacheKeyValue.CacheKey cacheKey) {
    DataInputStream dataInStream = null;
    List<CarbonBloomFilter> bloomFilters = new ArrayList<>();
    try {
      String indexFile = getIndexFileFromCacheKey(cacheKey);
      dataInStream = FileFactory.getDataInputStream(indexFile, FileFactory.getFileType(indexFile));
      while (dataInStream.available() > 0) {
        CarbonBloomFilter bloomFilter = new CarbonBloomFilter();
        bloomFilter.readFields(dataInStream);
        bloomFilters.add(bloomFilter);
      }
      LOGGER.info(String.format("Read %d bloom indices from %s", bloomFilters.size(), indexFile));

      return new BloomCacheKeyValue.CacheValue(bloomFilters);
    } catch (IOException e) {
      LOGGER.error(e, "Error occurs while reading bloom index");
      throw new RuntimeException("Error occurs while reading bloom index", e);
    } finally {
      CarbonUtil.closeStreams(dataInStream);
    }
  }

  /**
   * get bloom index file name from cachekey
   */
  private String getIndexFileFromCacheKey(BloomCacheKeyValue.CacheKey cacheKey) {
    return BloomCoarseGrainDataMap
        .getBloomIndexFile(cacheKey.getShardPath(), cacheKey.getIndexColumn());
  }

  @Override
  public void clearAccessCount(List<BloomCacheKeyValue.CacheKey> keys) {
  }
}
