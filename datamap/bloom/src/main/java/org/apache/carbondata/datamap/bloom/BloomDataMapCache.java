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
import java.io.EOFException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * This class is used to add cache for bloomfilter datamap to accelerate query through it.
 * The cache is implemented using guava cache and is a singleton which will be shared by all the
 * bloomfilter datamaps.
 * As for the cache, the key is a bloomindex file for a shard and the value is the bloomfilters
 * for the blocklets in this shard.
 * The size of cache can be configurable through CarbonProperties and the cache will be expired if
 * no one access it in the past 2 hours.
 */
@InterfaceAudience.Internal
public class BloomDataMapCache implements Serializable {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      BloomDataMapCache.class.getName());
  private static final long serialVersionUID = 20160822L;
  private static final int DEFAULT_CACHE_EXPIRED_HOURS = 2;
  private LoadingCache<CacheKey, List<BloomDMModel>> bloomDMCache = null;

  private BloomDataMapCache() {
    RemovalListener<CacheKey, List<BloomDMModel>> listener =
        new RemovalListener<CacheKey, List<BloomDMModel>>() {
      @Override
      public void onRemoval(RemovalNotification<CacheKey, List<BloomDMModel>> notification) {
        LOGGER.info(
            String.format("Remove bloom datamap entry %s from cache due to %s",
                notification.getKey(), notification.getCause()));
      }
    };
    CacheLoader<CacheKey, List<BloomDMModel>> cacheLoader =
        new CacheLoader<CacheKey, List<BloomDMModel>>() {
      @Override
      public List<BloomDMModel> load(CacheKey key) throws Exception {
        LOGGER.info(String.format("Load bloom datamap entry %s to cache", key));
        return loadBloomDataMapModel(key);
      }
    };

    int cacheSizeInBytes = validateAndGetCacheSize()
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR
        * CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR;
    this.bloomDMCache = CacheBuilder.newBuilder()
        .recordStats()
        .maximumSize(cacheSizeInBytes)
        .expireAfterAccess(DEFAULT_CACHE_EXPIRED_HOURS, TimeUnit.HOURS)
        .removalListener(listener)
        .build(cacheLoader);
  }

  private static class SingletonHolder {
    private static final BloomDataMapCache INSTANCE = new BloomDataMapCache();
  }

  /**
   * get instance
   */
  public static BloomDataMapCache getInstance() {
    return SingletonHolder.INSTANCE;
  }

  /**
   * for resolve from serialized
   */
  protected Object readResolve() {
    return getInstance();
  }

  private int validateAndGetCacheSize() {
    String cacheSizeStr = CarbonProperties.getInstance().getProperty(
        CarbonCommonConstants.CARBON_QUERY_DATAMAP_BLOOM_CACHE_SIZE,
        CarbonCommonConstants.CARBON_QUERY_DATAMAP_BLOOM_CACHE_SIZE_DEFAULT_VAL);
    int cacheSize;
    try {
      cacheSize = Integer.parseInt(cacheSizeStr);
      if (cacheSize <= 0) {
        throw new NumberFormatException("Value should be greater than 0: " + cacheSize);
      }
    } catch (NumberFormatException ex) {
      LOGGER.error(String.format(
          "The value '%s' for '%s' is invalid, it must be an Integer that greater than 0."
              + " Use default value '%s' instead.", cacheSizeStr,
          CarbonCommonConstants.CARBON_QUERY_DATAMAP_BLOOM_CACHE_SIZE,
          CarbonCommonConstants.CARBON_QUERY_DATAMAP_BLOOM_CACHE_SIZE_DEFAULT_VAL));
      cacheSize = Integer.parseInt(
          CarbonCommonConstants.CARBON_QUERY_DATAMAP_BLOOM_CACHE_SIZE_DEFAULT_VAL);
    }
    return cacheSize;
  }

  /**
   * load datamap from bloomindex file
   */
  private List<BloomDMModel> loadBloomDataMapModel(CacheKey cacheKey) {
    DataInputStream dataInStream = null;
    List<BloomDMModel> bloomDMModels = new ArrayList<BloomDMModel>();
    try {
      String indexFile = getIndexFileFromCacheKey(cacheKey);
      dataInStream = FileFactory.getDataInputStream(indexFile, FileFactory.getFileType(indexFile));
      try {
        while (dataInStream.available() > 0) {
          BloomDMModel model = new BloomDMModel();
          model.readFields(dataInStream);
          bloomDMModels.add(model);
        }
      } catch (EOFException e) {
        LOGGER.info(String.format("Read %d bloom indices from %s",
            bloomDMModels.size(), indexFile));
      }
      this.bloomDMCache.put(cacheKey, bloomDMModels);
      return bloomDMModels;
    } catch (IOException e) {
      clear(cacheKey);
      LOGGER.error(e, "Error occurs while reading bloom index");
      throw new RuntimeException("Error occurs while reading bloom index", e);
    } finally {
      CarbonUtil.closeStreams(dataInStream);
    }
  }

  /**
   * get bloom index file name from cachekey
   */
  private String getIndexFileFromCacheKey(CacheKey cacheKey) {
    return BloomCoarseGrainDataMap.getBloomIndexFile(cacheKey.shardPath, cacheKey.indexColumn);
  }

  /**
   * get bloom datamap from cache
   */
  public List<BloomDMModel> getBloomDMModelByKey(CacheKey cacheKey) {
    return this.bloomDMCache.getUnchecked(cacheKey);
  }

  /**
   * get cache status
   */
  private String getCacheStatus() {
    StringBuilder sb = new StringBuilder();
    CacheStats stats = this.bloomDMCache.stats();
    sb.append("hitCount: ").append(stats.hitCount()).append(System.lineSeparator())
        .append("hitRate: ").append(stats.hitCount()).append(System.lineSeparator())
        .append("loadCount: ").append(stats.loadCount()).append(System.lineSeparator())
        .append("averageLoadPenalty: ").append(stats.averageLoadPenalty())
        .append(System.lineSeparator())
        .append("evictionCount: ").append(stats.evictionCount());
    return sb.toString();
  }

  /**
   * clear this cache
   */
  private void clear(CacheKey cacheKey) {
    LOGGER.info(String.format("Current meta cache statistic: %s", getCacheStatus()));
    LOGGER.info("Trigger invalid cache for bloom datamap, key is " + cacheKey);
    this.bloomDMCache.invalidate(cacheKey);
  }

  public static class CacheKey {
    private String shardPath;
    private String indexColumn;

    CacheKey(String shardPath, String indexColumn) {
      this.shardPath = shardPath;
      this.indexColumn = indexColumn;
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("CacheKey{");
      sb.append("shardPath='").append(shardPath).append('\'');
      sb.append(", indexColumn='").append(indexColumn).append('\'');
      sb.append('}');
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof CacheKey)) return false;
      CacheKey cacheKey = (CacheKey) o;
      return Objects.equals(shardPath, cacheKey.shardPath)
          && Objects.equals(indexColumn, cacheKey.indexColumn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(shardPath, indexColumn);
    }
  }
}
