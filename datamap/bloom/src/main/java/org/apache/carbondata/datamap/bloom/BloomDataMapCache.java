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
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.CacheStats;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

public class BloomDataMapCache implements Serializable {
  private static final LogService LOGGER = LogServiceFactory.getLogService(
      BloomDataMapCache.class.getName());
  private static final long serialVersionUID = 20160822L;
  private static final long DEFAULT_CACHE_SIZE = 512 * 1024 * 1024;
  private static final int DEFAULT_CACHE_EXPIRED_HOURS = 2;
  private LoadingCache<CacheKey, List<BloomDMModel>> bloomDMCache = null;

  private BloomDataMapCache() {
    RemovalListener<CacheKey, List<BloomDMModel>> listener =
        new RemovalListener<CacheKey, List<BloomDMModel>>() {
      @Override
      public void onRemoval(RemovalNotification<CacheKey, List<BloomDMModel>> notification) {
        LOGGER.error(
            String.format("Remove bloom datamap entry %s from cache due to %s",
                notification.getKey(), notification.getCause()));
      }
    };
    CacheLoader<CacheKey, List<BloomDMModel>> cacheLoader =
        new CacheLoader<CacheKey, List<BloomDMModel>>() {
      @Override
      public List<BloomDMModel> load(CacheKey key) throws Exception {
        LOGGER.error(String.format("Load bloom datamap entry %s to cache", key));
        return loadBloomDataMapModel(key);
      }
    };

    this.bloomDMCache = CacheBuilder.newBuilder()
        .recordStats()
        .maximumSize(DEFAULT_CACHE_SIZE)
        .expireAfterAccess(DEFAULT_CACHE_EXPIRED_HOURS, TimeUnit.HOURS)
        .removalListener(listener)
        .build(cacheLoader);
  }

  private static class SingletonHolder {
    public static final BloomDataMapCache INSTANCE = new BloomDataMapCache();
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

  /**
   * load datamap from bloomindex file
   */
  private List<BloomDMModel> loadBloomDataMapModel(CacheKey cacheKey) {
    DataInputStream dataInStream = null;
    ObjectInputStream objectInStream = null;
    List<BloomDMModel> bloomDMModels = new ArrayList<BloomDMModel>();
    try {
      String indexFile = getIndexFileFromCacheKey(cacheKey);
      dataInStream = FileFactory.getDataInputStream(indexFile, FileFactory.getFileType(indexFile));
      objectInStream = new ObjectInputStream(dataInStream);
      try {
        BloomDMModel model = null;
        while ((model = (BloomDMModel) objectInStream.readObject()) != null) {
          bloomDMModels.add(model);
        }
      } catch (EOFException e) {
        LOGGER.info(String.format("Read %d bloom indices from %s",
            bloomDMModels.size(), indexFile));
      }
      this.bloomDMCache.put(cacheKey, bloomDMModels);
      return bloomDMModels;
    } catch (ClassNotFoundException | IOException e) {
      LOGGER.error(e, "Error occurs while reading bloom index");
      throw new RuntimeException("Error occurs while reading bloom index", e);
    } finally {
      clear();
      CarbonUtil.closeStreams(objectInStream, dataInStream);
    }
  }

  /**
   * get bloom index file name from cachekey
   */
  private String getIndexFileFromCacheKey(CacheKey cacheKey) {
    return cacheKey.shardPath.concat(File.separator).concat(cacheKey.indexColumn)
        .concat(BloomCoarseGrainDataMap.BLOOM_INDEX_SUFFIX);
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
  private void clear() {
    LOGGER.error(String.format("Current meta cache statistic: %s", getCacheStatus()));
    LOGGER.error("Trigger invalid all the cache for bloom datamap");
    this.bloomDMCache.invalidateAll();
  }

  public static class CacheKey {
    private String shardPath;
    private String indexColumn;

    public CacheKey(String shardPath, String indexColumn) {
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
