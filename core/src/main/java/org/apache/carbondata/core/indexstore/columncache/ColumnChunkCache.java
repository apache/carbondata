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

package org.apache.carbondata.core.indexstore.columncache;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.chunk.AbstractRawColumnChunk;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import org.apache.log4j.Logger;

/**
 * Column Chunk level data cache for table which is enabled for this feature.
 */
public class ColumnChunkCache {

  private static Logger LOG =
      LogServiceFactory.getLogService(ColumnChunkCache.class.getCanonicalName());

  // A global cache for all tables which is enabled for this feature.
  // The cached object is raw column chunks including raw binary data, so we can
  // leverage this cache to avoid IO read from underlying storage.
  private static Map<String, Cache<CacheKey, AbstractRawColumnChunk>> cacheForEachTable =
      new ConcurrentHashMap<>();

  /**
   * Enable or disable the column chunk cache for specified table, based on the whether the
   * corresponding table property is set
   */
  public static void setCacheForTable(CarbonTable carbonTable) {
    String tableId = carbonTable.getTableId();
    boolean shouldEnable = carbonTable.isColumnCacheEnabled();
    boolean alreadyEnabled = cacheForEachTable.containsKey(tableId);
    if (alreadyEnabled && !shouldEnable) {
      cacheForEachTable.get(tableId).invalidateAll();
      cacheForEachTable.remove(tableId);
    } else if (!alreadyEnabled && shouldEnable) {
      Cache<CacheKey, AbstractRawColumnChunk> cache = createCacheForTable(tableId);
      cacheForEachTable.put(tableId, cache);
    }
  }

  private static Cache<CacheKey, AbstractRawColumnChunk> createCacheForTable(String tableId) {
    return CacheBuilder.newBuilder()
        // expire after 10 minutes after last accessed
        .expireAfterAccess(10, TimeUnit.MINUTES)
        // max number of entries
        .maximumSize(1000)
        .removalListener(new RemovalListener<CacheKey, AbstractRawColumnChunk>() {
          @Override
          public void onRemoval(RemovalNotification<CacheKey, AbstractRawColumnChunk> entry) {
            AbstractRawColumnChunk chunk = entry.getValue();
            if (chunk != null) {
              chunk.setCached(false);
              chunk.freeMemory();
              if (LOG.isInfoEnabled()) {
                CacheKey key = entry.getKey();
                LOG.info(String.format("column cache removed: key %s", key.toString()));
              }
            }
          }
        }).build();
  }

  /**
   * Return true if column chunk cache is enabled for specified table
   */
  public static boolean isEnabledForTable(String tableId) {
    return tableId != null && cacheForEachTable.containsKey(tableId);
  }

  /**
   * Put column chunk into the cache
   */
  public static void put(String tableId, CacheKey key, AbstractRawColumnChunk chunk) {
    if (isEnabledForTable(tableId)) {
      cacheForEachTable.get(tableId).put(key, chunk);
      chunk.setCached(true);
      if (LOG.isInfoEnabled()) {
        LOG.info(String.format("column cache added: table %s, key %s", tableId, key.toString()));
      }
    }
  }

  /**
   * Get column chunk from cache
   */
  public static Optional<AbstractRawColumnChunk> get(String tableId, CacheKey key) {
    if (isEnabledForTable(tableId)) {
      AbstractRawColumnChunk data = cacheForEachTable.get(tableId).getIfPresent(key);
      if (data == null) {
        if (LOG.isInfoEnabled()) {
          LOG.info(String.format("column cache miss: table %s, key %s", tableId, key.toString()));
        }
        return Optional.empty();
      } else {
        if (LOG.isInfoEnabled()) {
          LOG.info(String.format("column cache hit: table %s, key %s", tableId, key.toString()));
        }
        return Optional.of(data);
      }
    } else {
      return Optional.empty();
    }
  }

  /**
   * Remove cached column chunks for specified table
   */
  public static void remove(String tableId) {
    if (isEnabledForTable(tableId)) {
      cacheForEachTable.get(tableId).invalidateAll();
    }
  }

  public static class CacheKey {
    private String dataFilePath;
    private long columnChunkOffset;

    public CacheKey(String dataFilePath, long columnChunkOffset) {
      this.dataFilePath = dataFilePath;
      this.columnChunkOffset = columnChunkOffset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      CacheKey cacheKey = (CacheKey) o;
      return columnChunkOffset == cacheKey.columnChunkOffset &&
          dataFilePath.equals(cacheKey.dataFilePath);
    }

    @Override
    public int hashCode() {
      return Objects.hash(dataFilePath, columnChunkOffset);
    }

    @Override
    public String toString() {
      return "{" + "path='" + dataFilePath + '\'' + ", offset=" + columnChunkOffset + '}';
    }
  }
}
