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

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import org.apache.carbondata.core.cache.Cacheable;

import org.apache.hadoop.util.bloom.CarbonBloomFilter;

/**
 * Key and values of bloom to keep in cache.
 */
public class BloomCacheKeyValue {

  public static class CacheKey implements Serializable {

    private static final long serialVersionUID = -1478238084352505372L;
    private String shardPath;
    private String indexColumn;

    public CacheKey(String shardPath, String indexColumn) {
      this.shardPath = shardPath;
      this.indexColumn = indexColumn;
    }

    public String getShardPath() {
      return shardPath;
    }

    public String getIndexColumn() {
      return indexColumn;
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

  public static class CacheValue implements Cacheable {

    private List<CarbonBloomFilter> bloomFilters;

    private int size;

    public CacheValue(List<CarbonBloomFilter> bloomFilters) {
      this.bloomFilters = bloomFilters;
      for (CarbonBloomFilter bloomFilter : bloomFilters) {
        size += bloomFilter.getSize();
      }
    }

    @Override
    public int getAccessCount() {
      return 0;
    }

    @Override
    public long getMemorySize() {
      return size;
    }

    @Override
    public void invalidate() {
      bloomFilters = null;
    }

    public List<CarbonBloomFilter> getBloomFilters() {
      return bloomFilters;
    }
  }

}
