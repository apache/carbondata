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

package org.apache.carbondata.datamap.minmax;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.CarbonUtil;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.log4j.Logger;

/**
 * cache minmax datamap models using carbon lru cache.
 */
public class MinMaxDataMapCache
    implements Cache<MinMaxDataMapCacheKeyValue.Key, MinMaxDataMapCacheKeyValue.Value> {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(MinMaxDataMapCache.class.getName());
  private CarbonLRUCache lruCache;

  public MinMaxDataMapCache(CarbonLRUCache lruCache) {
    this.lruCache = lruCache;
  }

  @Override
  public MinMaxDataMapCacheKeyValue.Value get(MinMaxDataMapCacheKeyValue.Key key)
      throws IOException {
    MinMaxDataMapCacheKeyValue.Value cacheValue = getIfPresent(key);
    if (null == cacheValue) {
      cacheValue = loadMinMaxModels(FileFactory.getPath(key.getShardPath()));
      lruCache.put(key.uniqueString(), cacheValue, cacheValue.getMemorySize());
    }
    return cacheValue;
  }

  private MinMaxDataMapCacheKeyValue.Value loadMinMaxModels(Path shardPath) throws IOException {
    FileSystem fs = FileFactory.getFileSystem(shardPath);
    if (!fs.exists(shardPath)) {
      throw new IOException(
          String.format("Path %s for MinMax index dataMap does not exist", shardPath));
    }
    if (!fs.isDirectory(shardPath)) {
      throw new IOException(
          String.format("Path %s for MinMax index dataMap must be a directory", shardPath));
    }
    FileStatus[] indexFileStatus = fs.listStatus(shardPath, new PathFilter() {
      @Override public boolean accept(Path path) {
        return path.getName().endsWith(".minmaxindex");
      }
    });

    List<MinMaxIndexHolder> minMaxIndexHolderList = new ArrayList<>();
    for (int i = 0; i < indexFileStatus.length; i++) {
      List<MinMaxIndexHolder> dataMapModels =
          loadMinMaxIndexFromFile(indexFileStatus[i].getPath().toString());
      minMaxIndexHolderList.addAll(dataMapModels);
    }
    return new MinMaxDataMapCacheKeyValue.Value(minMaxIndexHolderList);
  }

  private List<MinMaxIndexHolder> loadMinMaxIndexFromFile(String indexFile) throws IOException {
    LOGGER.info("load minmax datamap model from file " + indexFile);
    List<MinMaxIndexHolder> dataMapModels = new ArrayList<>();
    DataInputStream inputStream = null;
    try {
      inputStream = FileFactory.getDataInputStream(indexFile, FileFactory.getFileType(indexFile));
      String fileName = new Path(indexFile).getName();
      int indexColCnt = Integer.parseInt(fileName.substring(
          MinMaxIndexHolder.MINMAX_INDEX_PREFFIX.length(),
          fileName.indexOf(MinMaxIndexHolder.MINMAX_INDEX_SUFFIX)));
      while (inputStream.available() > 0) {
        MinMaxIndexHolder minMaxIndexHolder = new MinMaxIndexHolder(indexColCnt);
        minMaxIndexHolder.readFields(inputStream);
        dataMapModels.add(minMaxIndexHolder);
      }
      LOGGER.info(String.format("Read %d minmax indices from %s", dataMapModels.size(), indexFile));
      return dataMapModels;
    } catch (Exception e) {
      LOGGER.error("Failed to load minmax index from file", e);
      throw new IOException(e);
    } finally {
      CarbonUtil.closeStreams(inputStream);
    }
  }

  @Override
  public List<MinMaxDataMapCacheKeyValue.Value> getAll(List<MinMaxDataMapCacheKeyValue.Key> keys)
      throws IOException {
    List<MinMaxDataMapCacheKeyValue.Value> cacheValues = new ArrayList<>(keys.size());
    for (MinMaxDataMapCacheKeyValue.Key key : keys) {
      cacheValues.add(get(key));
    }
    return cacheValues;
  }

  @Override
  public MinMaxDataMapCacheKeyValue.Value getIfPresent(MinMaxDataMapCacheKeyValue.Key key) {
    return (MinMaxDataMapCacheKeyValue.Value) lruCache.get(key.uniqueString());
  }

  @Override
  public void invalidate(MinMaxDataMapCacheKeyValue.Key key) {
    lruCache.remove(key.uniqueString());
  }

  @Override
  public void put(MinMaxDataMapCacheKeyValue.Key key, MinMaxDataMapCacheKeyValue.Value value)
      throws IOException, MemoryException {
    lruCache.put(key.uniqueString(), value, value.getMemorySize());
  }

  @Override
  public void clearAccessCount(List<MinMaxDataMapCacheKeyValue.Key> keys) {
    LOGGER.error("clearAccessCount is not implemented for MinMaxDataMapCache");
  }
}
