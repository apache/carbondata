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
package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * CacheClient : Class used to request the segments cache
 */
public class CacheAccessClient<K, V> {
  /**
   * List of segments
   */
  private Set<K> segmentSet = new HashSet<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

  private Cache<K, V> cache;

  public CacheAccessClient(Cache<K, V> cache) {
    this.cache = cache;
  }

  /**
   * This method will return the value for the given key. It will not check and load
   * the data for the given key
   *
   * @param key
   * @return
   */
  public V getIfPresent(K key) {
    V value = cache.getIfPresent(key);
    if (value != null) {
      segmentSet.add(key);
    }
    return value;
  }

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * @param key
   * @return
   * @throws IOException in case memory is not sufficient to load data into memory
   */
  public V get(K key) throws IOException {
    V value = cache.get(key);
    if (value != null) {
      segmentSet.add(key);
    }
    return value;
  }

  /**
   * the method is used to clear access count of the unused segments cacheable object
   */
  public void close() {
    List<K> segmentArrayList = new ArrayList<>(segmentSet.size());
    segmentArrayList.addAll(segmentSet);
    cache.clearAccessCount(segmentArrayList);
    cache = null;
  }

  /**
   * This method will remove the cache for a given key
   *
   * @param keys
   */
  public void invalidateAll(List<K> keys) {
    for (K key : keys) {
      cache.invalidate(key);
    }
  }

  /**
   * This method will clear the access count for a given list of segments
   *
   * @param segmentList
   */
  public void clearAccessCount(List<K> segmentList) {
    cache.clearAccessCount(segmentList);
    // remove from segment set so that access count is not decremented again during close operation
    segmentSet.removeAll(segmentList);
  }

}
