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
import java.util.List;

import org.apache.carbondata.core.memory.MemoryException;

/**
 * A semi-persistent mapping from keys to values. Cache entries are manually added using
 * #get(Key), #getAll(List<Keys>) , and are stored in the cache until
 * either evicted or manually invalidated.
 * Implementations of this interface are expected to be thread-safe, and can be safely accessed
 * by multiple concurrent threads.
 * This class also responsible for incrementing and decrementing access count during get operation
 */
public interface Cache<K, V> {

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * Access count of Cacheable entry will be incremented
   *
   * @param key
   * @return
   * @throws IOException in case memory is not sufficient to load data into memory
   */
  V get(K key) throws IOException;

  /**
   * This method will return a list of values for the given list of keys.
   * For each key, this method will check and load the data if required.
   * Access count of Cacheable entry will be incremented
   * @param keys
   * @return
   * @throws IOException in case memory is not sufficient to load data into memory
   */
  List<V> getAll(List<K> keys) throws IOException;

  /**
   * This method will return the value for the given key. It will not check and load
   * the data for the given key
   * Access count of Cacheable entry will be incremented
   * @param key
   * @return
   */
  V getIfPresent(K key);

  /**
   * This method will remove the cache for a given key
   *
   * @param key
   */
  void invalidate(K key);

  /**
   * This method will add the value to the cache for the given key
   *
   * @param key
   * @param value
   */
  void put(K key, V value) throws IOException, MemoryException;

  /**
   * Access count of Cacheable entry will be decremented
   *
   * @param keys
   */
  void clearAccessCount(List<K> keys);
}

