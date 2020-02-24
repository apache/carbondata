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

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.datamap.dev.DataMapModel;

import org.apache.hadoop.conf.Configuration;

public class BloomDataMapModel extends DataMapModel {

  private Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> cache;

  public BloomDataMapModel(String filePath,
      Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> cache,
      Configuration configuration) {
    super(filePath, configuration);
    this.cache = cache;
  }

  public Cache<BloomCacheKeyValue.CacheKey, BloomCacheKeyValue.CacheValue> getCache() {
    return cache;
  }
}
