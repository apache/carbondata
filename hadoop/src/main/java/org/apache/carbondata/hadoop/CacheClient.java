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

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datastore.TableSegmentUniqueIdentifier;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndexWrapper;

import org.apache.hadoop.conf.Configuration;

/**
 * CacheClient : Holds all the Cache access clients for Btree, Dictionary
 */
public class CacheClient {

  // segment access client for driver LRU cache
  private CacheAccessClient<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper>
      segmentAccessClient;

  public CacheClient(Configuration configuration, String storePath) {
    Cache<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper> segmentCache =
        CacheProvider.getInstance().createCache(CacheType.DRIVER_BTREE, storePath, configuration);
    segmentAccessClient = new CacheAccessClient<>(segmentCache);
  }

  public CacheAccessClient<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper>
      getSegmentAccessClient() {
    return segmentAccessClient;
  }

  public void close() {
    segmentAccessClient.close();
  }
}
