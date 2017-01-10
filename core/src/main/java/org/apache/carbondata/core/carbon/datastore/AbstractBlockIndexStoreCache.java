/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.core.carbon.datastore;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.BlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * This class validate and load the B-Tree in the executor lru cache
 * @param <K> cache key
 * @param <V> Block Meta data details
 */
public abstract class AbstractBlockIndexStoreCache<K, V>
    implements Cache<TableBlockUniqueIdentifier, AbstractIndex> {
  /**
   * carbon store path
   */
  protected String carbonStorePath;
  /**
   * CarbonLRU cache
   */
  protected CarbonLRUCache lruCache;

  /**
   * table segment id vs blockInfo list
   */
  protected  Map<String, List<BlockInfo>> segmentIdToBlockListMap;


  /**
   * map of block info to lock object map, while loading the btree this will be filled
   * and removed after loading the tree for that particular block info, this will be useful
   * while loading the tree concurrently so only block level lock will be applied another
   * block can be loaded concurrently
   */
  protected Map<BlockInfo, Object> blockInfoLock;

  /**
   * The object will hold the segment ID lock so that at a time only 1 block that belongs to same
   * segment & table can create the list for holding the block info
   */
  protected Map<String, Object> segmentIDLock;

  public AbstractBlockIndexStoreCache(String carbonStorePath, CarbonLRUCache lruCache) {
    this.carbonStorePath = carbonStorePath;
    this.lruCache = lruCache;
    blockInfoLock = new ConcurrentHashMap<BlockInfo, Object>();
    segmentIDLock= new ConcurrentHashMap<String, Object>();
    segmentIdToBlockListMap = new ConcurrentHashMap<>();
  }

  /**
   * This method will get the value for the given key. If value does not exist
   * for the given key, it will check and load the value.
   *
   * @param tableBlock
   * @param tableBlockUniqueIdentifier
   * @param lruCacheKey
   */
  protected void checkAndLoadTableBlocks(AbstractIndex tableBlock,
      TableBlockUniqueIdentifier tableBlockUniqueIdentifier, String lruCacheKey)
      throws IOException {
    // calculate the required size is
    TableBlockInfo blockInfo = tableBlockUniqueIdentifier.getTableBlockInfo();
    long requiredMetaSize = CarbonUtil.calculateMetaSize(blockInfo);
    if (requiredMetaSize > 0) {
      tableBlock.setMemorySize(requiredMetaSize);
      tableBlock.incrementAccessCount();
      boolean isTableBlockAddedToLruCache = lruCache.put(lruCacheKey, tableBlock, requiredMetaSize);
      // if column is successfully added to lru cache then only load the
      // table blocks data
      if (isTableBlockAddedToLruCache) {
        // load table blocks data
        // getting the data file meta data of the block
        DataFileFooter footer = CarbonUtil.readMetadatFile(blockInfo);
        footer.setBlockInfo(new BlockInfo(blockInfo));
        // building the block
        tableBlock.buildIndex(Collections.singletonList(footer));
      } else {
        throw new IndexBuilderException(
            "Cannot load table blocks into memory. Not enough memory available");
      }
    }
  }
}
