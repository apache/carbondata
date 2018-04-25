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
package org.apache.carbondata.core.indexstore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapModel;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.util.BlockletDataMapUtil;

/**
 * Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class BlockletDataMapIndexStore
    implements Cache<TableBlockIndexUniqueIdentifier, BlockletDataMapIndexWrapper> {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockletDataMapIndexStore.class.getName());
  /**
   * CarbonLRU cache
   */
  protected CarbonLRUCache lruCache;

  /**
   * map of block info to lock object map, while loading the btree this will be filled
   * and removed after loading the tree for that particular block info, this will be useful
   * while loading the tree concurrently so only block level lock will be applied another
   * block can be loaded concurrently
   */
  private Map<String, Object> segmentLockMap;

  /**
   * constructor to initialize the SegmentTaskIndexStore
   *
   * @param lruCache
   */
  public BlockletDataMapIndexStore(CarbonLRUCache lruCache) {
    this.lruCache = lruCache;
    segmentLockMap = new ConcurrentHashMap<String, Object>();
  }

  @Override
  public BlockletDataMapIndexWrapper get(TableBlockIndexUniqueIdentifier identifier)
      throws IOException {
    String lruCacheKey = identifier.getUniqueTableSegmentIdentifier();
    BlockletDataMapIndexWrapper blockletDataMapIndexWrapper =
        (BlockletDataMapIndexWrapper) lruCache.get(lruCacheKey);
    List<DataMap> dataMaps = new ArrayList<>();
    if (blockletDataMapIndexWrapper == null) {
      try {
        SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
        Set<String> filesRead = new HashSet<>();
        long memorySize = 0L;
        String segmentFilePath = identifier.getIndexFilePath();
        Map<String, BlockMetaInfo> carbonDataFileBlockMetaInfoMapping = BlockletDataMapUtil
            .createCarbonDataFileBlockMetaInfoMapping(segmentFilePath);
        // if the identifier is not a merge file we can directly load the datamaps
        if (identifier.getMergeIndexFileName() == null) {
          Map<String, BlockMetaInfo> blockMetaInfoMap = BlockletDataMapUtil
              .getBlockMetaInfoMap(identifier, indexFileStore, filesRead,
                  carbonDataFileBlockMetaInfoMapping);
          BlockletDataMap blockletDataMap =
              loadAndGetDataMap(identifier, indexFileStore, blockMetaInfoMap);
          memorySize += blockletDataMap.getMemorySize();
          dataMaps.add(blockletDataMap);
          blockletDataMapIndexWrapper = new BlockletDataMapIndexWrapper(dataMaps);
        } else {
          // if the identifier is a merge file then collect the index files and load the datamaps
          List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
              BlockletDataMapUtil.getIndexFileIdentifiersFromMergeFile(identifier, indexFileStore);
          for (TableBlockIndexUniqueIdentifier blockIndexUniqueIdentifier :
              tableBlockIndexUniqueIdentifiers) {
            Map<String, BlockMetaInfo> blockMetaInfoMap = BlockletDataMapUtil
                .getBlockMetaInfoMap(blockIndexUniqueIdentifier, indexFileStore, filesRead,
                    carbonDataFileBlockMetaInfoMapping);
            BlockletDataMap blockletDataMap =
                loadAndGetDataMap(blockIndexUniqueIdentifier, indexFileStore, blockMetaInfoMap);
            memorySize += blockletDataMap.getMemorySize();
            dataMaps.add(blockletDataMap);
          }
          blockletDataMapIndexWrapper = new BlockletDataMapIndexWrapper(dataMaps);
        }
        lruCache.put(identifier.getUniqueTableSegmentIdentifier(), blockletDataMapIndexWrapper,
            memorySize);
      } catch (Throwable e) {
        // clear all the memory used by datamaps loaded
        for (DataMap dataMap : dataMaps) {
          dataMap.clear();
        }
        LOGGER.error("memory exception when loading datamap: " + e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return blockletDataMapIndexWrapper;
  }

  @Override
  public List<BlockletDataMapIndexWrapper> getAll(
      List<TableBlockIndexUniqueIdentifier> tableSegmentUniqueIdentifiers) throws IOException {
    List<BlockletDataMapIndexWrapper> blockletDataMapIndexWrappers =
        new ArrayList<>(tableSegmentUniqueIdentifiers.size());
    List<TableBlockIndexUniqueIdentifier> missedIdentifiers = new ArrayList<>();
    ExecutorService service = null;
    BlockletDataMapIndexWrapper blockletDataMapIndexWrapper = null;
    // Get the datamaps for each indexfile from cache.
    try {
      for (TableBlockIndexUniqueIdentifier identifier : tableSegmentUniqueIdentifiers) {
        BlockletDataMapIndexWrapper dataMapIndexWrapper = getIfPresent(identifier);
        if (dataMapIndexWrapper != null) {
          blockletDataMapIndexWrappers.add(dataMapIndexWrapper);
        } else {
          missedIdentifiers.add(identifier);
        }
      }
      if (missedIdentifiers.size() > 0) {
        for (TableBlockIndexUniqueIdentifier identifier : missedIdentifiers) {
          blockletDataMapIndexWrapper = get(identifier);
          blockletDataMapIndexWrappers.add(blockletDataMapIndexWrapper);
        }
      }
    } catch (Throwable e) {
      if (null != blockletDataMapIndexWrapper) {
        List<DataMap> dataMaps = blockletDataMapIndexWrapper.getDataMaps();
        for (DataMap dataMap : dataMaps) {
          dataMap.clear();
        }
      }
      throw new IOException("Problem in loading segment blocks.", e);
    } finally {
      if (service != null) {
        service.shutdownNow();
      }
    }
    return blockletDataMapIndexWrappers;
  }

  /**
   * returns the SegmentTaskIndexWrapper
   *
   * @param tableSegmentUniqueIdentifier
   * @return
   */
  @Override
  public BlockletDataMapIndexWrapper getIfPresent(
      TableBlockIndexUniqueIdentifier tableSegmentUniqueIdentifier) {
    return (BlockletDataMapIndexWrapper) lruCache
        .get(tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
  }

  /**
   * method invalidate the segment cache for segment
   *
   * @param tableSegmentUniqueIdentifier
   */
  @Override
  public void invalidate(TableBlockIndexUniqueIdentifier tableSegmentUniqueIdentifier) {
    lruCache.remove(tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
  }

  @Override
  public void put(TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier,
      BlockletDataMapIndexWrapper wrapper) throws IOException, MemoryException {
    String uniqueTableSegmentIdentifier =
        tableBlockIndexUniqueIdentifier.getUniqueTableSegmentIdentifier();
    Object lock = segmentLockMap.get(uniqueTableSegmentIdentifier);
    if (lock == null) {
      lock = addAndGetSegmentLock(uniqueTableSegmentIdentifier);
    }
    long memorySize = 0L;
    // As dataMap will use unsafe memory, it is not recommended to overwrite an existing entry
    // as in that case clearing unsafe memory need to be taken card. If at all datamap entry
    // in the cache need to be overwritten then use the invalidate interface
    // and then use the put interface
    if (null == getIfPresent(tableBlockIndexUniqueIdentifier)) {
      synchronized (lock) {
        if (null == getIfPresent(tableBlockIndexUniqueIdentifier)) {
          List<DataMap> dataMaps = wrapper.getDataMaps();
          try {
            for (DataMap dataMap: dataMaps) {
              BlockletDataMap blockletDataMap = (BlockletDataMap) dataMap;
              blockletDataMap.convertToUnsafeDMStore();
              memorySize += blockletDataMap.getMemorySize();
            }
            lruCache.put(tableBlockIndexUniqueIdentifier.getUniqueTableSegmentIdentifier(), wrapper,
                memorySize);
          } catch (Throwable e) {
            // clear all the memory acquired by data map in case of any failure
            for (DataMap blockletDataMap : dataMaps) {
              blockletDataMap.clear();
            }
            throw new IOException("Problem in adding datamap to cache.", e);
          }
        }
      }
    }
  }

  /**
   * Below method will be used to load the segment of segments
   * One segment may have multiple task , so  table segment will be loaded
   * based on task id and will return the map of taksId to table segment
   * map
   *
   * @return map of taks id to segment mapping
   * @throws IOException
   */
  private BlockletDataMap loadAndGetDataMap(
      TableBlockIndexUniqueIdentifier identifier,
      SegmentIndexFileStore indexFileStore,
      Map<String, BlockMetaInfo> blockMetaInfoMap)
      throws IOException, MemoryException {
    String uniqueTableSegmentIdentifier =
        identifier.getUniqueTableSegmentIdentifier();
    Object lock = segmentLockMap.get(uniqueTableSegmentIdentifier);
    if (lock == null) {
      lock = addAndGetSegmentLock(uniqueTableSegmentIdentifier);
    }
    BlockletDataMap dataMap;
    synchronized (lock) {
      dataMap = new BlockletDataMap();
      dataMap.init(new BlockletDataMapModel(
          identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getIndexFileName(), indexFileStore.getFileData(identifier.getIndexFileName()),
          blockMetaInfoMap, identifier.getSegmentId()));
    }
    return dataMap;
  }

  /**
   * Below method will be used to get the segment level lock object
   *
   * @param uniqueIdentifier
   * @return lock object
   */
  private synchronized Object addAndGetSegmentLock(String uniqueIdentifier) {
    // get the segment lock object if it is present then return
    // otherwise add the new lock and return
    Object segmentLoderLockObject = segmentLockMap.get(uniqueIdentifier);
    if (null == segmentLoderLockObject) {
      segmentLoderLockObject = new Object();
      segmentLockMap.put(uniqueIdentifier, segmentLoderLockObject);
    }
    return segmentLoderLockObject;
  }

  /**
   * The method clears the access count of table segments
   *
   * @param tableSegmentUniqueIdentifiers
   */
  @Override
  public void clearAccessCount(
      List<TableBlockIndexUniqueIdentifier> tableSegmentUniqueIdentifiers) {
    for (TableBlockIndexUniqueIdentifier segmentUniqueIdentifier : tableSegmentUniqueIdentifiers) {
      BlockletDataMap cacheable =
          (BlockletDataMap) lruCache.get(segmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
      cacheable.clear();
    }
  }
}
