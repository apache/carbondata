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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapModel;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.PartitionMapFileStore;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class BlockletDataMapIndexStore
    implements Cache<TableBlockIndexUniqueIdentifier, BlockletDataMap> {
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
  public BlockletDataMap get(TableBlockIndexUniqueIdentifier identifier)
      throws IOException {
    String lruCacheKey = identifier.getUniqueTableSegmentIdentifier();
    BlockletDataMap dataMap = (BlockletDataMap) lruCache.get(lruCacheKey);
    if (dataMap == null) {
      try {
        String segmentPath = CarbonTablePath.getSegmentPath(
            identifier.getAbsoluteTableIdentifier().getTablePath(),
            identifier.getSegmentId());
        Map<String, String[]> locationMap = new HashMap<>();
        CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
        CarbonFile[] carbonFiles = carbonFile.locationAwareListFiles();
        SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
        indexFileStore.readAllIIndexOfSegment(carbonFiles);
        PartitionMapFileStore partitionFileStore = new PartitionMapFileStore();
        partitionFileStore.readAllPartitionsOfSegment(carbonFiles, segmentPath);
        for (CarbonFile file : carbonFiles) {
          locationMap.put(file.getAbsolutePath(), file.getLocations());
        }
        dataMap = loadAndGetDataMap(identifier, indexFileStore, partitionFileStore, locationMap);
      } catch (MemoryException e) {
        LOGGER.error("memory exception when loading datamap: " + e.getMessage());
        throw new RuntimeException(e.getMessage(), e);
      }
    }
    return dataMap;
  }

  @Override
  public List<BlockletDataMap> getAll(
      List<TableBlockIndexUniqueIdentifier> tableSegmentUniqueIdentifiers) throws IOException {
    List<BlockletDataMap> blockletDataMaps = new ArrayList<>(tableSegmentUniqueIdentifiers.size());
    List<TableBlockIndexUniqueIdentifier> missedIdentifiers = new ArrayList<>();
    ExecutorService service = null;
    // Get the datamaps for each indexfile from cache.
    try {
      for (TableBlockIndexUniqueIdentifier identifier : tableSegmentUniqueIdentifiers) {
        BlockletDataMap ifPresent = getIfPresent(identifier);
        if (ifPresent != null) {
          blockletDataMaps.add(ifPresent);
        } else {
          missedIdentifiers.add(identifier);
        }
      }
      if (missedIdentifiers.size() > 0) {
        Map<String, SegmentIndexFileStore> segmentIndexFileStoreMap = new HashMap<>();
        Map<String, PartitionMapFileStore> partitionFileStoreMap = new HashMap<>();
        Map<String, String[]> locationMap = new HashMap<>();

        for (TableBlockIndexUniqueIdentifier identifier: missedIdentifiers) {
          SegmentIndexFileStore indexFileStore =
              segmentIndexFileStoreMap.get(identifier.getSegmentId());
          PartitionMapFileStore partitionFileStore =
              partitionFileStoreMap.get(identifier.getSegmentId());
          String segmentPath = CarbonTablePath.getSegmentPath(
              identifier.getAbsoluteTableIdentifier().getTablePath(),
              identifier.getSegmentId());
          if (indexFileStore == null) {
            CarbonFile carbonFile = FileFactory.getCarbonFile(segmentPath);
            CarbonFile[] carbonFiles = carbonFile.locationAwareListFiles();
            indexFileStore = new SegmentIndexFileStore();
            indexFileStore.readAllIIndexOfSegment(carbonFiles);
            segmentIndexFileStoreMap.put(identifier.getSegmentId(), indexFileStore);
            partitionFileStore = new PartitionMapFileStore();
            partitionFileStore.readAllPartitionsOfSegment(carbonFiles, segmentPath);
            partitionFileStoreMap.put(identifier.getSegmentId(), partitionFileStore);
            for (CarbonFile file : carbonFiles) {
              locationMap.put(file.getAbsolutePath(), file.getLocations());
            }
          }
          blockletDataMaps.add(
              loadAndGetDataMap(identifier, indexFileStore, partitionFileStore, locationMap));
        }
      }
    } catch (Throwable e) {
      for (BlockletDataMap dataMap : blockletDataMaps) {
        dataMap.clear();
      }
      throw new IOException("Problem in loading segment blocks.", e);
    } finally {
      if (service != null) {
        service.shutdownNow();
      }
    }
    return blockletDataMaps;
  }

  /**
   * returns the SegmentTaskIndexWrapper
   *
   * @param tableSegmentUniqueIdentifier
   * @return
   */
  @Override
  public BlockletDataMap getIfPresent(
      TableBlockIndexUniqueIdentifier tableSegmentUniqueIdentifier) {
    return (BlockletDataMap) lruCache.get(
        tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
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
      PartitionMapFileStore partitionFileStore,
      Map<String, String[]> locationMap)
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
      dataMap.init(new BlockletDataMapModel(identifier.getFilePath(),
          indexFileStore.getFileData(identifier.getCarbonIndexFileName()),
          partitionFileStore.getPartitions(identifier.getCarbonIndexFileName()),
          partitionFileStore.isPartionedSegment(), locationMap));
      lruCache.put(identifier.getUniqueTableSegmentIdentifier(), dataMap,
          dataMap.getMemorySize());
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
