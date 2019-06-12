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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.blockletindex.BlockDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapModel;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.memory.MemoryException;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.BlockletDataMapUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class BlockletDataMapIndexStore
    implements Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper> {
  private static final Logger LOGGER =
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
  public BlockletDataMapIndexWrapper get(TableBlockIndexUniqueIdentifierWrapper identifierWrapper)
      throws IOException {
    return get(identifierWrapper, null);
  }

  private BlockletDataMapIndexWrapper get(TableBlockIndexUniqueIdentifierWrapper identifierWrapper,
      Map<String, Map<String, BlockMetaInfo>> segInfoCache) throws IOException {
    TableBlockIndexUniqueIdentifier identifier =
        identifierWrapper.getTableBlockIndexUniqueIdentifier();
    String lruCacheKey = identifier.getUniqueTableSegmentIdentifier();
    BlockletDataMapIndexWrapper blockletDataMapIndexWrapper =
        (BlockletDataMapIndexWrapper) lruCache.get(lruCacheKey);
    List<BlockDataMap> dataMaps = new ArrayList<>();
    if (blockletDataMapIndexWrapper == null) {
      try {
        SegmentIndexFileStore indexFileStore =
            new SegmentIndexFileStore(identifierWrapper.getConfiguration());
        Set<String> filesRead = new HashSet<>();
        String segmentFilePath = identifier.getIndexFilePath();
        if (segInfoCache == null) {
          segInfoCache = new HashMap<>();
        }
        Map<String, BlockMetaInfo> carbonDataFileBlockMetaInfoMapping =
            segInfoCache.get(segmentFilePath);
        if (carbonDataFileBlockMetaInfoMapping == null) {
          carbonDataFileBlockMetaInfoMapping =
              BlockletDataMapUtil.createCarbonDataFileBlockMetaInfoMapping(segmentFilePath,
                  identifierWrapper.getConfiguration());
          segInfoCache.put(segmentFilePath, carbonDataFileBlockMetaInfoMapping);
        }
        // if the identifier is not a merge file we can directly load the datamaps
        if (identifier.getMergeIndexFileName() == null) {
          List<DataFileFooter> indexInfos = new ArrayList<>();
          Map<String, BlockMetaInfo> blockMetaInfoMap = BlockletDataMapUtil
              .getBlockMetaInfoMap(identifierWrapper, indexFileStore, filesRead,
                  carbonDataFileBlockMetaInfoMapping, indexInfos);
          BlockDataMap blockletDataMap =
              loadAndGetDataMap(identifier, indexFileStore, blockMetaInfoMap,
                  identifierWrapper.getCarbonTable(),
                  identifierWrapper.isAddTableBlockToUnsafeAndLRUCache(),
                  identifierWrapper.getConfiguration(), indexInfos);
          dataMaps.add(blockletDataMap);
          blockletDataMapIndexWrapper =
              new BlockletDataMapIndexWrapper(identifier.getSegmentId(), dataMaps);
        } else {
          // if the identifier is a merge file then collect the index files and load the datamaps
          List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
              BlockletDataMapUtil.getIndexFileIdentifiersFromMergeFile(identifier, indexFileStore);
          for (TableBlockIndexUniqueIdentifier blockIndexUniqueIdentifier :
              tableBlockIndexUniqueIdentifiers) {
            List<DataFileFooter> indexInfos = new ArrayList<>();
            Map<String, BlockMetaInfo> blockMetaInfoMap = BlockletDataMapUtil.getBlockMetaInfoMap(
                new TableBlockIndexUniqueIdentifierWrapper(blockIndexUniqueIdentifier,
                    identifierWrapper.getCarbonTable()), indexFileStore, filesRead,
                carbonDataFileBlockMetaInfoMapping, indexInfos);
            if (!blockMetaInfoMap.isEmpty()) {
              BlockDataMap blockletDataMap =
                  loadAndGetDataMap(blockIndexUniqueIdentifier, indexFileStore, blockMetaInfoMap,
                      identifierWrapper.getCarbonTable(),
                      identifierWrapper.isAddTableBlockToUnsafeAndLRUCache(),
                      identifierWrapper.getConfiguration(), indexInfos);
              dataMaps.add(blockletDataMap);
            }
          }
          blockletDataMapIndexWrapper =
              new BlockletDataMapIndexWrapper(identifier.getSegmentId(), dataMaps);
        }
        if (identifierWrapper.isAddTableBlockToUnsafeAndLRUCache()) {
          lruCache.put(identifier.getUniqueTableSegmentIdentifier(), blockletDataMapIndexWrapper,
              blockletDataMapIndexWrapper.getMemorySize());
        }
      } catch (Throwable e) {
        // clear all the memory used by datamaps loaded
        for (DataMap dataMap : dataMaps) {
          dataMap.clear();
        }
        LOGGER.error("memory exception when loading datamap: " + e.getMessage(), e);
        throw new RuntimeException(e);
      }
    }
    return blockletDataMapIndexWrapper;
  }

  @Override public List<BlockletDataMapIndexWrapper> getAll(
      List<TableBlockIndexUniqueIdentifierWrapper> tableSegmentUniqueIdentifiers)
      throws IOException {
    Map<String, Map<String, BlockMetaInfo>> segInfoCache
        = new HashMap<String, Map<String, BlockMetaInfo>>();

    List<BlockletDataMapIndexWrapper> blockletDataMapIndexWrappers =
        new ArrayList<>(tableSegmentUniqueIdentifiers.size());
    List<TableBlockIndexUniqueIdentifierWrapper> missedIdentifiersWrapper = new ArrayList<>();
    BlockletDataMapIndexWrapper blockletDataMapIndexWrapper = null;
    // Get the datamaps for each indexfile from cache.
    try {
      for (TableBlockIndexUniqueIdentifierWrapper
               identifierWrapper : tableSegmentUniqueIdentifiers) {
        BlockletDataMapIndexWrapper dataMapIndexWrapper =
            getIfPresent(identifierWrapper);
        if (dataMapIndexWrapper != null) {
          blockletDataMapIndexWrappers.add(dataMapIndexWrapper);
        } else {
          missedIdentifiersWrapper.add(identifierWrapper);
        }
      }
      if (missedIdentifiersWrapper.size() > 0) {
        for (TableBlockIndexUniqueIdentifierWrapper identifierWrapper : missedIdentifiersWrapper) {
          blockletDataMapIndexWrapper = get(identifierWrapper, segInfoCache);
          blockletDataMapIndexWrappers.add(blockletDataMapIndexWrapper);
        }
      }
    } catch (Throwable e) {
      if (null != blockletDataMapIndexWrapper) {
        List<BlockDataMap> dataMaps = blockletDataMapIndexWrapper.getDataMaps();
        for (DataMap dataMap : dataMaps) {
          dataMap.clear();
        }
      }
      throw new IOException("Problem in loading segment blocks: " + e.getMessage(), e);
    }

    return blockletDataMapIndexWrappers;
  }

  /**
   * returns the SegmentTaskIndexWrapper
   *
   * @param tableSegmentUniqueIdentifierWrapper
   * @return
   */
  @Override public BlockletDataMapIndexWrapper getIfPresent(
      TableBlockIndexUniqueIdentifierWrapper tableSegmentUniqueIdentifierWrapper) {
    return (BlockletDataMapIndexWrapper) lruCache.get(
        tableSegmentUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier()
            .getUniqueTableSegmentIdentifier());
  }

  /**
   * method invalidate the segment cache for segment
   *
   * @param tableSegmentUniqueIdentifierWrapper
   */
  @Override public void invalidate(
      TableBlockIndexUniqueIdentifierWrapper tableSegmentUniqueIdentifierWrapper) {
    BlockletDataMapIndexWrapper blockletDataMapIndexWrapper =
        getIfPresent(tableSegmentUniqueIdentifierWrapper);
    if (null != blockletDataMapIndexWrapper) {
      // clear the segmentProperties cache
      List<BlockDataMap> dataMaps = blockletDataMapIndexWrapper.getDataMaps();
      if (null != dataMaps && !dataMaps.isEmpty()) {
        String segmentId =
            tableSegmentUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier().getSegmentId();
        // as segmentId will be same for all the dataMaps and segmentProperties cache is
        // maintained at segment level so it need to be called only once for clearing
        SegmentPropertiesAndSchemaHolder.getInstance()
            .invalidate(segmentId, dataMaps.get(0).getSegmentPropertiesIndex(),
                tableSegmentUniqueIdentifierWrapper.isAddTableBlockToUnsafeAndLRUCache());
      }
    }
    lruCache.remove(tableSegmentUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier()
        .getUniqueTableSegmentIdentifier());
  }

  @Override
  public void put(TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper,
      BlockletDataMapIndexWrapper wrapper) throws IOException, MemoryException {
    // As dataMap will use unsafe memory, it is not recommended to overwrite an existing entry
    // as in that case clearing unsafe memory need to be taken card. If at all datamap entry
    // in the cache need to be overwritten then use the invalidate interface
    // and then use the put interface
    if (null == getIfPresent(tableBlockIndexUniqueIdentifierWrapper)) {
      List<BlockDataMap> dataMaps = wrapper.getDataMaps();
      try {
        for (BlockDataMap blockletDataMap : dataMaps) {
          blockletDataMap.convertToUnsafeDMStore();
        }
        // Locking is not required here because in LRU cache map add method is synchronized to add
        // only one entry at a time and if a key already exists it will not overwrite the entry
        lruCache.put(tableBlockIndexUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier()
            .getUniqueTableSegmentIdentifier(), wrapper, wrapper.getMemorySize());
      } catch (Throwable e) {
        // clear all the memory acquired by data map in case of any failure
        for (DataMap blockletDataMap : dataMaps) {
          blockletDataMap.clear();
        }
        throw new IOException("Problem in adding datamap to cache.", e);
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
  private BlockDataMap loadAndGetDataMap(TableBlockIndexUniqueIdentifier identifier,
      SegmentIndexFileStore indexFileStore, Map<String, BlockMetaInfo> blockMetaInfoMap,
      CarbonTable carbonTable, boolean addTableBlockToUnsafe, Configuration configuration,
      List<DataFileFooter> indexInfos)
      throws IOException, MemoryException {
    String uniqueTableSegmentIdentifier =
        identifier.getUniqueTableSegmentIdentifier();
    Object lock = segmentLockMap.get(uniqueTableSegmentIdentifier);
    if (lock == null) {
      lock = addAndGetSegmentLock(uniqueTableSegmentIdentifier);
    }
    BlockDataMap dataMap;
    synchronized (lock) {
      dataMap = (BlockDataMap) BlockletDataMapFactory.createDataMap(carbonTable);
      final BlockletDataMapModel blockletDataMapModel = new BlockletDataMapModel(carbonTable,
          identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
              .getIndexFileName(), indexFileStore.getFileData(identifier.getIndexFileName()),
          blockMetaInfoMap, identifier.getSegmentId(), addTableBlockToUnsafe, configuration);
      blockletDataMapModel.setIndexInfos(indexInfos);
      dataMap.init(blockletDataMapModel);
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
   * @param tableSegmentUniqueIdentifiersWrapper
   */
  @Override public void clearAccessCount(
      List<TableBlockIndexUniqueIdentifierWrapper> tableSegmentUniqueIdentifiersWrapper) {
    for (TableBlockIndexUniqueIdentifierWrapper
             identifierWrapper : tableSegmentUniqueIdentifiersWrapper) {
      BlockDataMap cacheable = (BlockDataMap) lruCache.get(
          identifierWrapper.getTableBlockIndexUniqueIdentifier().getUniqueTableSegmentIdentifier());
      cacheable.clear();
    }
  }
}
