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

package org.apache.carbondata.core.datastore;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.BlockIndex;
import org.apache.carbondata.core.datastore.block.BlockInfo;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TableBlockUniqueIdentifier;
import org.apache.carbondata.core.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonTableIdentifier;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.TaskMetricsMap;

/**
 * This class is used to load the B-Tree in Executor LRU Cache
 */
public class BlockIndexStore<K, V> extends AbstractBlockIndexStoreCache<K, V> {

  /**
   * LOGGER instance
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockIndexStore.class.getName());
  public BlockIndexStore(String carbonStorePath, CarbonLRUCache lruCache) {
    super(lruCache);
  }

  /**
   * The method loads the block meta in B-tree lru cache and returns the block meta.
   *
   * @param tableBlockUniqueIdentifier Uniquely identifies the block
   * @return returns the blocks B-Tree meta
   */
  @Override public AbstractIndex get(TableBlockUniqueIdentifier tableBlockUniqueIdentifier)
      throws IOException {
    TableBlockInfo tableBlockInfo = tableBlockUniqueIdentifier.getTableBlockInfo();
    BlockInfo blockInfo = new BlockInfo(tableBlockInfo);
    String lruCacheKey =
        getLruCacheKey(tableBlockUniqueIdentifier.getAbsoluteTableIdentifier(), blockInfo);
    AbstractIndex tableBlock = (AbstractIndex) lruCache.get(lruCacheKey);

    // if block is not loaded
    if (null == tableBlock) {
      // check any lock object is present in
      // block info lock map
      Object blockInfoLockObject = blockInfoLock.get(blockInfo);
      // if lock object is not present then acquire
      // the lock in block info lock and add a lock object in the map for
      // particular block info, added double checking mechanism to add the lock
      // object so in case of concurrent query we for same block info only one lock
      // object will be added
      if (null == blockInfoLockObject) {
        synchronized (blockInfoLock) {
          // again checking the block info lock, to check whether lock object is present
          // or not if now also not present then add a lock object
          blockInfoLockObject = blockInfoLock.get(blockInfo);
          if (null == blockInfoLockObject) {
            blockInfoLockObject = new Object();
            blockInfoLock.put(blockInfo, blockInfoLockObject);
          }
        }
      }
      //acquire the lock for particular block info
      synchronized (blockInfoLockObject) {
        // check again whether block is present or not to avoid the
        // same block is loaded
        //more than once in case of concurrent query
        tableBlock = (AbstractIndex) lruCache.get(
            getLruCacheKey(tableBlockUniqueIdentifier.getAbsoluteTableIdentifier(), blockInfo));
        // if still block is not present then load the block
        if (null == tableBlock) {
          tableBlock = loadBlock(tableBlockUniqueIdentifier);
          fillSegmentIdToBlockListMap(tableBlockUniqueIdentifier.getAbsoluteTableIdentifier(),
              blockInfo);
        }
      }
    } else {
      tableBlock.incrementAccessCount();
    }
    return tableBlock;
  }

  /**
   * @param absoluteTableIdentifier
   * @param blockInfo
   */
  private void fillSegmentIdToBlockListMap(AbsoluteTableIdentifier absoluteTableIdentifier,
      BlockInfo blockInfo) {
    TableSegmentUniqueIdentifier segmentIdentifier =
        new TableSegmentUniqueIdentifier(absoluteTableIdentifier,
            blockInfo.getTableBlockInfo().getSegmentId());
    String uniqueTableSegmentIdentifier = segmentIdentifier.getUniqueTableSegmentIdentifier();
    List<BlockInfo> blockInfos =
        segmentIdToBlockListMap.get(uniqueTableSegmentIdentifier);
    if (null == blockInfos) {
      Object segmentLockObject = segmentIDLock.get(uniqueTableSegmentIdentifier);
      if (null == segmentLockObject) {
        synchronized (segmentIDLock) {
          segmentLockObject = segmentIDLock.get(uniqueTableSegmentIdentifier);
          if (null == segmentLockObject) {
            segmentLockObject = new Object();
            segmentIDLock.put(uniqueTableSegmentIdentifier, segmentLockObject);
          }
        }
      }
      synchronized (segmentLockObject) {
        blockInfos =
            segmentIdToBlockListMap.get(segmentIdentifier.getUniqueTableSegmentIdentifier());
        if (null == blockInfos) {
          blockInfos = new CopyOnWriteArrayList<>();
          segmentIdToBlockListMap.put(uniqueTableSegmentIdentifier, blockInfos);
        }
        blockInfos.add(blockInfo);
      }
    } else {
      blockInfos.add(blockInfo);
    }
  }

  /**
   * The method takes list of tableblocks as input and load them in btree lru cache
   * and returns the list of data blocks meta
   *
   * @param tableBlocksInfos List of unique table blocks
   * @return List<AbstractIndex>
   * @throws IndexBuilderException
   */
  @Override public List<AbstractIndex> getAll(List<TableBlockUniqueIdentifier> tableBlocksInfos)
      throws IndexBuilderException {
    AbstractIndex[] loadedBlock = new AbstractIndex[tableBlocksInfos.size()];
    int numberOfCores = 1;
    try {
      numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException e) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    ExecutorService executor = Executors.newFixedThreadPool(numberOfCores);
    List<Future<AbstractIndex>> blocksList = new ArrayList<Future<AbstractIndex>>();
    for (TableBlockUniqueIdentifier tableBlockUniqueIdentifier : tableBlocksInfos) {
      blocksList.add(executor.submit(new BlockLoaderThread(tableBlockUniqueIdentifier)));
    }
    // shutdown the executor gracefully and wait until all the task is finished
    executor.shutdown();
    try {
      executor.awaitTermination(1, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      throw new IndexBuilderException(e);
    }
    // fill the block which were not loaded before to loaded blocks array
    fillLoadedBlocks(loadedBlock, blocksList);
    return Arrays.asList(loadedBlock);
  }

  private String getLruCacheKey(AbsoluteTableIdentifier absoluteTableIdentifier,
      BlockInfo blockInfo) {
    CarbonTableIdentifier carbonTableIdentifier =
        absoluteTableIdentifier.getCarbonTableIdentifier();
    return carbonTableIdentifier.getDatabaseName() + CarbonCommonConstants.FILE_SEPARATOR
        + carbonTableIdentifier.getTableName() + CarbonCommonConstants.UNDERSCORE
        + carbonTableIdentifier.getTableId() + CarbonCommonConstants.FILE_SEPARATOR + blockInfo
        .getBlockUniqueName();
  }

  /**
   * method returns the B-Tree meta
   *
   * @param tableBlockUniqueIdentifier Unique table block info
   * @return
   */
  @Override public AbstractIndex getIfPresent(
      TableBlockUniqueIdentifier tableBlockUniqueIdentifier) {
    BlockInfo blockInfo = new BlockInfo(tableBlockUniqueIdentifier.getTableBlockInfo());
    BlockIndex cacheable = (BlockIndex) lruCache
        .get(getLruCacheKey(tableBlockUniqueIdentifier.getAbsoluteTableIdentifier(), blockInfo));
    if (null != cacheable) {
      cacheable.incrementAccessCount();
    }
    return cacheable;
  }

  /**
   * the method removes the entry from cache.
   *
   * @param tableBlockUniqueIdentifier
   */
  @Override public void invalidate(TableBlockUniqueIdentifier tableBlockUniqueIdentifier) {
    BlockInfo blockInfo = new BlockInfo(tableBlockUniqueIdentifier.getTableBlockInfo());
    lruCache
        .remove(getLruCacheKey(tableBlockUniqueIdentifier.getAbsoluteTableIdentifier(), blockInfo));
  }

  @Override public void clearAccessCount(List<TableBlockUniqueIdentifier> keys) {
    for (TableBlockUniqueIdentifier tableBlockUniqueIdentifier : keys) {
      SegmentTaskIndexWrapper cacheable = (SegmentTaskIndexWrapper) lruCache
          .get(tableBlockUniqueIdentifier.getUniqueTableBlockName());
      cacheable.clear();
    }
  }

  /**
   * Below method will be used to fill the loaded blocks to the array
   * which will be used for query execution
   *
   * @param loadedBlockArray array of blocks which will be filled
   * @param blocksList       blocks loaded in thread
   * @throws IndexBuilderException in case of any failure
   */
  private void fillLoadedBlocks(AbstractIndex[] loadedBlockArray,
      List<Future<AbstractIndex>> blocksList) throws IndexBuilderException {
    int blockCounter = 0;
    boolean exceptionOccurred = false;
    Throwable exceptionRef = null;
    for (int i = 0; i < loadedBlockArray.length; i++) {
      try {
        loadedBlockArray[i] = blocksList.get(blockCounter++).get();
      } catch (Throwable e) {
        exceptionOccurred = true;
        exceptionRef = e;
      }
    }
    if (exceptionOccurred) {
      LOGGER.error("Block B-tree loading failed. Clearing the access count of the loaded blocks.");
      // in case of any failure clear the access count for the valid loaded blocks
      clearAccessCountForLoadedBlocks(loadedBlockArray);
      throw new IndexBuilderException("Block B-tree loading failed", exceptionRef);
    }
  }

  /**
   * This method will clear the access count for the loaded blocks
   *
   * @param loadedBlockArray
   */
  private void clearAccessCountForLoadedBlocks(AbstractIndex[] loadedBlockArray) {
    for (int i = 0; i < loadedBlockArray.length; i++) {
      if (null != loadedBlockArray[i]) {
        loadedBlockArray[i].clear();
      }
    }
  }

  /**
   * Thread class which will be used to load the blocks
   */
  private class BlockLoaderThread implements Callable<AbstractIndex> {
    // table  block unique identifier
    private TableBlockUniqueIdentifier tableBlockUniqueIdentifier;

    private BlockLoaderThread(TableBlockUniqueIdentifier tableBlockUniqueIdentifier) {
      this.tableBlockUniqueIdentifier = tableBlockUniqueIdentifier;
    }

    @Override public AbstractIndex call() throws Exception {
      try {
        //register thread callback for calculating metrics
        TaskMetricsMap.getInstance().registerThreadCallback();
        // load and return the loaded blocks
        return get(tableBlockUniqueIdentifier);
      } finally {
        // update read bytes metrics for this thread
        TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId());
      }
    }
  }

  private AbstractIndex loadBlock(TableBlockUniqueIdentifier tableBlockUniqueIdentifier)
      throws IOException {
    AbstractIndex tableBlock = new BlockIndex();
    BlockInfo blockInfo = new BlockInfo(tableBlockUniqueIdentifier.getTableBlockInfo());
    String lruCacheKey =
        getLruCacheKey(tableBlockUniqueIdentifier.getAbsoluteTableIdentifier(), blockInfo);
    checkAndLoadTableBlocks(tableBlock, tableBlockUniqueIdentifier, lruCacheKey);
    // finally remove the lock object from block info lock as once block is loaded
    // it will not come inside this if condition
    blockInfoLock.remove(blockInfo);
    return tableBlock;
  }

  /**
   * This will be used to remove a particular blocks useful in case of
   * deletion of some of the blocks in case of retention or may be some other
   * scenario
   *
   * @param segmentIds              list of table blocks to be removed
   * @param absoluteTableIdentifier absolute table identifier
   */
  public void removeTableBlocks(List<String> segmentIds,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    if (null == segmentIds) {
      return;
    }
    for (String segmentId : segmentIds) {
      TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
          new TableSegmentUniqueIdentifier(absoluteTableIdentifier, segmentId);
      List<BlockInfo> blockInfos = segmentIdToBlockListMap
          .remove(tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
      if (null != blockInfos) {
        for (BlockInfo blockInfo : blockInfos) {
          String lruCacheKey = getLruCacheKey(absoluteTableIdentifier, blockInfo);
          lruCache.remove(lruCacheKey);
        }
      }
    }
  }

  /**
   * remove TableBlocks executer level If Horizontal Compaction Done
   * @param queryModel
   */
  public void removeTableBlocksIfHorizontalCompactionDone(QueryModel queryModel) {
    // get the invalid segments blocks details
    Map<String, UpdateVO> invalidBlocksVO = queryModel.getInvalidBlockVOForSegmentId();
    if (!invalidBlocksVO.isEmpty()) {
      UpdateVO updateMetadata;
      Iterator<Map.Entry<String, UpdateVO>> itr = invalidBlocksVO.entrySet().iterator();
      String blockTimestamp = null;
      while (itr.hasNext()) {
        Map.Entry<String, UpdateVO> entry = itr.next();
        TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier =
            new TableSegmentUniqueIdentifier(queryModel.getAbsoluteTableIdentifier(),
                entry.getKey());
        List<BlockInfo> blockInfos = segmentIdToBlockListMap
            .get(tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
        if (null != blockInfos) {
          for (BlockInfo blockInfo : blockInfos) {
            // reading the updated block names from status manager instance
            blockTimestamp = blockInfo.getBlockUniqueName()
                .substring(blockInfo.getBlockUniqueName().lastIndexOf('-') + 1,
                    blockInfo.getBlockUniqueName().length());
            updateMetadata = entry.getValue();
            if (CarbonUpdateUtil.isMaxQueryTimeoutExceeded(Long.parseLong(blockTimestamp))) {
              Long blockTimeStamp = Long.parseLong(blockTimestamp);
              if (blockTimeStamp > updateMetadata.getFactTimestamp() && (
                  updateMetadata.getUpdateDeltaStartTimestamp() != null
                      && blockTimeStamp < updateMetadata.getUpdateDeltaStartTimestamp())) {
                String lruCacheKey =
                    getLruCacheKey(queryModel.getAbsoluteTableIdentifier(), blockInfo);
                lruCache.remove(lruCacheKey);
              }
            }
          }
        }
      }
    }
  }
}
