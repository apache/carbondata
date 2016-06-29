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
package org.carbondata.core.carbon.datastore;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.BlockIndex;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;

/**
 * Singleton Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class BlockIndexStore {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(BlockIndexStore.class.getName());
  /**
   * singleton instance
   */
  private static final BlockIndexStore CARBONTABLEBLOCKSINSTANCE = new BlockIndexStore();

  /**
   * map to hold the table and its list of blocks
   */
  private Map<AbsoluteTableIdentifier, Map<TableBlockInfo, AbstractIndex>> tableBlocksMap;

  /**
   * map of block info to lock object map, while loading the btree this will be filled
   * and removed after loading the tree for that particular block info, this will be useful
   * while loading the tree concurrently so only block level lock will be applied another
   * block can be loaded concurrently
   */
  private Map<TableBlockInfo, Object> blockInfoLock;

  /**
   * table and its lock object to this will be useful in case of concurrent
   * query scenario when more than one query comes for same table and in that
   * case it will ensure that only one query will able to load the blocks
   */
  private Map<AbsoluteTableIdentifier, Object> tableLockMap;

  private BlockIndexStore() {
    tableBlocksMap =
        new ConcurrentHashMap<AbsoluteTableIdentifier, Map<TableBlockInfo, AbstractIndex>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    tableLockMap = new ConcurrentHashMap<AbsoluteTableIdentifier, Object>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    blockInfoLock = new ConcurrentHashMap<TableBlockInfo, Object>();
  }

  /**
   * Return the instance of this class
   *
   * @return singleton instance
   */
  public static BlockIndexStore getInstance() {
    return CARBONTABLEBLOCKSINSTANCE;
  }

  /**
   * below method will be used to load the block which are not loaded and to
   * get the loaded blocks if all the blocks which are passed is loaded then
   * it will not load , else it will load.
   *
   * @param tableBlocksInfos        list of blocks to be loaded
   * @param absoluteTableIdentifier absolute Table Identifier to identify the table
   * @throws IndexBuilderException
   */
  public List<AbstractIndex> loadAndGetBlocks(List<TableBlockInfo> tableBlocksInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IndexBuilderException {
    AbstractIndex[] loadedBlock = new AbstractIndex[tableBlocksInfos.size()];
    addTableLockObject(absoluteTableIdentifier);
    // sort the block info
    // so block will be loaded in sorted order this will be required for
    // query execution
    Collections.sort(tableBlocksInfos);
    // get the instance
    Object lockObject = tableLockMap.get(absoluteTableIdentifier);
    Map<TableBlockInfo, AbstractIndex> tableBlockMapTemp = null;
    int numberOfCores = 1;
    try {
      numberOfCores = Integer.parseInt(CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.NUM_CORES,
              CarbonCommonConstants.NUM_CORES_DEFAULT_VAL));
    } catch (NumberFormatException e) {
      numberOfCores = Integer.parseInt(CarbonCommonConstants.NUM_CORES_DEFAULT_VAL);
    }
    ExecutorService executor = Executors.newFixedThreadPool(numberOfCores);
    // Acquire the lock to ensure only one query is loading the table blocks
    // if same block is assigned to both the queries
    synchronized (lockObject) {
      tableBlockMapTemp = tableBlocksMap.get(absoluteTableIdentifier);
      // if it is loading for first time
      if (null == tableBlockMapTemp) {
        tableBlockMapTemp = new ConcurrentHashMap<TableBlockInfo, AbstractIndex>();
        tableBlocksMap.put(absoluteTableIdentifier, tableBlockMapTemp);
      }
    }
    AbstractIndex tableBlock = null;
    List<Future<AbstractIndex>> blocksList = new ArrayList<Future<AbstractIndex>>();
    int counter = -1;
    for (TableBlockInfo blockInfo : tableBlocksInfos) {
      counter++;
      // if table block is already loaded then do not load
      // that block
      tableBlock = tableBlockMapTemp.get(blockInfo);
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
          tableBlock = tableBlockMapTemp.get(blockInfo);
          // if still block is not present then load the block
          if (null == tableBlock) {
            blocksList.add(executor.submit(new BlockLoaderThread(blockInfo, tableBlockMapTemp)));
          }
        }
      } else {
        // if blocks is already loaded then directly set the block at particular position
        //so block will be present in sorted order
        loadedBlock[counter] = tableBlock;
      }
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
    for (int i = 0; i < loadedBlockArray.length; i++) {
      if (null == loadedBlockArray[i]) {
        try {
          loadedBlockArray[i] = blocksList.get(blockCounter++).get();
        } catch (InterruptedException | ExecutionException e) {
          throw new IndexBuilderException(e);
        }
      }

    }
  }

  private AbstractIndex loadBlock(Map<TableBlockInfo, AbstractIndex> tableBlockMapTemp,
      TableBlockInfo blockInfo) throws CarbonUtilException {
    AbstractIndex tableBlock;
    DataFileFooter footer;
    // getting the data file meta data of the block
    footer = CarbonUtil.readMetadatFile(blockInfo.getFilePath(), blockInfo.getBlockOffset(),
        blockInfo.getBlockLength());
    tableBlock = new BlockIndex();
    footer.setTableBlockInfo(blockInfo);
    // building the block
    tableBlock.buildIndex(Arrays.asList(footer));
    tableBlockMapTemp.put(blockInfo, tableBlock);
    // finally remove the lock object from block info lock as once block is loaded
    // it will not come inside this if condition
    blockInfoLock.remove(blockInfo);
    return tableBlock;
  }

  /**
   * Method to add table level lock if lock is not present for the table
   *
   * @param absoluteTableIdentifier
   */
  private synchronized void addTableLockObject(AbsoluteTableIdentifier absoluteTableIdentifier) {
    // add the instance to lock map if it is not present
    if (null == tableLockMap.get(absoluteTableIdentifier)) {
      tableLockMap.put(absoluteTableIdentifier, new Object());
    }
  }

  /**
   * This will be used to remove a particular blocks useful in case of
   * deletion of some of the blocks in case of retention or may be some other
   * scenario
   *
   * @param removeTableBlocksInfos  blocks to be removed
   * @param absoluteTableIdentifier absolute table identifier
   */
  public void removeTableBlocks(List<TableBlockInfo> removeTableBlocksInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    // get the lock object if lock object is not present then it is not
    // loaded at all
    // we can return from here
    Object lockObject = tableLockMap.get(absoluteTableIdentifier);
    if (null == lockObject) {
      return;
    }
    Map<TableBlockInfo, AbstractIndex> map = tableBlocksMap.get(absoluteTableIdentifier);
    // if there is no loaded blocks then return
    if (null == map) {
      return;
    }
    for (TableBlockInfo blockInfos : removeTableBlocksInfos) {
      map.remove(blockInfos);
    }
  }

  /**
   * remove all the details of a table this will be used in case of drop table
   *
   * @param absoluteTableIdentifier absolute table identifier to find the table
   */
  public void clear(AbsoluteTableIdentifier absoluteTableIdentifier) {
    // removing all the details of table
    tableLockMap.remove(absoluteTableIdentifier);
    tableBlocksMap.remove(absoluteTableIdentifier);
  }

  /**
   * Thread class which will be used to load the blocks
   */
  private class BlockLoaderThread implements Callable<AbstractIndex> {
    /**
     * table block info to block index map
     */
    private Map<TableBlockInfo, AbstractIndex> tableBlockMap;

    // block info
    private TableBlockInfo blockInfo;

    private BlockLoaderThread(TableBlockInfo blockInfo,
        Map<TableBlockInfo, AbstractIndex> tableBlockMap) {
      this.tableBlockMap = tableBlockMap;
      this.blockInfo = blockInfo;
    }

    @Override public AbstractIndex call() throws Exception {
      // load and return the loaded blocks
      return loadBlock(tableBlockMap, blockInfo);
    }
  }
}
