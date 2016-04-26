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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.BlockIndex;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.metadata.leafnode.DataFileFooter;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonCoreLogEvent;
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
    List<AbstractIndex> loadedBlocksList =
        new ArrayList<AbstractIndex>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // add the instance to lock map if it is not present
    if (null == tableLockMap.get(absoluteTableIdentifier)) {
      tableLockMap.put(absoluteTableIdentifier, new Object());
    }
    // sort the block infos
    // so block will be loaded in sorted order this will be required for
    // query execution
    Collections.sort(tableBlocksInfos);
    // get the instance
    Object lockObject = tableLockMap.get(absoluteTableIdentifier);
    // Acquire the lock to ensure only one query is loading the table blocks
    // if same block is assigned to both the queries
    synchronized (lockObject) {
      Map<TableBlockInfo, AbstractIndex> tableBlockMapTemp =
          tableBlocksMap.get(absoluteTableIdentifier);
      // if it is loading for first time
      if (null == tableBlockMapTemp) {
        tableBlockMapTemp = new HashMap<TableBlockInfo, AbstractIndex>();
        tableBlocksMap.put(absoluteTableIdentifier, tableBlockMapTemp);
      }
      AbstractIndex tableBlock = null;
      DataFileFooter footer = null;
      try {
        for (TableBlockInfo blockInfo : tableBlocksInfos) {
          // if table block is already loaded then do not load
          // that block
          tableBlock = tableBlockMapTemp.get(blockInfo);
          if (null == tableBlock) {
            // getting the data file metadata of the block
            footer =
                CarbonUtil.readMetadatFile(blockInfo.getFilePath(), blockInfo.getBlockOffset());
            tableBlock = new BlockIndex();
            footer.setTableBlockInfo(blockInfo);
            // building the block
            tableBlock.buildIndex(Arrays.asList(footer));
            tableBlockMapTemp.put(blockInfo, tableBlock);
          }
          loadedBlocksList.add(tableBlock);
        }
      } catch (CarbonUtilException e) {
        LOGGER.error(CarbonCoreLogEvent.UNIBI_CARBONCORE_MSG, "Problem while loading the block");
        throw new IndexBuilderException(e);
      }
    }
    return loadedBlocksList;
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
    // Acquire the lock and remove only those instance which was loaded
    synchronized (lockObject) {
      Map<TableBlockInfo, AbstractIndex> map = tableBlocksMap.get(absoluteTableIdentifier);
      // if there is no loaded blocks then return
      if (null == map) {
        return;
      }
      for (TableBlockInfo blockInfos : removeTableBlocksInfos) {
        map.remove(blockInfos);
      }
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
}
