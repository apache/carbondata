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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.carbondata.core.carbon.datastore.block.SegmentTaskIndex;
import org.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.carbondata.core.carbon.path.CarbonTablePath.DataFileUtil;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.core.util.CarbonUtilException;

/**
 * Singleton Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class SegmentTaskIndexStore {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SegmentTaskIndexStore.class.getName());
  /**
   * singleton instance
   */
  private static final SegmentTaskIndexStore SEGMENTTASKINDEXSTORE = new SegmentTaskIndexStore();

  /**
   * mapping of table identifier to map of segmentId_taskId to table segment
   * reason of so many map as each segment can have multiple data file and
   * each file will have its own btree
   */
  private Map<AbsoluteTableIdentifier, Map<String, Map<String, AbstractIndex>>> tableSegmentMap;

  /**
   * table and its lock object to this will be useful in case of concurrent
   * query scenario when more than one query comes for same table and in  that
   * case it will ensure that only one query will able to load the blocks
   */
  private Map<AbsoluteTableIdentifier, Object> tableLockMap;

  private SegmentTaskIndexStore() {
    tableSegmentMap =
        new ConcurrentHashMap<AbsoluteTableIdentifier, Map<String, Map<String, AbstractIndex>>>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    tableLockMap = new ConcurrentHashMap<AbsoluteTableIdentifier, Object>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * Return the instance of this class
   *
   * @return singleton instance
   */
  public static SegmentTaskIndexStore getInstance() {
    return SEGMENTTASKINDEXSTORE;
  }

  /**
   * Below method will be used to load the segment of segments
   * One segment may have multiple task , so  table segment will be loaded
   * based on task id and will return the map of taksId to table segment
   * map
   *
   * @param segmentToTableBlocksInfos segment id to block info
   * @param absoluteTableIdentifier   absolute table identifier
   * @return map of taks id to segment mapping
   * @throws IndexBuilderException
   */
  public Map<String, AbstractIndex> loadAndGetTaskIdToSegmentsMap(
      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IndexBuilderException {
    // task id to segment map
    Map<String, AbstractIndex> taskIdToTableSegmentMap =
        new HashMap<String, AbstractIndex>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    // add the instance to lock map if it is not present
    if (null == tableLockMap.get(absoluteTableIdentifier)) {
      tableLockMap.put(absoluteTableIdentifier, new Object());
    }
    Iterator<Entry<String, List<TableBlockInfo>>> iteratorOverSegmentBlocksInfos =
        segmentToTableBlocksInfos.entrySet().iterator();
    // get the instance of lock object
    Object lockObject = tableLockMap.get(absoluteTableIdentifier);

    synchronized (lockObject) {
      // segment id to task id to table segment map
      Map<String, Map<String, AbstractIndex>> tableSegmentMapTemp =
          tableSegmentMap.get(absoluteTableIdentifier);
      if (null == tableSegmentMapTemp) {
        tableSegmentMapTemp = new HashMap<String, Map<String, AbstractIndex>>();
        tableSegmentMap.put(absoluteTableIdentifier, tableSegmentMapTemp);
      }
      Map<String, AbstractIndex> map = null;
      DataFileFooter footer = null;
      try {
        while (iteratorOverSegmentBlocksInfos.hasNext()) {
          // segment id to table block mapping
          Entry<String, List<TableBlockInfo>> next = iteratorOverSegmentBlocksInfos.next();
          // group task id to table block info mapping for the segment
          Map<String, List<TableBlockInfo>> taskIdToTableBlockInfoMap =
              mappedAndGetTaskIdToTableBlockInfo(segmentToTableBlocksInfos);
          // get the existing map of task id to table segment map
          map = tableSegmentMapTemp.get(next.getKey());
          if (map == null) {
            // creating a map of tak if to table segment
            map = new HashMap<String, AbstractIndex>();
            tableSegmentMapTemp.put(next.getKey(), map);
            Iterator<Entry<String, List<TableBlockInfo>>> iterator =
                taskIdToTableBlockInfoMap.entrySet().iterator();
            while (iterator.hasNext()) {
              Entry<String, List<TableBlockInfo>> taskIdToBlockInfoIterator = iterator.next();
              // all the block of one task id will be loaded together
              // so creating a list which will have all the data file metadata to of one task
              List<DataFileFooter> footerList = new ArrayList<DataFileFooter>();

              for (TableBlockInfo tableBlockInfo : taskIdToBlockInfoIterator.getValue()) {
                footer = CarbonUtil
                    .readMetadatFile(tableBlockInfo.getFilePath(), tableBlockInfo.getBlockOffset(),
                        tableBlockInfo.getBlockLength());
                footer.setTableBlockInfo(tableBlockInfo);
                footerList.add(footer);
              }
              AbstractIndex segment = new SegmentTaskIndex();
              // file path of only first block is passed as it all table block info path of
              // same task id will be same
              segment.buildIndex(footerList);
              map.put(taskIdToBlockInfoIterator.getKey(), segment);
            }

          }
          taskIdToTableSegmentMap.putAll(map);
        }
      } catch (CarbonUtilException e) {
        LOGGER.error("Problem while loading the segment");
        throw new IndexBuilderException(e);
      }
    }
    return taskIdToTableSegmentMap;
  }

  /**
   * Below method will be used to get the task id to all the table block info belongs to
   * that task id mapping
   *
   * @param segmentToTableBlocksInfos segment if to table blocks info map
   * @return task id to table block info mapping
   */
  private Map<String, List<TableBlockInfo>> mappedAndGetTaskIdToTableBlockInfo(
      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos) {
    Map<String, List<TableBlockInfo>> taskIdToTableBlockInfoMap =
        new HashMap<String, List<TableBlockInfo>>();
    Iterator<Entry<String, List<TableBlockInfo>>> iterator =
        segmentToTableBlocksInfos.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, List<TableBlockInfo>> next = iterator.next();
      List<TableBlockInfo> value = next.getValue();
      for (TableBlockInfo blockInfo : value) {
        String taskNo = DataFileUtil.getTaskNo(blockInfo.getFilePath());
        List<TableBlockInfo> list = taskIdToTableBlockInfoMap.get(taskNo);
        if (null == list) {
          list = new ArrayList<TableBlockInfo>();
          taskIdToTableBlockInfoMap.put(taskNo, list);
        }
        list.add(blockInfo);
      }

    }
    return taskIdToTableBlockInfoMap;
  }

  /**
   * remove all the details of a table this will be used in case of drop table
   *
   * @param absoluteTableIdentifier absolute table identifier to find the table
   */
  public void clear(AbsoluteTableIdentifier absoluteTableIdentifier) {
    // removing all the details of table
    tableLockMap.remove(absoluteTableIdentifier);
    tableSegmentMap.remove(absoluteTableIdentifier);
  }

  /**
   * Below method will be used to remove the segment block based on
   * segment id is passed
   *
   * @param segmentToBeRemoved      segment to be removed
   * @param absoluteTableIdentifier absoluteTableIdentifier
   */
  public void removeTableBlocks(List<Integer> segmentToBeRemoved,
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
      Map<String, Map<String, AbstractIndex>> map = tableSegmentMap.get(absoluteTableIdentifier);
      // if there is no loaded blocks then return
      if (null == map) {
        return;
      }
      for (Integer segmentId : segmentToBeRemoved) {
        map.remove(segmentId);
      }
    }
  }

  public Map<String, AbstractIndex> getSegmentBTreeIfExists(
      AbsoluteTableIdentifier absoluteTableIdentifier, String segmentId) {
    Map<String, Map<String, AbstractIndex>> tableSegment
            = tableSegmentMap.get(absoluteTableIdentifier);
    if (null == tableSegment){
      return null;
    }
    return tableSegment.get(segmentId);
  }
}
