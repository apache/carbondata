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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.apache.carbondata.core.carbon.datastore.block.AbstractIndex;
import org.apache.carbondata.core.carbon.datastore.block.SegmentTaskIndex;
import org.apache.carbondata.core.carbon.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.carbon.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.carbon.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.carbon.path.CarbonTablePath.DataFileUtil;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.CarbonUtilException;

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
  private Map<AbsoluteTableIdentifier,
      Map<String, Map<TaskBucketHolder, AbstractIndex>>> tableSegmentMap;

  /**
   * map of block info to lock object map, while loading the btree this will be filled
   * and removed after loading the tree for that particular block info, this will be useful
   * while loading the tree concurrently so only block level lock will be applied another
   * block can be loaded concurrently
   */
  private Map<String, Object> segmentLockMap;

  /**
   * table and its lock object to this will be useful in case of concurrent
   * query scenario when more than one query comes for same table and in  that
   * case it will ensure that only one query will able to load the blocks
   */
  private Map<AbsoluteTableIdentifier, Object> tableLockMap;

  private SegmentTaskIndexStore() {
    tableSegmentMap =
        new ConcurrentHashMap<>(
            CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    tableLockMap = new ConcurrentHashMap<AbsoluteTableIdentifier, Object>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    segmentLockMap = new ConcurrentHashMap<String, Object>();
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
  public Map<TaskBucketHolder, AbstractIndex> loadAndGetTaskIdToSegmentsMap(
      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier) throws IndexBuilderException {
    // task id to segment map
    Map<TaskBucketHolder, AbstractIndex> taskIdToTableSegmentMap =
        new HashMap<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    addLockObject(absoluteTableIdentifier);
    Iterator<Entry<String, List<TableBlockInfo>>> iteratorOverSegmentBlocksInfos =
        segmentToTableBlocksInfos.entrySet().iterator();
    Map<String, Map<TaskBucketHolder, AbstractIndex>> tableSegmentMapTemp =
        addTableSegmentMap(absoluteTableIdentifier);
    Map<TaskBucketHolder, AbstractIndex> taskIdToSegmentIndexMap = null;
    String segmentId = null;
    TaskBucketHolder taskId = null;
    try {
      while (iteratorOverSegmentBlocksInfos.hasNext()) {
        // segment id to table block mapping
        Entry<String, List<TableBlockInfo>> next = iteratorOverSegmentBlocksInfos.next();
        // group task id to table block info mapping for the segment
        Map<TaskBucketHolder, List<TableBlockInfo>> taskIdToTableBlockInfoMap =
            mappedAndGetTaskIdToTableBlockInfo(segmentToTableBlocksInfos);
        // get the existing map of task id to table segment map
        segmentId = next.getKey();
        // check if segment is already loaded, if segment is already loaded
        //no need to load the segment block
        taskIdToSegmentIndexMap = tableSegmentMapTemp.get(segmentId);
        if (taskIdToSegmentIndexMap == null) {
          // get the segment loader lock object this is to avoid
          // same segment is getting loaded multiple times
          // in case of concurrent query
          Object segmentLoderLockObject = segmentLockMap.get(segmentId);
          if (null == segmentLoderLockObject) {
            segmentLoderLockObject = addAndGetSegmentLock(segmentId);
          }
          // acquire lock to lod the segment
          synchronized (segmentLoderLockObject) {
            taskIdToSegmentIndexMap = tableSegmentMapTemp.get(segmentId);
            if (null == taskIdToSegmentIndexMap) {
              // creating a map of task id to table segment
              taskIdToSegmentIndexMap = new ConcurrentHashMap<TaskBucketHolder, AbstractIndex>();
              Iterator<Entry<TaskBucketHolder, List<TableBlockInfo>>> iterator =
                  taskIdToTableBlockInfoMap.entrySet().iterator();
              while (iterator.hasNext()) {
                Entry<TaskBucketHolder, List<TableBlockInfo>> taskToBlockInfoList = iterator.next();
                taskId = taskToBlockInfoList.getKey();
                taskIdToSegmentIndexMap.put(taskId,
                    loadBlocks(taskId, taskToBlockInfoList.getValue(), absoluteTableIdentifier));
              }
              tableSegmentMapTemp.put(next.getKey(), taskIdToSegmentIndexMap);
              // removing from segment lock map as once segment is loaded
              //if concurrent query is coming for same segment
              // it will wait on the lock so after this segment will be already
              //loaded so lock is not required, that is why removing the
              // the lock object as it wont be useful
              segmentLockMap.remove(segmentId);
            }
          }
          taskIdToTableSegmentMap.putAll(taskIdToSegmentIndexMap);
        }
      }
    } catch (CarbonUtilException e) {
      LOGGER.error("Problem while loading the segment");
      throw new IndexBuilderException(e);
    }
    return taskIdToTableSegmentMap;
  }

  /**
   * Below method will be used to get the segment level lock object
   *
   * @param segmentId
   * @return lock object
   */
  private synchronized Object addAndGetSegmentLock(String segmentId) {
    // get the segment lock object if it is present then return
    // otherwise add the new lock and return
    Object segmentLoderLockObject = segmentLockMap.get(segmentId);
    if (null == segmentLoderLockObject) {
      segmentLoderLockObject = new Object();
      segmentLockMap.put(segmentId, segmentLoderLockObject);
    }
    return segmentLoderLockObject;
  }

  /**
   * Below code is to add table lock map which will be used to
   * add
   *
   * @param absoluteTableIdentifier
   */
  private synchronized void addLockObject(AbsoluteTableIdentifier absoluteTableIdentifier) {
    // add the instance to lock map if it is not present
    if (null == tableLockMap.get(absoluteTableIdentifier)) {
      tableLockMap.put(absoluteTableIdentifier, new Object());
    }
  }

  /**
   * Below method will be used to get the table segment map
   * if table segment is not present then it will add and return
   *
   * @param absoluteTableIdentifier
   * @return table segment map
   */
  private Map<String, Map<TaskBucketHolder, AbstractIndex>> addTableSegmentMap(
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    // get the instance of lock object
    Object lockObject = tableLockMap.get(absoluteTableIdentifier);
    Map<String, Map<TaskBucketHolder, AbstractIndex>> tableSegmentMapTemp =
        tableSegmentMap.get(absoluteTableIdentifier);
    if (null == tableSegmentMapTemp) {
      synchronized (lockObject) {
        // segment id to task id to table segment map
        tableSegmentMapTemp = tableSegmentMap.get(absoluteTableIdentifier);
        if (null == tableSegmentMapTemp) {
          tableSegmentMapTemp = new ConcurrentHashMap<>();
          tableSegmentMap.put(absoluteTableIdentifier, tableSegmentMapTemp);
        }
      }
    }
    return tableSegmentMapTemp;
  }

  /**
   * Below method will be used to load the blocks
   *
   * @param tableBlockInfoList
   * @return loaded segment
   * @throws CarbonUtilException
   */
  private AbstractIndex loadBlocks(TaskBucketHolder holder, List<TableBlockInfo> tableBlockInfoList,
      AbsoluteTableIdentifier tableIdentifier) throws CarbonUtilException {
    // all the block of one task id will be loaded together
    // so creating a list which will have all the data file meta data to of one task
    List<DataFileFooter> footerList = CarbonUtil
        .readCarbonIndexFile(holder.taskNo, holder.bucketNumber, tableBlockInfoList,
            tableIdentifier);
    AbstractIndex segment = new SegmentTaskIndex();
    // file path of only first block is passed as it all table block info path of
    // same task id will be same
    segment.buildIndex(footerList);
    return segment;
  }

  /**
   * Below method will be used to get the task id to all the table block info belongs to
   * that task id mapping
   *
   * @param segmentToTableBlocksInfos segment if to table blocks info map
   * @return task id to table block info mapping
   */
  private Map<TaskBucketHolder, List<TableBlockInfo>> mappedAndGetTaskIdToTableBlockInfo(
      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos) {
    Map<TaskBucketHolder, List<TableBlockInfo>> taskIdToTableBlockInfoMap =
        new ConcurrentHashMap<>();
    Iterator<Entry<String, List<TableBlockInfo>>> iterator =
        segmentToTableBlocksInfos.entrySet().iterator();
    while (iterator.hasNext()) {
      Entry<String, List<TableBlockInfo>> next = iterator.next();
      List<TableBlockInfo> value = next.getValue();
      for (TableBlockInfo blockInfo : value) {
        String taskNo = DataFileUtil.getTaskNo(blockInfo.getFilePath());
        String bucketNo = DataFileUtil.getBucketNo(blockInfo.getFilePath());
        TaskBucketHolder bucketHolder = new TaskBucketHolder(taskNo, bucketNo);
        List<TableBlockInfo> list = taskIdToTableBlockInfoMap.get(bucketHolder);
        if (null == list) {
          list = new ArrayList<TableBlockInfo>();
          taskIdToTableBlockInfoMap.put(bucketHolder, list);
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
  public void removeTableBlocks(List<String> segmentToBeRemoved,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    // get the lock object if lock object is not present then it is not
    // loaded at all
    // we can return from here
    Object lockObject = tableLockMap.get(absoluteTableIdentifier);
    if (null == lockObject) {
      return;
    }
    // Acquire the lock and remove only those instance which was loaded
    Map<String, Map<TaskBucketHolder, AbstractIndex>> map =
        tableSegmentMap.get(absoluteTableIdentifier);
    // if there is no loaded blocks then return
    if (null == map) {
      return;
    }
    for (String segmentId : segmentToBeRemoved) {
      map.remove(segmentId);
    }
  }

  /**
   * Below method will be used to check if segment blocks
   * is already loaded or not
   *
   * @param absoluteTableIdentifier
   * @param segmentId
   * @return is loaded then return the loaded blocks otherwise null
   */
  public Map<TaskBucketHolder, AbstractIndex> getSegmentBTreeIfExists(
      AbsoluteTableIdentifier absoluteTableIdentifier, String segmentId) {
    Map<String, Map<TaskBucketHolder, AbstractIndex>> tableSegment =
        tableSegmentMap.get(absoluteTableIdentifier);
    if (null == tableSegment) {
      return null;
    }
    return tableSegment.get(segmentId);
  }

  public static class TaskBucketHolder implements Serializable {

    public String taskNo;

    public String bucketNumber;

    public TaskBucketHolder(String taskNo, String bucketNumber) {
      this.taskNo = taskNo;
      this.bucketNumber = bucketNumber;
    }

    @Override public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      TaskBucketHolder that = (TaskBucketHolder) o;

      if (taskNo != null ? !taskNo.equals(that.taskNo) : that.taskNo != null) return false;
      return bucketNumber != null ?
          bucketNumber.equals(that.bucketNumber) :
          that.bucketNumber == null;

    }

    @Override public int hashCode() {
      int result = taskNo != null ? taskNo.hashCode() : 0;
      result = 31 * result + (bucketNumber != null ? bucketNumber.hashCode() : 0);
      return result;
    }
  }
}
