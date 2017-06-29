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
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CarbonLRUCache;
import org.apache.carbondata.core.datastore.block.AbstractIndex;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndex;
import org.apache.carbondata.core.datastore.block.SegmentTaskIndexWrapper;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.exception.IndexBuilderException;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.mutate.UpdateVO;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ObjectSizeCalculator;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.core.util.path.CarbonTablePath.DataFileUtil;

/**
 * Class to handle loading, unloading,clearing,storing of the table
 * blocks
 */
public class SegmentTaskIndexStore
    implements Cache<TableSegmentUniqueIdentifier, SegmentTaskIndexWrapper> {
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(SegmentTaskIndexStore.class.getName());
  /**
   * carbon store path
   */
  protected String carbonStorePath;
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

  private Map<SegmentPropertiesWrapper, SegmentProperties> segmentProperties =
      new HashMap<SegmentPropertiesWrapper, SegmentProperties>();

  /**
   * constructor to initialize the SegmentTaskIndexStore
   *
   * @param carbonStorePath
   * @param lruCache
   */
  public SegmentTaskIndexStore(String carbonStorePath, CarbonLRUCache lruCache) {
    this.carbonStorePath = carbonStorePath;
    this.lruCache = lruCache;
    segmentLockMap = new ConcurrentHashMap<String, Object>();
  }

  @Override
  public SegmentTaskIndexWrapper get(TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier)
      throws IOException {
    SegmentTaskIndexWrapper segmentTaskIndexWrapper = null;
    try {
      segmentTaskIndexWrapper =
          loadAndGetTaskIdToSegmentsMap(tableSegmentUniqueIdentifier.getSegmentToTableBlocksInfos(),
              tableSegmentUniqueIdentifier.getAbsoluteTableIdentifier(),
              tableSegmentUniqueIdentifier);
    } catch (IndexBuilderException e) {
      throw new IOException(e.getMessage(), e);
    } catch (Throwable e) {
      throw new IOException("Problem in loading segment block.", e);
    }
    return segmentTaskIndexWrapper;
  }

  @Override public List<SegmentTaskIndexWrapper> getAll(
      List<TableSegmentUniqueIdentifier> tableSegmentUniqueIdentifiers) throws IOException {
    List<SegmentTaskIndexWrapper> segmentTaskIndexWrappers =
        new ArrayList<>(tableSegmentUniqueIdentifiers.size());
    try {
      for (TableSegmentUniqueIdentifier segmentUniqueIdentifier : tableSegmentUniqueIdentifiers) {
        segmentTaskIndexWrappers.add(get(segmentUniqueIdentifier));
      }
    } catch (Throwable e) {
      for (SegmentTaskIndexWrapper segmentTaskIndexWrapper : segmentTaskIndexWrappers) {
        segmentTaskIndexWrapper.clear();
      }
      throw new IOException("Problem in loading segment blocks.", e);
    }
    return segmentTaskIndexWrappers;
  }

  /**
   * returns the SegmentTaskIndexWrapper
   *
   * @param tableSegmentUniqueIdentifier
   * @return
   */
  @Override public SegmentTaskIndexWrapper getIfPresent(
      TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier) {
    SegmentTaskIndexWrapper segmentTaskIndexWrapper = (SegmentTaskIndexWrapper) lruCache
        .get(tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
    if (null != segmentTaskIndexWrapper) {
      segmentTaskIndexWrapper.incrementAccessCount();
    }
    return segmentTaskIndexWrapper;
  }

  /**
   * method invalidate the segment cache for segment
   *
   * @param tableSegmentUniqueIdentifier
   */
  @Override public void invalidate(TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier) {
    lruCache.remove(tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
  }

  /**
   * returns block timestamp value from the given task
   * @param taskKey
   * @param listOfUpdatedFactFiles
   * @return
   */
  private String getTimeStampValueFromBlock(String taskKey, List<String> listOfUpdatedFactFiles) {
    for (String blockName : listOfUpdatedFactFiles) {
      if (taskKey.equals(CarbonTablePath.DataFileUtil.getTaskNo(blockName))) {
        blockName = blockName.substring(blockName.lastIndexOf('-') + 1, blockName.lastIndexOf('.'));
        return blockName;
      }
    }
    return null;
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
   * @throws IOException
   */
  private SegmentTaskIndexWrapper loadAndGetTaskIdToSegmentsMap(
      Map<String, List<TableBlockInfo>> segmentToTableBlocksInfos,
      AbsoluteTableIdentifier absoluteTableIdentifier,
      TableSegmentUniqueIdentifier tableSegmentUniqueIdentifier) throws IOException {
    // task id to segment map
    Iterator<Map.Entry<String, List<TableBlockInfo>>> iteratorOverSegmentBlocksInfos =
        segmentToTableBlocksInfos.entrySet().iterator();
    Map<TaskBucketHolder, AbstractIndex> taskIdToSegmentIndexMap = null;
    SegmentTaskIndexWrapper segmentTaskIndexWrapper = null;
    SegmentUpdateStatusManager updateStatusManager =
        new SegmentUpdateStatusManager(absoluteTableIdentifier);
    String segmentId = null;
    TaskBucketHolder taskBucketHolder = null;
    try {
      while (iteratorOverSegmentBlocksInfos.hasNext()) {
        // Initialize the UpdateVO to Null for each segment.
        UpdateVO updateVO = null;
        // segment id to table block mapping
        Map.Entry<String, List<TableBlockInfo>> next = iteratorOverSegmentBlocksInfos.next();
        // group task id to table block info mapping for the segment
        Map<TaskBucketHolder, List<TableBlockInfo>> taskIdToTableBlockInfoMap =
            mappedAndGetTaskIdToTableBlockInfo(segmentToTableBlocksInfos);
        segmentId = next.getKey();
        // updateVO is only required when Updates Or Delete performed on the Table.
        if (updateStatusManager.getUpdateStatusDetails().length != 0) {
          // get the existing map of task id to table segment map
          updateVO = updateStatusManager.getInvalidTimestampRange(segmentId);
        }
        // check if segment is already loaded, if segment is already loaded
        //no need to load the segment block
        String lruCacheKey = tableSegmentUniqueIdentifier.getUniqueTableSegmentIdentifier();
        segmentTaskIndexWrapper = (SegmentTaskIndexWrapper) lruCache.get(lruCacheKey);
        if ((segmentTaskIndexWrapper == null) || ((null != updateVO)
            && (tableSegmentUniqueIdentifier.isSegmentUpdated()))) {
          // get the segment loader lock object this is to avoid
          // same segment is getting loaded multiple times
          // in case of concurrent query
          Object segmentLoderLockObject = segmentLockMap.get(lruCacheKey);
          if (null == segmentLoderLockObject) {
            segmentLoderLockObject = addAndGetSegmentLock(lruCacheKey);
          }
          // acquire lock to lod the segment
          synchronized (segmentLoderLockObject) {
            segmentTaskIndexWrapper = (SegmentTaskIndexWrapper) lruCache.get(lruCacheKey);
            if ((null == segmentTaskIndexWrapper) || ((null != updateVO)
                && (tableSegmentUniqueIdentifier.isSegmentUpdated()))) {
              // if the segment is updated then get the existing block task id map details
              // so that the same can be updated after loading the btree.
              if (tableSegmentUniqueIdentifier.isSegmentUpdated()
                  && null != segmentTaskIndexWrapper) {
                taskIdToSegmentIndexMap = segmentTaskIndexWrapper.getTaskIdToTableSegmentMap();
              } else {
                // creating a map of take if to table segment
                taskIdToSegmentIndexMap = new HashMap<TaskBucketHolder, AbstractIndex>();
                segmentTaskIndexWrapper = new SegmentTaskIndexWrapper(taskIdToSegmentIndexMap);
                segmentTaskIndexWrapper.incrementAccessCount();
              }
              Iterator<Map.Entry<TaskBucketHolder, List<TableBlockInfo>>> iterator =
                  taskIdToTableBlockInfoMap.entrySet().iterator();
              long requiredSize =
                  calculateRequiredSize(taskIdToTableBlockInfoMap, absoluteTableIdentifier);
              segmentTaskIndexWrapper.setMemorySize(requiredSize);
              boolean canAddToLruCache =
                  lruCache.tryPut(lruCacheKey, requiredSize);
              if (canAddToLruCache) {
                while (iterator.hasNext()) {
                  Map.Entry<TaskBucketHolder, List<TableBlockInfo>> taskToBlockInfoList =
                      iterator.next();
                  taskBucketHolder = taskToBlockInfoList.getKey();
                  taskIdToSegmentIndexMap.put(taskBucketHolder,
                      loadBlocks(taskBucketHolder, taskToBlockInfoList.getValue(),
                          absoluteTableIdentifier));
                }
                long updatedRequiredSize =
                    ObjectSizeCalculator.estimate(segmentTaskIndexWrapper, requiredSize);
                // update the actual size of object
                segmentTaskIndexWrapper.setMemorySize(updatedRequiredSize);
                if (!lruCache.put(lruCacheKey, segmentTaskIndexWrapper, updatedRequiredSize)) {
                  throw new IndexBuilderException(
                          "Can not load the segment. No Enough space available.");
                }

              } else {
                throw new IndexBuilderException(
                    "Can not load the segment. No Enough space available.");
              }

              // Refresh the Timestamp for those tables which underwent through IUD Operations.
              if (null != updateVO) {
                // set the latest timestamp.
                segmentTaskIndexWrapper
                    .setRefreshedTimeStamp(updateVO.getCreatedOrUpdatedTimeStamp());
              } else {
                segmentTaskIndexWrapper.setRefreshedTimeStamp(0L);
              }
              // tableSegmentMapTemp.put(next.getKey(), taskIdToSegmentIndexMap);
              // removing from segment lock map as once segment is loaded
              // if concurrent query is coming for same segment
              // it will wait on the lock so after this segment will be already
              // loaded so lock is not required, that is why removing the
              // the lock object as it wont be useful
              segmentLockMap.remove(lruCacheKey);
            } else {
              segmentTaskIndexWrapper.incrementAccessCount();
            }
          }
        } else {
          segmentTaskIndexWrapper.incrementAccessCount();
        }
      }
    } catch (IndexBuilderException e) {
      LOGGER.error("Problem while loading the segment");
      throw e;
    }
    return segmentTaskIndexWrapper;
  }

  private long calculateRequiredSize(
      Map<TaskBucketHolder, List<TableBlockInfo>> taskIdToTableBlockInfoMap,
      AbsoluteTableIdentifier absoluteTableIdentifier) {
    Iterator<Map.Entry<TaskBucketHolder, List<TableBlockInfo>>> iterator =
        taskIdToTableBlockInfoMap.entrySet().iterator();
    TaskBucketHolder taskBucketHolder;
    long driverBTreeSize = 0;
    while (iterator.hasNext()) {
      Map.Entry<TaskBucketHolder, List<TableBlockInfo>> taskToBlockInfoList = iterator.next();
      taskBucketHolder = taskToBlockInfoList.getKey();
      driverBTreeSize += CarbonUtil
          .calculateDriverBTreeSize(taskBucketHolder.taskNo, taskBucketHolder.bucketNumber,
              taskToBlockInfoList.getValue(), absoluteTableIdentifier);
    }
    return driverBTreeSize;
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
   * Below method will be used to load the blocks
   *
   * @param tableBlockInfoList
   * @return loaded segment
   * @throws IOException
   */
  private AbstractIndex loadBlocks(TaskBucketHolder taskBucketHolder,
      List<TableBlockInfo> tableBlockInfoList, AbsoluteTableIdentifier tableIdentifier)
      throws IOException {
    // all the block of one task id will be loaded together
    // so creating a list which will have all the data file meta data to of one task
    List<DataFileFooter> footerList = CarbonUtil
        .readCarbonIndexFile(taskBucketHolder.taskNo, taskBucketHolder.bucketNumber,
            tableBlockInfoList, tableIdentifier);

    // Reuse SegmentProperties object if tableIdentifier, columnsInTable and columnCardinality are
    // the same.
    List<ColumnSchema> columnsInTable = footerList.get(0).getColumnInTable();
    int[] columnCardinality = footerList.get(0).getSegmentInfo().getColumnCardinality();
    SegmentPropertiesWrapper segmentPropertiesWrapper =
        new SegmentPropertiesWrapper(tableIdentifier, columnsInTable, columnCardinality);
    SegmentProperties segmentProperties;
    if (this.segmentProperties.containsKey(segmentPropertiesWrapper)) {
      segmentProperties = this.segmentProperties.get(segmentPropertiesWrapper);
    } else {
      // create a metadata details
      // this will be useful in query handling
      // all the data file metadata will have common segment properties we
      // can use first one to get create the segment properties
      segmentProperties = new SegmentProperties(columnsInTable, columnCardinality);
      this.segmentProperties.put(segmentPropertiesWrapper, segmentProperties);
    }

    AbstractIndex segment = new SegmentTaskIndex(segmentProperties);
    // file path of only first block is passed as it all table block info path of
    // same task id will be same
    segment.buildIndex(footerList);
    return segment;
  }

  /**
   * The method clears the access count of table segments
   *
   * @param tableSegmentUniqueIdentifiers
   */
  @Override
  public void clearAccessCount(List<TableSegmentUniqueIdentifier> tableSegmentUniqueIdentifiers) {
    for (TableSegmentUniqueIdentifier segmentUniqueIdentifier : tableSegmentUniqueIdentifiers) {
      SegmentTaskIndexWrapper cacheable = (SegmentTaskIndexWrapper) lruCache
          .get(segmentUniqueIdentifier.getUniqueTableSegmentIdentifier());
      cacheable.clear();
    }
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

  /**
   * This class wraps tableIdentifier, columnsInTable and columnCardinality as a key to determine
   * whether the SegmentProperties object can be reused.
   */
  public static class SegmentPropertiesWrapper {
    private AbsoluteTableIdentifier tableIdentifier;
    private List<ColumnSchema> columnsInTable;
    private int[] columnCardinality;

    public SegmentPropertiesWrapper(AbsoluteTableIdentifier tableIdentifier,
        List<ColumnSchema> columnsInTable, int[] columnCardinality) {
      this.tableIdentifier = tableIdentifier;
      this.columnsInTable = columnsInTable;
      this.columnCardinality = columnCardinality;
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof SegmentPropertiesWrapper)) {
        return false;
      }
      SegmentPropertiesWrapper other = (SegmentPropertiesWrapper) obj;
      return tableIdentifier.equals(other.tableIdentifier)
        && columnsInTable.equals(other.columnsInTable)
        && Arrays.equals(columnCardinality, other.columnCardinality);
    }

    @Override
    public int hashCode() {
      return tableIdentifier.hashCode()
        + columnsInTable.hashCode() + Arrays.hashCode(columnCardinality);
    }
  }
}
