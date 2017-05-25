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

package org.apache.carbondata.core.datastore.block;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.core.cache.Cacheable;
import org.apache.carbondata.core.datastore.SegmentTaskIndexStore;
import org.apache.carbondata.core.mutate.UpdateVO;

/**
 * SegmentTaskIndexWrapper class holds the  taskIdToTableSegmentMap
 */
public class SegmentTaskIndexWrapper implements Cacheable {

  /**
   * task_id to table segment index map
   */
  private Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskIdToTableSegmentMap;
  /**
   * atomic integer to maintain the access count for a column access
   */
  protected AtomicInteger accessCount = new AtomicInteger();

  /**
   * Table block meta size.
   */
  protected AtomicLong memorySize = new AtomicLong();

  private Long refreshedTimeStamp;
  private UpdateVO invalidTaskKey;
  public SegmentTaskIndexWrapper(
      Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskIdToTableSegmentMap) {
    this.taskIdToTableSegmentMap = taskIdToTableSegmentMap;
  }

  public Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> getTaskIdToTableSegmentMap() {
    return taskIdToTableSegmentMap;
  }

  public void setTaskIdToTableSegmentMap(
      Map<SegmentTaskIndexStore.TaskBucketHolder, AbstractIndex> taskIdToTableSegmentMap) {
    this.taskIdToTableSegmentMap = taskIdToTableSegmentMap;
  }

  /**
   * return segment size
   *
   * @param memorySize
   */
  public void setMemorySize(long memorySize) {
    this.memorySize.set(memorySize);
  }

  /**
   * returns the timestamp
   *
   * @return
   */
  @Override public long getFileTimeStamp() {
    return 0;
  }

  /**
   * returns the access count
   *
   * @return
   */
  @Override public int getAccessCount() {
    return accessCount.get();
  }

  /**
   * returns the memory size
   *
   * @return
   */
  @Override public long getMemorySize() {
    return memorySize.get();
  }

  /**
   * The method is used to set the access count
   */
  public void incrementAccessCount() {
    accessCount.incrementAndGet();
  }

  /**
   * This method will release the objects and set default value for primitive types
   */
  public void clear() {
    decrementAccessCount();
  }

  /**
   * This method will decrement the access count for a column by 1
   * whenever a column usage is complete
   */
  private void decrementAccessCount() {
    if (accessCount.get() > 0) {
      accessCount.decrementAndGet();
    }
  }

  public Long getRefreshedTimeStamp() {
    return refreshedTimeStamp;
  }

  public void setRefreshedTimeStamp(Long refreshedTimeStamp) {
    this.refreshedTimeStamp = refreshedTimeStamp;
  }

  public void removeEntryFromCacheAndRefresh(String taskId) {
    AbstractIndex blockEntry = this.getTaskIdToTableSegmentMap().remove(taskId);
    if (null != blockEntry) {
      memorySize.set(memorySize.get() - blockEntry.getMemorySize());
    }
  }

  public void setLastUpdateVO(UpdateVO invalidTaskKey) {
    this.invalidTaskKey = invalidTaskKey;
  }

  public UpdateVO getInvalidTaskKey() {
    return invalidTaskKey;
  }

  @Override public void release() {
    // TODO remove resources if any
  }
}
