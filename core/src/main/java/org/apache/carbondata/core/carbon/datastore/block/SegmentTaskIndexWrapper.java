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

package org.apache.carbondata.core.carbon.datastore.block;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.core.cache.Cacheable;
import org.apache.carbondata.core.carbon.datastore.SegmentTaskIndexStore;

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
  protected long memorySize;

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
    this.memorySize = memorySize;
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
    return memorySize;
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

}
