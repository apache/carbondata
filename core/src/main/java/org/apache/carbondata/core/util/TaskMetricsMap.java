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

package org.apache.carbondata.core.util;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.carbondata.common.logging.LogServiceFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;

/**
 * This class maintains task level metrics info for all spawned child threads and parent task thread
 */
public class TaskMetricsMap {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(TaskMetricsMap.class.getName());

  private static final InheritableThreadLocal<Long> threadLocal = new InheritableThreadLocal<>();
  /**
   * In this map we are maintaining all spawned child threads callback info for each parent thread
   * here key = parent thread id & values =  list of spawned child threads callbacks
   */
  public static final Map<Long, List<CarbonFSBytesReadOnThreadCallback>> metricMap =
      new ConcurrentHashMap<>();

  public static final TaskMetricsMap taskMetricsMap = new TaskMetricsMap();

  public static TaskMetricsMap getInstance() {
    return taskMetricsMap;
  }

  public static InheritableThreadLocal<Long> getThreadLocal() {
    return threadLocal;
  }

  /**
   * initializes thread local to current thread id
   *
   * @return
   */
  public static void initializeThreadLocal() {
    // In case of multi level RDD (say insert into scenario, where DataFrameRDD calling ScanRDD)
    // parent thread id should not be overwritten by child thread id.
    // so don't set if it is already set.
    if (threadLocal.get() == null) {
      threadLocal.set(Thread.currentThread().getId());
    }
  }

  /**
   * registers current thread callback using parent thread id
   *
   * @return
   */
  public void registerThreadCallback() {
    // parent thread id should not be null as we are setting the same for all RDDs
    if (null != threadLocal.get()) {
      long parentThreadId = threadLocal.get();
      new CarbonFSBytesReadOnThreadCallback(parentThreadId);
    }
  }

  /**
   * removes parent thread entry from map.
   *
   * @param threadId
   */
  public void removeEntry(long threadId) {
    metricMap.remove(threadId);
  }

  /**
   * returns all spawned child threads callback list of given parent thread
   *
   * @param threadId
   * @return
   */
  public List<CarbonFSBytesReadOnThreadCallback> getCallbackList(long threadId) {
    return metricMap.get(threadId);
  }

  public boolean isCallbackEmpty(long threadId) {
    List<CarbonFSBytesReadOnThreadCallback> callbackList = getCallbackList(threadId);
    if (null == callbackList) {
      return true;
    }
    return callbackList.isEmpty();
  }

  /**
   * This function updates read bytes of given thread
   * After completing the task, each spawned child thread should update current read bytes,
   * by calling this function.
   *
   * @param callbackThreadId
   */
  public void updateReadBytes(long callbackThreadId) {
    // parent thread id should not be null as we are setting the same for all RDDs
    if (null != threadLocal.get()) {
      long parentThreadId = threadLocal.get();
      List<CarbonFSBytesReadOnThreadCallback> callbackList = getCallbackList(parentThreadId);
      if (null != callbackList) {
        for (CarbonFSBytesReadOnThreadCallback callback : callbackList) {
          if (callback.threadId == callbackThreadId) {
            callback.updatedReadBytes += callback.readBytes();
            break;
          }
        }
      }
    }
  }

  /**
   * returns total task read bytes, by summing all parent & spawned threads read bytes
   *
   * @param threadName
   * @return
   */
  public long getReadBytesSum(long threadName) {
    List<CarbonFSBytesReadOnThreadCallback> callbacks = getCallbackList(threadName);
    long sum = 0;
    if (null != callbacks) {
      for (CarbonFSBytesReadOnThreadCallback callback : callbacks) {
        sum += callback.getReadBytes();
      }
    }
    return sum;
  }

  /**
   * adds spawned thread callback entry in metric map using parentThreadId
   *
   * @param parentThreadId
   * @param callback
   */
  private void addEntry(long parentThreadId, CarbonFSBytesReadOnThreadCallback callback) {
    List<CarbonFSBytesReadOnThreadCallback> callbackList = getCallbackList(parentThreadId);
    if (null == callbackList) {
      //create new list
      List<CarbonFSBytesReadOnThreadCallback> list = new CopyOnWriteArrayList<>();
      list.add(callback);
      metricMap.put(parentThreadId, list);
    } else {
      // add to existing list
      callbackList.add(callback);
    }
  }

  /**
   * This class maintains getReadBytes info of each thread
   */
  class CarbonFSBytesReadOnThreadCallback {
    long baseline = 0;
    long updatedReadBytes = 0;
    long threadId = Thread.currentThread().getId();

    CarbonFSBytesReadOnThreadCallback(long parentThread) {
      // reads current thread readBytes
      this.baseline = readBytes();
      addEntry(parentThread, this);
    }

    /**
     * returns current thread read bytes from FileSystem Statistics
     *
     * @return
     */
    public long readBytes() {
      List<FileSystem.Statistics> statisticsList = FileSystem.getAllStatistics();
      long sum = 0;
      try {
        for (FileSystem.Statistics statistics : statisticsList) {
          Class statisticsClass = Class.forName(statistics.getClass().getName());
          Method getThreadStatisticsMethod =
              statisticsClass.getDeclaredMethod("getThreadStatistics");
          Class statisticsDataClass =
              Class.forName("org.apache.hadoop.fs.FileSystem$Statistics$StatisticsData");
          Method getBytesReadMethod = statisticsDataClass.getDeclaredMethod("getBytesRead");
          sum += (Long) getBytesReadMethod
              .invoke(statisticsDataClass.cast(getThreadStatisticsMethod.invoke(statistics, null)),
                  null);
        }
      } catch (Exception ex) {
        LOGGER.debug(ex.getLocalizedMessage());
      }
      return sum;
    }

    /**
     * After completing task, each child thread should update corresponding
     * read bytes using updatedReadBytes method.
     * if updatedReadBytes > 0 then return updatedReadBytes (i.e thread read bytes).
     *
     * @return
     */
    public long getReadBytes() {
      return updatedReadBytes - baseline;
    }
  }
}
