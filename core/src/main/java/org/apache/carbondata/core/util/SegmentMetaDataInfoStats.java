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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds segment level meta data information such as min,max, sortColumn info for the
 * corresponding table
 */
public class SegmentMetaDataInfoStats {

  private SegmentMetaDataInfoStats() {
    tableSegmentMetaDataInfo = new ConcurrentHashMap<>();
  }

  public static SegmentMetaDataInfoStats getInstance() {
    if (null == segmentMetaDataInfoStats) {
      segmentMetaDataInfoStats = new SegmentMetaDataInfoStats();
      return segmentMetaDataInfoStats;
    } else {
      return segmentMetaDataInfoStats;
    }
  }

  private Map<String, Map<String, SegmentMetaDataInfo>> tableSegmentMetaDataInfo;

  private static SegmentMetaDataInfoStats segmentMetaDataInfoStats;

  public Map<String, Map<String, SegmentMetaDataInfo>> getTableSegmentMetaDataInfo() {
    return tableSegmentMetaDataInfo;
  }

  public synchronized void setBlockMetaDataInfo(String tableName, String segmentId,
      Map<String, BlockColumnMetaDataInfo> currentBlockColumnMetaInfo) {
    // check if tableName is present in segmentMinMaxMap
    if (!this.tableSegmentMetaDataInfo.isEmpty() && null != this.tableSegmentMetaDataInfo
        .get(tableName) && !this.tableSegmentMetaDataInfo.get(tableName).isEmpty()
        && null != this.tableSegmentMetaDataInfo.get(tableName).get(segmentId)) {
      Map<String, BlockColumnMetaDataInfo> previousBlockColumnMetaInfo =
          this.tableSegmentMetaDataInfo.get(tableName).get(segmentId).getSegmentMetaDataInfo();
      for (Map.Entry<String, BlockColumnMetaDataInfo> entry : previousBlockColumnMetaInfo
          .entrySet()) {
        if (currentBlockColumnMetaInfo.containsKey(entry.getKey())) {
          BlockColumnMetaDataInfo currentBlockMinMaxInfo =
              currentBlockColumnMetaInfo.get(entry.getKey());
          byte[] blockMaxValue = compareAndUpdateMinMax(currentBlockMinMaxInfo.getBlockMaxValue(),
              entry.getValue().getBlockMaxValue(), false);
          byte[] blockMinValue = compareAndUpdateMinMax(currentBlockMinMaxInfo.getBlockMinValue(),
              entry.getValue().getBlockMinValue(), true);
          currentBlockColumnMetaInfo.get(entry.getKey()).setBlockMaxValue(blockMaxValue);
          currentBlockColumnMetaInfo.get(entry.getKey()).setBlockMinValue(blockMinValue);
        }
      }
      this.tableSegmentMetaDataInfo.get(tableName).get(segmentId)
          .setSegmentMetaDataInfo(currentBlockColumnMetaInfo);
    } else {
      Map<String, SegmentMetaDataInfo> segmentMinMaxMap = new HashMap<>();
      if (null != this.tableSegmentMetaDataInfo.get(tableName) && !this.tableSegmentMetaDataInfo
          .get(tableName).isEmpty()) {
        segmentMinMaxMap = this.tableSegmentMetaDataInfo.get(tableName);
      }
      segmentMinMaxMap.put(segmentId, new SegmentMetaDataInfo(currentBlockColumnMetaInfo));
      this.tableSegmentMetaDataInfo.put(tableName, segmentMinMaxMap);
    }
  }

  /**
   * Clear the corresponding segmentId and tableName from the segmentMinMaxMap
   */
  public void clear(String tableName, String segmentId) {
    if (null != tableSegmentMetaDataInfo.get(tableName)) {
      if (null != tableSegmentMetaDataInfo.get(tableName).get(segmentId)) {
        tableSegmentMetaDataInfo.get(tableName).remove(segmentId);
      }
      if (tableSegmentMetaDataInfo.get(tableName).isEmpty()) {
        tableSegmentMetaDataInfo.remove(tableName);
      }
    }
  }

  /**
   * This method will do min/max comparison of values and update if required
   */
  public synchronized byte[] compareAndUpdateMinMax(byte[] minMaxValueCompare1,
      byte[] minMaxValueCompare2, boolean isMinValueComparison) {
    // Compare and update min max values
    byte[] updatedMinMaxValues = new byte[minMaxValueCompare1.length];
    System.arraycopy(minMaxValueCompare1, 0, updatedMinMaxValues, 0, minMaxValueCompare1.length);
    int compare =
        ByteUtil.UnsafeComparer.INSTANCE.compareTo(minMaxValueCompare2, minMaxValueCompare1);
    if (isMinValueComparison) {
      if (compare < 0) {
        updatedMinMaxValues = minMaxValueCompare2;
      }
    } else if (compare > 0) {
      updatedMinMaxValues = minMaxValueCompare2;
    }
    return updatedMinMaxValues;
  }

}