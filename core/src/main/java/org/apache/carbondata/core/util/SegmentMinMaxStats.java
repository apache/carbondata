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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Holds list of block level min max of each segment for the corresponding table
 */
public class SegmentMinMaxStats {

  private SegmentMinMaxStats() {
    tableSegmentMinMaxMap = new ConcurrentHashMap<>();
  }

  public static SegmentMinMaxStats getInstance() {
    if (null == segmentMinMaxStats) {
      segmentMinMaxStats = new SegmentMinMaxStats();
      return segmentMinMaxStats;
    } else {
      return segmentMinMaxStats;
    }
  }

  private Map<String, Map<String, List<SegmentMinMax>>> tableSegmentMinMaxMap;

  private static SegmentMinMaxStats segmentMinMaxStats;

  public Map<String, Map<String, List<SegmentMinMax>>> getTableSegmentMinMaxMap() {
    return tableSegmentMinMaxMap;
  }

  public synchronized void setSegmentMinMaxList(String tableName, String segmentId,
      Map<String, SegmentBlockMinMaxInfo> segmentBlockMinMaxInfo) {
    // check if tableName is present in segmentMinMaxMap
    if (!this.tableSegmentMinMaxMap.isEmpty() && null != this.tableSegmentMinMaxMap.get(tableName)
        && !this.tableSegmentMinMaxMap.get(tableName).isEmpty()
        && null != this.tableSegmentMinMaxMap.get(tableName).get(segmentId)) {
      this.tableSegmentMinMaxMap.get(tableName).get(segmentId)
          .add(new SegmentMinMax(segmentBlockMinMaxInfo));
    } else {
      addSegmentEntryToMap(tableName, segmentId, segmentBlockMinMaxInfo);
    }
  }

  /**
   * Add's a list of block level minMax info of a segment to the
   * corresponding table name
   */
  private void addSegmentEntryToMap(String tableName, String segmentId,
      Map<String, SegmentBlockMinMaxInfo> segmentBlockMinMaxInfo) {
    Map<String, List<SegmentMinMax>> segmentMinMaxMap = new HashMap<>();
    if (null != this.tableSegmentMinMaxMap.get(tableName) && !this.tableSegmentMinMaxMap
        .get(tableName).isEmpty()) {
      segmentMinMaxMap = this.tableSegmentMinMaxMap.get(tableName);
    }
    List<SegmentMinMax> blockMinMaxList = new ArrayList<>();
    blockMinMaxList.add(new SegmentMinMax(segmentBlockMinMaxInfo));
    segmentMinMaxMap.put(segmentId, blockMinMaxList);
    this.tableSegmentMinMaxMap.put(tableName, segmentMinMaxMap);
  }

  /**
   * Clear the corresponding segmentId and tableName from the segmentMinMaxMap
   */
  public void clear(String tableName, String segmentId) {
    if (null != tableSegmentMinMaxMap.get(tableName)) {
      if (null != tableSegmentMinMaxMap.get(tableName).get(segmentId)) {
        tableSegmentMinMaxMap.get(tableName).remove(segmentId);
      }
      if (tableSegmentMinMaxMap.get(tableName).isEmpty()) {
        tableSegmentMinMaxMap.remove(tableName);
      }
    }
  }

}