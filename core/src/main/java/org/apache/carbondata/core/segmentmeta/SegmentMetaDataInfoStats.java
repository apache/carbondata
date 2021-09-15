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

package org.apache.carbondata.core.segmentmeta;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.indexstore.blockletindex.BlockIndex;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Holds segment level meta data information such as min,max, sortColumn info for the
 * corresponding table
 */
public class SegmentMetaDataInfoStats {

  private SegmentMetaDataInfoStats() {
    tableSegmentMetaDataInfoMap = new LinkedHashMap<>();
  }

  public static synchronized SegmentMetaDataInfoStats getInstance() {
    if (null == segmentMetaDataInfoStats) {
      segmentMetaDataInfoStats = new SegmentMetaDataInfoStats();
    }
    return segmentMetaDataInfoStats;
  }

  private Map<String, Map<String, BlockColumnMetaDataInfo>> tableSegmentMetaDataInfoMap;

  private static SegmentMetaDataInfoStats segmentMetaDataInfoStats;

  /**
   * Prepare of map with key as column-id and value as SegmentColumnMetaDataInfo using the
   * tableSegmentMetaDataInfoMap
   *
   * @param tableName get corresponding tableName from map
   * @param segmentId get corresponding segment Id from map
   * @return segmentMetaDataInfo for the corresponding segment
   */
  public SegmentMetaDataInfo getTableSegmentMetaDataInfo(String tableName,
      String segmentId) {
    Map<String, SegmentColumnMetaDataInfo> segmentColumnMetaDataInfoMap = new LinkedHashMap<>();
    Map<String, BlockColumnMetaDataInfo> segmentMetaDataInfoMap =
        this.tableSegmentMetaDataInfoMap.get(tableName);
    if (null != segmentMetaDataInfoMap && !segmentMetaDataInfoMap.isEmpty()
        && null != segmentMetaDataInfoMap.get(segmentId)) {
      BlockColumnMetaDataInfo blockColumnMetaDataInfo = segmentMetaDataInfoMap.get(segmentId);
      for (int i = 0; i < blockColumnMetaDataInfo.getColumnSchemas().size(); i++) {
        org.apache.carbondata.format.ColumnSchema columnSchema =
            blockColumnMetaDataInfo.getColumnSchemas().get(i);
        boolean isSortColumn = false;
        boolean isColumnDrift = false;
        if (null != columnSchema.columnProperties && !columnSchema.columnProperties.isEmpty()) {
          if (null != columnSchema.columnProperties.get(CarbonCommonConstants.SORT_COLUMNS)) {
            isSortColumn = true;
          }
          if (null != columnSchema.columnProperties.get(CarbonCommonConstants.COLUMN_DRIFT)) {
            isColumnDrift = true;
          }
        }
        segmentColumnMetaDataInfoMap.put(columnSchema.column_id,
            new SegmentColumnMetaDataInfo(isSortColumn, blockColumnMetaDataInfo.getMin()[i],
                blockColumnMetaDataInfo.getMax()[i], isColumnDrift));
      }
    }
    return new SegmentMetaDataInfo(segmentColumnMetaDataInfoMap);
  }

  public synchronized void setBlockMetaDataInfo(String tableName, String segmentId,
      BlockColumnMetaDataInfo currentBlockColumnMetaInfo, List<ColumnSchema> columnSchemaList) {
    // check if tableName is present in tableSegmentMetaDataInfoMap
    if (!this.tableSegmentMetaDataInfoMap.isEmpty() && null != this.tableSegmentMetaDataInfoMap
        .get(tableName) && !this.tableSegmentMetaDataInfoMap.get(tableName).isEmpty()
        && null != this.tableSegmentMetaDataInfoMap.get(tableName).get(segmentId)) {
      // get previous blockColumn metadata information
      BlockColumnMetaDataInfo previousBlockColumnMetaInfo =
          this.tableSegmentMetaDataInfoMap.get(tableName).get(segmentId);
      // compare and get updated min and max values
      byte[][] updatedMin = BlockIndex.compareAndUpdateMinMax(previousBlockColumnMetaInfo.getMin(),
          currentBlockColumnMetaInfo.getMin(), true, columnSchemaList);
      byte[][] updatedMax = BlockIndex.compareAndUpdateMinMax(previousBlockColumnMetaInfo.getMax(),
          currentBlockColumnMetaInfo.getMax(), false, columnSchemaList);
      // update the segment
      this.tableSegmentMetaDataInfoMap.get(tableName).get(segmentId)
          .setMinMax(updatedMin, updatedMax);
    } else {
      Map<String, BlockColumnMetaDataInfo> segmentMinMaxMap = new HashMap<>();
      if (!this.tableSegmentMetaDataInfoMap.isEmpty()
          && null != this.tableSegmentMetaDataInfoMap.get(tableName)
          && !this.tableSegmentMetaDataInfoMap.get(tableName).isEmpty()) {
        segmentMinMaxMap = this.tableSegmentMetaDataInfoMap.get(tableName);
      }
      segmentMinMaxMap.put(segmentId, currentBlockColumnMetaInfo);
      this.tableSegmentMetaDataInfoMap.put(tableName, segmentMinMaxMap);
    }
  }

  /**
   * Clear the corresponding segmentId and tableName from the segmentMinMaxMap
   */
  public synchronized void clear(String tableName, String segmentId) {
    if (null != tableSegmentMetaDataInfoMap.get(tableName)) {
      if (null != tableSegmentMetaDataInfoMap.get(tableName).get(segmentId)) {
        tableSegmentMetaDataInfoMap.get(tableName).remove(segmentId);
      }
      if (tableSegmentMetaDataInfoMap.get(tableName).isEmpty()) {
        tableSegmentMetaDataInfoMap.remove(tableName);
      }
    }
  }

  /**
   * This method will do min/max comparison of values and update if required
   */
  public byte[] compareAndUpdateMinMax(byte[] minMaxValueCompare1,
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