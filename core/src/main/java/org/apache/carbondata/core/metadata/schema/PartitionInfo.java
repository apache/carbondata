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

package org.apache.carbondata.core.metadata.schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Partition information of carbon partition table
 */
public class PartitionInfo implements Serializable {

  private static final long serialVersionUID = -0L;

  private List<ColumnSchema> columnSchemaList;

  private PartitionType partitionType;

  /**
   * range information defined for range partition table
   */
  private List<String> rangeInfo;

  /**
   * value list defined for list partition table
   */
  private List<List<String>> listInfo;

  /**
   * total count of partitions
   */
  private int numPartitions;

  /**
   * current max partition id, increase only, will be used in alter table partition operation
   */
  private int MAX_PARTITION;

  /**
   * record the partitionId in the logical ascending order
   * initiate when table created and changed when alter table
   */
  private List<Integer> partitionIds;

  public PartitionInfo(List<ColumnSchema> columnSchemaList, PartitionType partitionType) {
    this.columnSchemaList = columnSchemaList;
    this.partitionType = partitionType;
    this.partitionIds = new ArrayList<>();
  }

  public List<ColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

  public void setColumnSchemaList(List<ColumnSchema> columnSchemaList) {
    this.columnSchemaList = columnSchemaList;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public void setRangeInfo(List<String> rangeInfo) {
    this.rangeInfo = rangeInfo;
  }

  public List<String> getRangeInfo() {
    return rangeInfo;
  }

  public void setListInfo(List<List<String>> listInfo) {
    this.listInfo = listInfo;
  }

  public List<List<String>> getListInfo() {
    return listInfo;
  }

  public void initialize(int partitionNum) {
    for (int i = 0; i < partitionNum; i++) {
      partitionIds.add(i);
    }
    MAX_PARTITION = partitionNum - 1;
    numPartitions = partitionNum;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getMAX_PARTITION() {
    return MAX_PARTITION;
  }

  public void setMAX_PARTITION(int max_partition) {
    this.MAX_PARTITION = max_partition;
  }

  public List<Integer> getPartitionIds() {
    return partitionIds;
  }

  public void setPartitionIds(List<Integer> partitionIdList) {
    this.partitionIds = partitionIdList;
  }

  public int getPartitionId(int index) {
    return partitionIds.get(index);
  }

}
