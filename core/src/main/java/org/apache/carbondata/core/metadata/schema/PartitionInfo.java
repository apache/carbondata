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
   * number of hash partitions
   */
  private int hashNumber;

  /**
   * total count of partitions
   */
  private int numberOfPartitions;

  /**
   * current max partition id, increase only, will be used in alter table partition operation
   */
  private int MAX_PARTITION;

  /**
   * record the partitionId in the logical ascending order
   * initiate when table created and changed when alter table
   */
  private List<Integer> partitionIdList;

  public PartitionInfo(List<ColumnSchema> columnSchemaList, PartitionType partitionType) {
    this.columnSchemaList = columnSchemaList;
    this.partitionType = partitionType;
    this.partitionIdList = new ArrayList<>();
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

  public void setHashNumber(int numPartitions) {
    this.hashNumber = numPartitions;
  }

  public int getHashNumber() {
    return hashNumber;
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
      partitionIdList.add(i);
    }
    MAX_PARTITION = partitionNum - 1;
    numberOfPartitions = partitionNum;
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public int getMAX_PARTITION() {
    return MAX_PARTITION;
  }

  public void setMAX_PARTITION(int max_partition) {
    this.MAX_PARTITION = max_partition;
  }

  public List<Integer> getPartitionIdList() {
    return partitionIdList;
  }

  public void setPartitionIdList(List<Integer> partitionIdList) {
    this.partitionIdList = partitionIdList;
  }

  public void setNumberOfPartitions(int numberOfPartitions) {
    this.numberOfPartitions = numberOfPartitions;
  }

  public int getPartitionId(int index) {
    return partitionIdList.get(index);
  }

}
