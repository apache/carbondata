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
import java.util.List;

import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Partition Information of carbon partition table
 */
public class PartitionInfo implements Serializable {

  /**
   * Partition columns
   */
  private List<ColumnSchema> columnSchemaList;

  /**
   * partition type
   */
  private PartitionType partitionType;

  /**
   * Range Partition definition
   */
  private List<String> rangeInfo;

  /**
   * List Partition definition
   */
  private List<List<String>> listInfo;

  /**
   * Hash Partition numbers
   */
  private int hashNumber;

  /**
   * For range partition table
   * @param columnSchemaList
   * @param partitionType
   */
  public PartitionInfo(List<ColumnSchema> columnSchemaList, PartitionType partitionType) {
    this.columnSchemaList = columnSchemaList;
    this.partitionType = partitionType;
  }

  public List<ColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

  public PartitionType getPartitionType() {
    return partitionType;
  }

  public void setHashNumber(int hashNumber) {
    this.hashNumber = hashNumber;
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

}
