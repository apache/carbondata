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
package org.apache.carbondata.core.metadata.schema.partition;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

public class SinglePartition implements Serializable {

  /**
   * partition id
   */
  private int partition_id;

  /**
   * partition name
   */
  private String partition_name;

  /**
   * Partition columns
   */
  private List<ColumnSchema> columnSchemaList;

  /**
   * partition type
   */
  private List<Partitioning> partitioning_list;

  /**
   * boundary value list for multi-level partition
   */
  private List<String> boundary_value_list;

  /**
   *
   * @param columnSchemaList
   * @param partitioning_list
   * @param value_list
   */
  public SinglePartition(List<ColumnSchema> columnSchemaList, List<Partitioning> partitioning_list,
      List<String> value_list) {
    this.columnSchemaList = columnSchemaList;
    this.partitioning_list = partitioning_list;
    this.boundary_value_list = value_list;
  }

  /**
   *
   * @param partition_name
   * @param columnSchemaList
   * @param partitioning_list
   * @param value_list
   */
  public SinglePartition(String partition_name, List<ColumnSchema> columnSchemaList,
      List<Partitioning> partitioning_list,
      List<String> value_list) {
    this(columnSchemaList, partitioning_list, value_list);
    this.partition_name = partition_name;
  }

  /**
   * @param id
   */
  public void setPartitionId(int id) {
    this.partition_id = id;
  }

  public int getPartition_id() {
    return partition_id;
  }

  /**
   * @param value_list
   */
  public void setBoundaryValue(List<String> value_list) {
    this.boundary_value_list = value_list;
  }

  /**
   * @return boundary_value_list
   */
  public List<String> getBoundaryValue() {
    return boundary_value_list;
  }

  /**
   * @param partitionName
   */
  public void setPartitionName(String partitionName) {
    this.partition_name = partitionName;
  }

  /**
   * set partition name
   * @param id
   */
  public void setPartitionNameById(int id) {
    this.partition_name = CarbonCommonConstants.CARBON_PARTITION_NAME_PREFIX + id;
  }

  /**
   * @param id
   * @return
   */
  public String getPartitionNameById(int id) {
    this.partition_name = CarbonCommonConstants.CARBON_PARTITION_NAME_PREFIX + id;
    return partition_name;
  }
}
