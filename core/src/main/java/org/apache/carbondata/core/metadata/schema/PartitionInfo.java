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

import org.apache.carbondata.core.metadata.schema.partition.SinglePartition;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Partition information
 */
public class PartitionInfo implements Serializable{

  /**
   * Partition numbers
   */
  private int numberOfPartitions;

  /**
   * Partition columns
   */
  private List<ColumnSchema> columnSchemaList;

  /**
   * Partition list
   */
  private List<SinglePartition> partitionList;

  public PartitionInfo(List<SinglePartition> partitionList, List<ColumnSchema> columnSchemaList,
      int numberOfPartitions) {
    this.columnSchemaList = columnSchemaList;
    this.partitionList = partitionList;
    this.numberOfPartitions = numberOfPartitions;
  }

  public List<ColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

  public int getNumberOfPartitions() {
    return  numberOfPartitions;
  }

  public List<SinglePartition> getPartitionList() {
    return partitionList;
  }

}
