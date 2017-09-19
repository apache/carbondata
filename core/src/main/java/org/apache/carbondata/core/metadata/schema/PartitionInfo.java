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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.partition.PartitionType;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Partition information of carbon partition table
 */
public class PartitionInfo implements Serializable, Writable {

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
  private int maxPartitionId;

  /**
   * record the partitionId in the logical ascending order
   * initiate when table created and changed when alter table
   */
  private List<Integer> partitionIds;

  public PartitionInfo() {

  }

  public PartitionInfo(List<ColumnSchema> columnSchemaList, PartitionType partitionType) {
    this.columnSchemaList = columnSchemaList;
    this.partitionType = partitionType;
    this.partitionIds = new ArrayList<>();
  }

  /**
   * add partition means split default partition, add in last directly
   */
  public void  addPartition(int addPartitionCount) {
    for (int i = 0; i < addPartitionCount; i++) {
      partitionIds.add(++maxPartitionId);
      numPartitions++;
    }
  }

  /**
   * e.g. original partition[0,1,2,3,4,5]
   * split partition 2 to partition 6,7,8 (will not reuse 2)
   * then sourcePartitionId is 2, newPartitionNumbers is 3
   * @param sourcePartitionIndex
   * @param newPartitionNumbers
   */
  public void splitPartition(int sourcePartitionIndex, int newPartitionNumbers) {
    partitionIds.remove(sourcePartitionIndex);
    for (int i = 0; i < newPartitionNumbers; i++) {
      partitionIds.add(sourcePartitionIndex + i, ++maxPartitionId);
    }
    numPartitions = numPartitions - 1 + newPartitionNumbers;
  }

  public void dropPartition(int index) {
    partitionIds.remove(index);
    numPartitions--;
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
    maxPartitionId = partitionNum - 1;
    numPartitions = partitionNum;
  }

  public void setNumPartitions(int numPartitions) {
    this.numPartitions = numPartitions;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public int getMaxPartitionId() {
    return maxPartitionId;
  }

  public void setMaxPartitionId(int maxPartitionId) {
    this.maxPartitionId = maxPartitionId;
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

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(columnSchemaList.size());
    for (ColumnSchema columnSchema: columnSchemaList) {
      columnSchema.write(output);
    }
    output.writeInt(partitionType.ordinal());
    if (PartitionType.RANGE.equals(partitionType)) {
      output.writeInt(rangeInfo.size());
      for (String value: rangeInfo) {
        output.writeUTF(value);
      }
    }
    output.writeInt(partitionIds.size());
    for (Integer value: partitionIds) {
      output.writeInt(value);
    }
    if (PartitionType.LIST.equals(partitionType)) {
      output.writeInt(listInfo.size());
      for (List<String> listValue: listInfo) {
        output.writeInt(listValue.size());
        for (String value: listValue) {
          output.writeUTF(value);
        }
      }
    }
    output.writeInt(numPartitions);
    output.writeInt(maxPartitionId);
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int colSchemaSize = input.readInt();
    this.columnSchemaList = new ArrayList<>(colSchemaSize);
    for (int i = 0;i < colSchemaSize; i++) {
      ColumnSchema colSchema = new ColumnSchema();
      colSchema.readFields(input);
      this.columnSchemaList.add(colSchema);
    }
    this.partitionType = PartitionType.values()[input.readInt()];
    if (PartitionType.RANGE.equals(this.partitionType)) {
      int rangeSize = input.readInt();
      this.rangeInfo = new ArrayList<>(rangeSize);
      for (int i = 0; i < rangeSize; i++) {
        rangeInfo.add(input.readUTF());
      }
    }
    int partitionIdSize = input.readInt();
    partitionIds = new ArrayList<>(partitionIdSize);
    for (int i = 0; i < partitionIdSize; i++) {
      partitionIds.add(input.readInt());
    }
    if (PartitionType.LIST.equals(partitionType)) {
      int listInfoSize = input.readInt();
      int aListSize;
      this.listInfo = new ArrayList<>(listInfoSize);
      for (int i = 0; i < listInfoSize; i++) {
        aListSize = input.readInt();
        List<String> aList = new ArrayList<>(aListSize);
        for (int j = 0; j < aListSize; j++) {
          aList.add(input.readUTF());
        }
        this.listInfo.add(aList);
      }
    }

    numPartitions = input.readInt();
    maxPartitionId = input.readInt();
  }
}
