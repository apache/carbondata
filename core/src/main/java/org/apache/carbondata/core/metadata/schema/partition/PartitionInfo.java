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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Partition information of carbon partition table
 */
public class PartitionInfo implements Serializable, Writable {

  private static final long serialVersionUID = -0L;

  private List<ColumnSchema> columnSchemaList;

  private PartitionType partitionType;

  public PartitionInfo() {

  }

  public PartitionInfo(List<ColumnSchema> columnSchemaList, PartitionType partitionType) {
    this.columnSchemaList = columnSchemaList;
    this.partitionType = partitionType;
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

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(columnSchemaList.size());
    for (ColumnSchema columnSchema: columnSchemaList) {
      columnSchema.write(output);
    }
    output.writeInt(partitionType.ordinal());
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    int colSchemaSize = input.readInt();
    this.columnSchemaList = new ArrayList<>(colSchemaSize);
    for (int i = 0; i < colSchemaSize; i++) {
      ColumnSchema colSchema = new ColumnSchema();
      colSchema.readFields(input);
      this.columnSchemaList.add(colSchema);
    }
    this.partitionType = PartitionType.values()[input.readInt()];
  }
}
