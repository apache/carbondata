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

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * Bucketing information
 */
@InterfaceAudience.Internal
public class BucketingInfo implements ColumnRangeInfo, Serializable, Writable {
  private static final long serialVersionUID = -0L;
  private List<ColumnSchema> listOfColumns;
  // number of value ranges
  private int numOfRanges;

  public BucketingInfo() {

  }

  public BucketingInfo(List<ColumnSchema> listOfColumns, int numberOfRanges) {
    this.listOfColumns = listOfColumns;
    this.numOfRanges = numberOfRanges;
  }

  public List<ColumnSchema> getListOfColumns() {
    return listOfColumns;
  }

  @Override
  public int getNumOfRanges() {
    return numOfRanges;
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(numOfRanges);
    output.writeInt(listOfColumns.size());
    for (ColumnSchema aColSchema : listOfColumns) {
      aColSchema.write(output);
    }
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    this.numOfRanges = input.readInt();
    int colSchemaSize = input.readInt();
    this.listOfColumns = new ArrayList<>(colSchemaSize);
    for (int i = 0; i < colSchemaSize; i++) {
      ColumnSchema aSchema = new ColumnSchema();
      aSchema.readFields(input);
      this.listOfColumns.add(aSchema);
    }
  }
}
