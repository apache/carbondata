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

package org.apache.carbondata.core.metadata.schema.table;

import org.apache.carbondata.core.metadata.datatype.DataType;

public class PartitionerField {
  private String partitionColumn;
  private DataType dataType;
  private String columnComment;

  public PartitionerField(String partitionColumn, DataType dataType, String columnComment) {
    this.partitionColumn = partitionColumn;
    this.dataType = dataType;
    this.columnComment = columnComment;
  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public DataType getDataType() {
    return dataType;
  }

  public String getColumnComment() {
    return columnComment;
  }
}
