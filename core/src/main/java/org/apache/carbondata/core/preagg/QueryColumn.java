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

package org.apache.carbondata.core.preagg;

import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * column present in query
 */
public class QueryColumn {

  /**
   * parent column schema
   */
  private ColumnSchema columnSchema;

  /**
   * to store the change data type in case of cast
   */
  private String changedDataType;

  /**
   * aggregation function applied
   */
  private String aggFunction;

  /**
   * is filter column
   */
  private boolean isFilterColumn;

  public QueryColumn(ColumnSchema columnSchema, String changedDataType, String aggFunction,
      boolean isFilterColumn) {
    this.columnSchema = columnSchema;
    this.changedDataType = changedDataType;
    this.aggFunction = aggFunction;
    this.isFilterColumn = isFilterColumn;
  }

  public ColumnSchema getColumnSchema() {
    return columnSchema;
  }

  public String getChangedDataType() {
    return changedDataType;
  }

  public String getAggFunction() {
    return aggFunction;
  }

  public boolean isFilterColumn() {
    return isFilterColumn;
  }
}
