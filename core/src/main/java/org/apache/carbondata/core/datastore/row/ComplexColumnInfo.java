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

package org.apache.carbondata.core.datastore.row;

import org.apache.carbondata.core.datastore.ColumnType;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Wrapper object to hold the complex column details
 */
public class ComplexColumnInfo {
  private ColumnType complexColumnType;
  private DataType columnDataTypes;
  private String columnNames;
  private boolean isNoDictionary;

  public ComplexColumnInfo(ColumnType complexColumnType, DataType columnDataTypes,
      String columnNames, boolean isNoDictionary) {
    this.complexColumnType = complexColumnType;
    this.columnDataTypes = columnDataTypes;
    this.columnNames = columnNames;
    this.isNoDictionary = isNoDictionary;
  }

  public ColumnType getComplexColumnType() {
    return complexColumnType;
  }

  public DataType getColumnDataTypes() {
    return columnDataTypes;
  }

  public String getColumnNames() {
    return columnNames;
  }

  public boolean isNoDictionary() {
    return isNoDictionary;
  }
}


