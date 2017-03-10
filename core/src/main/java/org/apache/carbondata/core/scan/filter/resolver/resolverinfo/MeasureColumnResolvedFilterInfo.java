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

package org.apache.carbondata.core.scan.filter.resolver.resolverinfo;

import java.io.Serializable;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

public class MeasureColumnResolvedFilterInfo implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 4222568289115151561L;

  private int columnIndex = -1;

  private int rowIndex = -1;

  private Object defaultValue;

  private CarbonColumn carbonColumn;

  private org.apache.carbondata.core.metadata.datatype.DataType type;

  public int getColumnIndex() {
    return columnIndex;
  }

  public void setColumnIndex(int columnIndex) {
    this.columnIndex = columnIndex;
  }

  public int getRowIndex() {
    return rowIndex;
  }

  public void setRowIndex(int rowIndex) {
    this.rowIndex = rowIndex;
  }

  public org.apache.carbondata.core.metadata.datatype.DataType getType() {
    return type;
  }

  public void setType(org.apache.carbondata.core.metadata.datatype.DataType dataType) {
    this.type = dataType;
  }

  public boolean isMeasureExistsInCurrentSlice() {
    return true;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public CarbonColumn getCarbonColumn() {
    return carbonColumn;
  }

  public void setCarbonColumn(CarbonColumn carbonColumn) {
    this.carbonColumn = carbonColumn;
  }
}
