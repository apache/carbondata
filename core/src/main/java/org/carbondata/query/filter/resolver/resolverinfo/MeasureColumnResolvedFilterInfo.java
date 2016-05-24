/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.query.filter.resolver.resolverinfo;

import java.io.Serializable;

public class MeasureColumnResolvedFilterInfo implements Serializable {
  /**
   *
   */
  private static final long serialVersionUID = 4222568289115151561L;

  private int columnIndex = -1;

  private int rowIndex = -1;

  private boolean isCustomMeasureValue;

  private Object uniqueValue;

  private String aggregator;

  private boolean isMeasureExistsInCurrentSlice = true;

  private Object defaultValue;

  private org.carbondata.core.carbon.metadata.datatype.DataType type;

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

  public boolean isCustomMeasureValue() {
    return isCustomMeasureValue;
  }

  public void setCustomMeasureValue(boolean isCustomMeasureValue) {
    this.isCustomMeasureValue = isCustomMeasureValue;
  }

  public Object getUniqueValue() {
    return uniqueValue;
  }

  public void setUniqueValue(Object uniqueValue) {
    this.uniqueValue = uniqueValue;
  }

  public org.carbondata.core.carbon.metadata.datatype.DataType getType() {
    return type;
  }

  public void setType(org.carbondata.core.carbon.metadata.datatype.DataType dataType) {
    this.type = dataType;
  }

  /**
   * @return Returns the aggregator.
   */
  public String getAggregator() {
    return aggregator;
  }

  /**
   * @param aggregator The aggregator to set.
   */
  public void setAggregator(String aggregator) {
    this.aggregator = aggregator;
  }

  public boolean isMeasureExistsInCurrentSlice() {
    return isMeasureExistsInCurrentSlice;
  }

  public void setMeasureExistsInCurrentSlice(boolean isMeasureExistsInCurrentSlice) {
    this.isMeasureExistsInCurrentSlice = isMeasureExistsInCurrentSlice;
  }

  public Object getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultValue(double defaultValue) {
    this.defaultValue = defaultValue;
  }
}
