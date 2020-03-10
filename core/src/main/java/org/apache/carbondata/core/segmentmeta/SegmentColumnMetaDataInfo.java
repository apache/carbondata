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

package org.apache.carbondata.core.segmentmeta;

import java.io.Serializable;

/**
 * Represent segment level column metadata information
 */
public class SegmentColumnMetaDataInfo implements Serializable {

  /**
   * true if column is a sort column
   */
  private boolean isSortColumn;

  /**
   * segment level  min value for a column
   */
  private byte[] columnMinValue;

  /**
   * segment level max value for a column
   */
  private byte[] columnMaxValue;

  /**
   * true if column measure if changed to dimension
   */
  private boolean isColumnDrift;

  public SegmentColumnMetaDataInfo(boolean isSortColumn, byte[] columnMinValue,
      byte[] columnMaxValue, boolean isColumnDrift) {
    this.isSortColumn = isSortColumn;
    this.columnMinValue = columnMinValue;
    this.columnMaxValue = columnMaxValue;
    this.isColumnDrift = isColumnDrift;
  }

  public boolean isSortColumn() {
    return isSortColumn;
  }

  public byte[] getColumnMinValue() {
    return columnMinValue;
  }

  public byte[] getColumnMaxValue() {
    return columnMaxValue;
  }

  public boolean isColumnDrift() {
    return isColumnDrift;
  }

  public void setColumnMinValue(byte[] columnMinValue) {
    this.columnMinValue = columnMinValue;
  }

  public void setColumnMaxValue(byte[] columnMaxValue) {
    this.columnMaxValue = columnMaxValue;
  }
}

