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

package org.apache.carbondata.core.datastore.page.statistics;

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class MeasurePageStatsVO {
  // statistics of each measure column
  private Object[] min, max;
  private int[] decimal;

  private DataType[] dataType;
  private byte[] selectedDataType;

  private MeasurePageStatsVO() {
  }

  public static MeasurePageStatsVO build(ColumnPage[] measurePages) {
    MeasurePageStatsVO stats = new MeasurePageStatsVO();
    stats.min = new Object[measurePages.length];
    stats.max = new Object[measurePages.length];
    stats.decimal = new int[measurePages.length];
    stats.dataType = new DataType[measurePages.length];
    stats.selectedDataType = new byte[measurePages.length];
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageStatsVO vo = measurePages[i].getStatistics();
      stats.min[i] = vo.getMin();
      stats.max[i] = vo.getMax();
      stats.decimal[i] = vo.getDecimal();
      stats.dataType[i] = measurePages[i].getDataType();
    }
    return stats;
  }

  public static MeasurePageStatsVO build(ValueEncoderMeta[] encoderMetas) {
    Object[] max = new Object[encoderMetas.length];
    Object[] min = new Object[encoderMetas.length];
    int[] decimal = new int[encoderMetas.length];
    DataType[] dataType = new DataType[encoderMetas.length];
    byte[] selectedDataType = new byte[encoderMetas.length];
    for (int i = 0; i < encoderMetas.length; i++) {
      max[i] = encoderMetas[i].getMaxValue();
      min[i] = encoderMetas[i].getMinValue();
      decimal[i] = encoderMetas[i].getDecimal();
      dataType[i] = encoderMetas[i].getType();
      selectedDataType[i] = encoderMetas[i].getDataTypeSelected();
    }

    MeasurePageStatsVO stats = new MeasurePageStatsVO();
    stats.dataType = dataType;
    stats.selectedDataType = selectedDataType;
    stats.min = min;
    stats.max = max;
    stats.decimal = decimal;
    return stats;
  }

  public DataType getDataType(int measureIndex) {
    return dataType[measureIndex];
  }

  public Object getMin(int measureIndex) {
    return min[measureIndex];
  }

  public Object getMax(int measureIndex) {
    return max[measureIndex];
  }

  public int getDecimal(int measureIndex) {
    return decimal[measureIndex];
  }

  public byte getDataTypeSelected(int measureIndex) {
    return selectedDataType[measureIndex];
  }
}
