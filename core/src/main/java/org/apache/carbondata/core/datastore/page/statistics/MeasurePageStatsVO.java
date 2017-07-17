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

  private DataType[] dataType;
  private byte[] selectedDataType;

  private MeasurePageStatsVO() {
  }

  public MeasurePageStatsVO(ColumnPage[] measurePages) {
    min = new Object[measurePages.length];
    max = new Object[measurePages.length];
    dataType = new DataType[measurePages.length];
    selectedDataType = new byte[measurePages.length];
    for (int i = 0; i < measurePages.length; i++) {
      ColumnPageStatsVO stats = measurePages[i].getStatistics();
      min[i] = stats.getMin();
      max[i] = stats.getMax();
      dataType[i] = measurePages[i].getDataType();
    }
  }

  public static MeasurePageStatsVO build(ValueEncoderMeta[] encoderMetas) {
    Object[] max = new Object[encoderMetas.length];
    Object[] min = new Object[encoderMetas.length];
    DataType[] dataType = new DataType[encoderMetas.length];
    byte[] selectedDataType = new byte[encoderMetas.length];
    for (int i = 0; i < encoderMetas.length; i++) {
      max[i] = encoderMetas[i].getMaxValue();
      min[i] = encoderMetas[i].getMinValue();
      dataType[i] = encoderMetas[i].getType();
      selectedDataType[i] = encoderMetas[i].getDataTypeSelected();
    }

    MeasurePageStatsVO stats = new MeasurePageStatsVO();
    stats.dataType = dataType;
    stats.selectedDataType = selectedDataType;
    stats.min = min;
    stats.max = max;
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

  public byte getDataTypeSelected(int measureIndex) {
    return selectedDataType[measureIndex];
  }
}
