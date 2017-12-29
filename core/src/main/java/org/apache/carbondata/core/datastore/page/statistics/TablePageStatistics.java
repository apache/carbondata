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

import org.apache.carbondata.core.datastore.page.encoding.EncodedColumnPage;
import org.apache.carbondata.core.util.CarbonUtil;

// Statistics of dimension and measure column in a TablePage
public class TablePageStatistics {

  // min of each dimension column
  private byte[][] dimensionMinValue;

  // max of each dimension column
  private byte[][] dimensionMaxValue;

  // Null values for dimension columns.
  private byte[] dimensionNullValue;

  // min of each measure column
  private byte[][] measureMinValue;

  // max os each measure column
  private byte[][] measureMaxValue;

  // Null Value in Column Page.
  private byte[] measureNullValue;

  public TablePageStatistics(EncodedColumnPage[] dimensions,
      EncodedColumnPage[] measures) {
    int numDimensionsExpanded = dimensions.length;
    int numMeasures = measures.length;
    this.dimensionMinValue = new byte[numDimensionsExpanded][];
    this.dimensionMaxValue = new byte[numDimensionsExpanded][];
    this.dimensionNullValue = new byte[numDimensionsExpanded];
    this.measureMinValue = new byte[numMeasures][];
    this.measureMaxValue = new byte[numMeasures][];
    this.measureNullValue = new byte[numMeasures];
    updateDimensionMinMaxAndNull(dimensions);
    updateMeasureMinMaxAndNull(measures);

  }

  private void updateDimensionMinMaxAndNull(EncodedColumnPage[] dimensions) {
    for (int i = 0; i < dimensions.length; i++) {
      SimpleStatsResult stats = dimensions[i].getStats();
      dimensionMaxValue[i] = CarbonUtil.getValueAsBytes(stats.getDataType(), stats.getMax());
      dimensionMinValue[i] = CarbonUtil.getValueAsBytes(stats.getDataType(), stats.getMin());
      dimensionNullValue[i] = stats.getNull();
    }
  }

  private void updateMeasureMinMaxAndNull(EncodedColumnPage[] measures) {
    for (int i = 0; i < measures.length; i++) {
      SimpleStatsResult stats = measures[i].getStats();
      measureMaxValue[i] = CarbonUtil.getValueAsBytes(stats.getDataType(), stats.getMax());
      measureMinValue[i] = CarbonUtil.getValueAsBytes(stats.getDataType(), stats.getMin());
      measureNullValue[i] = (byte) stats.getNull();
    }
  }

  public byte[][] getDimensionMinValue() {
    return dimensionMinValue;
  }

  public byte[][] getDimensionMaxValue() {
    return dimensionMaxValue;
  }

  public byte[] getDimensionNullValue() {
    return dimensionNullValue;
  }

  public byte[][] getMeasureMinValue() {
    return measureMinValue;
  }

  public byte[][] getMeasureMaxValue() {
    return measureMaxValue;
  }

  public byte[] getMeasureNullValue() {
    return measureNullValue;
  }
}
