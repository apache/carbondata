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

package org.apache.carbondata.processing.store;

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedData;
import org.apache.carbondata.core.datastore.page.statistics.ColumnPageStatsVO;
import org.apache.carbondata.core.datastore.page.statistics.MeasurePageStatsVO;

// Statistics of dimension and measure column in a TablePage
public class TablePageStatistics {

  // number of dimension after complex column expanded
  private int numDimensionsExpanded;

  // min of each dimension column
  private byte[][] dimensionMinValue;

  // max of each dimension column
  private byte[][] dimensionMaxValue;

  // min of each measure column
  private byte[][] measureMinValue;

  // max os each measure column
  private byte[][] measureMaxValue;

  // null bit set for each measure column
  private BitSet[] nullBitSet;

  // measure stats
  // TODO: there are redundant stats
  private MeasurePageStatsVO measurePageStatistics;

  private TableSpec tableSpec;

  TablePageStatistics(TableSpec tableSpec, TablePage tablePage,
      EncodedData encodedData, MeasurePageStatsVO measurePageStatistics) {
    this.numDimensionsExpanded = tableSpec.getDimensionSpec().getNumExpandedDimensions();
    int numMeasures = tableSpec.getMeasureSpec().getNumMeasures();
    this.dimensionMinValue = new byte[numDimensionsExpanded][];
    this.dimensionMaxValue = new byte[numDimensionsExpanded][];
    this.measureMinValue = new byte[numMeasures][];
    this.measureMaxValue = new byte[numMeasures][];
    this.nullBitSet = new BitSet[numMeasures];
    this.tableSpec = tableSpec;
    this.measurePageStatistics = measurePageStatistics;
    updateMinMax(tablePage, encodedData);
    updateNullBitSet(tablePage);
  }

  private void updateMinMax(TablePage tablePage, EncodedData encodedData) {
    IndexStorage[] keyStorageArray = encodedData.indexStorages;
    byte[][] measureArray = encodedData.measures;

    for (int i = 0; i < numDimensionsExpanded; i++) {
      switch (tableSpec.getDimensionSpec().getType(i)) {
        case GLOBAL_DICTIONARY:
        case DIRECT_DICTIONARY:
        case COLUMN_GROUP:
        case COMPLEX:
          dimensionMinValue[i] = keyStorageArray[i].getMin();
          dimensionMaxValue[i] = keyStorageArray[i].getMax();
          break;
        case PLAIN_VALUE:
          dimensionMinValue[i] = updateMinMaxForNoDictionary(keyStorageArray[i].getMin());
          dimensionMaxValue[i] = updateMinMaxForNoDictionary(keyStorageArray[i].getMax());
          break;
      }
    }
    for (int i = 0; i < measureArray.length; i++) {
      ColumnPageStatsVO stats = tablePage.getMeasurePage()[i].getStatistics();
      measureMaxValue[i] = stats.minBytes();
      measureMinValue[i] = stats.maxBytes();
    }
  }

  private void updateNullBitSet(TablePage tablePage) {
    nullBitSet = new BitSet[tablePage.getMeasurePage().length];
    ColumnPage[] measurePages = tablePage.getMeasurePage();
    for (int i = 0; i < nullBitSet.length; i++) {
      nullBitSet[i] = measurePages[i].getNullBitSet();
    }
  }

  /**
   * Below method will be used to update the min or max value
   * by removing the length from it
   *
   * @return min max value without length
   */
  private byte[] updateMinMaxForNoDictionary(byte[] valueWithLength) {
    ByteBuffer buffer = ByteBuffer.wrap(valueWithLength);
    byte[] actualValue = new byte[buffer.getShort()];
    buffer.get(actualValue);
    return actualValue;
  }

  public byte[][] getDimensionMinValue() {
    return dimensionMinValue;
  }

  public byte[][] getDimensionMaxValue() {
    return dimensionMaxValue;
  }

  public byte[][] getMeasureMinValue() {
    return measureMinValue;
  }

  public byte[][] getMeasureMaxValue() {
    return measureMaxValue;
  }

  public BitSet[] getNullBitSet() {
    return nullBitSet;
  }

  public MeasurePageStatsVO getMeasurePageStatistics() {
    return measurePageStatistics;
  }
}
