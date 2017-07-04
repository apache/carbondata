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

import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.columnar.IndexStorage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedDimensionPage;
import org.apache.carbondata.core.datastore.page.encoding.EncodedMeasurePage;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;

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

  public TablePageStatistics(EncodedDimensionPage[] dimensions,
      EncodedMeasurePage[] measures) {
    this.numDimensionsExpanded = dimensions.length;
    int numMeasures = measures.length;
    this.dimensionMinValue = new byte[numDimensionsExpanded][];
    this.dimensionMaxValue = new byte[numDimensionsExpanded][];
    this.measureMinValue = new byte[numMeasures][];
    this.measureMaxValue = new byte[numMeasures][];
    this.nullBitSet = new BitSet[numMeasures];
    updateDimensionMinMax(dimensions);
    updateMeasureMinMax(measures);
  }

  private void updateDimensionMinMax(EncodedDimensionPage[] dimensions) {
    for (int i = 0; i < dimensions.length; i++) {
      IndexStorage keyStorageArray = dimensions[i].getIndexStorage();
      switch (dimensions[i].getDimensionType()) {
        case GLOBAL_DICTIONARY:
        case DIRECT_DICTIONARY:
        case COLUMN_GROUP:
        case COMPLEX:
          dimensionMinValue[i] = keyStorageArray.getMin();
          dimensionMaxValue[i] = keyStorageArray.getMax();
          break;
        case PLAIN_VALUE:
          dimensionMinValue[i] = updateMinMaxForNoDictionary(keyStorageArray.getMin());
          dimensionMaxValue[i] = updateMinMaxForNoDictionary(keyStorageArray.getMax());
          break;
      }
    }
  }

  private void updateMeasureMinMax(EncodedMeasurePage[] measures) {
    for (int i = 0; i < measures.length; i++) {
      ValueEncoderMeta meta = measures[i].getMetaData();
      measureMaxValue[i] = meta.getMaxAsBytes();
      measureMinValue[i] = meta.getMinAsBytes();
      nullBitSet[i] = meta.getNullBitSet();
    }
  }

  /**
   * Below method will be used to update the min or max value
   * by removing the length from it
   *
   * @return min max value without length
   */
  public static byte[] updateMinMaxForNoDictionary(byte[] valueWithLength) {
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
}
