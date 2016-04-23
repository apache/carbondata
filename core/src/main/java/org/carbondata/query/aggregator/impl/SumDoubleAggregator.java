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

package org.carbondata.query.aggregator.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;

public class SumDoubleAggregator extends AbstractMeasureAggregatorBasic {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 623750056131364540L;

  /**
   * aggregate value
   */
  private double aggVal;

  /**
   * This method will update the aggVal it will add new value to aggVal
   *
   * @param newVal new value
   */
  @Override public void agg(double newVal) {
    aggVal += newVal;
    firstTime = false;
  }

  /**
   * This method will update the aggVal it will add new value to aggVal
   *
   * @param newVal new value
   */
  @Override public void agg(Object newVal) {
    aggVal += (Double) newVal;
    firstTime = false;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      aggVal = dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index);
      firstTime = false;
    }
  }

  /**
   * Below method will be used to get the value byte array
   */
  @Override public byte[] getByteArray() {
    if (firstTime) {
      return new byte[0];
    }
    ByteBuffer buffer = ByteBuffer.allocate(CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
    buffer.putDouble(aggVal);
    return buffer.array();
  }

  /**
   * This method will return aggVal
   *
   * @return sum value
   */

  @Override public Double getDoubleValue() {
    return aggVal;
  }

  /* Merge the value, it will update the sum aggregate value it will add new
   * value to aggVal
   *
   * @param aggregator  SumAggregator
   *
   */
  @Override public void merge(MeasureAggregator aggregator) {
    if (!aggregator.isFirstTime()) {
      agg(aggregator.getDoubleValue());
    }
  }

  /**
   * This method return the sum value as an object
   *
   * @return sum value as an object
   */
  @Override public Object getValueObject() {
    return aggVal;
  }

  @Override public void setNewValue(Object newValue) {
    aggVal = (Double) newValue;
  }

  @Override public boolean isFirstTime() {
    return firstTime;
  }

  @Override public void readData(DataInput inPut) throws IOException {
    firstTime = inPut.readBoolean();
    aggVal = inPut.readDouble();
  }

  @Override public void writeData(DataOutput output) throws IOException {
    output.writeBoolean(firstTime);
    output.writeDouble(aggVal);

  }

  @Override public MeasureAggregator getCopy() {
    SumDoubleAggregator aggr = new SumDoubleAggregator();
    aggr.aggVal = aggVal;
    aggr.firstTime = firstTime;
    return aggr;
  }

  @Override public void merge(byte[] value) {
    if (0 == value.length) {
      return;
    }
    aggVal += ByteBuffer.wrap(value).getDouble();
    firstTime = false;
  }

  public String toString() {
    return aggVal + "";
  }

  @Override public int compareTo(MeasureAggregator o) {
    double value = getDoubleValue();
    double otherVal = o.getDoubleValue();
    if (value > otherVal) {
      return 1;
    }
    if (value < otherVal) {
      return -1;
    }
    return 0;
  }

  @Override public MeasureAggregator getNew() {
    return new SumDoubleAggregator();
  }
}
