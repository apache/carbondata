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

public class AvgDoubleAggregator extends AbstractMeasureAggregatorBasic {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 5463736686281089871L;

  /**
   * total number of aggregate values
   */
  protected double count;

  /**
   * aggregate value
   */
  protected double aggVal;

  /**
   * Average Aggregate function which will add all the aggregate values and it
   * will increment the total count every time, for average value
   *
   * @param newVal new value
   */
  @Override public void agg(double newVal) {
    aggVal += newVal;
    count++;
    firstTime = false;
  }

  /**
   * Average Aggregate function which will add all the aggregate values and it
   * will increment the total count every time, for average value
   *
   * @param newVal new value
   */
  @Override public void agg(Object newVal) {
    if (newVal instanceof byte[]) {
      ByteBuffer buffer = ByteBuffer.wrap((byte[]) newVal);
      buffer.rewind();
      while (buffer.hasRemaining()) {
        aggVal += buffer.getDouble();
        count += buffer.getDouble();
        firstTime = false;
      }
      return;
    }
    aggVal += (Double) newVal;
    count++;
    firstTime = false;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      aggVal += dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index);
      count++;
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
    ByteBuffer buffer = ByteBuffer.allocate(2 * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
    buffer.putDouble(aggVal);
    buffer.putDouble(count);
    return buffer.array();
  }

  /**
   * Return the average of the aggregate values
   *
   * @return average aggregate value
   */
  @Override public Double getDoubleValue() {
    return aggVal / count;
  }

  /**
   * This method merge the aggregated value, in average aggregator it will add
   * count and aggregate value
   *
   * @param aggregator Avg Aggregator
   */
  @Override public void merge(MeasureAggregator aggregator) {
    AvgDoubleAggregator avgAggregator = (AvgDoubleAggregator) aggregator;
    if (!avgAggregator.isFirstTime()) {
      aggVal += avgAggregator.aggVal;
      count += avgAggregator.count;
      firstTime = false;
    }
  }

  /**
   * This method return the average value as an object
   *
   * @return average value as an object
   */
  @Override public Object getValueObject() {
    return aggVal / count;
  }

  /**
   * @see MeasureAggregator#setNewValue(Object)
   */
  @Override public void setNewValue(Object newValue) {
    aggVal = (Double) newValue;
    count = 1;
  }

  @Override public void writeData(DataOutput output) throws IOException {
    output.writeBoolean(firstTime);
    output.writeDouble(aggVal);
    output.writeDouble(count);

  }

  @Override public void readData(DataInput inPut) throws IOException {
    firstTime = inPut.readBoolean();
    aggVal = inPut.readDouble();
    count = inPut.readDouble();
  }

  @Override public MeasureAggregator getCopy() {
    AvgDoubleAggregator avg = new AvgDoubleAggregator();
    avg.aggVal = aggVal;
    avg.count = count;
    avg.firstTime = firstTime;
    return avg;
  }

  @Override public int compareTo(MeasureAggregator o) {
    double val = getDoubleValue();
    double otherVal = o.getDoubleValue();
    if (val > otherVal) {
      return 1;
    }
    if (val < otherVal) {
      return -1;
    }
    return 0;
  }

  @Override public void merge(byte[] value) {
    if (0 == value.length) {
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(value);
    aggVal += buffer.getDouble();
    count += buffer.getDouble();
    firstTime = false;
  }

  public String toString() {
    return (aggVal / count) + "";
  }

  @Override public MeasureAggregator getNew() {
    return new AvgDoubleAggregator();
  }
}
