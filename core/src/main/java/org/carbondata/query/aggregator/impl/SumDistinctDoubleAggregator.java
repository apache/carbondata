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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * The sum distinct aggregator
 * Ex:
 * ID NAME Sales
 * 1 a 200
 * 2 a 100
 * 3 a 200
 * select sum(distinct sales) # would result 300
 */

public class SumDistinctDoubleAggregator extends AbstractMeasureAggregatorBasic {

  /**
   *
   */
  private static final long serialVersionUID = 6313463368629960155L;

  /**
   * For Spark CARBON to avoid heavy object transfer it better to flatten the
   * Aggregators. There is no aggregation expected after setting this value.
   */
  private Double computedFixedValue;

  /**
   *
   */
  private Set<Double> valueSet;

  public SumDistinctDoubleAggregator() {
    valueSet = new HashSet<Double>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * just need to add the unique values to agg set
   */
  @Override public void agg(double newVal) {
    valueSet.add(newVal);
    firstTime = false;
  }

  /**
   * Distinct Aggregate function which update the Distinct set
   *
   * @param newVal new value
   */
  @Override public void agg(Object newVal) {
    valueSet.add(newVal instanceof Double ? (Double) newVal : new Double(newVal.toString()));
    firstTime = false;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      valueSet.add(dataChunk.getMeasureDataHolder().getReadableDoubleValueByIndex(index));
      firstTime = false;
    }
  }

  /**
   * Below method will be used to get the value byte array
   */
  @Override public byte[] getByteArray() {
    Iterator<Double> iterator = valueSet.iterator();
    ByteBuffer buffer =
        ByteBuffer.allocate(valueSet.size() * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
    while (iterator.hasNext()) {
      buffer.putDouble(iterator.next());
    }
    buffer.rewind();
    return buffer.array();
  }

  private void agg(Set<Double> set2) {
    valueSet.addAll(set2);
  }

  /**
   * merge the valueset so that we get the count of unique values
   */
  @Override public void merge(MeasureAggregator aggregator) {
    SumDistinctDoubleAggregator distinctAggregator = (SumDistinctDoubleAggregator) aggregator;
    if (!aggregator.isFirstTime()) {
      agg(distinctAggregator.valueSet);
      firstTime = false;
    }
  }

  @Override public Double getDoubleValue() {
    if (computedFixedValue == null) {
      double result = 0;
      for (Double aValue : valueSet) {
        result += aValue;
      }
      return result;
    }
    return computedFixedValue;
  }

  @Override public Object getValueObject() {
    return getDoubleValue();
  }

  @Override public void setNewValue(Object newValue) {
    computedFixedValue = (Double) newValue;
    valueSet = null;
  }

  @Override public boolean isFirstTime() {
    return firstTime;
  }

  @Override public void writeData(DataOutput dataOutput) throws IOException {
    if (computedFixedValue != null) {
      ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 8);
      byteBuffer.putInt(-1);
      byteBuffer.putDouble(computedFixedValue);
      byteBuffer.flip();
      dataOutput.write(byteBuffer.array());
    } else {
      int length = valueSet.size() * 8;
      ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4 + 1);
      byteBuffer.putInt(length);
      for (double val : valueSet) {
        byteBuffer.putDouble(val);
      }
      byteBuffer.flip();
      dataOutput.write(byteBuffer.array());
    }
  }

  @Override public void readData(DataInput inPut) throws IOException {
    int length = inPut.readInt();

    if (length == -1) {
      computedFixedValue = inPut.readDouble();
      valueSet = null;
    } else {
      length = length / 8;
      valueSet = new HashSet<Double>(length + 1, 1.0f);
      for (int i = 0; i < length; i++) {
        valueSet.add(inPut.readDouble());
      }
    }
  }

  @Override public void merge(byte[] value) {
    if (0 == value.length) {
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(value);
    buffer.rewind();
    while (buffer.hasRemaining()) {
      agg(buffer.getDouble());
    }
  }

  public String toString() {
    if (computedFixedValue == null) {
      return valueSet.size() + "";
    }
    return computedFixedValue + "";
  }

  @Override public MeasureAggregator getCopy() {
    SumDistinctDoubleAggregator aggregator = new SumDistinctDoubleAggregator();
    aggregator.valueSet = new HashSet<Double>(valueSet);
    return aggregator;
  }

  @Override public int compareTo(MeasureAggregator msr) {
    double msrValObj = getDoubleValue();
    double otherVal = msr.getDoubleValue();
    if (msrValObj > otherVal) {
      return 1;
    }
    if (msrValObj < otherVal) {
      return -1;
    }
    return 0;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof SumDistinctDoubleAggregator)) {
      return false;
    }
    SumDistinctDoubleAggregator o = (SumDistinctDoubleAggregator) obj;
    return getDoubleValue().equals(o.getDoubleValue());
  }

  @Override public int hashCode() {
    return getDoubleValue().hashCode();
  }

  @Override public MeasureAggregator getNew() {
    // TODO Auto-generated method stub
    return new SumDistinctDoubleAggregator();
  }
}
