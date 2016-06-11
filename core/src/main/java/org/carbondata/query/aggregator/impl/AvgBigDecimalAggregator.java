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
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.query.aggregator.MeasureAggregator;

public class AvgBigDecimalAggregator extends AbstractMeasureAggregatorBasic {

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
  protected BigDecimal aggVal;

  public AvgBigDecimalAggregator() {
    aggVal = new BigDecimal(0);
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
        byte[] valueByte = new byte[buffer.getInt()];
        buffer.get(valueByte);
        BigDecimal valueBigDecimal = DataTypeUtil.byteToBigDecimal(valueByte);
        aggVal = aggVal.add(valueBigDecimal);

        count += buffer.getDouble();
        firstTime = false;
      }
      return;
    }

    if (firstTime) {
      aggVal = (BigDecimal) newVal;
      firstTime = false;
    } else {
      aggVal = aggVal.add((BigDecimal) newVal);
    }
    count++;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      BigDecimal value = dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(index);
      aggVal = aggVal.add(value);
      firstTime = false;
      count++;
    }
  }

  /**
   * Below method will be used to get the value byte array
   */
  @Override public byte[] getByteArray() {
    if (firstTime) {
      return new byte[0];
    }
    byte[] bytes = DataTypeUtil.bigDecimalToByte(aggVal);
    ByteBuffer allocate =
        ByteBuffer.allocate(4 + bytes.length + CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
    allocate.putInt(bytes.length);
    allocate.put(bytes);
    allocate.putDouble(count);
    allocate.rewind();

    return allocate.array();
  }

  /**
   * Return the average of the aggregate values
   *
   * @return average aggregate value
   */
  @Override public BigDecimal getBigDecimalValue() {
    return aggVal.divide(new BigDecimal(count), 6);
  }

  /**
   * This method merge the aggregated value, in average aggregator it will add
   * count and aggregate value
   *
   * @param aggregator Avg Aggregator
   */
  @Override public void merge(MeasureAggregator aggregator) {
    AvgBigDecimalAggregator avgAggregator = (AvgBigDecimalAggregator) aggregator;
    if (!avgAggregator.isFirstTime()) {
      aggVal = aggVal.add(avgAggregator.aggVal);
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
    return aggVal.divide(new BigDecimal(count));
  }

  /**
   * @see MeasureAggregator#setNewValue(Object)
   */
  @Override public void setNewValue(Object newValue) {
    aggVal = (BigDecimal) newValue;
    count = 1;
  }

  @Override public void writeData(DataOutput output) throws IOException {
    output.writeBoolean(firstTime);
    output.writeUTF(aggVal.toString());
    output.writeDouble(count);

  }

  @Override public void readData(DataInput inPut) throws IOException {
    firstTime = inPut.readBoolean();
    aggVal = new BigDecimal(inPut.readUTF());
    count = inPut.readDouble();
  }

  @Override public MeasureAggregator getCopy() {
    AvgBigDecimalAggregator avg = new AvgBigDecimalAggregator();
    avg.aggVal = aggVal;
    avg.count = count;
    avg.firstTime = firstTime;
    return avg;
  }

  @Override public int compareTo(MeasureAggregator o) {
    BigDecimal val = getBigDecimalValue();
    BigDecimal otherVal = o.getBigDecimalValue();

    return val.compareTo(otherVal);
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof AvgBigDecimalAggregator)) {
      return false;
    }
    AvgBigDecimalAggregator o = (AvgBigDecimalAggregator) obj;
    return getBigDecimalValue().equals(o.getBigDecimalValue());
  }

  @Override public int hashCode() {
    return getBigDecimalValue().hashCode();
  }

  @Override public void merge(byte[] value) {
    if (0 == value.length) {
      return;
    }
    ByteBuffer buffer = ByteBuffer.wrap(value);

    byte[] valueByte = new byte[buffer.getInt()];
    buffer.get(valueByte);
    BigDecimal valueBigDecimal = DataTypeUtil.byteToBigDecimal(valueByte);
    aggVal = aggVal.add(valueBigDecimal);
    count += buffer.getDouble();
    firstTime = false;
  }

  public String toString() {
    return (aggVal.divide(new BigDecimal(count))) + "";
  }

  @Override public MeasureAggregator getNew() {
    return new AvgBigDecimalAggregator();
  }
}
