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
import org.carbondata.core.util.DataTypeUtil;
import org.carbondata.query.aggregator.MeasureAggregator;

public class SumBigDecimalAggregator extends AbstractMeasureAggregatorBasic {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 623750056131364540L;

  /**
   * aggregate value
   */
  private BigDecimal aggVal;

  public SumBigDecimalAggregator() {
    aggVal = new BigDecimal(0);
    firstTime = false;
  }

  /**
   * This method will update the aggVal it will add new value to aggVal
   *
   * @param newVal new value
   */
  @Override public void agg(Object newVal) {
    if (firstTime) {
      aggVal = (BigDecimal) newVal;
      firstTime = false;
    } else {
      aggVal = aggVal.add((BigDecimal) newVal);
    }
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      BigDecimal value = dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(index);
      aggVal = aggVal.add(value);
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
    byte[] bytes = DataTypeUtil.bigDecimalToByte(aggVal);
    ByteBuffer allocate = ByteBuffer.allocate(4 + bytes.length);

    allocate.putInt(bytes.length);
    allocate.put(bytes);
    allocate.rewind();
    return allocate.array();
  }

  /**
   * This method will return aggVal
   *
   * @return sum value
   */
  @Override public BigDecimal getBigDecimalValue() {
    return aggVal;
  }

  /* Merge the value, it will update the sum aggregate value it will add new
   * value to aggVal
   *
   * @param aggregator
   *            SumAggregator
   *
   */
  @Override public void merge(MeasureAggregator aggregator) {
    if (!aggregator.isFirstTime()) {
      agg(aggregator.getBigDecimalValue());
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
    aggVal = (BigDecimal) newValue;
  }

  @Override public void readData(DataInput inPut) throws IOException {
    firstTime = inPut.readBoolean();
    aggVal = new BigDecimal(inPut.readUTF());
  }

  @Override public void writeData(DataOutput output) throws IOException {
    output.writeBoolean(firstTime);
    output.writeUTF(aggVal.toString());

  }

  @Override public MeasureAggregator getCopy() {
    SumBigDecimalAggregator aggr = new SumBigDecimalAggregator();
    aggr.aggVal = aggVal;
    aggr.firstTime = firstTime;
    return aggr;
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
    firstTime = false;
  }

  public String toString() {
    return aggVal + "";
  }

  @Override public int compareTo(MeasureAggregator o) {
    BigDecimal value = getBigDecimalValue();
    BigDecimal otherVal = o.getBigDecimalValue();
    return value.compareTo(otherVal);
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof SumBigDecimalAggregator)) {
      return false;
    }
    SumBigDecimalAggregator o = (SumBigDecimalAggregator) obj;
    return getBigDecimalValue().equals(o.getBigDecimalValue());
  }

  @Override public int hashCode() {
    return getBigDecimalValue().hashCode();
  }

  @Override public MeasureAggregator getNew() {
    return new SumBigDecimalAggregator();
  }
}
