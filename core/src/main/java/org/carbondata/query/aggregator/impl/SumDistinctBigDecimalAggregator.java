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
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.util.DataTypeUtil;
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
public class SumDistinctBigDecimalAggregator extends AbstractMeasureAggregatorBasic {

  /**
   *
   */
  private static final long serialVersionUID = 6313463368629960155L;

  /**
   * For Spark CARBON to avoid heavy object transfer it better to flatten the
   * Aggregators. There is no aggregation expected after setting this value.
   */
  private BigDecimal computedFixedValue;

  /**
   *
   */
  private Set<BigDecimal> valueSet;

  public SumDistinctBigDecimalAggregator() {
    valueSet = new HashSet<BigDecimal>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * Distinct Aggregate function which update the Distinct set
   *
   * @param newVal new value
   */
  @Override public void agg(Object newVal) {
    valueSet.add(
        newVal instanceof BigDecimal ? (BigDecimal) newVal : new BigDecimal(newVal.toString()));
    firstTime = false;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      valueSet.add(dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(index));
      firstTime = false;
    }
  }

  /**
   * Below method will be used to get the value byte array
   */
  @Override public byte[] getByteArray() {
    Iterator<BigDecimal> iterator = valueSet.iterator();
    ByteBuffer buffer =
        ByteBuffer.allocate(valueSet.size() * CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE);
    while (iterator.hasNext()) {
      byte[] bytes = DataTypeUtil.bigDecimalToByte(iterator.next());
      buffer.putInt(bytes.length);
      buffer.put(bytes);
    }
    buffer.rewind();
    return buffer.array();
  }

  private void agg(Set<BigDecimal> set2) {
    valueSet.addAll(set2);
  }

  /**
   * merge the valueset so that we get the count of unique values
   */
  @Override public void merge(MeasureAggregator aggregator) {
    SumDistinctBigDecimalAggregator distinctAggregator =
        (SumDistinctBigDecimalAggregator) aggregator;
    if (!aggregator.isFirstTime()) {
      agg(distinctAggregator.valueSet);
      firstTime = false;
    }
  }

  @Override public BigDecimal getBigDecimalValue() {
    if (computedFixedValue == null) {
      BigDecimal result = new BigDecimal(0);
      for (BigDecimal aValue : valueSet) {
        result = result.add(aValue);
      }
      return result;
    }
    return computedFixedValue;
  }

  @Override public Object getValueObject() {
    return getBigDecimalValue();
  }

  @Override public void setNewValue(Object newValue) {
    computedFixedValue = (BigDecimal) newValue;
    valueSet = null;
  }

  @Override public boolean isFirstTime() {
    return firstTime;
  }

  @Override public void writeData(DataOutput dataOutput) throws IOException {
    if (computedFixedValue != null) {
      byte[] bytes = DataTypeUtil.bigDecimalToByte(computedFixedValue);
      ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + bytes.length);
      byteBuffer.putInt(-1);
      byteBuffer.putInt(bytes.length);
      byteBuffer.put(bytes);
      byteBuffer.flip();
      dataOutput.write(byteBuffer.array());
    } else {
      int length = valueSet.size() * 8 + valueSet.size() * 4;
      ByteBuffer byteBuffer = ByteBuffer.allocate(length + 4 + 1);
      byteBuffer.putInt(length);
      for (BigDecimal val : valueSet) {
        byte[] bytes =
            val.toString().getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        byteBuffer.putInt(-1);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
      }
      byteBuffer.flip();
      dataOutput.write(byteBuffer.array());
    }
  }

  @Override public void readData(DataInput inPut) throws IOException {
    int length = inPut.readInt();

    if (length == -1) {
      computedFixedValue = new BigDecimal(inPut.readUTF());
      valueSet = null;
    } else {
      length = length / 8;
      valueSet = new HashSet<BigDecimal>(length + 1, 1.0f);
      for (int i = 0; i < length; i++) {
        valueSet.add(new BigDecimal(inPut.readUTF()));
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
      byte[] valueByte = new byte[buffer.getInt()];
      buffer.get(valueByte);
      BigDecimal valueBigDecimal = DataTypeUtil.byteToBigDecimal(valueByte);
      agg(valueBigDecimal);
    }
  }

  public String toString() {
    if (computedFixedValue == null) {
      return valueSet.size() + "";
    }
    return computedFixedValue + "";
  }

  @Override public MeasureAggregator getCopy() {
    SumDistinctBigDecimalAggregator aggregator = new SumDistinctBigDecimalAggregator();
    aggregator.valueSet = new HashSet<BigDecimal>(valueSet);
    return aggregator;
  }

  @Override public int compareTo(MeasureAggregator msr) {
    BigDecimal msrValObj = getBigDecimalValue();
    BigDecimal otherVal = msr.getBigDecimalValue();

    return msrValObj.compareTo(otherVal);
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof SumDistinctBigDecimalAggregator)) {
      return false;
    }
    SumDistinctBigDecimalAggregator o = (SumDistinctBigDecimalAggregator) obj;
    return getBigDecimalValue().equals(o.getBigDecimalValue());
  }

  @Override public int hashCode() {
    return getBigDecimalValue().hashCode();
  }

  @Override public MeasureAggregator getNew() {
    // TODO Auto-generated method stub
    return new SumDistinctBigDecimalAggregator();
  }
}
