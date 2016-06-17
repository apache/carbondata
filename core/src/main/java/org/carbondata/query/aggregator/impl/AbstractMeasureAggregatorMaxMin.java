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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigDecimal;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.aggregator.impl.max.MaxAggregator;

/**
 * AbstractMeasureAggregatorMaxMin
 * Used for custom Carbon Aggregator max  min
 */
public abstract class AbstractMeasureAggregatorMaxMin implements MeasureAggregator {
  private static final long serialVersionUID = 1L;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MaxAggregator.class.getName());

  public Comparable<Object> aggVal;

  public boolean firstTime = true;

  protected abstract void internalAgg(Object value);

  @Override public void agg(double newVal) {
    internalAgg((Double) newVal);
    firstTime = false;
  }

  @Override public Double getDoubleValue() {
    return (Double) ((Object) aggVal);
  }

  @Override public void agg(Object newVal) {
    internalAgg(newVal);
    firstTime = false;
  }

  @Override public Long getLongValue() {
    return (Long) ((Object) aggVal);
  }

  @Override public BigDecimal getBigDecimalValue() {
    return (BigDecimal) ((Object) aggVal);
  }

  /**
   * @see MeasureAggregator#setNewValue(Object)
   */
  @Override public void setNewValue(Object newValue) {
  }

  /**
   * This method return the max value as an object
   * a8
   *
   * @return max value as an object
   */
  @Override public Object getValueObject() {
    return aggVal;
  }

  @Override public boolean isFirstTime() {
    return firstTime;
  }

  @Override public MeasureAggregator get() {
    return this;

  }

  public String toString() {
    return aggVal + "";
  }

  @Override public int compareTo(MeasureAggregator msrAggr) {
    @SuppressWarnings("unchecked") Comparable<Object> other =
        (Comparable<Object>) msrAggr.getValueObject();

    return aggVal.compareTo(other);
  }

  @Override public void writeData(DataOutput dataOutput) throws IOException {
    ByteArrayOutputStream bos = null;
    ObjectOutput out = null;

    try {
      dataOutput.writeBoolean(firstTime);
      bos = new ByteArrayOutputStream();
      out = new ObjectOutputStream(bos);
      out.writeObject(aggVal);
      byte[] objectBytes = bos.toByteArray();
      dataOutput.write(objectBytes.length);
      dataOutput.write(objectBytes, 0, objectBytes.length);
    } catch (Exception e) {
      LOGGER.error(e,
          "Problem while getting byte array in maxMinAggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bos);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readData(DataInput inPut)
      throws IOException {
    ByteArrayInputStream bis = null;
    ObjectInput in = null;
    try {
      int length = inPut.readInt();
      firstTime = inPut.readBoolean();
      byte[] data = new byte[length];
      bis = new ByteArrayInputStream(data);
      in = new ObjectInputStream(bis);
      aggVal = (Comparable<Object>) in.readObject();
    } catch (Exception e) {
      LOGGER.error(e,
          "Problem while getting byte array in maxMinAggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bis);
    }
  }

  /**
   * Below method will be used to get the value byte array
   */
  @Override public byte[] getByteArray() {
    byte[] objectBytes = new byte[0];
    if (firstTime) {
      return objectBytes;
    }
    ObjectOutput out = null;
    ByteArrayOutputStream bos = null;
    try {
      bos = new ByteArrayOutputStream();
      out = new ObjectOutputStream(bos);
      out.writeObject(aggVal);
      objectBytes = bos.toByteArray();
    } catch (IOException e) {
      LOGGER.error(e,
          "Problem while getting byte array in maxMinAggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bos);
    }
    return objectBytes;
  }
}
