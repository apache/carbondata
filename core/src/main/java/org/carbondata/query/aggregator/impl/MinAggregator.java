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

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * Class Description : It will return min of values
 */
public class MinAggregator extends AbstractMeasureAggregatorMaxMin {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = -8077547753784906280L;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(MinAggregator.class.getName());

  protected void internalAgg(Object value) {
    if (value instanceof Comparable) {
      @SuppressWarnings("unchecked") Comparable<Object> newValue = ((Comparable<Object>) value);
      aggVal = (aggVal == null || aggVal.compareTo(newValue) > 0) ? newValue : aggVal;
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
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
          "Problem while getting byte array in minaggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bos);
    }
    return objectBytes;
  }

  /**
   * Merge the value, it will update the min aggregate value if aggregator
   * passed as an argument will have value less than aggVal
   *
   * @param aggregator MinAggregator
   */
  @Override public void merge(MeasureAggregator aggregator) {
    MinAggregator minAggregator = (MinAggregator) aggregator;
    agg(minAggregator.aggVal);
  }

  @Override public void writeData(DataOutput dataOutputVal1) throws IOException {
    ByteArrayOutputStream bos = null;
    ObjectOutput out = null;
    try {
      dataOutputVal1.writeBoolean(firstTime);
      bos = new ByteArrayOutputStream();
      out = new ObjectOutputStream(bos);
      out.writeObject(aggVal);
      byte[] objectBytes = bos.toByteArray();
      dataOutputVal1.write(objectBytes.length);
      dataOutputVal1.write(objectBytes, 0, objectBytes.length);
    } catch (Exception e) {

      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
          "Problem while getting byte array in minaggregator: " + e.getMessage());
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
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
          "Problem while getting byte array in minaggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bis);
    }
  }

  @Override public MeasureAggregator getCopy() {
    MinAggregator aggregator = new MinAggregator();
    aggregator.aggVal = aggVal;
    aggregator.firstTime = firstTime;
    return aggregator;
  }

  @Override public void merge(byte[] value) {
    if (0 == value.length) {
      return;
    }
    ByteArrayInputStream bis = null;
    ObjectInput objectInput = null;
    try {
      bis = new ByteArrayInputStream(value);
      objectInput = new ObjectInputStream(bis);
      Object newVal = (Comparable<Object>) objectInput.readObject();
      internalAgg(newVal);
      firstTime = false;
    } catch (Exception e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e,
          "Problem while merging byte array in minaggregator: " + e.getMessage());
    } finally {
      CarbonUtil.closeStreams(bis);
    }

  }

  @Override public MeasureAggregator getNew() {
    return new MinAggregator();
  }
}
