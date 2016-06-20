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

import java.nio.ByteBuffer;
import java.sql.Timestamp;

import org.carbondata.query.aggregator.MeasureAggregator;

public class AvgTimestampAggregator extends AvgDoubleAggregator {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 5463736686281089871L;

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
    } else if (newVal instanceof Timestamp) {
      aggVal += ((Timestamp) newVal).getTime();
      count++;
      firstTime = false;
    } else if (newVal instanceof Number) {
      aggVal += ((Number) newVal).doubleValue();
      count++;
      firstTime = false;
    }
  }

  /**
   * Return the average of the aggregate values
   *
   * @return average aggregate value
   */
  @Override public Double getDoubleValue() {
    return aggVal / (count * 1000L);
  }

  /**
   * This method return the average value as an object
   *
   * @return average value as an object
   */
  @Override public Object getValueObject() {
    return aggVal / (count * 1000L);
  }

  /**
   * @see MeasureAggregator#setNewValue(Object)
   */
  @Override public void setNewValue(Object newValue) {
    aggVal = (Double) newValue;
    count = 1;
  }

  @Override public MeasureAggregator getCopy() {
    AvgTimestampAggregator avg = new AvgTimestampAggregator();
    avg.aggVal = aggVal;
    avg.count = count;
    avg.firstTime = firstTime;
    return avg;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof AvgTimestampAggregator)) {
      return false;
    }
    AvgTimestampAggregator o = (AvgTimestampAggregator) obj;
    return getDoubleValue().equals(o.getDoubleValue());
  }

  @Override public int hashCode() {
    return getDoubleValue().hashCode();
  }

  public String toString() {
    return (aggVal / (count * 1000L)) + "";
  }

  @Override public MeasureAggregator getNew() {
    return new AvgTimestampAggregator();
  }
}
