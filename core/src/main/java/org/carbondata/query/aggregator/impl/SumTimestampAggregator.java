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

import java.sql.Timestamp;

import org.carbondata.query.aggregator.MeasureAggregator;

public class SumTimestampAggregator extends SumDoubleAggregator {

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
  @Override public void agg(Object newVal) {
    if (newVal instanceof Timestamp) {
      aggVal += ((Timestamp) newVal).getTime();
      firstTime = false;
    } else if (newVal instanceof Number) {
      aggVal += ((Number) newVal).doubleValue();
      firstTime = false;
    }
  }

  /**
   * This method will return aggVal
   *
   * @return sum value
   */

  @Override public Double getDoubleValue() {
    return aggVal / 1000L;
  }

  @Override public MeasureAggregator getCopy() {
    SumTimestampAggregator aggr = new SumTimestampAggregator();
    aggr.aggVal = aggVal;
    aggr.firstTime = firstTime;
    return aggr;
  }

  public String toString() {
    return (aggVal / 1000L) + "";
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof SumTimestampAggregator)) {
      return false;
    }
    SumTimestampAggregator o = (SumTimestampAggregator) obj;
    return getDoubleValue().equals(o.getDoubleValue());
  }

  @Override public int hashCode() {
    return getDoubleValue().hashCode();
  }

  @Override public MeasureAggregator getNew() {
    return new SumTimestampAggregator();
  }
}
