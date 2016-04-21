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

/**
 *
 */
package org.carbondata.query.scanner.impl;

import java.io.Serializable;
import java.util.Arrays;

import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * @author R00900208
 */
public class CarbonValue implements Serializable, Comparable<CarbonValue> {

  /**
   *
   */
  private static final long serialVersionUID = 8034398963696130423L;

  private MeasureAggregator[] values;

  private int topNIndex;

  public CarbonValue(MeasureAggregator[] values) {
    this.values = values;
  }

  /**
   * @return the values
   */
  public MeasureAggregator[] getValues() {
    return values;
  }

  public CarbonValue merge(CarbonValue another) {
    for (int i = 0; i < values.length; i++) {
      values[i].merge(another.values[i]);
    }
    return this;
  }

  public void setTopNIndex(int index) {
    this.topNIndex = index;
  }

  public void addGroup(CarbonKey key, CarbonValue value) {

  }

  public CarbonValue mergeKeyVal(CarbonValue another) {
    return another;
  }

  @Override public String toString() {
    return Arrays.toString(values);
  }

  @Override public int compareTo(CarbonValue o) {
    return values[topNIndex].compareTo(o.values[topNIndex]);
  }

}
