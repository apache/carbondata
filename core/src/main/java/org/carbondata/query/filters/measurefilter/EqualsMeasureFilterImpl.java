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

package org.carbondata.query.filters.measurefilter;

import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.executer.calcexp.CarbonCalcFunction;

public class EqualsMeasureFilterImpl implements MeasureFilter {

  private double filterValue;

  private int index;

  private CarbonCalcFunction calcFunction;

  /**
   * Constructor that takes filter value
   *
   * @param filterValue
   */
  public EqualsMeasureFilterImpl(double filterValue, int index, CarbonCalcFunction calcFunction) {
    this.filterValue = filterValue;
    this.index = index;
    this.calcFunction = calcFunction;
  }

  /**
   * See interface commnets.
   *
   * @param filterValue
   */
  @Override public boolean filter(MeasureAggregator[] msrValue) {
    if (calcFunction != null) {
      return 0 == Double.compare(calcFunction.calculate(msrValue), filterValue) ? true : false;
    }
    double value;
    if (msrValue[index].toString().contains("Long")) {
      value = msrValue[index].getLongValue();
    } else if (msrValue[index].toString().contains("Decimal")) {
      value = msrValue[index].getBigDecimalValue().doubleValue();
    } else {
      value = msrValue[index].getDoubleValue();
    }
    return 0 == Double.compare(value, filterValue) ? true : false;
  }

  /**
   * See interface commnets.
   *
   * @param filterValue
   */
  @Override public boolean filter(double[] msrValue, int msrStartIndex) {
    return 0 == Double.compare(msrValue[index + msrStartIndex], filterValue) ? true : false;
  }

}
