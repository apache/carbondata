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

package org.carbondata.query.aggregator.impl.distinct;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;

public abstract class AbstractDistinctCountAggregatorObjectSet implements MeasureAggregator {

  private static final long serialVersionUID = 6313463368629960186L;

  protected Set<Object> valueSetForObj;

  public AbstractDistinctCountAggregatorObjectSet() {
    valueSetForObj = new HashSet<Object>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * just need to add the unique values to agg set
   */
  @Override public void agg(double newVal) {
  }

  /**
   * Distinct count Aggregate function which update the Distinct count
   *
   * @param newVal new value
   */
  @Override public void agg(Object newVal) {
    valueSetForObj.add(newVal);
  }

  /**
   * Below method will be used to get the value byte array
   */
  @Override public byte[] getByteArray() {
    return null;
  }

  @Override public Double getDoubleValue() {
    return (double) valueSetForObj.size();
  }

  @Override public Long getLongValue() {
    return (long) valueSetForObj.size();
  }

  @Override public BigDecimal getBigDecimalValue() {
    return new BigDecimal(valueSetForObj.size());
  }

  @Override public Object getValueObject() {
    return valueSetForObj.size();
  }

  @Override public void setNewValue(Object newValue) {
    valueSetForObj.add(newValue);
  }

  @Override public boolean isFirstTime() {
    return false;
  }

  @Override public void writeData(DataOutput output) throws IOException {

  }

  @Override public void readData(DataInput inPut) throws IOException {

  }

  public String toString() {
    return valueSetForObj.size() + "";
  }

  @Override public void merge(byte[] value) {
  }

}
