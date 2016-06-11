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

import java.math.BigDecimal;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.query.aggregator.MeasureAggregator;

public class DummyBigDecimalAggregator extends AbstractMeasureAggregatorDummy {
  private static final long serialVersionUID = 1L;

  /**
   * aggregate value
   */
  private BigDecimal aggVal;

  @Override public void agg(Object newVal) {
    aggVal = (BigDecimal) newVal;
    firstTime = false;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      aggVal = dataChunk.getMeasureDataHolder().getReadableBigDecimalValueByIndex(index);
      firstTime = false;
    }
  }

  @Override public BigDecimal getBigDecimalValue() {
    return aggVal;
  }

  @Override public Object getValueObject() {
    return aggVal;
  }

  @Override public void setNewValue(Object newValue) {
    aggVal = (BigDecimal) newValue;
  }

  @Override public MeasureAggregator getNew() {
    // TODO Auto-generated method stub
    return new DummyBigDecimalAggregator();
  }
}
