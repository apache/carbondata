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

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.query.aggregator.MeasureAggregator;

/**
 * Class Description : It will return total count of values
 */
public class CountStarAggregator extends CountAggregator {

  @Override public void agg(double newVal) {
    super.agg(newVal);
  }

  @Override public void agg(Object newVal) {
    aggVal++;
  }

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    aggVal++;
  }

  @Override public MeasureAggregator getCopy() {
    CountStarAggregator aggregator = new CountStarAggregator();
    aggregator.aggVal = aggVal;
    return aggregator;
  }

  @Override public MeasureAggregator getNew() {
    return new CountStarAggregator();
  }
}
