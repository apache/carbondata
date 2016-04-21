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

import org.carbondata.core.datastorage.store.dataholder.CarbonReadDataHolder;

public class AvgOfAvgLongAggregator extends AvgLongAggregator {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 6482976744603672084L;

  /**
   * Overloaded Aggregate function will be used for Aggregate tables because
   * aggregate table will have fact_count as a measure.
   *
   * @param newVal new value
   * @param index  index
   */
  @Override public void agg(CarbonReadDataHolder newVal, int index) {
    byte[] value = newVal.getReadableByteArrayValueByIndex(index);
    ByteBuffer buffer = ByteBuffer.wrap(value);
    double newValue = buffer.getLong();
    double factCount = buffer.getDouble();
    aggVal += (newValue * factCount);
    count += factCount;
    firstTime = false;
  }

}
