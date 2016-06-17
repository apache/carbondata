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

import java.util.HashSet;
import java.util.Set;

import org.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.carbondata.query.aggregator.MeasureAggregator;

public class DistinctCountLongAggregatorObjectSet extends AbstractDistinctCountAggregatorObjectSet {

  private static final long serialVersionUID = 6313463368629960186L;

  @Override public void agg(MeasureColumnDataChunk dataChunk, int index) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      valueSetForObj.add(dataChunk.getMeasureDataHolder().getReadableLongValueByIndex(index));
    }
  }

  private void agg(Set<Object> set2) {
    valueSetForObj.addAll(set2);
  }

  /**
   * merge the valueset so that we get the count of unique values
   */
  @Override public void merge(MeasureAggregator aggregator) {
    DistinctCountLongAggregatorObjectSet distinctCountAggregator =
        (DistinctCountLongAggregatorObjectSet) aggregator;
    agg(distinctCountAggregator.valueSetForObj);
  }

  @Override public MeasureAggregator getCopy() {
    DistinctCountLongAggregatorObjectSet aggregator = new DistinctCountLongAggregatorObjectSet();
    aggregator.valueSetForObj = new HashSet<Object>(valueSetForObj);
    return aggregator;
  }

  @Override public int compareTo(MeasureAggregator measureAggr) {
    long valueSetForObjSize = getLongValue();
    long otherVal = measureAggr.getLongValue();
    if (valueSetForObjSize > otherVal) {
      return 1;
    }
    if (valueSetForObjSize < otherVal) {
      return -1;
    }
    return 0;
  }

  @Override public boolean equals(Object obj) {
    if (!(obj instanceof DistinctCountLongAggregatorObjectSet)) {
      return false;
    }
    DistinctCountLongAggregatorObjectSet o = (DistinctCountLongAggregatorObjectSet) obj;
    return getLongValue().equals(o.getLongValue());
  }

  @Override public int hashCode() {
    return getLongValue().hashCode();
  }

  @Override public MeasureAggregator get() {
    return this;
  }

  @Override public MeasureAggregator getNew() {
    return new DistinctCountLongAggregatorObjectSet();
  }

}
