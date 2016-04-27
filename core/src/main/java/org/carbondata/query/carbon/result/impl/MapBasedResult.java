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

package org.carbondata.query.carbon.result.impl;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.carbon.result.Result;
import org.carbondata.query.carbon.wrappers.ByteArrayWrapper;

/**
 * To store aggregated result
 */
public class MapBasedResult implements Result<Map<ByteArrayWrapper, MeasureAggregator[]>> {
  /**
   * iterator over result
   */
  private Iterator<Entry<ByteArrayWrapper, MeasureAggregator[]>> resultIterator;

  /**
   * result entry
   */
  private Entry<ByteArrayWrapper, MeasureAggregator[]> resultEntry;

  /**
   * scanned result
   */
  private Map<ByteArrayWrapper, MeasureAggregator[]> scannerResult;

  /**
   * total number of result
   */
  private int resulSize;

  public MapBasedResult() {
    scannerResult = new HashMap<ByteArrayWrapper, MeasureAggregator[]>(
        CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    this.resultIterator = scannerResult.entrySet().iterator();
  }

  /**
   * @return the key
   */
  @Override public ByteArrayWrapper getKey() {
    resultEntry = this.resultIterator.next();
    return resultEntry.getKey();
  }

  /**
   * return the value
   */
  @Override public MeasureAggregator[] getValue() {
    return resultEntry.getValue();
  }

  /**
   * Method to check more result is present
   * or not
   */
  @Override public boolean hasNext() {
    return this.resultIterator.hasNext();
  }

  /***
   * below method will be used to merge the
   * scanned result
   *
   * @param otherResult return to be merged
   */
  @Override public void addScannedResult(Map<ByteArrayWrapper, MeasureAggregator[]> scannerResult) {
    this.scannerResult = scannerResult;
    resulSize = scannerResult.size();
    this.resultIterator = scannerResult.entrySet().iterator();
  }

  /***
   * below method will be used to merge the
   * scanned result, in case of map based the
   * result we need to aggregate the result
   *
   * @param otherResult return to be merged
   */
  @Override public void merge(Result<Map<ByteArrayWrapper, MeasureAggregator[]>> result) {
    ByteArrayWrapper key = null;
    MeasureAggregator[] value = null;
    Map<ByteArrayWrapper, MeasureAggregator[]> otherResult = result.getResult();
    if (otherResult != null) {
      while (resultIterator.hasNext()) {
        Entry<ByteArrayWrapper, MeasureAggregator[]> entry = resultIterator.next();
        key = entry.getKey();
        value = entry.getValue();
        MeasureAggregator[] agg = otherResult.get(key);
        if (agg != null) {
          for (int j = 0; j < agg.length; j++) {
            agg[j].merge(value[j]);
          }
        } else {
          otherResult.put(key, value);
        }
      }
      resulSize = otherResult.size();
      this.resultIterator = otherResult.entrySet().iterator();
      this.scannerResult = otherResult;
    }
  }

  /**
   * Return the size of the result
   */
  @Override public int size() {
    return resulSize;
  }

  /**
   * @return the complete result
   */
  @Override public Map<ByteArrayWrapper, MeasureAggregator[]> getResult() {
    return this.scannerResult;
  }
}
