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

package org.carbondata.query.result.impl;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.query.aggregator.MeasureAggregator;
import org.carbondata.query.result.Result;
import org.carbondata.query.wrappers.ByteArrayWrapper;

public class ListBasedResult implements Result<List<ByteArrayWrapper>, List<MeasureAggregator[]>> {
  private List<ByteArrayWrapper> keys;

  private List<List<ByteArrayWrapper>> allKeys;

  private List<List<MeasureAggregator[]>> allValues;

  private List<MeasureAggregator[]> values;

  private int counter = -1;

  private int listSize;

  private int currentCounter = -1;

  private int currentListCounter;

  public ListBasedResult() {
    keys = new ArrayList<ByteArrayWrapper>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    values = new ArrayList<MeasureAggregator[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    allKeys = new ArrayList<List<ByteArrayWrapper>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    allValues =
        new ArrayList<List<MeasureAggregator[]>>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  @Override
  public void addScannedResult(List<ByteArrayWrapper> keys, List<MeasureAggregator[]> values) {
    this.keys = keys;
    this.values = values;
    listSize = keys.size();
  }

  @Override public boolean hasNext() {
    if (allKeys.size() == 0) {
      return false;
    }
    counter++;
    currentCounter++;
    if (currentCounter == 0 || (currentCounter >= keys.size() && currentListCounter < allKeys
        .size())) {
      currentCounter = 0;
      keys = allKeys.get(currentListCounter);
      values = allValues.get(currentListCounter);
      currentListCounter++;
    }
    return counter < listSize;
  }

  @Override public ByteArrayWrapper getKey() {
    try {
      return keys.get(currentCounter);
    } catch (IndexOutOfBoundsException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override public MeasureAggregator[] getValue() {
    return values.get(currentCounter);
  }

  @Override
  public void merge(Result<List<ByteArrayWrapper>, List<MeasureAggregator[]>> otherResult) {
    if (otherResult.getKeys().size() > 0 && otherResult.getKeys().size() > 0) {
      listSize += otherResult.getKeys().size();
      this.allKeys.add(otherResult.getKeys());
      this.allValues.add(otherResult.getValues());
    }
  }

  @Override public int size() {
    return listSize;
  }

  @Override public List<ByteArrayWrapper> getKeys() {
    return keys;
  }

  @Override public List<MeasureAggregator[]> getValues() {
    return values;
  }
}
