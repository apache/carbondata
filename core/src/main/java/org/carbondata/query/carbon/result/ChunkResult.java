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

package org.carbondata.query.carbon.result;

import java.util.ArrayList;
import java.util.List;

import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.scanner.impl.CarbonKey;
import org.carbondata.query.scanner.impl.CarbonValue;

/**
 * Below class holds the query result
 */
public class ChunkResult implements CarbonIterator<RowResult> {

  /**
   * list of keys
   */
  private List<CarbonKey> keys;

  /**
   * list of values
   */
  private List<CarbonValue> values;

  /**
   * counter to check whether all the records are processed or not
   */
  private int counter;

  public ChunkResult() {
    keys = new ArrayList<CarbonKey>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    values = new ArrayList<CarbonValue>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
  }

  /**
   * Below method will be used to get the key
   *
   * @return
   */
  public List<CarbonKey> getKeys() {
    return keys;
  }

  /**
   * below method will be used to set the key
   *
   * @param keys
   */
  public void setKeys(List<CarbonKey> keys) {
    this.keys = keys;
  }

  /**
   * Below method will be used to get the values
   *
   * @return
   */
  public List<CarbonValue> getValues() {
    return values;
  }

  /**
   * Below method will be used to get the set the values
   *
   * @param values
   */
  public void setValues(List<CarbonValue> values) {
    this.values = values;
  }

  /**
   * Returns {@code true} if the iteration has more elements.
   *
   * @return {@code true} if the iteration has more elements
   */
  @Override public boolean hasNext() {
    return counter < keys.size();
  }

  /**
   * Returns the next element in the iteration.
   *
   * @return the next element in the iteration
   */
  @Override public RowResult next() {
    RowResult rowResult = new RowResult();
    rowResult.setKey(keys.get(counter));
    rowResult.setValue(values.get(counter));
    counter++;
    return rowResult;
  }
}
