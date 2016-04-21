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

package org.carbondata.processing.aggregatesurrogategenerator;

public class AggregateSurrogateGenerator {
  /**
   * measureIndex
   */
  private int[] measureIndex;

  /**
   * isMdkeyInOutRowRequired
   */
  private boolean isMdkeyInOutRowRequired;

  /**
   * AggregateSurrogateGenerator constructor
   *
   * @param factLevels
   * @param aggreateLevels
   * @param factMeasures
   * @param aggregateMeasures
   */
  public AggregateSurrogateGenerator(String[] factLevels, String[] aggreateLevels,
      String[] factMeasures, String[] aggregateMeasures, boolean isMdkeyInOutRowRequired,
      int[] aggDimensioncardinality) {
    measureIndex = new int[2];
    this.isMdkeyInOutRowRequired = isMdkeyInOutRowRequired;

  }

  /**
   * Below method will be used to generate the surrogate for aggregate table
   *
   * @param factTuple
   * @return aggregate tuple
   */
  public Object[] generateSurrogate(Object[] factTuple) {
    // added 1 for the high card dims
    int size = measureIndex.length + 1 + 1;
    if (isMdkeyInOutRowRequired) {
      size += 1;
    }
    Object[] records = new Object[size];
    int count = 0;
    int i = 0;
    for (; i < measureIndex.length - 1; i++) {
      records[count++] = factTuple[i];
    }
    records[count++] = factTuple[i++];
    // for high card cols.
    records[count++] = (byte[]) factTuple[i++];
    byte[] mdkey = (byte[]) factTuple[i++];
    records[count++] = mdkey;
    if (isMdkeyInOutRowRequired) {
      records[records.length - 1] = mdkey;
    }
    return records;
  }
}
