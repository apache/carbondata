/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.datastore.page.statistics;

import java.math.BigDecimal;

import org.apache.carbondata.core.bloom.BloomFilterUtil;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

public abstract class ColumnPageStatsCollector {

  protected BloomFilter bloomFilter;

  public abstract void updateNull(int rowId);

  public abstract void update(byte value);

  public abstract void update(short value);

  public abstract void update(int value);

  public abstract void update(long value);

  public abstract void update(double value);

  public abstract void update(float value);

  public abstract void update(BigDecimal value);

  public abstract void update(byte[] value);

  /**
   * return the collected statistics
   */
  public abstract SimpleStatsResult getPageStats();

  public void initBloom() {
    int[] bloomParas = BloomFilterUtil.getPageBloomParameters();
    bloomFilter = new BloomFilter(bloomParas[0], bloomParas[1], Hash.MURMUR_HASH);
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  void addValueToBloom(byte[] value) {
    if (value.length == 0) {
      bloomFilter.add(new Key(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY));
    } else {
      bloomFilter.add(new Key(value));
    }
  }
}
