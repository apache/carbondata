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
package org.apache.carbondata.core.scan.filter.executer;

import java.util.BitSet;

import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;

/**
 * Class for updating the bitset
 * If include it will set the bit
 * If exclude it will flip the bit
 */
public final class BitSetUpdaterFactory {

  public static final BitSetUpdaterFactory INSTANCE = new BitSetUpdaterFactory();

  public FilterBitSetUpdater getBitSetUpdater(FilterExecuterType filterExecuterType) {
    switch (filterExecuterType) {
      case INCLUDE:
        return new IncludeFilterBitSetUpdater();
      case EXCLUDE:
        return new ExcludeFilterBitSetUpdater();
      default:
        throw new UnsupportedOperationException(
            "Invalid filter executor type:" + filterExecuterType);
    }
  }

  /**
   * Below class will be used to updating the bitset in case of include filter
   */
  static class IncludeFilterBitSetUpdater implements FilterBitSetUpdater {
    @Override public void updateBitset(BitSet bitSet, int bitIndex) {
      bitSet.set(bitIndex);
    }
  }

  /**
   * Below class will be used to updating the bitset in case of exclude filter
   */
  static class ExcludeFilterBitSetUpdater implements FilterBitSetUpdater {
    @Override public void updateBitset(BitSet bitSet, int bitIndex) {
      bitSet.flip(bitIndex);
    }
  }
}
