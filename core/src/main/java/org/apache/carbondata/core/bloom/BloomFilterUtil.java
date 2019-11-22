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

package org.apache.carbondata.core.bloom;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.roaringbitmap.RoaringBitmap;

public class BloomFilterUtil {

  /**
   *  Calculate page bloom parameters by item size and fpp
   *  Formula:
   *     Number of bits (m) = -n*ln(p) / (ln(2)^2)
   *     Number of hashes(k) = m/n * ln(2)
   *
   * @param n Number of items in the filter
   * @param p False positive probability
   *
   */
  public static int[] getBloomParameters(int n, double p) {
    double numOfBit = -n * Math.log(p) / (Math.pow(Math.log(2), 2));
    double numOfHash = numOfBit / n * Math.log(2);
    return new int[]{(int) Math.ceil(numOfBit), (int) Math.ceil(numOfHash)};
  }

  public static int[] getPageBloomParameters() {
    // Use maximum page size(not quite large) to reduce number of bloom parameter to set.
    // And same parameters of bloom has same hash functions, so hashing the filter value one time
    // then we can check all pages in blocklet when query
    return getBloomParameters(
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT,
            CarbonCommonConstants.DEFAULT_PAGE_BLOOM_FPP);
  }

  /**
   * Get bitset from super class using reflection, in some cases java cannot access
   * the fields if jars are loaded in separate class loaders.
   *
   */
  public static BitSet getBitSet(BloomFilter bf) throws IOException {
    try {
      Field field = BloomFilter.class.getDeclaredField("bits");
      field.setAccessible(true);
      return (BitSet)field.get(bf);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Set bitset from super class using reflection, in some cases java cannot access
   * the fields if jars are loaded in separte class loaders.
   */
  public static void setBitSet(BitSet bitSet, BloomFilter bf) throws IOException {
    try {
      Field field = BloomFilter.class.getDeclaredField("bits");
      field.setAccessible(true);
      field.set(bf, bitSet);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public static RoaringBitmap convertBitSetToRoaringBitmap(BitSet bits) {
    RoaringBitmap bitmap = new RoaringBitmap();
    for (int i = bits.nextSetBit(0); i >= 0; i = bits.nextSetBit(i + 1)) {
      bitmap.add(i);
    }
    return bitmap;
  }

  public static RoaringBitmap convertBloomFilterToRoaringBitmap(BloomFilter bf)
          throws IOException {
    return convertBitSetToRoaringBitmap(getBitSet(bf));
  }
}
