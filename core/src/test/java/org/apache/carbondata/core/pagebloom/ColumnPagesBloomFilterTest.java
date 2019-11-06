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

package org.apache.carbondata.core.pagebloom;

import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.carbondata.core.bloom.BloomFilterUtil;
import org.apache.carbondata.core.bloom.ColumnPagesBloomFilter;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.format.PageBloomChunk;

import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;
import org.junit.Test;

public class ColumnPagesBloomFilterTest {

  @Test
  public void testColumnPagesBloomFilter() throws IOException {
    // use same parameters as query
    int[] bloomParas = BloomFilterUtil.getPageBloomParameters();
    // build bloom filter
    ColumnPagesBloomFilter pagesBloomFilter = new ColumnPagesBloomFilter();
    for (int i = 0; i < 10; i++) {
      // as bloom filter of a column page
      BloomFilter bloomFilter = new BloomFilter(bloomParas[0], bloomParas[1], Hash.MURMUR_HASH);
      for (int j = 0; j < 10; j++) {
        bloomFilter.add(new Key(CarbonUtil.getValueAsBytes(DataTypes.INT, i * 10 + j)));
      }
      pagesBloomFilter.addPageBloomFilter(bloomFilter);
    }
    PageBloomChunk pageBloomChunk = pagesBloomFilter.toPageBloomChunk();

    // simulate query recover the filter from file
    ColumnPagesBloomFilter pagesBloomFilterRecovered = new ColumnPagesBloomFilter(pageBloomChunk);
    // make minmax hit all pages
    BitSet bitset = new BitSet(10);
    bitset.flip(0, 10);
    // can only check True Positive value since bloom has False Positive cases
    List<byte[]> filterKeyBytes = new ArrayList<>();
    filterKeyBytes.add(CarbonUtil.getValueAsBytes(DataTypes.INT, 0));
    filterKeyBytes.add(CarbonUtil.getValueAsBytes(DataTypes.INT, 33));
    filterKeyBytes.add(CarbonUtil.getValueAsBytes(DataTypes.INT, 71));
    // check result
    BitSet result = pagesBloomFilterRecovered.prunePages(filterKeyBytes.toArray(new byte[0][0]), bitset);
    assert result.get(0);
    assert result.get(3);
    assert result.get(7);
  }
}
