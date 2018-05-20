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
package org.apache.carbondata.datamap.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.CarbonBloomFilter;

/**
 * This class holds a bloom filter for one blocklet
 */
@InterfaceAudience.Internal
public class BloomDMModel implements Writable {
  private int blockletNo;
  private CarbonBloomFilter bloomFilter;

  public BloomDMModel() {
  }

  public BloomDMModel(int blockletNo, CarbonBloomFilter bloomFilter) {
    this.blockletNo = blockletNo;
    this.bloomFilter = bloomFilter;
  }

  public int getBlockletNo() {
    return blockletNo;
  }

  public BloomFilter getBloomFilter() {
    return bloomFilter;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("BloomDMModel{");
    sb.append(", blockletNo=").append(blockletNo);
    sb.append(", bloomFilter=").append(bloomFilter);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(blockletNo);
    bloomFilter.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    blockletNo = in.readInt();
    bloomFilter = new CarbonBloomFilter();
    bloomFilter.readFields(in);
  }
}
