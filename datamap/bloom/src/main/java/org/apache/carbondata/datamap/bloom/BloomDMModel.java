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

import java.io.Serializable;

import org.apache.carbondata.common.annotations.InterfaceAudience;

import com.google.common.hash.BloomFilter;

@InterfaceAudience.Internal
public class BloomDMModel implements Serializable {
  private static final long serialVersionUID = 7281578747306832771L;
  private int blockletNo;
  private BloomFilter<byte[]> bloomFilter;

  public BloomDMModel(int blockletNo, BloomFilter<byte[]> bloomFilter) {
    this.blockletNo = blockletNo;
    this.bloomFilter = bloomFilter;
  }

  public int getBlockletNo() {
    return blockletNo;
  }

  public BloomFilter<byte[]> getBloomFilter() {
    return bloomFilter;
  }

  @Override public String toString() {
    final StringBuilder sb = new StringBuilder("BloomDMModel{");
    sb.append(", blockletNo=").append(blockletNo);
    sb.append(", bloomFilter=").append(bloomFilter);
    sb.append('}');
    return sb.toString();
  }
}
