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

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;

/**
 * It checks if filter is required on given block and if required, it does
 * linear search on block data and set the bitset.
 */
public class ExcludeColGroupFilterExecuterImpl extends ExcludeFilterExecuterImpl {

  /**
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   */
  public ExcludeColGroupFilterExecuterImpl(DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties) {
    super(dimColResolvedFilterInfo, null, segmentProperties, false);
  }

  /**
   * Check if scan is required on given block based on min and max value
   */
  public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    bitSet.flip(0, 1);
    return bitSet;
  }

}