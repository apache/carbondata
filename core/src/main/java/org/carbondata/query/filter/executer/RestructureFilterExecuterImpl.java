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
package org.carbondata.query.filter.executer;

import java.util.BitSet;

import org.carbondata.core.keygenerator.KeyGenerator;
import org.carbondata.query.carbon.processor.BlocksChunkHolder;
import org.carbondata.query.evaluators.DimColumnExecuterFilterInfo;
import org.carbondata.query.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.carbondata.query.filters.measurefilter.util.FilterUtil;

public class RestructureFilterExecuterImpl implements FilterExecuter {

  DimColumnExecuterFilterInfo dimColumnExecuterInfo;

  public RestructureFilterExecuterImpl(DimColumnResolvedFilterInfo dimColumnResolvedFilterInfo,
      KeyGenerator blockKeyGenerator) {
    dimColumnExecuterInfo = new DimColumnExecuterFilterInfo();
    FilterUtil
        .prepareKeysFromSurrogates(dimColumnResolvedFilterInfo.getFilterValues(), blockKeyGenerator,
            dimColumnResolvedFilterInfo.getDimension(), dimColumnExecuterInfo);
  }

  @Override public BitSet applyFilter(BlocksChunkHolder blocksChunkHolder) {
    BitSet bitSet = new BitSet(blocksChunkHolder.getDataBlock().nodeSize());
    byte[][] filterValues = dimColumnExecuterInfo.getFilterKeys();
    if (null != filterValues && filterValues.length > 0) {
      bitSet.set(0, blocksChunkHolder.getDataBlock().nodeSize());
    }
    return bitSet;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0);
    return bitSet;
  }
}
