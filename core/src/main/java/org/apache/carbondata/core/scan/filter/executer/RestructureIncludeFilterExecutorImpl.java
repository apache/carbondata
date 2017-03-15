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

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.scan.filter.FilterUtil;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;

public class RestructureIncludeFilterExecutorImpl extends RestructureEvaluatorImpl {

  protected DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;
  protected SegmentProperties segmentProperties;

  /**
   * flag to check whether filter values contain the default value applied on the dimension column
   * which does not exist in the current block
   */
  protected boolean isDefaultValuePresentInFilterValues;

  public RestructureIncludeFilterExecutorImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      SegmentProperties segmentProperties) {
    this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
    this.segmentProperties = segmentProperties;
    isDefaultValuePresentInFilterValues =
        isDimensionDefaultValuePresentInFilterValues(dimColumnEvaluatorInfo);
  }

  @Override public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder) throws IOException {
    int numberOfRows = blockChunkHolder.getDataBlock().nodeSize();
    return FilterUtil
        .createBitSetGroupWithDefaultValue(blockChunkHolder.getDataBlock().numberOfPages(),
            numberOfRows, isDefaultValuePresentInFilterValues);
  }

  public BitSet isScanRequired(byte[][] blkMaxVal, byte[][] blkMinVal) {
    BitSet bitSet = new BitSet(1);
    bitSet.set(0, isDefaultValuePresentInFilterValues);
    return bitSet;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {

  }

}
