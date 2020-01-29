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
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.path.CarbonTablePath;

/**
 * This class will implement the blocklet and block pruning logic based
 * on the implicit column filter values
 */
public class ImplicitIncludeFilterExecutorImpl
    implements FilterExecuter, ImplicitColumnFilterExecutor {

  private final DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;

  public ImplicitIncludeFilterExecutorImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo) {
    this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeline) {
    BitSetGroup bitSetGroup = new BitSetGroup(
        rawBlockletColumnChunks.getDataBlock().numberOfPages());
    for (int i = 0; i < rawBlockletColumnChunks.getDataBlock().numberOfPages(); i++) {
      bitSetGroup.setBitSet(
          setBitSetForCompleteDimensionData(
              rawBlockletColumnChunks.getDataBlock().getPageRowCount(i)), i);
    }
    return bitSetGroup;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks) {
    int numberOfPages = rawBlockletColumnChunks.getDataBlock().numberOfPages();
    BitSet bitSet = new BitSet(numberOfPages);
    bitSet.set(0, numberOfPages);
    return bitSet;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    return false;
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    return null;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) {

  }

  @Override
  public BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
      String uniqueBlockPath, boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    boolean isScanRequired = false;
    String shortBlockId = CarbonTablePath.getShortBlockId(uniqueBlockPath);
    if (uniqueBlockPath.endsWith(".carbondata")) {
      if (dimColumnEvaluatorInfo.getFilterValues().getImplicitColumnFilterBlockToBlockletsMap()
          .containsKey(shortBlockId)) {
        isScanRequired = true;
      }
    } else {
      // in case of CACHE_LEVEL = BLOCKLET, shortBlockId contains both block id and blocklet id
      // so separating out block id for look up in implicit filter
      String blockId =
          shortBlockId.substring(0, shortBlockId.lastIndexOf(CarbonCommonConstants.FILE_SEPARATOR));
      Set<Integer> blockletIds =
          dimColumnEvaluatorInfo.getFilterValues().getImplicitColumnFilterBlockToBlockletsMap()
              .get(blockId);
      if (null != blockletIds) {
        int idInUniqueBlockPath = Integer.parseInt(shortBlockId.substring(blockId.length() + 1));
        if (blockletIds.contains(idInUniqueBlockPath)) {
          isScanRequired = true;
        }
      }
    }
    if (isScanRequired) {
      bitSet.set(0);
    }
    return bitSet;
  }

  /**
   * For implicit column filtering, complete data need to be selected. As it is a special case
   * no data need to be discarded, implicit filtering is only for slecting block and blocklets
   *
   * @param numberOfRows
   * @return
   */
  private BitSet setBitSetForCompleteDimensionData(int numberOfRows) {
    BitSet bitSet = new BitSet();
    bitSet.set(0, numberOfRows, true);
    return bitSet;
  }

  @Override
  public Boolean isFilterValuesPresentInAbstractIndex(byte[][] maxValue, byte[][] minValue,
      boolean[] isMinMaxSet) {
    return true;
  }
}
