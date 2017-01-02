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

package org.apache.carbondata.scan.filter.executer;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.block.SegmentProperties;
import org.apache.carbondata.core.carbon.path.CarbonTablePath;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;

/**
 * This class will implement the blocklet and block pruning logic based
 * on the implicit column filter values
 */
public class ImplicitIncludeFilterExecutorImpl implements ImplicitColumnFilterExecutor {

  private final DimColumnResolvedFilterInfo dimColumnEvaluatorInfo;
  private final SegmentProperties segmentProperties;
  /**
   * List with implicit column filter values
   */
  private Set<String> implicitFilterValueList;

  public ImplicitIncludeFilterExecutorImpl(DimColumnResolvedFilterInfo dimColumnEvaluatorInfo,
      SegmentProperties segmentProperties) {
    this.dimColumnEvaluatorInfo = dimColumnEvaluatorInfo;
    this.segmentProperties = segmentProperties;
    this.implicitFilterValueList =
        new HashSet(dimColumnEvaluatorInfo.getFilterValues().getImplicitColumnFilterList());
  }


  public BitSet applyFilter(BlocksChunkHolder blockChunkHolder)
      throws FilterUnsupportedException {
    return setBitSetForCompleteDimensionData(blockChunkHolder.getDataBlock().nodeSize());
  }

  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    return null;
  }

  public BitSet isFilterValuesPresentInBlockOrBlocklet(DataRefNode dataRefNode,
                                                       String uniqueBlockPath) {
    BitSet bitSet = new BitSet(1);
    boolean isScanRequired = false;
    if (uniqueBlockPath.endsWith(".carbondata")) {
      String shortBlockId = CarbonTablePath.getShortBlockId(uniqueBlockPath);
      for (String blockletId : implicitFilterValueList) {
        if (blockletId.startsWith(shortBlockId)) {
          isScanRequired = true;
          break;
        }
      }
    } else if (implicitFilterValueList.contains(uniqueBlockPath)) {
      isScanRequired = true;
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
}
