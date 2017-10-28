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

import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;

public class AndFilterExecuterImpl implements FilterExecuter {

  private FilterExecuter leftExecuter;
  private FilterExecuter rightExecuter;

  public AndFilterExecuterImpl(FilterExecuter leftExecuter, FilterExecuter rightExecuter) {
    this.leftExecuter = leftExecuter;
    this.rightExecuter = rightExecuter;
  }

  @Override
  public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder, boolean useBitsetPipeLine)
      throws FilterUnsupportedException, IOException {
    BitSetGroup leftFilters = leftExecuter.applyFilter(blockChunkHolder, useBitsetPipeLine);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSetGroup rightFilter = rightExecuter.applyFilter(blockChunkHolder, useBitsetPipeLine);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    blockChunkHolder.setBitSetGroup(leftFilters);
    return leftFilters;
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet leftFilters = leftExecuter.isScanRequired(blockMaxValue, blockMinValue);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = rightExecuter.isScanRequired(blockMaxValue, blockMinValue);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    return leftFilters;
  }

  @Override public void readBlocks(BlocksChunkHolder blocksChunkHolder) throws IOException {
    leftExecuter.readBlocks(blocksChunkHolder);
    rightExecuter.readBlocks(blocksChunkHolder);
  }
}
