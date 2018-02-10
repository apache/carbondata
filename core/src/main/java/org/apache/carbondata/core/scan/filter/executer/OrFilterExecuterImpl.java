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
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.util.BitSetGroup;

public class OrFilterExecuterImpl implements FilterExecuter {

  private FilterExecuter leftExecuter;
  private FilterExecuter rightExecuter;

  public OrFilterExecuterImpl(FilterExecuter leftExecuter, FilterExecuter rightExecuter) {
    this.leftExecuter = leftExecuter;
    this.rightExecuter = rightExecuter;
  }

  @Override
  public BitSetGroup applyFilter(BlocksChunkHolder blockChunkHolder, boolean useBitsetPipeLine)
      throws FilterUnsupportedException, IOException {
    BitSetGroup leftFilters = leftExecuter.applyFilter(blockChunkHolder, false);
    BitSetGroup rightFilters = rightExecuter.applyFilter(blockChunkHolder, false);
    leftFilters.or(rightFilters);
    blockChunkHolder.setBitSetGroup(leftFilters);
    return leftFilters;
  }

  @Override public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    return leftExecuter.applyFilter(value, dimOrdinalMax) ||
        rightExecuter.applyFilter(value, dimOrdinalMax);
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue) {
    BitSet leftFilters = leftExecuter.isScanRequired(blockMaxValue, blockMinValue);
    BitSet rightFilters = rightExecuter.isScanRequired(blockMaxValue, blockMinValue);
    leftFilters.or(rightFilters);
    return leftFilters;
  }

  @Override public void readBlocks(BlocksChunkHolder blockChunkHolder) throws IOException {
    leftExecuter.readBlocks(blockChunkHolder);
    rightExecuter.readBlocks(blockChunkHolder);
  }
}
