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

import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;

/**
 * This filter executor class will be called when the CDC pruning is enabled.
 */
public class CDCBlockImplicitExecutorImpl implements FilterExecutor, ImplicitColumnFilterExecutor {

  private final Set<String> blocksToScan;

  public CDCBlockImplicitExecutorImpl(Set<String> blocksToScan) {
    this.blocksToScan = blocksToScan;
  }

  @Override
  public BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
      String uniqueBlockPath, boolean[] isMinMaxSet) {
    BitSet bitSet = new BitSet(1);
    if (blocksToScan.contains(uniqueBlockPath)) {
      bitSet.set(0);
    }
    return bitSet;
  }

  @Override
  public Boolean isFilterValuesPresentInAbstractIndex(byte[][] maxValue, byte[][] minValue,
      boolean[] isMinMaxSet) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    throw new UnsupportedOperationException("Unsupported operation");
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) {
    throw new UnsupportedOperationException("Unsupported operation");
  }
}
