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
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.util.BitSetGroup;

public class AndFilterExecutorImpl implements FilterExecutor, ImplicitColumnFilterExecutor {

  private FilterExecutor leftExecutor;
  private FilterExecutor rightExecutor;

  public AndFilterExecutorImpl(FilterExecutor leftExecutor, FilterExecutor rightExecutor) {
    this.leftExecutor = leftExecutor;
    this.rightExecutor = rightExecutor;
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
    BitSetGroup leftFilters = leftExecutor.applyFilter(rawBlockletColumnChunks, useBitsetPipeLine);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSetGroup rightFilter = rightExecutor.applyFilter(rawBlockletColumnChunks, useBitsetPipeLine);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    rawBlockletColumnChunks.setBitSetGroup(leftFilters);
    return leftFilters;
  }

  @Override
  public BitSet prunePages(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws FilterUnsupportedException, IOException {
    BitSet leftFilters = leftExecutor.prunePages(rawBlockletColumnChunks);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = rightExecutor.prunePages(rawBlockletColumnChunks);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    return leftFilters;
  }

  @Override
  public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    return leftExecutor.applyFilter(value, dimOrdinalMax) &&
        rightExecutor.applyFilter(value, dimOrdinalMax);
  }

  @Override
  public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet leftFilters = leftExecutor.isScanRequired(blockMaxValue, blockMinValue, isMinMaxSet);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = rightExecutor.isScanRequired(blockMaxValue, blockMinValue, isMinMaxSet);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    return leftFilters;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    leftExecutor.readColumnChunks(rawBlockletColumnChunks);
    rightExecutor.readColumnChunks(rawBlockletColumnChunks);
  }

  @Override
  public BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
      String uniqueBlockPath, boolean[] isMinMaxSet) {
    BitSet leftFilters = null;
    if (leftExecutor instanceof ImplicitColumnFilterExecutor) {
      leftFilters = ((ImplicitColumnFilterExecutor) leftExecutor)
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue, uniqueBlockPath, isMinMaxSet);
    } else {
      leftFilters = leftExecutor
          .isScanRequired(maxValue, minValue, isMinMaxSet);
    }
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = null;
    if (rightExecutor instanceof ImplicitColumnFilterExecutor) {
      rightFilter = ((ImplicitColumnFilterExecutor) rightExecutor)
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue, uniqueBlockPath, isMinMaxSet);
    } else {
      rightFilter = rightExecutor.isScanRequired(maxValue, minValue, isMinMaxSet);
    }
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    return leftFilters;
  }

  @Override
  public Boolean isFilterValuesPresentInAbstractIndex(byte[][] maxValue, byte[][] minValue,
      boolean[] isMinMaxSet) {
    Boolean leftRes;
    BitSet tempFilter;
    if (leftExecutor instanceof ImplicitColumnFilterExecutor) {
      leftRes = ((ImplicitColumnFilterExecutor) leftExecutor)
          .isFilterValuesPresentInAbstractIndex(maxValue, minValue, isMinMaxSet);
    } else {
      tempFilter = leftExecutor
          .isScanRequired(maxValue, minValue, isMinMaxSet);
      leftRes = !tempFilter.isEmpty();
    }
    if (!leftRes) {
      return leftRes;
    }

    Boolean rightRes = null;
    if (rightExecutor instanceof ImplicitColumnFilterExecutor) {
      rightRes = ((ImplicitColumnFilterExecutor) rightExecutor)
          .isFilterValuesPresentInAbstractIndex(maxValue, minValue, isMinMaxSet);
    } else {
      tempFilter = rightExecutor
          .isScanRequired(maxValue, minValue, isMinMaxSet);
      rightRes = !tempFilter.isEmpty();
    }

    // Equivalent to leftRes && rightRes.
    return rightRes;
  }
}
