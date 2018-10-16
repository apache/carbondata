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

public class AndFilterExecuterImpl implements FilterExecuter, ImplicitColumnFilterExecutor {

  private FilterExecuter leftExecuter;
  private FilterExecuter rightExecuter;

  public AndFilterExecuterImpl(FilterExecuter leftExecuter, FilterExecuter rightExecuter) {
    this.leftExecuter = leftExecuter;
    this.rightExecuter = rightExecuter;
  }

  @Override
  public BitSetGroup applyFilter(RawBlockletColumnChunks rawBlockletColumnChunks,
      boolean useBitsetPipeLine) throws FilterUnsupportedException, IOException {
    BitSetGroup leftFilters = leftExecuter.applyFilter(rawBlockletColumnChunks, useBitsetPipeLine);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSetGroup rightFilter = rightExecuter.applyFilter(rawBlockletColumnChunks, useBitsetPipeLine);
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
    BitSet leftFilters = leftExecuter.prunePages(rawBlockletColumnChunks);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = rightExecuter.prunePages(rawBlockletColumnChunks);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    return leftFilters;
  }

  @Override public boolean applyFilter(RowIntf value, int dimOrdinalMax)
      throws FilterUnsupportedException, IOException {
    return leftExecuter.applyFilter(value, dimOrdinalMax) &&
        rightExecuter.applyFilter(value, dimOrdinalMax);
  }

  @Override public BitSet isScanRequired(byte[][] blockMaxValue, byte[][] blockMinValue,
      boolean[] isMinMaxSet) {
    BitSet leftFilters = leftExecuter.isScanRequired(blockMaxValue, blockMinValue, isMinMaxSet);
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = rightExecuter.isScanRequired(blockMaxValue, blockMinValue, isMinMaxSet);
    if (rightFilter.isEmpty()) {
      return rightFilter;
    }
    leftFilters.and(rightFilter);
    return leftFilters;
  }

  @Override
  public void readColumnChunks(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    leftExecuter.readColumnChunks(rawBlockletColumnChunks);
    rightExecuter.readColumnChunks(rawBlockletColumnChunks);
  }

  @Override
  public BitSet isFilterValuesPresentInBlockOrBlocklet(byte[][] maxValue, byte[][] minValue,
      String uniqueBlockPath, boolean[] isMinMaxSet) {
    BitSet leftFilters = null;
    if (leftExecuter instanceof ImplicitColumnFilterExecutor) {
      leftFilters = ((ImplicitColumnFilterExecutor) leftExecuter)
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue,uniqueBlockPath, isMinMaxSet);
    } else {
      leftFilters = leftExecuter
          .isScanRequired(maxValue, minValue, isMinMaxSet);
    }
    if (leftFilters.isEmpty()) {
      return leftFilters;
    }
    BitSet rightFilter = null;
    if (rightExecuter instanceof ImplicitColumnFilterExecutor) {
      rightFilter = ((ImplicitColumnFilterExecutor) rightExecuter)
          .isFilterValuesPresentInBlockOrBlocklet(maxValue, minValue, uniqueBlockPath, isMinMaxSet);
    } else {
      rightFilter = rightExecuter.isScanRequired(maxValue, minValue, isMinMaxSet);
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
    if (leftExecuter instanceof ImplicitColumnFilterExecutor) {
      leftRes = ((ImplicitColumnFilterExecutor) leftExecuter)
          .isFilterValuesPresentInAbstractIndex(maxValue, minValue, isMinMaxSet);
    } else {
      tempFilter = leftExecuter
          .isScanRequired(maxValue, minValue, isMinMaxSet);
      leftRes = !tempFilter.isEmpty();
    }
    if (!leftRes) {
      return leftRes;
    }

    Boolean rightRes = null;
    if (rightExecuter instanceof ImplicitColumnFilterExecutor) {
      rightRes = ((ImplicitColumnFilterExecutor) rightExecuter)
          .isFilterValuesPresentInAbstractIndex(maxValue, minValue, isMinMaxSet);
    } else {
      tempFilter = rightExecuter
          .isScanRequired(maxValue, minValue, isMinMaxSet);
      rightRes = !tempFilter.isEmpty();
    }

    // Equivalent to leftRes && rightRes.
    return rightRes;
  }
}
