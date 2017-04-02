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

package org.apache.carbondata.core.scan.scanner.impl;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.impl.FilterQueryScannedReverseSortMdkResult;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Below class will be used for filter query processing
 * this class will be first apply the filter then it will read the block if
 * required and return the scanned result
 */
public class FilterSortMdkScanner extends FilterScanner {

  private SortOrderType sortType = SortOrderType.ASC;

  public FilterSortMdkScanner(BlockExecutionInfo blockExecutionInfo,
      QueryStatisticsModel queryStatisticsModel, SortOrderType sortType) {
    super(blockExecutionInfo, queryStatisticsModel);
    this.sortType = sortType;
  }

  @Override
  protected AbstractScannedResult getScannedResult() {
    if (SortOrderType.ASC.equals(sortType)) {
      return super.getScannedResult();
    }
    return new FilterQueryScannedReverseSortMdkResult(blockExecutionInfo);
  }
}
