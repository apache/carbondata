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
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedResult;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedSortMdkResult;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Non filter processor which will be used for non filter query
 * In case of non filter query we just need to read all the blocks requested in the
 * query and pass it to scanned result
 */
public class NonFilterSortMdkScanner extends NonFilterScanner {
  private SortOrderType sortType = SortOrderType.ASC;
  public NonFilterSortMdkScanner(BlockExecutionInfo blockExecutionInfo,
                          QueryStatisticsModel queryStatisticsModel, SortOrderType sortType) {
    super(blockExecutionInfo, queryStatisticsModel);
    this.sortType = sortType;
  }

  @Override
  protected AbstractScannedResult getScannedResult() {
    if (SortOrderType.ASC.equals(sortType)) {
      return new NonFilterQueryScannedResult(blockExecutionInfo);
    }
    return new NonFilterQueryScannedSortMdkResult(blockExecutionInfo);
  }
}
