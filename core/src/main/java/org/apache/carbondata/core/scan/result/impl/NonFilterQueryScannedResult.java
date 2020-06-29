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

package org.apache.carbondata.core.scan.result.impl;

import java.util.List;

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Result provide class for non filter query
 * In case of no filter query we need to return
 * complete data
 */
public class NonFilterQueryScannedResult extends BlockletScannedResult {

  public NonFilterQueryScannedResult(BlockExecutionInfo blockExecutionInfo,
      QueryStatisticsModel queryStatisticsModel) {
    super(blockExecutionInfo, queryStatisticsModel);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override
  public int[] getDictionaryKeyIntegerArray() {
    return getDictionaryKeyIntegerArray(currentRow);
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override
  public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(currentRow);
  }

  @Override
  public List<byte[][]> getComplexTypeKeyArrayBatch(int batchSize) {
    return getComplexTypeKeyArrayBatch();
  }

  /**
   * Below method will be used to get the no dictionary key array for all the
   * no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override
  public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(currentRow);
  }

  @Override
  public void fillValidRowIdsBatchFilling(int rowId, int batchSize) {
    // row id will be different for every batch so clear it before filling
//IC see: https://issues.apache.org/jira/browse/CARBONDATA-3343
    clearValidRowIdList();
    int startPosition = rowId;
    for (int i = 0; i < batchSize; i++) {
      if (!containsDeletedRow(startPosition)) {
        validRowIds.add(startPosition);
      }
      startPosition++;
    }
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override
  public int getCurrentRowId() {
    return currentRow;
  }

}
