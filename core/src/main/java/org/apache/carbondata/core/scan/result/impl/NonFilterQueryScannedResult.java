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

import java.util.ArrayList;
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
  @Override public int[] getDictionaryKeyIntegerArray() {
    ++currentRow;
    return getDictionaryKeyIntegerArray(currentRow);
  }

  @Override public List<byte[]> getDictionaryKeyArrayBatch(int batchSize) {
    // rowId from where computing need to start
    int startRowId = currentRow + 1;
    fillValidRowIdsBatchFilling(startRowId, batchSize);
    List<byte[]> dictionaryKeyArrayList = new ArrayList<>(validRowIds.size());
    int[] columnDataOffsets = null;
    byte[] completeKey = null;
    // everyTime it is initialized new as in case of prefetch it can modify the data
    for (int i = 0; i < validRowIds.size(); i++) {
      completeKey = new byte[fixedLengthKeySize];
      dictionaryKeyArrayList.add(completeKey);
    }
    // initialize offset array onli if data is present
    if (this.dictionaryColumnChunkIndexes.length > 0) {
      columnDataOffsets = new int[validRowIds.size()];
    }
    for (int i = 0; i < this.dictionaryColumnChunkIndexes.length; i++) {
      for (int j = 0; j < validRowIds.size(); j++) {
        columnDataOffsets[j] += dimensionColumnPages[dictionaryColumnChunkIndexes[i]][pageCounter]
            .fillRawData(validRowIds.get(j), columnDataOffsets[j], dictionaryKeyArrayList.get(j));
      }
    }
    return dictionaryKeyArrayList;
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(currentRow);
  }

  @Override public List<byte[][]> getComplexTypeKeyArrayBatch(int batchSize) {
    return getComplexTypeKeyArrayBatch();
  }

  /**
   * Below method will be used to get the no dictionary key array for all the
   * no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(currentRow);
  }

  /**
   * Below method will be used to get the dimension key array
   * for all the no dictionary dimension present in the query
   * This method will fill the data column wise for the given batch size
   *
   * @return no dictionary keys for all no dictionary dimension
   */
  @Override public List<byte[][]> getNoDictionaryKeyArrayBatch(int batchSize) {
    List<byte[][]> noDictionaryKeyArrayList = new ArrayList<>(validRowIds.size());
    byte[][] noDictionaryColumnsKeys = null;
    // everyTime it is initialized new as in case of prefetch it can modify the data
    for (int i = 0; i < validRowIds.size(); i++) {
      noDictionaryColumnsKeys = new byte[noDictionaryColumnChunkIndexes.length][];
      noDictionaryKeyArrayList.add(noDictionaryColumnsKeys);
    }
    int columnPosition = 0;
    for (int i = 0; i < this.noDictionaryColumnChunkIndexes.length; i++) {
      for (int j = 0; j < validRowIds.size(); j++) {
        byte[][] noDictionaryArray = noDictionaryKeyArrayList.get(j);
        noDictionaryArray[columnPosition] =
            dimensionColumnPages[noDictionaryColumnChunkIndexes[i]][pageCounter]
                .getChunkData(validRowIds.get(j));
      }
      columnPosition++;
    }
    return noDictionaryKeyArrayList;
  }


  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrentRowId() {
    return currentRow;
  }

}
