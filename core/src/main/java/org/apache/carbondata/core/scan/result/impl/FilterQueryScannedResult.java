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
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedResult extends BlockletScannedResult {

  public FilterQueryScannedResult(BlockExecutionInfo tableBlockExecutionInfos,
      QueryStatisticsModel queryStatisticsModel) {
    super(tableBlockExecutionInfos, queryStatisticsModel);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override public int[] getDictionaryKeyIntegerArray() {
    ++currentRow;
    return getDictionaryKeyIntegerArray(pageFilteredRowId[pageCounter][currentRow]);
  }

  @Override public List<byte[]> getDictionaryKeyArrayBatch(int batchSize) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(pageFilteredRowId[pageCounter][currentRow]);
  }

  @Override public List<byte[][]> getComplexTypeKeyArrayBatch(int batchSize) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(pageFilteredRowId[pageCounter][currentRow]);
  }

  @Override public List<byte[][]> getNoDictionaryKeyArrayBatch(int batchSize) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrentRowId() {
    return pageFilteredRowId[pageCounter][currentRow];
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int chunkIndex : this.dictionaryColumnChunkIndexes) {
      column = dimensionColumnPages[chunkIndex][pageCounter].fillVector(
          pageFilteredRowId[pageCounter],
          vectorInfo,
          column);
    }
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarNoDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    for (int index = 0; index < this.noDictionaryColumnChunkIndexes.length; index++) {
      dimensionColumnPages[noDictionaryColumnChunkIndexes[index]][pageCounter]
          .fillVector(pageFilteredRowId[pageCounter], vectorInfo, index);
    }
  }

  /**
   * Fill the measure column data to vector
   */
  public void fillColumnarMeasureBatch(ColumnVectorInfo[] vectorInfo, int[] measuresOrdinal) {
    for (int i = 0; i < measuresOrdinal.length; i++) {
      vectorInfo[i].measureVectorFiller.fillMeasureVector(
          pageFilteredRowId[pageCounter],
          measureColumnPages[measuresOrdinal[i]][pageCounter],
          vectorInfo[i]);
    }
  }
}
