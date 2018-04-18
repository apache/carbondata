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

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedResult extends BlockletScannedResult {

  public FilterQueryScannedResult(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  @Override public byte[] getDictionaryKeyArray() {
    ++currentRow;
    return getDictionaryKeyArray(pageFilteredRowId[pageCounter][currentRow]);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override public int[] getDictionaryKeyIntegerArray() {
    ++currentRow;
    return getDictionaryKeyIntegerArray(pageFilteredRowId[pageCounter][currentRow]);
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(pageFilteredRowId[pageCounter][currentRow]);
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
          column,
          columnGroupKeyStructureInfo.get(chunkIndex));
    }
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarNoDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int chunkIndex : this.noDictionaryColumnChunkIndexes) {
      column = dimensionColumnPages[chunkIndex][pageCounter].fillVector(
          pageFilteredRowId[pageCounter],
          vectorInfo,
          column,
          columnGroupKeyStructureInfo.get(chunkIndex));
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
