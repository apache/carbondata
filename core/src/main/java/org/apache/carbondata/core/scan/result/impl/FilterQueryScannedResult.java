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
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;

/**
 * Result provider class in case of filter query
 * In case of filter query data will be send
 * based on filtered row index
 */
public class FilterQueryScannedResult extends AbstractScannedResult {

  public FilterQueryScannedResult(BlockExecutionInfo tableBlockExecutionInfos) {
    super(tableBlockExecutionInfos);
  }

  /**
   * @return dictionary key array for all the dictionary dimension
   * selected in query
   */
  @Override public byte[] getDictionaryKeyArray() {
    ++currentRow;
    return getDictionaryKeyArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * @return dictionary key integer array for all the dictionary dimension
   * selected in query
   */
  @Override public int[] getDictionaryKeyIntegerArray() {
    ++currentRow;
    return getDictionaryKeyIntegerArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * Below method will be used to get the complex type key array
   *
   * @return complex type key array
   */
  @Override public byte[][] getComplexTypeKeyArray() {
    return getComplexTypeKeyArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * Below method will be used to get the no dictionary key
   * array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public byte[][] getNoDictionaryKeyArray() {
    return getNoDictionaryKeyArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * Below method will be used to get the no dictionary key
   * string array for all the no dictionary dimension selected in query
   *
   * @return no dictionary key array for all the no dictionary dimension
   */
  @Override public String[] getNoDictionaryKeyStringArray() {
    return getNoDictionaryKeyStringArray(rowMapping[pageCounter][currentRow]);
  }

  /**
   * will return the current valid row id
   *
   * @return valid row id
   */
  @Override public int getCurrentRowId() {
    return rowMapping[pageCounter][currentRow];
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int i = 0; i < this.dictionaryColumnBlockIndexes.length; i++) {
      column = dataChunks[dictionaryColumnBlockIndexes[i]][pageCounter]
          .fillConvertedChunkData(rowMapping[pageCounter], vectorInfo, column,
              columnGroupKeyStructureInfo.get(dictionaryColumnBlockIndexes[i]));
    }
  }

  /**
   * Fill the column data to vector
   */
  public void fillColumnarNoDictionaryBatch(ColumnVectorInfo[] vectorInfo) {
    int column = 0;
    for (int i = 0; i < this.noDictionaryColumnBlockIndexes.length; i++) {
      column = dataChunks[noDictionaryColumnBlockIndexes[i]][pageCounter]
          .fillConvertedChunkData(rowMapping[pageCounter], vectorInfo, column,
              columnGroupKeyStructureInfo.get(noDictionaryColumnBlockIndexes[i]));
    }
  }

  /**
   * Fill the measure column data to vector
   */
  public void fillColumnarMeasureBatch(ColumnVectorInfo[] vectorInfo, int[] measuresOrdinal) {
    for (int i = 0; i < measuresOrdinal.length; i++) {
      vectorInfo[i].measureVectorFiller.fillMeasureVectorForFilter(rowMapping[pageCounter],
          measureDataChunks[measuresOrdinal[i]][pageCounter], vectorInfo[i]);
    }
  }
}
