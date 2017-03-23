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
package org.apache.carbondata.core.scan.collector.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;

/**
 * class for handling restructure scenarios for filling result
 */
public class RestructureBasedDictionaryResultCollector extends DictionaryBasedResultCollector {

  public RestructureBasedDictionaryResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    queryDimensions = tableBlockExecutionInfos.getActualQueryDimensions();
    queryMeasures = tableBlockExecutionInfos.getActualQueryMeasures();
    initDimensionAndMeasureIndexesForFillingData();
    initDimensionAndMeasureIndexesForFillingData();
    isDimensionExists = queryDimensions.length > 0;
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {
    // scan the record and add to list
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    int rowCounter = 0;
    int[] surrogateResult;
    String[] noDictionaryKeys;
    byte[][] complexTypeKeyArray;
    BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache =
        scannedResult.getDeleteDeltaDataCache();
    Map<Integer, GenericQueryType> comlexDimensionInfoMap =
        tableBlockExecutionInfos.getComlexDimensionInfoMap();
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      Object[] row = new Object[queryDimensions.length + queryMeasures.length];
      if (isDimensionExists) {
        surrogateResult = scannedResult.getDictionaryKeyIntegerArray();
        noDictionaryKeys = scannedResult.getNoDictionaryKeyStringArray();
        complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
        dictionaryColumnIndex = 0;
        noDictionaryColumnIndex = 0;
        complexTypeColumnIndex = 0;
        for (int i = 0; i < queryDimensions.length; i++) {
          // fill default value in case the dimension does not exist in the current block
          if (!dimensionInfo.getDimensionExists()[i]) {
            if (dictionaryEncodingArray[i] || directDictionaryEncodingArray[i]) {
              row[order[i]] = dimensionInfo.getDefaultValues()[i];
              dictionaryColumnIndex++;
            } else {
              row[order[i]] = dimensionInfo.getDefaultValues()[i];
            }
            continue;
          }
          fillDimensionData(scannedResult, surrogateResult, noDictionaryKeys, complexTypeKeyArray,
              comlexDimensionInfoMap, row, i);
        }
      } else {
        scannedResult.incrementCounter();
      }
      if (null != deleteDeltaDataCache && deleteDeltaDataCache
          .contains(scannedResult.getCurrentRowId())) {
        continue;
      }
      fillMeasureData(scannedResult, row);
      listBasedResult.add(row);
      rowCounter++;
    }
    return listBasedResult;
  }

}
