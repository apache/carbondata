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

import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.util.RestructureUtil;
import org.apache.carbondata.core.scan.filter.GenericQueryType;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;

/**
 * class for handling restructure scenarios for filling result
 */
public class RestructureBasedDictionaryResultCollector extends DictionaryBasedResultCollector {

  private Object[] measureDefaultValues = null;

  public RestructureBasedDictionaryResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    queryDimensions = executionInfo.getActualQueryDimensions();
    queryMeasures = executionInfo.getActualQueryMeasures();
    measureDefaultValues = new Object[queryMeasures.length];
    fillMeasureDefaultValues();
    initDimensionAndMeasureIndexesForFillingData();
    initDimensionAndMeasureIndexesForFillingData();
    isDimensionExists = queryDimensions.length > 0;
  }

  /**
   * Fill measure default measure columns
   */
  private void fillMeasureDefaultValues() {
    for (int i = 0; i < queryMeasures.length; i++) {
      if (!measureInfo.getMeasureExists()[i]) {
        measureDefaultValues[i] = RestructureUtil
            .getMeasureDefaultValueByType(queryMeasures[i].getMeasure().getColumnSchema(),
                queryMeasures[i].getMeasure().getDefaultValue());
      }
    }
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {
    // scan the record and add to list
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    int rowCounter = 0;
    int[] surrogateResult;
    byte[][] noDictionaryKeys;
    byte[][] complexTypeKeyArray;
    Map<Integer, GenericQueryType> comlexDimensionInfoMap =
        executionInfo.getComlexDimensionInfoMap();
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      Object[] row = new Object[queryDimensions.length + queryMeasures.length];
      if (isDimensionExists) {
        surrogateResult = scannedResult.getDictionaryKeyIntegerArray();
        noDictionaryKeys = scannedResult.getNoDictionaryKeyArray();
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
      if (scannedResult.containsDeletedRow(scannedResult.getCurrentRowId())) {
        continue;
      }
      fillMeasureData(scannedResult, row);
      listBasedResult.add(row);
      rowCounter++;
    }
    return listBasedResult;
  }

  protected void fillMeasureData(Object[] msrValues, int offset,
      BlockletScannedResult scannedResult) {
    int measureExistIndex = 0;
    for (short i = 0; i < measureInfo.getMeasureDataTypes().length; i++) {
      // if measure exists is block then pass measure column
      // data chunk to the collector
      if (measureInfo.getMeasureExists()[i]) {
        ProjectionMeasure queryMeasure = executionInfo.getProjectionMeasures()[measureExistIndex];
        msrValues[i + offset] = getMeasureData(
            scannedResult.getMeasureChunk(measureInfo.getMeasureOrdinals()[measureExistIndex]),
            scannedResult.getCurrentRowId(), queryMeasure.getMeasure());
        measureExistIndex++;
      } else {
        // if not then get the default value
        msrValues[i + offset] = measureDefaultValues[i];
      }
    }
  }

}
