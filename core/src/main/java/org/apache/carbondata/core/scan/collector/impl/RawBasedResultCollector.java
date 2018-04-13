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

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class RawBasedResultCollector extends AbstractScannedResultCollector {

  public RawBasedResultCollector(BlockExecutionInfo blockExecutionInfos,
      QueryStatisticsModel queryStatisticsModel) {
    super(blockExecutionInfos, queryStatisticsModel);
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {
    long startTime = System.currentTimeMillis();
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    QueryMeasure[] queryMeasures = tableBlockExecutionInfos.getQueryMeasures();
    // scan the record and add to list
    scanAndFillData(scannedResult, batchSize, listBasedResult, queryMeasures);
    QueryStatistic resultPrepTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.RESULT_PREP_TIME);
    resultPrepTime.addCountStatistic(QueryStatisticsConstants.RESULT_PREP_TIME,
        resultPrepTime.getCount() + (System.currentTimeMillis() - startTime));
    return listBasedResult;
  }

  /**
   * This method will scan and fill dimension and measure data
   *
   * @param scannedResult
   * @param batchSize
   * @param listBasedResult
   * @param queryMeasures
   */
  protected void scanAndFillData(AbstractScannedResult scannedResult, int batchSize,
      List<Object[]> listBasedResult, QueryMeasure[] queryMeasures) {
    int numberOfPages = scannedResult.numberOfpages();
    // loop will exit once the batchSize data has been read or the pages have been exhausted
    while (scannedResult.getCurrentPageCounter() < numberOfPages) {
      int currentPageRowCount = scannedResult.getCurrentPageRowCount();
      if (currentPageRowCount == 0) {
        scannedResult.incrementPageCounter();
        continue;
      }
      int rowCounter = scannedResult.getRowCounter();
      // getRowCounter holds total number rows processed. Calculate the
      // Left over space through getRowCounter only.
      int availableRows = currentPageRowCount - rowCounter;
      // rows available in current page that can be processed from current page
      int availableBatchRowCount = Math.min(batchSize, availableRows);
      // this condition will be true if no data left in the current block/blocklet to be scanned
      if (availableBatchRowCount < 1) {
        break;
      }
      if (batchSize > availableRows) {
        batchSize = batchSize - availableRows;
      } else {
        // this is done because in IUD cases actuals rows fetch can be less than batch size as
        // some of the rows could have deleted. So in those cases batchSize need to be
        // re initialized with left over value
        batchSize = 0;
      }
      // fill dimension data
      fillDimensionData(scannedResult, listBasedResult, queryMeasures, availableBatchRowCount);
      fillMeasureData(scannedResult, listBasedResult);
      // increment the number of rows scanned in scanned result statistics
      incrementScannedResultRowCounter(scannedResult, availableBatchRowCount);
      // assign the left over rows to batch size if the number of rows fetched are lesser
      // than batchSize
      if (listBasedResult.size() < availableBatchRowCount) {
        batchSize += availableBatchRowCount - listBasedResult.size();
      }
    }
  }

  private void fillDimensionData(AbstractScannedResult scannedResult,
      List<Object[]> listBasedResult, QueryMeasure[] queryMeasures, int batchSize) {
    long startTime = System.currentTimeMillis();
    List<byte[]> dictionaryKeyArrayBatch = scannedResult.getDictionaryKeyArrayBatch(batchSize);
    List<byte[][]> noDictionaryKeyArrayBatch =
        scannedResult.getNoDictionaryKeyArrayBatch(batchSize);
    List<byte[][]> complexTypeKeyArrayBatch = scannedResult.getComplexTypeKeyArrayBatch(batchSize);
    // it will same for one blocklet so can be computed only once
    byte[] implicitColumnByteArray = scannedResult.getBlockletId()
        .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    // Note: size check in for loop is for dictionaryKeyArrayBatch as this size can be lesser than
    // batch size in case of IUD scenarios
    for (int i = 0; i < dictionaryKeyArrayBatch.size(); i++) {
      // 1 for ByteArrayWrapper object which will contain dictionary and no dictionary data
      Object[] row = new Object[1 + queryMeasures.length];
      ByteArrayWrapper wrapper = new ByteArrayWrapper();
      wrapper.setDictionaryKey(dictionaryKeyArrayBatch.get(i));
      wrapper.setNoDictionaryKeys(noDictionaryKeyArrayBatch.get(i));
      wrapper.setComplexTypesKeys(complexTypeKeyArrayBatch.get(i));
      wrapper.setImplicitColumnByteArray(implicitColumnByteArray);
      row[0] = wrapper;
      listBasedResult.add(row);
    }
    QueryStatistic dimensionFillingTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.DIMENSION_FILLING_TIME);
    dimensionFillingTime.addCountStatistic(QueryStatisticsConstants.DIMENSION_FILLING_TIME,
        dimensionFillingTime.getCount() + (System.currentTimeMillis() - startTime));
  }

  private void fillMeasureData(AbstractScannedResult scannedResult,
      List<Object[]> listBasedResult) {
    long startTime = System.currentTimeMillis();
    // if list is not empty after filling the dimension data then only fill the measure data
    if (!listBasedResult.isEmpty()) {
      fillMeasureDataBatch(listBasedResult, 1, scannedResult);
    }
    QueryStatistic measureFillingTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.MEASURE_FILLING_TIME);
    measureFillingTime.addCountStatistic(QueryStatisticsConstants.MEASURE_FILLING_TIME,
        measureFillingTime.getCount() + (System.currentTimeMillis() - startTime));
  }

  private void incrementScannedResultRowCounter(AbstractScannedResult scannedResult,
      int batchSize) {
    // increment row counter by batch size as those many number of rows have been processed at once
    scannedResult.incrementCounter(batchSize);
  }

}

