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

import org.apache.carbondata.core.cache.update.BlockletLevelDeleteDeltaDataCache;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.wrappers.ByteArrayWrapper;


/**
 * It is not a collector it is just a scanned result holder.
 */
public class RawBasedResultCollector extends AbstractScannedResultCollector {

  protected ByteArrayWrapper wrapper;

  protected byte[] dictionaryKeyArray;

  protected byte[][] noDictionaryKeyArray;

  protected byte[][] complexTypeKeyArray;

  protected byte[] implicitColumnByteArray;

  public RawBasedResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
  }

  /**
   * This method will add a record both key and value to list object
   * it will keep track of how many record is processed, to handle limit scenario
   */
  @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {
    List<Object[]> listBasedResult = new ArrayList<>(batchSize);
    QueryMeasure[] queryMeasures = tableBlockExecutionInfos.getQueryMeasures();
    BlockletLevelDeleteDeltaDataCache deleteDeltaDataCache =
        scannedResult.getDeleteDeltaDataCache();
    // scan the record and add to list
    int rowCounter = 0;
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      scanResultAndGetData(scannedResult);
      if (null != deleteDeltaDataCache && deleteDeltaDataCache
          .contains(scannedResult.getCurrentRowId())) {
        continue;
      }
      prepareRow(scannedResult, listBasedResult, queryMeasures);
      rowCounter++;
    }
    return listBasedResult;
  }

  protected void prepareRow(AbstractScannedResult scannedResult, List<Object[]> listBasedResult,
      QueryMeasure[] queryMeasures) {
    Object[] row = new Object[1 + queryMeasures.length];
    wrapper = new ByteArrayWrapper();
    wrapper.setDictionaryKey(dictionaryKeyArray);
    wrapper.setNoDictionaryKeys(noDictionaryKeyArray);
    wrapper.setComplexTypesKeys(complexTypeKeyArray);
    wrapper.setImplicitColumnByteArray(implicitColumnByteArray);
    row[0] = wrapper;
    fillMeasureData(row, 1, scannedResult);
    listBasedResult.add(row);
  }

  protected void scanResultAndGetData(AbstractScannedResult scannedResult) {
    dictionaryKeyArray = scannedResult.getDictionaryKeyArray();
    noDictionaryKeyArray = scannedResult.getNoDictionaryKeyArray();
    complexTypeKeyArray = scannedResult.getComplexTypeKeyArray();
    implicitColumnByteArray = scannedResult.getBlockletId()
        .getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
  }
}
