/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.carbondata.scan.collector.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryMeasure;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.wrappers.ByteArrayWrapper;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class RawBasedResultCollector extends AbstractScannedResultCollector {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(RawBasedResultCollector.class.getName());

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
    ByteArrayWrapper wrapper = null;
    // scan the record and add to list
    int rowCounter = 0;
    while (scannedResult.hasNext() && rowCounter < batchSize) {
      Object[] row = new Object[1 + queryMeasures.length];
      wrapper = new ByteArrayWrapper();
      wrapper.setDictionaryKey(scannedResult.getDictionaryKeyArray());
      wrapper.setNoDictionaryKeys(scannedResult.getNoDictionaryKeyArray());
      wrapper.setComplexTypesKeys(scannedResult.getComplexTypeKeyArray());
      row[0] = wrapper;
      fillMeasureData(row, 1, scannedResult);
      listBasedResult.add(row);
      rowCounter++;
    }
    updateData(listBasedResult);
    return listBasedResult;
  }
}
