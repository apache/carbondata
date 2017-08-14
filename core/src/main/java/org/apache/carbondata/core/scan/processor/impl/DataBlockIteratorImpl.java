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
package org.apache.carbondata.core.scan.processor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.processor.AbstractDataBlockIterator;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import static org.apache.carbondata.core.constants.CarbonCommonConstants.COLUMNAR_DATA_READ_BATCH_SIZE;

/**
 * Below class will be used to process the block for detail query
 */
public class DataBlockIteratorImpl extends AbstractDataBlockIterator {
  /**
   * DataBlockIteratorImpl Constructor
   *
   * @param blockExecutionInfo execution information
   */
  public DataBlockIteratorImpl(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      int batchSize, QueryStatisticsModel queryStatisticsModel, ExecutorService executorService) {
    super(blockExecutionInfo, fileReader, batchSize, queryStatisticsModel, executorService);
  }

  /**
   * It scans the block and returns the result with @batchSize
   *
   * @return Result of @batchSize
   */
  public List<Object[]> next() {
    List<Object[]> collectedResult = null;
    batchSize = Integer.parseInt(COLUMNAR_DATA_READ_BATCH_SIZE);
    if (updateScanner()) {
      collectedResult = this.scannerResultAggregator.collectData(scannedResult, batchSize);
      while (collectedResult.size() < batchSize && updateScanner())  {
        List<Object[]> data = this.scannerResultAggregator
            .collectData(scannedResult, batchSize - collectedResult.size());
        collectedResult.addAll(data);
      }
    } else {
      collectedResult = new ArrayList<>();
    }
    return collectedResult;
  }

  public void processNextBatch(CarbonColumnarBatch columnarBatch) {
    if (updateScanner()) {
      this.scannerResultAggregator.collectVectorBatch(scannedResult, columnarBatch);
    }
  }

  @Override public List<Object[]>  processNextColumnBatch() {
    List<Object[]> collectedResult ;
    batchSize = Integer.parseInt(COLUMNAR_DATA_READ_BATCH_SIZE);
    if (updateScanner()) {
      collectedResult = this.scannerResultAggregator.collectData(scannedResult, batchSize);
    } else {
      collectedResult = new ArrayList<>();
    }
    return collectedResult;

  }

}