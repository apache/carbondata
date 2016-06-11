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
package org.carbondata.query.carbon.processor.impl;

import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.processor.AbstractDataBlockProcessor;

/**
 * Below class will be used to process the blocks for
 * aggregated query
 */
public class AggregateQueryBlockProcessor extends AbstractDataBlockProcessor {

  /**
   * AggregateQueryScanner constructor
   *
   * @param blockExecutionInfos
   */
  public AggregateQueryBlockProcessor(BlockExecutionInfo tableBlockExecutionInfos,
      FileHolder fileReader) {
    super(tableBlockExecutionInfos, fileReader);
  }

  /**
   * Below method will be used to scan the block
   * then it will call processor to process the data
   * and the it will call aggregator to aggregate the data
   * it will call finish once all the blocks of a table is scanned
   *
   * @throws QueryExecutionException
   */
  @Override public void processBlock() throws QueryExecutionException {
    while (dataBlockIterator.hasNext()) {
      try {
        blocksChunkHolder.setDataBlock(dataBlockIterator.next());
        blocksChunkHolder.reset();
        this.scannerResultAggregator.aggregateData(blockletScanner.scanBlocklet(blocksChunkHolder));
      } catch (Exception e) {
        throw new QueryExecutionException(e);
      }
    }
    finishScanning();
  }
}
