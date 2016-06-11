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
 * Below class will be used to process the block for detail query
 */
public class DetailQueryBlockProcessor extends AbstractDataBlockProcessor {

  /**
   * counter for number of records processed
   */
  private int counter;

  /**
   * DetailQueryScanner Constructor
   *
   * @param blockExecutionInfo execution information
   */
  public DetailQueryBlockProcessor(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader) {
    super(blockExecutionInfo, fileReader);
  }

  /**
   * Below method will be used scan the blocks and then process the scanned blocks
   * as its a detail query so its will use dummy aggregator
   * to aggregate the data.
   * This scanner will handle the limit scenario if detail query is without order by.
   * In case of detail query once one block is process it will pass to scanned result processor
   * as in this case number of records will be more and it will take more memory
   *
   * @throws QueryExecutionException
   */
  @Override public void processBlock() throws QueryExecutionException {

    while (dataBlockIterator.hasNext()) {
      blocksChunkHolder.setDataBlock(dataBlockIterator.next());
      blocksChunkHolder.reset();
      counter += this.scannerResultAggregator
          .aggregateData(blockletScanner.scanBlocklet(blocksChunkHolder));
      //      finishScanning();
      if (blockExecutionInfo.getLimit() != -1 && counter >= blockExecutionInfo.getLimit()) {
        break;
      }
    }
    finishScanning();
  }

}
