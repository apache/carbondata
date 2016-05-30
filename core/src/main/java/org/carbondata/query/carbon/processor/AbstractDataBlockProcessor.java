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
package org.carbondata.query.carbon.processor;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.query.carbon.aggregator.DataAggregator;
import org.carbondata.query.carbon.aggregator.ScannedResultAggregator;
import org.carbondata.query.carbon.aggregator.impl.ListBasedResultAggregator;
import org.carbondata.query.carbon.aggregator.impl.MapBasedResultAggregator;
import org.carbondata.query.carbon.executor.exception.QueryExecutionException;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.scanner.BlockletScanner;
import org.carbondata.query.carbon.scanner.impl.FilterScanner;
import org.carbondata.query.carbon.scanner.impl.NonFilterScanner;

/**
 * This class provides a skeletal implementation of the
 * {@link BlockProcessor} interface to minimize the effort required to
 * implement this interface.
 */
public abstract class AbstractDataBlockProcessor implements BlockProcessor {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDataBlockProcessor.class.getName());
  /**
   * iterator which will be used to iterate over data blocks
   */
  protected CarbonIterator<DataRefNode> dataBlockIterator;

  /**
   * execution details
   */
  protected BlockExecutionInfo blockExecutionInfo;

  /**
   * result aggregator which will be used to aggregate the scanned result
   */
  protected ScannedResultAggregator scannerResultAggregator;

  /**
   * processor which will be used to process the block processing can be
   * filter processing or non filter processing
   */
  protected BlockletScanner blockletScanner;

  /**
   * to hold the data block
   */
  protected BlocksChunkHolder blocksChunkHolder;

  public AbstractDataBlockProcessor(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader) {
    this.blockExecutionInfo = blockExecutionInfo;
    dataBlockIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
    blocksChunkHolder = new BlocksChunkHolder(blockExecutionInfo.getTotalNumberDimensionBlock(),
        blockExecutionInfo.getTotalNumberOfMeasureBlock());
    blocksChunkHolder.setFileReader(fileReader);

    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new FilterScanner(blockExecutionInfo);
    } else {
      blockletScanner = new NonFilterScanner(blockExecutionInfo);
    }

    if (blockExecutionInfo.isDetailQuery() || blockExecutionInfo.isRawRecordDetailQuery()) {
      this.scannerResultAggregator =
          new ListBasedResultAggregator(blockExecutionInfo, new DataAggregator(blockExecutionInfo));
    } else {
      this.scannerResultAggregator =
          new MapBasedResultAggregator(blockExecutionInfo, new DataAggregator(blockExecutionInfo));
    }
  }

  /**
   * Below method will be used to add the scanned result to scanned result
   * processor
   */
  protected void finishScanning() {
    try {
      this.blockExecutionInfo.getScannedResultProcessor()
          .addScannedResult(scannerResultAggregator.getAggregatedResult());
    } catch (QueryExecutionException e) {
      LOGGER.error(e,
          "Problem while adding the result to Scanned Result Processor");
    }
  }
}
