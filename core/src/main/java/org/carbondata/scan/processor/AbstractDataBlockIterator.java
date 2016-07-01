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
package org.carbondata.scan.processor;

import org.carbondata.common.CarbonIterator;
import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.scan.collector.ScannedResultCollector;
import org.carbondata.scan.collector.impl.ListBasedResultCollector;
import org.carbondata.scan.executor.exception.QueryExecutionException;
import org.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.carbondata.scan.result.AbstractScannedResult;
import org.carbondata.scan.result.Result;
import org.carbondata.scan.scanner.BlockletScanner;
import org.carbondata.scan.scanner.impl.FilterScanner;
import org.carbondata.scan.scanner.impl.NonFilterScanner;

/**
 * This abstract class provides a skeletal implementation of the
 * Block iterator.
 */
public abstract class AbstractDataBlockIterator extends CarbonIterator<Result> {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDataBlockIterator.class.getName());
  /**
   * iterator which will be used to iterate over data blocks
   */
  protected CarbonIterator<DataRefNode> dataBlockIterator;

  /**
   * execution details
   */
  protected BlockExecutionInfo blockExecutionInfo;

  /**
   * result collector which will be used to aggregate the scanned result
   */
  protected ScannedResultCollector scannerResultAggregator;

  /**
   * processor which will be used to process the block processing can be
   * filter processing or non filter processing
   */
  protected BlockletScanner blockletScanner;

  /**
   * to hold the data block
   */
  protected BlocksChunkHolder blocksChunkHolder;

  /**
   * batch size of result
   */
  protected int batchSize;

  protected AbstractScannedResult scannedResult;

  public AbstractDataBlockIterator(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, int batchSize) {
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

    this.scannerResultAggregator =
        new ListBasedResultCollector(blockExecutionInfo);
    this.batchSize = batchSize;
  }

  public boolean hasNext() {
    try {
      if (scannedResult != null && scannedResult.hasNext()) {
        return true;
      } else {
        scannedResult = getNextScannedResult();
        while (scannedResult != null) {
          if (scannedResult.hasNext()) {
            return true;
          }
          scannedResult = getNextScannedResult();
        }
        return false;
      }
    } catch (QueryExecutionException ex) {
      throw new RuntimeException(ex);
    }
  }

  private AbstractScannedResult getNextScannedResult() throws QueryExecutionException {
    if (dataBlockIterator.hasNext()) {
      blocksChunkHolder.setDataBlock(dataBlockIterator.next());
      blocksChunkHolder.reset();
      return blockletScanner.scanBlocklet(blocksChunkHolder);
    }
    return null;
  }


}
