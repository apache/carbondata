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
package org.apache.carbondata.scan.processor;

import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.scan.collector.ScannedResultCollector;
import org.apache.carbondata.scan.collector.impl.DictionaryBasedResultCollector;
import org.apache.carbondata.scan.collector.impl.RawBasedResultCollector;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.scanner.BlockletScanner;
import org.apache.carbondata.scan.scanner.impl.FilterScanner;
import org.apache.carbondata.scan.scanner.impl.NonFilterScanner;

/**
 * This abstract class provides a skeletal implementation of the
 * Block iterator.
 */
public abstract class AbstractDataBlockIterator extends CarbonIterator<List<Object[]>> {

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

  QueryStatisticsModel queryStatisticsModel;

  public AbstractDataBlockIterator(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, int batchSize, QueryStatisticsModel queryStatisticsModel) {
    this.blockExecutionInfo = blockExecutionInfo;
    dataBlockIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
    blocksChunkHolder = new BlocksChunkHolder(blockExecutionInfo.getTotalNumberDimensionBlock(),
        blockExecutionInfo.getTotalNumberOfMeasureBlock());
    blocksChunkHolder.setFileReader(fileReader);

    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new FilterScanner(blockExecutionInfo, queryStatisticsModel);
    } else {
      blockletScanner = new NonFilterScanner(blockExecutionInfo);
    }
    if (blockExecutionInfo.isRawRecordDetailQuery()) {
      this.scannerResultAggregator =
          new RawBasedResultCollector(blockExecutionInfo);
    } else {
      this.scannerResultAggregator =
          new DictionaryBasedResultCollector(blockExecutionInfo);
    }
    this.batchSize = batchSize;
    this.queryStatisticsModel = queryStatisticsModel;
  }

  public boolean hasNext() {
    if (scannedResult != null && scannedResult.hasNext()) {
      return true;
    } else {
      return dataBlockIterator.hasNext();
    }
  }

  protected boolean updateScanner() {
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
