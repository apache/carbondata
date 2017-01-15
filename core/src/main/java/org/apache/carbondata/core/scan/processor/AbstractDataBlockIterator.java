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
package org.apache.carbondata.core.scan.processor;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.collector.impl.DictionaryBasedResultCollector;
import org.apache.carbondata.core.scan.collector.impl.DictionaryBasedVectorResultCollector;
import org.apache.carbondata.core.scan.collector.impl.RawBasedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.scan.scanner.impl.FilterScanner;
import org.apache.carbondata.core.scan.scanner.impl.NonFilterScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

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

  public AbstractDataBlockIterator(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      int batchSize, QueryStatisticsModel queryStatisticsModel,
      BlocksChunkHolder blockChunkHolder) {
    dataBlockIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
    blocksChunkHolder = blockChunkHolder;
    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new FilterScanner(blockExecutionInfo, queryStatisticsModel);
    } else {
      blockletScanner = new NonFilterScanner(blockExecutionInfo, queryStatisticsModel);
    }
    if (blockExecutionInfo.isRawRecordDetailQuery()) {
      LOGGER.info("Row based raw collector is used to scan and collect the data");
      this.scannerResultAggregator =
          new RawBasedResultCollector(blockExecutionInfo);
    } else if (blockExecutionInfo.isVectorBatchCollector()) {
      LOGGER.info("Vector based dictionary collector is used to scan and collect the data");
      this.scannerResultAggregator =
          new DictionaryBasedVectorResultCollector(blockExecutionInfo);
    } else {
      LOGGER.info("Row based dictionary collector is used to scan and collect the data");
      this.scannerResultAggregator =
          new DictionaryBasedResultCollector(blockExecutionInfo);
    }
    this.batchSize = batchSize;
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
    } catch (IOException | FilterUnsupportedException ex) {
      throw new RuntimeException(ex);
    }
  }

  private AbstractScannedResult getNextScannedResult()
      throws IOException, FilterUnsupportedException {
    if (dataBlockIterator.hasNext()) {
      blocksChunkHolder.setDataBlock(dataBlockIterator.next());
      blocksChunkHolder.reset();
      return blockletScanner.scanBlocklet(blocksChunkHolder);
    }
    return null;
  }

  public abstract void processNextBatch(CarbonColumnarBatch columnarBatch);
}
