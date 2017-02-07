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
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.scan.scanner.impl.FilterScanner;
import org.apache.carbondata.core.scan.scanner.impl.NonFilterScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.util.StatisticObject;

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
   * batch size of result
   */
  protected int batchSize;

  protected ExecutorService executorService;

  private Future<AbstractScannedResult> future;

  private Future<BlocksChunkHolder> futureIo;

  protected AbstractScannedResult scannedResult;

  private BlockExecutionInfo blockExecutionInfo;

  private FileHolder fileReader;

  private AtomicBoolean nextBlock;

  private AtomicBoolean nextRead;
  
  private StatisticObject statisticObject;

  public AbstractDataBlockIterator(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, int batchSize, QueryStatisticsModel queryStatisticsModel,
      ExecutorService executorService) {
    this.blockExecutionInfo = blockExecutionInfo;
    this.fileReader = fileReader;
    dataBlockIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
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
    this.executorService = executorService;
    this.nextBlock = new AtomicBoolean(false);
    this.nextRead = new AtomicBoolean(false);
    this.statisticObject = blockExecutionInfo.getStatisticObject();
  }

  public boolean hasNext() {
    if (scannedResult != null && scannedResult.hasNext()) {
      return true;
    } else {
      return dataBlockIterator.hasNext() || nextBlock.get() || nextRead.get();
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
        nextBlock.set(false);
        nextRead.set(false);
        return false;
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private AbstractScannedResult getNextScannedResult() throws Exception {
    long currentTimeMillis = System.currentTimeMillis();
    AbstractScannedResult result = null;
    if (dataBlockIterator.hasNext() || nextBlock.get() || nextRead.get()) {
      if (future == null) {
        future = execute();
      }
      result = future.get();
      nextBlock.set(false);
      if (dataBlockIterator.hasNext() || nextRead.get()) {
        nextBlock.set(true);
        future = execute();
      }
    }
    statisticObject.setBlockletProcessingTime((System.currentTimeMillis()-currentTimeMillis));
    return result;
  }

  private BlocksChunkHolder getBlocksChunkHolder() throws IOException {
    BlocksChunkHolder blocksChunkHolder = getBlocksChunkHolderInternal();
    while (blocksChunkHolder == null && dataBlockIterator.hasNext()) {
      blocksChunkHolder = getBlocksChunkHolderInternal();
    }
    return blocksChunkHolder;
  }

  private BlocksChunkHolder getBlocksChunkHolderInternal() throws IOException {
    BlocksChunkHolder blocksChunkHolder =
        new BlocksChunkHolder(blockExecutionInfo.getTotalNumberDimensionBlock(),
            blockExecutionInfo.getTotalNumberOfMeasureBlock(), fileReader);
    blocksChunkHolder.setDataBlock(dataBlockIterator.next());
    if (blockletScanner.isScanRequired(blocksChunkHolder)) {
      return blocksChunkHolder;
    }
    return null;
  }

  private Future<AbstractScannedResult> execute() {
    return executorService.submit(new Callable<AbstractScannedResult>() {
      @Override public AbstractScannedResult call() throws Exception {
        if (futureIo == null) {
          futureIo = executeRead();
        }
        BlocksChunkHolder blocksChunkHolder = futureIo.get();
        futureIo = null;
        nextRead.set(false);
        if (blocksChunkHolder != null) {
          if (dataBlockIterator.hasNext()) {
            nextRead.set(true);
            futureIo = executeRead();
          }
          statisticObject.setScanBlockletNumber(1);
          return blockletScanner.scanBlocklet(blocksChunkHolder);
        }
        return null;
      }
    });
  }

  private Future<BlocksChunkHolder> executeRead() {
    return executorService.submit(new Callable<BlocksChunkHolder>() {
      @Override public BlocksChunkHolder call() throws Exception {
        if (dataBlockIterator.hasNext()) {
          BlocksChunkHolder blocksChunkHolder = getBlocksChunkHolder();
          if (blocksChunkHolder != null) {
            synchronized (fileReader) {
              long currentTimeMillis = System.currentTimeMillis();
              blockletScanner.readBlocklet(blocksChunkHolder);
              statisticObject.setReadTime((System.currentTimeMillis()-currentTimeMillis));
            }
            return blocksChunkHolder;
          }
        }
        return null;
      }
    });
  }

  public abstract void processNextBatch(CarbonColumnarBatch columnarBatch);
}