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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.scan.collector.ResultCollectorFactory;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFilterScanner;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFullScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.util.TaskMetricsMap;

/**
 * This abstract class provides a skeletal implementation of the
 * Block iterator.
 */
public class DataBlockIterator extends CarbonIterator<List<Object[]>> {

  /**
   * iterator which will be used to iterate over blocklets
   */
  private BlockletIterator blockletIterator;

  /**
   * result collector which will be used to aggregate the scanned result
   */
  private ScannedResultCollector scannerResultAggregator;

  /**
   * processor which will be used to process the block processing can be
   * filter processing or non filter processing
   */
  private BlockletScanner blockletScanner;

  /**
   * batch size of result
   */
  private int batchSize;

  private ExecutorService executorService;

  private Future<BlockletScannedResult> future;

  private Future<RawBlockletColumnChunks> futureIo;

  private BlockletScannedResult scannedResult;

  private BlockExecutionInfo blockExecutionInfo;

  private FileReader fileReader;

  private AtomicBoolean nextBlock;

  private AtomicBoolean nextRead;

  public DataBlockIterator(BlockExecutionInfo blockExecutionInfo, FileReader fileReader,
      int batchSize, QueryStatisticsModel queryStatisticsModel, ExecutorService executorService) {
    this.blockExecutionInfo = blockExecutionInfo;
    this.fileReader = fileReader;
    blockletIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new BlockletFilterScanner(blockExecutionInfo, queryStatisticsModel);
    } else {
      blockletScanner = new BlockletFullScanner(blockExecutionInfo, queryStatisticsModel);
    }
    this.scannerResultAggregator =
        ResultCollectorFactory.getScannedResultCollector(blockExecutionInfo);
    this.batchSize = batchSize;
    this.executorService = executorService;
    this.nextBlock = new AtomicBoolean(false);
    this.nextRead = new AtomicBoolean(false);
  }

  @Override
  public List<Object[]> next() {
    List<Object[]> collectedResult = null;
    if (updateScanner()) {
      collectedResult = this.scannerResultAggregator.collectResultInRow(scannedResult, batchSize);
      while (collectedResult.size() < batchSize && updateScanner()) {
        List<Object[]> data = this.scannerResultAggregator
            .collectResultInRow(scannedResult, batchSize - collectedResult.size());
        collectedResult.addAll(data);
      }
    } else {
      collectedResult = new ArrayList<>();
    }
    return collectedResult;
  }

  @Override
  public boolean hasNext() {
    if (scannedResult != null && scannedResult.hasNext()) {
      return true;
    } else {
      if (null != scannedResult) {
        scannedResult.freeMemory();
      }
      return blockletIterator.hasNext() || nextBlock.get() || nextRead.get();
    }
  }

  /**
   * Return true if scan result if non-empty
   */
  private boolean updateScanner() {
    try {
      if (scannedResult != null && scannedResult.hasNext()) {
        return true;
      } else {
        scannedResult = processNextBlocklet();
        while (scannedResult != null) {
          if (scannedResult.hasNext()) {
            return true;
          }
          scannedResult = processNextBlocklet();
        }
        nextBlock.set(false);
        nextRead.set(false);
        return false;
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private BlockletScannedResult processNextBlocklet() throws Exception {
    BlockletScannedResult result = null;
    if (blockExecutionInfo.isPrefetchBlocklet()) {
      if (blockletIterator.hasNext() || nextBlock.get() || nextRead.get()) {
        if (future == null) {
          future = scanNextBlockletAsync();
        }
        result = future.get();
        nextBlock.set(false);
        if (blockletIterator.hasNext() || nextRead.get()) {
          nextBlock.set(true);
          future = scanNextBlockletAsync();
        }
      }
    } else {
      if (blockletIterator.hasNext()) {
        RawBlockletColumnChunks rawChunks = readNextBlockletColumnChunks();
        if (rawChunks != null) {
          result = blockletScanner.scanBlocklet(rawChunks);
        }
      }
    }
    return result;
  }

  private RawBlockletColumnChunks readNextBlockletColumnChunks() throws IOException {
    RawBlockletColumnChunks rawBlockletColumnChunks = getNextBlockletColumnChunks();
    if (rawBlockletColumnChunks != null) {
      blockletScanner.readBlocklet(rawBlockletColumnChunks);
      return rawBlockletColumnChunks;
    }
    return null;
  }

  private RawBlockletColumnChunks getNextBlockletColumnChunks() {
    RawBlockletColumnChunks rawBlockletColumnChunks = null;
    do {
      DataRefNode dataBlock = blockletIterator.next();
      if (dataBlock.getColumnsMaxValue() == null || blockletScanner.isScanRequired(dataBlock)) {
        rawBlockletColumnChunks =  RawBlockletColumnChunks.newInstance(
            blockExecutionInfo.getTotalNumberDimensionToRead(),
            blockExecutionInfo.getTotalNumberOfMeasureToRead(), fileReader, dataBlock);
      }
    } while (rawBlockletColumnChunks == null && blockletIterator.hasNext());
    return rawBlockletColumnChunks;
  }

  private Future<BlockletScannedResult> scanNextBlockletAsync() {
    return executorService.submit(new Callable<BlockletScannedResult>() {
      @Override public BlockletScannedResult call() throws Exception {
        if (futureIo == null) {
          futureIo = readNextBlockletAsync();
        }
        RawBlockletColumnChunks rawBlockletColumnChunks = futureIo.get();
        futureIo = null;
        nextRead.set(false);
        if (rawBlockletColumnChunks != null) {
          if (blockletIterator.hasNext()) {
            nextRead.set(true);
            futureIo = readNextBlockletAsync();
          }
          return blockletScanner.scanBlocklet(rawBlockletColumnChunks);
        }
        return null;
      }
    });
  }

  private Future<RawBlockletColumnChunks> readNextBlockletAsync() {
    return executorService.submit(new Callable<RawBlockletColumnChunks>() {
      @Override public RawBlockletColumnChunks call() throws Exception {
        try {
          TaskMetricsMap.getInstance().registerThreadCallback();
          if (blockletIterator.hasNext()) {
            return readNextBlockletColumnChunks();
          } else {
            return null;
          }
        } finally {
          // update read bytes metrics for this thread
          TaskMetricsMap.getInstance().updateReadBytes(Thread.currentThread().getId());
        }
      }
    });
  }

  public void processNextBatch(CarbonColumnarBatch columnarBatch) {
    if (updateScanner()) {
      this.scannerResultAggregator.collectResultInColumnarBatch(scannedResult, columnarBatch);
    }
  }


  /**
   * Close the resources
   */
  public void close() {
    // free the current scanned result
    if (null != scannedResult && !scannedResult.hasNext()) {
      scannedResult.freeMemory();
    }
    // free any pre-fetched memory if present
    if (null != future) {
      try {
        BlockletScannedResult blockletScannedResult = future.get();
        if (blockletScannedResult != null) {
          blockletScannedResult.freeMemory();
        }
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
  }
}