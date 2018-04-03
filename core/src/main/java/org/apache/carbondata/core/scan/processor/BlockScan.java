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

import java.util.LinkedList;
import java.util.List;

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

public class BlockScan {
  private BlockExecutionInfo blockExecutionInfo;
  private FileReader fileReader;
  private BlockletScanner blockletScanner;
  private BlockletIterator blockletIterator;
  private ScannedResultCollector scannerResultAggregator;

  private List<BlockletScannedResult> scannedResults = new LinkedList<>();
  private int nextResultIndex = 0;
  private BlockletScannedResult curResult;

  public BlockScan(BlockExecutionInfo blockExecutionInfo, FileReader fileReader,
      QueryStatisticsModel queryStatisticsModel) {
    this.blockExecutionInfo = blockExecutionInfo;
    this.fileReader = fileReader;
    this.blockletIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new BlockletFilterScanner(blockExecutionInfo, queryStatisticsModel);
    } else {
      blockletScanner = new BlockletFullScanner(blockExecutionInfo, queryStatisticsModel);
    }
    this.scannerResultAggregator =
        ResultCollectorFactory.getScannedResultCollector(blockExecutionInfo);
  }

  public void scan() throws Exception {
    BlockletScannedResult blockletScannedResult = null;
    while (blockletIterator.hasNext()) {
      DataRefNode dataBlock = blockletIterator.next();
      if (dataBlock.getColumnsMaxValue() == null || blockletScanner.isScanRequired(dataBlock)) {
        RawBlockletColumnChunks rawBlockletColumnChunks =  RawBlockletColumnChunks.newInstance(
            blockExecutionInfo.getTotalNumberDimensionToRead(),
            blockExecutionInfo.getTotalNumberOfMeasureToRead(), fileReader, dataBlock);
        blockletScanner.readBlocklet(rawBlockletColumnChunks);
        blockletScannedResult = blockletScanner.scanBlocklet(rawBlockletColumnChunks);
        if (blockletScannedResult != null && blockletScannedResult.hasNext()) {
          scannedResults.add(blockletScannedResult);
        }
      }
    }
    fileReader.finish();
  }

  public boolean hasNext() {
    if (curResult != null && curResult.hasNext()) {
      return true;
    } else {
      if (null != curResult) {
        curResult.freeMemory();
      }
      if (nextResultIndex < scannedResults.size()) {
        curResult = scannedResults.get(nextResultIndex++);
        return true;
      } else {
        return false;
      }
    }
  }

  public void processNextBatch(CarbonColumnarBatch columnarBatch) {
    this.scannerResultAggregator.collectResultInColumnarBatch(curResult, columnarBatch);
  }

}
