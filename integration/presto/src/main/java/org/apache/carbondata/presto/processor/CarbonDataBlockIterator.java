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
package org.apache.carbondata.presto.processor;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.processor.AbstractDataBlockIterator;
import org.apache.carbondata.core.scan.processor.BlockletIterator;
import org.apache.carbondata.core.scan.scanner.impl.FilterScanner;
import org.apache.carbondata.core.scan.scanner.impl.NonFilterScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.presto.scan.collector.ResultCollectorFactory;

/**
 * This abstract class provides a skeletal implementation of the
 * Block iterator.
 */
public abstract class CarbonDataBlockIterator extends AbstractDataBlockIterator {

  public CarbonDataBlockIterator(BlockExecutionInfo blockExecutionInfo, FileHolder fileReader,
      int batchSize, QueryStatisticsModel queryStatisticsModel, ExecutorService executorService) {
    super(blockExecutionInfo, fileReader, batchSize, queryStatisticsModel,  executorService);
    this.blockExecutionInfo = blockExecutionInfo;
    this.fileReader = fileReader;
    dataBlockIterator = new BlockletIterator(blockExecutionInfo.getFirstDataBlock(),
        blockExecutionInfo.getNumberOfBlockToScan());
    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new FilterScanner(blockExecutionInfo, queryStatisticsModel);
    } else {
      blockletScanner = new NonFilterScanner(blockExecutionInfo, queryStatisticsModel);
    }
    this.scannerResultAggregator =
        ResultCollectorFactory.getScannedResultCollector(blockExecutionInfo);
    this.batchSize = batchSize;
    this.executorService = executorService;
    this.nextBlock = new AtomicBoolean(false);
    this.nextRead = new AtomicBoolean(false);
  }

  public abstract List<Object[]>  processNextColumnBatch();


}