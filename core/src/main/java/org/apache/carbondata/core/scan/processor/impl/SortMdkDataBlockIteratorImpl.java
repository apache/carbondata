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
package org.apache.carbondata.core.scan.processor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.SortOrderType;
import org.apache.carbondata.core.scan.processor.SortMdkBlockletIterator;
import org.apache.carbondata.core.scan.scanner.impl.FilterSortMdkScanner;
import org.apache.carbondata.core.scan.scanner.impl.NonFilterSortMdkScanner;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Below class will be used to process the block for detail query
 */
public class SortMdkDataBlockIteratorImpl extends DataBlockIteratorImpl {

  /**
   * DataBlockIteratorImpl Constructor
   *
   * @param blockExecutionInfo
   *          execution information
   */
  public SortMdkDataBlockIteratorImpl(BlockExecutionInfo blockExecutionInfo,
      FileHolder fileReader, int batchSize, QueryStatisticsModel queryStatisticsModel,
      ExecutorService executorService, SortOrderType sortType) {
    super(blockExecutionInfo, fileReader, batchSize, queryStatisticsModel, executorService,
        sortType);
  }

  @Override
  protected void getBlockletScanner(BlockExecutionInfo blockExecutionInfo,
      QueryStatisticsModel queryStatisticsModel) {
    if (blockExecutionInfo.getFilterExecuterTree() != null) {
      blockletScanner = new FilterSortMdkScanner(blockExecutionInfo, queryStatisticsModel,
          sortType);
    } else {
      blockletScanner = new NonFilterSortMdkScanner(blockExecutionInfo, queryStatisticsModel,
          sortType);
    }
  }

  @Override
  protected void getBlockIterator(BlockExecutionInfo blockExecutionInfo) {
    List<DataRefNode> dataList = new ArrayList<DataRefNode>(
        blockExecutionInfo.getNumberOfBlockletToScan());
    DataRefNode startDataBlock = blockExecutionInfo.getFirstDataBlock();
    while (startDataBlock != null) {
      dataList.add(startDataBlock);
      startDataBlock = startDataBlock.getNextDataRefNode();
    }
    dataBlockIterator = new SortMdkBlockletIterator(dataList, sortType);
  }

}
