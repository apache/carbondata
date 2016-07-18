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
package org.carbondata.query.carbon.result.iterator;

import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.carbon.datastore.DataRefNode;
import org.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.carbondata.core.constants.CarbonCommonConstants;
import org.carbondata.core.iterator.CarbonIterator;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.query.carbon.executor.impl.QueryExecutorProperties;
import org.carbondata.query.carbon.executor.infos.BlockExecutionInfo;
import org.carbondata.query.carbon.executor.internal.InternalQueryExecutor;
import org.carbondata.query.carbon.model.QueryModel;

/**
 * In case of detail query we cannot keep all the records in memory so for
 * executing that query are returning a iterator over block and every time next
 * call will come it will execute the block and return the result
 */
public abstract class AbstractDetailQueryResultIterator extends CarbonIterator {

  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractDetailQueryResultIterator.class.getName());

  /**
   * execution info of the block
   */
  protected List<BlockExecutionInfo> blockExecutionInfos;

  /**
   * executor which will execute the query
   */
  protected InternalQueryExecutor executor;
  /**
   * current counter to check how blocklet has been executed
   */
  protected long currentCounter;
  /**
   * block index to be executed
   */
  protected int[] blockIndexToBeExecuted;
  /**
   * to store the statistics
   */
  protected QueryStatistic statistic;
  /**
   * total number of records processed
   */
  protected long totalNumberOfOutputRecords;
  /**
   * number of cores which can be used
   */
  private long numberOfCores;
  /**
   * keep track of number of blocklet per block
   */
  private long[] totalNumberBlockletPerSlice;
  /**
   * total number of blocklet to be executed
   */
  private long totalNumberOfNode;
  /**
   * keep the track of number of blocklet of a block has been executed
   */
  private long[] numberOfBlockletExecutedPerBlock;

  public AbstractDetailQueryResultIterator(List<BlockExecutionInfo> infos,
      QueryExecutorProperties executerProperties, QueryModel queryModel,
      InternalQueryExecutor queryExecutor) {
    int recordSize = 0;
    String defaultInMemoryRecordsSize =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.INMEMORY_REOCRD_SIZE);
    if (null != defaultInMemoryRecordsSize) {
      try {
        recordSize = Integer.parseInt(defaultInMemoryRecordsSize);
      } catch (NumberFormatException ne) {
        LOGGER.error("Invalid inmemory records size. Using default value");
        recordSize = CarbonCommonConstants.INMEMORY_REOCRD_SIZE_DEFAULT;
      }
    }
    this.numberOfCores = recordSize / Integer.parseInt(CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BLOCKLET_SIZE,
            CarbonCommonConstants.BLOCKLET_SIZE_DEFAULT_VAL));
    if (numberOfCores == 0) {
      numberOfCores++;
    }
    executor = queryExecutor;
    this.blockExecutionInfos = infos;
    this.blockIndexToBeExecuted = new int[(int) numberOfCores];
    statistic = new QueryStatistic();
    intialiseInfos();
  }

  private void intialiseInfos() {
    this.totalNumberBlockletPerSlice = new long[blockExecutionInfos.size()];
    this.numberOfBlockletExecutedPerBlock = new long[blockExecutionInfos.size()];
    int index = -1;
    for (BlockExecutionInfo blockInfo : blockExecutionInfos) {
      ++index;
      DataRefNodeFinder finder = new BTreeDataRefNodeFinder(blockInfo.getEachColumnValueSize());
      DataRefNode startDataBlock = finder
          .findFirstDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getStartKey());
      DataRefNode endDataBlock = finder
          .findLastDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getEndKey());

      this.totalNumberBlockletPerSlice[index] =
          endDataBlock.nodeNumber() - startDataBlock.nodeNumber() + 1;
      totalNumberOfNode += this.totalNumberBlockletPerSlice[index];
      blockInfo.setFirstDataBlock(startDataBlock);
      blockInfo.setNumberOfBlockToScan(1);
    }

  }

  @Override public boolean hasNext() {
    if (currentCounter < totalNumberOfNode) {
      return true;
    }
    statistic.addStatistics("Time taken to processed " + blockExecutionInfos.size()
            + " block(s) of output record size: " + totalNumberOfOutputRecords,
        System.currentTimeMillis());
    blockExecutionInfos.get(0).getStatisticsRecorder().recordStatistics(statistic);
    return false;
  }

  protected int updateSliceIndexToBeExecuted() {
    Arrays.fill(blockIndexToBeExecuted, -1);
    int currentSliceIndex = 0;
    int i = 0;
    for (; i < (int) numberOfCores; ) {
      if (this.totalNumberBlockletPerSlice[currentSliceIndex]
          > this.numberOfBlockletExecutedPerBlock[currentSliceIndex]) {
        this.numberOfBlockletExecutedPerBlock[currentSliceIndex]++;
        blockIndexToBeExecuted[i] = currentSliceIndex;
        i++;
      }
      currentSliceIndex++;
      if (currentSliceIndex >= totalNumberBlockletPerSlice.length) {
        break;
      }
    }
    return i;
  }

}
