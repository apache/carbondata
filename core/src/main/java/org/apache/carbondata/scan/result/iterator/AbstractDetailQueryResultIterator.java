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
package org.apache.carbondata.scan.result.iterator;

import java.util.List;
import java.util.concurrent.ExecutorService;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.carbon.datastore.DataRefNode;
import org.apache.carbondata.core.carbon.datastore.DataRefNodeFinder;
import org.apache.carbondata.core.carbon.datastore.impl.btree.BTreeDataRefNodeFinder;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsConstants;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsRecorder;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.datastorage.store.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryModel;
import org.apache.carbondata.scan.processor.AbstractDataBlockIterator;
import org.apache.carbondata.scan.processor.impl.DataBlockIteratorImpl;

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

  protected ExecutorService execService;
  /**
   * execution info of the block
   */
  protected List<BlockExecutionInfo> blockExecutionInfos;
  /**
   * file reader which will be used to execute the query
   */
  protected FileHolder fileReader;
  protected AbstractDataBlockIterator dataBlockIterator;
  protected boolean nextBatch = false;
  /**
   * total time scan the blocks
   */
  protected long totalScanTime;
  /**
   * is the statistic recorded
   */
  protected boolean isStatisticsRecorded;
  /**
   * QueryStatisticsRecorder
   */
  protected QueryStatisticsRecorder recorder;
  /**
   * number of cores which can be used
   */
  private int batchSize;
  /**
   * queryStatisticsModel to store query statistics object
   */
  QueryStatisticsModel queryStatisticsModel;

  public AbstractDetailQueryResultIterator(List<BlockExecutionInfo> infos, QueryModel queryModel,
      ExecutorService execService) {
    String batchSizeString =
        CarbonProperties.getInstance().getProperty(CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE);
    if (null != batchSizeString) {
      try {
        batchSize = Integer.parseInt(batchSizeString);
      } catch (NumberFormatException ne) {
        LOGGER.error("Invalid inmemory records size. Using default value");
        batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
      }
    } else {
      batchSize = CarbonCommonConstants.DETAIL_QUERY_BATCH_SIZE_DEFAULT;
    }
    this.recorder = queryModel.getStatisticsRecorder();
    this.blockExecutionInfos = infos;
    this.fileReader = FileFactory.getFileHolder(
        FileFactory.getFileType(queryModel.getAbsoluteTableIdentifier().getStorePath()));
    this.execService = execService;
    intialiseInfos();
    initQueryStatiticsModel();
  }

  private void intialiseInfos() {
    totalScanTime = System.currentTimeMillis();
    for (BlockExecutionInfo blockInfo : blockExecutionInfos) {
      DataRefNodeFinder finder = new BTreeDataRefNodeFinder(blockInfo.getEachColumnValueSize());
      DataRefNode startDataBlock = finder
          .findFirstDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getStartKey());
      while (startDataBlock.nodeNumber() != blockInfo.getStartBlockletIndex()) {
        startDataBlock = startDataBlock.getNextDataRefNode();
      }

      long numberOfBlockToScan = blockInfo.getNumberOfBlockletToScan();
      //if number of block is less than 0 then take end block.
      if (numberOfBlockToScan <= 0) {
        DataRefNode endDataBlock = finder
            .findLastDataBlock(blockInfo.getDataBlock().getDataRefNode(), blockInfo.getEndKey());
        numberOfBlockToScan = endDataBlock.nodeNumber() - startDataBlock.nodeNumber() + 1;
      }
      blockInfo.setFirstDataBlock(startDataBlock);
      blockInfo.setNumberOfBlockToScan(numberOfBlockToScan);
    }
  }

  @Override public boolean hasNext() {
    if ((dataBlockIterator != null && dataBlockIterator.hasNext()) || nextBatch) {
      return true;
    } else if (blockExecutionInfos.size() > 0) {
      return true;
    } else {
      if (!isStatisticsRecorded) {
        QueryStatistic statistic = new QueryStatistic();
        statistic.addFixedTimeStatistic(QueryStatisticsConstants.SCAN_BLOCKS_TIME,
            System.currentTimeMillis() - totalScanTime);
        recorder.recordStatistics(statistic);
        isStatisticsRecorded = true;
      }
      return false;
    }
  }

  protected void updateDataBlockIterator() {
    if (dataBlockIterator == null || !dataBlockIterator.hasNext()) {
      dataBlockIterator = getDataBlockIterator();
      while (dataBlockIterator != null && !dataBlockIterator.hasNext()) {
        dataBlockIterator = getDataBlockIterator();
      }
    }
  }

  private DataBlockIteratorImpl getDataBlockIterator() {
    if (blockExecutionInfos.size() > 0) {
      BlockExecutionInfo executionInfo = blockExecutionInfos.get(0);
      blockExecutionInfos.remove(executionInfo);
      queryStatisticsModel.setRecorder(recorder);
      return new DataBlockIteratorImpl(executionInfo, fileReader, batchSize, queryStatisticsModel);
    }
    return null;
  }

  protected void initQueryStatiticsModel() {
    this.queryStatisticsModel = new QueryStatisticsModel();
    QueryStatistic queryStatisticTotalBlocklet = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, queryStatisticTotalBlocklet);
    QueryStatistic queryStatisticScanBlocklet = new QueryStatistic();
    QueryStatistic queryStatisticValidScanBlocklet = new QueryStatistic();
    queryStatisticsModel.getStatisticsTypeAndObjMap()
        .put(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM, queryStatisticValidScanBlocklet);
  }

}
