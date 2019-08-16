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

import java.util.List;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.scan.collector.ResultCollectorFactory;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFilterScanner;
import org.apache.carbondata.core.scan.scanner.impl.BlockletFullScanner;

/**
 * Below class will be used to iterate over data block
 */
public class BlockletIterator extends CarbonIterator<BlockletIterator.DataRefNodeWrapper> {
  /**
   * data store block
   */
  protected DataRefNodeWrapper nodeWrapper;
  /**
   * block counter to keep a track how many block has been processed
   */
  private int blockCounter;

  /**
   * flag to be used to check any more data block is present or not
   */
  private boolean hasNext = true;

  /**
   * total number blocks assgned to this iterator
   */
  private long totalNumberOfBlocksToScan;

  private List<BlockExecutionInfo> blockExecutionInfos;

  private BlockExecutionInfo executionInfo;
  /**
   * Constructor
   *
   * @param blockExecutionInfos                 blocks to scan
   */
  BlockletIterator(List<BlockExecutionInfo> blockExecutionInfos) {
    this.blockExecutionInfos = blockExecutionInfos;
    this.executionInfo = blockExecutionInfos.remove(0);
    this.nodeWrapper = getWrapper(executionInfo, true);
    this.totalNumberOfBlocksToScan = executionInfo.getNumberOfBlockToScan();
  }

  /**
   * is all the blocks assigned to this iterator has been processed
   */
  @Override
  public boolean hasNext() {
    return hasNext;
  }


  /**
   * To get the next block
   * @return next data block
   *
   */
  @Override
  public DataRefNodeWrapper next() {
    // get the current blocks
    DataRefNodeWrapper datablockTemp = nodeWrapper;
    // store the next data block
    nodeWrapper = getWrapper(this.executionInfo, false);
    // increment the counter
    blockCounter++;
    // if all the data block is processed then
    // set the has next flag to false
    // or if number of blocks assigned to this iterator is processed
    // then also set the hasnext flag to false
    if (null == nodeWrapper || blockCounter >= this.totalNumberOfBlocksToScan) {
      if (blockExecutionInfos.size() > 0) {
        this.executionInfo = blockExecutionInfos.remove(0);
        this.nodeWrapper = getWrapper(executionInfo, true);
        this.totalNumberOfBlocksToScan = executionInfo.getNumberOfBlockToScan();
        blockCounter = 0;
      } else {
        hasNext = false;
      }
    }
    return datablockTemp;
  }

  private DataRefNodeWrapper getWrapper(BlockExecutionInfo executionInfo, boolean isFirst) {
    DataRefNode nextDataRefNode;
    if (isFirst) {
      nextDataRefNode = executionInfo.getFirstDataBlock();
    } else {
      nextDataRefNode = nodeWrapper.datablock.getNextDataRefNode();
      if (nextDataRefNode == null) {
        return null;
      }
    }
    DataRefNodeWrapper nodeWrapper = new DataRefNodeWrapper();
    if (executionInfo.getFilterExecuterTree() != null) {
      nodeWrapper.blockletScanner =
          new BlockletFilterScanner(executionInfo, executionInfo.getQueryStatisticsModel());
    } else {
      nodeWrapper.blockletScanner =
          new BlockletFullScanner(executionInfo, executionInfo.getQueryStatisticsModel());
    }
    nodeWrapper.scannerResultAggregator =
        ResultCollectorFactory.getScannedResultCollector(executionInfo);
    nodeWrapper.datablock = nextDataRefNode;
    nodeWrapper.executionInfo = executionInfo;
    return nodeWrapper;
  }

  public static class DataRefNodeWrapper {

    DataRefNode datablock;

    /**
     * result collector which will be used to aggregate the scanned result
     */
    ScannedResultCollector scannerResultAggregator;

    /**
     * processor which will be used to process the block processing can be
     * filter processing or non filter processing
     */
    BlockletScanner blockletScanner;

    BlockExecutionInfo executionInfo;

  }
}
