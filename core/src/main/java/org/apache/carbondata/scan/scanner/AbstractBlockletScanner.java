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
package org.apache.carbondata.scan.scanner;

import java.io.IOException;

import org.apache.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsConstants;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.scan.result.AbstractScannedResult;

/**
 * Blocklet scanner class to process the block
 */
public abstract class AbstractBlockletScanner implements BlockletScanner {

  /**
   * scanner result
   */
  protected AbstractScannedResult scannedResult;

  /**
   * block execution info
   */
  protected BlockExecutionInfo blockExecutionInfo;

  public QueryStatisticsModel queryStatisticsModel;

  public AbstractBlockletScanner(BlockExecutionInfo tableBlockExecutionInfos) {
    this.blockExecutionInfo = tableBlockExecutionInfos;
  }

  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws IOException, FilterUnsupportedException {
    fillKeyValue(blocksChunkHolder);
    return scannedResult;
  }

  protected void fillKeyValue(BlocksChunkHolder blocksChunkHolder) throws IOException {

    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
            .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
            totalBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(totalBlockletStatistic);
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel
            .getStatisticsTypeAndObjMap().get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
            .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
                    validScannedBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(validScannedBlockletStatistic);
    scannedResult.reset();
    scannedResult.setNumberOfRows(blocksChunkHolder.getDataBlock().nodeSize());
    scannedResult.setDimensionChunks(blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedDimensionBlocksIndexes()));
    scannedResult.setMeasureChunks(blocksChunkHolder.getDataBlock()
            .getMeasureChunks(blocksChunkHolder.getFileReader(),
                blockExecutionInfo.getAllSelectedMeasureBlocksIndexes()));
  }
}
