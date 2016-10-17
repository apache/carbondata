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
package org.apache.carbondata.scan.scanner.impl;

import java.util.BitSet;

import org.apache.carbondata.core.carbon.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.carbon.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatistic;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsConstants;
import org.apache.carbondata.core.carbon.querystatistics.QueryStatisticsModel;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastorage.store.FileHolder;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.result.impl.FilterQueryScannedResult;
import org.apache.carbondata.scan.scanner.AbstractBlockletScanner;

/**
 * Below class will be used for filter query processing
 * this class will be first apply the filter then it will read the block if
 * required and return the scanned result
 */
public class FilterScanner extends AbstractBlockletScanner {

  /**
   * filter tree
   */
  private FilterExecuter filterExecuter;
  /**
   * this will be used to apply min max
   * this will be useful for dimension column which is on the right side
   * as node finder will always give tentative blocks, if column data stored individually
   * and data is in sorted order then we can check whether filter is in the range of min max or not
   * if it present then only we can apply filter on complete data.
   * this will be very useful in case of sparse data when rows are
   * repeating.
   */
  private boolean isMinMaxEnabled;

  private QueryStatisticsModel queryStatisticsModel;

  public FilterScanner(BlockExecutionInfo blockExecutionInfo,
                       QueryStatisticsModel queryStatisticsModel) {
    super(blockExecutionInfo);
    scannedResult = new FilterQueryScannedResult(blockExecutionInfo);
    // to check whether min max is enabled or not
    String minMaxEnableValue = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.CARBON_QUERY_MIN_MAX_ENABLED,
            CarbonCommonConstants.MIN_MAX_DEFAULT_VALUE);
    if (null != minMaxEnableValue) {
      isMinMaxEnabled = Boolean.parseBoolean(minMaxEnableValue);
    }
    // get the filter tree
    this.filterExecuter = blockExecutionInfo.getFilterExecuterTree();
    this.queryStatisticsModel = queryStatisticsModel;
  }

  /**
   * Below method will be used to process the block
   *
   * @param blocksChunkHolder block chunk holder which holds the data
   * @throws QueryExecutionException
   * @throws FilterUnsupportedException
   */
  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws QueryExecutionException {
    try {
      fillScannedResult(blocksChunkHolder);
    } catch (FilterUnsupportedException e) {
      throw new QueryExecutionException(e.getMessage());
    }
    return scannedResult;
  }

  /**
   * This method will process the data in below order
   * 1. first apply min max on the filter tree and check whether any of the filter
   * is fall on the range of min max, if not then return empty result
   * 2. If filter falls on min max range then apply filter on actual
   * data and get the filtered row index
   * 3. if row index is empty then return the empty result
   * 4. if row indexes is not empty then read only those blocks(measure or dimension)
   * which was present in the query but not present in the filter, as while applying filter
   * some of the blocks where already read and present in chunk holder so not need to
   * read those blocks again, this is to avoid reading of same blocks which was already read
   * 5. Set the blocks and filter indexes to result
   *
   * @param blocksChunkHolder
   * @throws FilterUnsupportedException
   */
  private void fillScannedResult(BlocksChunkHolder blocksChunkHolder)
      throws FilterUnsupportedException {

    scannedResult.reset();
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(
        QueryStatisticsConstants.TOTAL_BLOCKLET_NUM, totalBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(totalBlockletStatistic);
    // apply min max
    if (isMinMaxEnabled) {
      BitSet bitSet = this.filterExecuter
          .isScanRequired(blocksChunkHolder.getDataBlock().getColumnsMaxValue(),
              blocksChunkHolder.getDataBlock().getColumnsMinValue());
      if (bitSet.isEmpty()) {
        scannedResult.setNumberOfRows(0);
        scannedResult.setIndexes(new int[0]);
        return;
      }
    }
    // apply filter on actual data
    BitSet bitSet = this.filterExecuter.applyFilter(blocksChunkHolder);
    // if indexes is empty then return with empty result
    if (bitSet.isEmpty()) {
      scannedResult.setNumberOfRows(0);
      scannedResult.setIndexes(new int[0]);
      return;
    }
    // valid scanned blocklet
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    queryStatisticsModel.getRecorder().recordStatistics(validScannedBlockletStatistic);
    // get the row indexes from bot set
    int[] indexes = new int[bitSet.cardinality()];
    int index = 0;
    for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
      indexes[index++] = i;
    }

    FileHolder fileReader = blocksChunkHolder.getFileReader();
    int[] allSelectedDimensionBlocksIndexes =
        blockExecutionInfo.getAllSelectedDimensionBlocksIndexes();
    DimensionColumnDataChunk[] dimensionColumnDataChunk =
        new DimensionColumnDataChunk[blockExecutionInfo.getTotalNumberDimensionBlock()];
    // read dimension chunk blocks from file which is not present
    for (int i = 0; i < allSelectedDimensionBlocksIndexes.length; i++) {
      if (null == blocksChunkHolder.getDimensionDataChunk()[allSelectedDimensionBlocksIndexes[i]]) {
        dimensionColumnDataChunk[allSelectedDimensionBlocksIndexes[i]] =
            blocksChunkHolder.getDataBlock()
                .getDimensionChunk(fileReader, allSelectedDimensionBlocksIndexes[i]);
      } else {
        dimensionColumnDataChunk[allSelectedDimensionBlocksIndexes[i]] =
            blocksChunkHolder.getDimensionDataChunk()[allSelectedDimensionBlocksIndexes[i]];
      }
    }
    MeasureColumnDataChunk[] measureColumnDataChunk =
        new MeasureColumnDataChunk[blockExecutionInfo.getTotalNumberOfMeasureBlock()];
    int[] allSelectedMeasureBlocksIndexes = blockExecutionInfo.getAllSelectedMeasureBlocksIndexes();

    // read the measure chunk blocks which is not present
    for (int i = 0; i < allSelectedMeasureBlocksIndexes.length; i++) {

      if (null == blocksChunkHolder.getMeasureDataChunk()[allSelectedMeasureBlocksIndexes[i]]) {
        measureColumnDataChunk[allSelectedMeasureBlocksIndexes[i]] =
            blocksChunkHolder.getDataBlock()
                .getMeasureChunk(fileReader, allSelectedMeasureBlocksIndexes[i]);
      } else {
        measureColumnDataChunk[allSelectedMeasureBlocksIndexes[i]] =
            blocksChunkHolder.getMeasureDataChunk()[allSelectedMeasureBlocksIndexes[i]];
      }
    }
    scannedResult.setDimensionChunks(dimensionColumnDataChunk);
    scannedResult.setIndexes(indexes);
    scannedResult.setMeasureChunks(measureColumnDataChunk);
    scannedResult.setNumberOfRows(indexes.length);
  }
}
