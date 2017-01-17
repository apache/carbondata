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

package org.apache.carbondata.core.scan.scanner.impl;

import java.io.IOException;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.FileHolder;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.mutate.data.BlockletDeleteDeltaCacheLoader;
import org.apache.carbondata.core.mutate.data.DeleteDeltaCacheLoaderIntf;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.impl.FilterQueryScannedResult;
import org.apache.carbondata.core.scan.scanner.AbstractBlockletScanner;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

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
   * @throws FilterUnsupportedException
   */
  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws IOException, FilterUnsupportedException {
    fillScannedResult(blocksChunkHolder);
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
      throws FilterUnsupportedException, IOException {
    scannedResult.reset();
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR + blocksChunkHolder
            .getDataBlock().nodeNumber());
    // apply min max
    if (isMinMaxEnabled) {
      BitSet bitSet = this.filterExecuter
          .isScanRequired(blocksChunkHolder.getDataBlock().getColumnsMaxValue(),
              blocksChunkHolder.getDataBlock().getColumnsMinValue());
      if (bitSet.isEmpty()) {
        scannedResult.setNumberOfRows(0);
        scannedResult.setIndexes(new int[0]);
        CarbonUtil.freeMemory(blocksChunkHolder.getDimensionDataChunk(),
            blocksChunkHolder.getMeasureDataChunk());
        return;
      }
    }
    // apply filter on actual data
    BitSet bitSet = this.filterExecuter.applyFilter(blocksChunkHolder);
    // if indexes is empty then return with empty result
    if (bitSet.isEmpty()) {
      scannedResult.setNumberOfRows(0);
      scannedResult.setIndexes(new int[0]);
      CarbonUtil.freeMemory(blocksChunkHolder.getDimensionDataChunk(),
          blocksChunkHolder.getMeasureDataChunk());
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
    // loading delete data cache in blockexecutioninfo instance
    DeleteDeltaCacheLoaderIntf deleteCacheLoader =
        new BlockletDeleteDeltaCacheLoader(scannedResult.getBlockletId(),
            blocksChunkHolder.getDataBlock(), blockExecutionInfo.getAbsoluteTableIdentifier());
    deleteCacheLoader.loadDeleteDeltaFileDataToCache();
    scannedResult
        .setBlockletDeleteDeltaCache(blocksChunkHolder.getDataBlock().getDeleteDeltaDataCache());
    FileHolder fileReader = blocksChunkHolder.getFileReader();
    int[][] allSelectedDimensionBlocksIndexes =
        blockExecutionInfo.getAllSelectedDimensionBlocksIndexes();
    DimensionColumnDataChunk[] projectionListDimensionChunk = blocksChunkHolder.getDataBlock()
        .getDimensionChunks(fileReader, allSelectedDimensionBlocksIndexes);

    DimensionColumnDataChunk[] dimensionColumnDataChunk =
        new DimensionColumnDataChunk[blockExecutionInfo.getTotalNumberDimensionBlock()];
    // read dimension chunk blocks from file which is not present
    for (int i = 0; i < dimensionColumnDataChunk.length; i++) {
      if (null != blocksChunkHolder.getDimensionDataChunk()[i]) {
        dimensionColumnDataChunk[i] = blocksChunkHolder.getDimensionDataChunk()[i];
      }
    }
    for (int i = 0; i < allSelectedDimensionBlocksIndexes.length; i++) {
      System.arraycopy(projectionListDimensionChunk, allSelectedDimensionBlocksIndexes[i][0],
          dimensionColumnDataChunk, allSelectedDimensionBlocksIndexes[i][0],
          allSelectedDimensionBlocksIndexes[i][1] + 1 - allSelectedDimensionBlocksIndexes[i][0]);
    }
    MeasureColumnDataChunk[] measureColumnDataChunk =
        new MeasureColumnDataChunk[blockExecutionInfo.getTotalNumberOfMeasureBlock()];
    int[][] allSelectedMeasureBlocksIndexes =
        blockExecutionInfo.getAllSelectedMeasureBlocksIndexes();
    MeasureColumnDataChunk[] projectionListMeasureChunk = blocksChunkHolder.getDataBlock()
        .getMeasureChunks(fileReader, allSelectedMeasureBlocksIndexes);
    // read the measure chunk blocks which is not present
    for (int i = 0; i < measureColumnDataChunk.length; i++) {
      if (null != blocksChunkHolder.getMeasureDataChunk()[i]) {
        measureColumnDataChunk[i] = blocksChunkHolder.getMeasureDataChunk()[i];
      }
    }
    for (int i = 0; i < allSelectedMeasureBlocksIndexes.length; i++) {
      System.arraycopy(projectionListMeasureChunk, allSelectedMeasureBlocksIndexes[i][0],
          measureColumnDataChunk, allSelectedMeasureBlocksIndexes[i][0],
          allSelectedMeasureBlocksIndexes[i][1] + 1 - allSelectedMeasureBlocksIndexes[i][0]);
    }
    scannedResult.setDimensionChunks(dimensionColumnDataChunk);
    scannedResult.setIndexes(indexes);
    scannedResult.setMeasureChunks(measureColumnDataChunk);
    scannedResult.setNumberOfRows(indexes.length);
  }
}
