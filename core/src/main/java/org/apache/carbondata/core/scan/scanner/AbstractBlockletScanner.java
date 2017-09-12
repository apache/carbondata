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
package org.apache.carbondata.core.scan.scanner;

import java.io.IOException;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnDataChunk;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.processor.BlocksChunkHolder;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedResult;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Blocklet scanner class to process the block
 */
public abstract class AbstractBlockletScanner implements BlockletScanner {

  /**
   * block execution info
   */
  protected BlockExecutionInfo blockExecutionInfo;

  public QueryStatisticsModel queryStatisticsModel;

  private AbstractScannedResult emptyResult;

  public AbstractBlockletScanner(BlockExecutionInfo tableBlockExecutionInfos) {
    this.blockExecutionInfo = tableBlockExecutionInfos;
  }

  @Override public AbstractScannedResult scanBlocklet(BlocksChunkHolder blocksChunkHolder)
      throws IOException, FilterUnsupportedException {
    long startTime = System.currentTimeMillis();
    AbstractScannedResult scannedResult = new NonFilterQueryScannedResult(blockExecutionInfo);
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    // adding statistics for valid number of pages
    QueryStatistic validPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_PAGE_SCANNED);
    validPages.addCountStatistic(QueryStatisticsConstants.VALID_PAGE_SCANNED,
        validPages.getCount() + blocksChunkHolder.getDataBlock().getNumberOfPages());
    // adding statistics for number of pages
    QueryStatistic totalPagesScanned = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_PAGE_SCANNED);
    totalPagesScanned.addCountStatistic(QueryStatisticsConstants.TOTAL_PAGE_SCANNED,
        totalPagesScanned.getCount() + blocksChunkHolder.getDataBlock().getNumberOfPages());
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR + blocksChunkHolder
            .getDataBlock().getNodeNumber());
    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        blocksChunkHolder.getDimensionRawDataChunk();
    DimensionColumnDataChunk[][] dimensionColumnDataChunks =
        new DimensionColumnDataChunk[dimensionRawColumnChunks.length][];
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (dimensionRawColumnChunks[i] != null) {
        dimensionColumnDataChunks[i] = dimensionRawColumnChunks[i].convertToDimColDataChunks();
      }
    }
    scannedResult.setDimensionChunks(dimensionColumnDataChunks);
    MeasureRawColumnChunk[] measureRawColumnChunks = blocksChunkHolder.getMeasureRawDataChunk();
    ColumnPage[][] columnPages =
        new ColumnPage[measureRawColumnChunks.length][];
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      if (measureRawColumnChunks[i] != null) {
        columnPages[i] = measureRawColumnChunks[i].convertToColumnPage();
      }
    }
    scannedResult.setMeasureChunks(columnPages);
    int[] numberOfRows = null;
    if (blockExecutionInfo.getAllSelectedDimensionBlocksIndexes().length > 0) {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          numberOfRows = dimensionRawColumnChunks[i].getRowCount();
          break;
        }
      }
    } else if (blockExecutionInfo.getAllSelectedMeasureBlocksIndexes().length > 0) {
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (measureRawColumnChunks[i] != null) {
          numberOfRows = measureRawColumnChunks[i].getRowCount();
          break;
        }
      }
    }
    // count(*)  case there would not be any dimensions are measures selected.
    if (numberOfRows == null) {
      numberOfRows = new int[blocksChunkHolder.getDataBlock().getNumberOfPages()];
      for (int i = 0; i < numberOfRows.length; i++) {
        numberOfRows[i] =
            CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      }
      int lastPageSize = blocksChunkHolder.getDataBlock().nodeSize()
          % CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
      ;
      if (lastPageSize > 0) {
        numberOfRows[numberOfRows.length - 1] = lastPageSize;
      }
    }
    scannedResult.setNumberOfRows(numberOfRows);
    scannedResult.setRawColumnChunks(dimensionRawColumnChunks);
    // adding statistics for carbon scan time
    QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
    scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
        scanTime.getCount() + (System.currentTimeMillis() - startTime));
    return scannedResult;
  }

  @Override public void readBlocklet(BlocksChunkHolder blocksChunkHolder) throws IOException {
    long startTime = System.currentTimeMillis();
    DimensionRawColumnChunk[] dimensionRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getDimensionChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedDimensionBlocksIndexes());
    blocksChunkHolder.setDimensionRawDataChunk(dimensionRawColumnChunks);
    MeasureRawColumnChunk[] measureRawColumnChunks = blocksChunkHolder.getDataBlock()
        .getMeasureChunks(blocksChunkHolder.getFileReader(),
            blockExecutionInfo.getAllSelectedMeasureBlocksIndexes());
    blocksChunkHolder.setMeasureRawDataChunk(measureRawColumnChunks);
    // adding statistics for carbon read time
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + (System.currentTimeMillis() - startTime));
  }

  @Override public AbstractScannedResult createEmptyResult() {
    if (emptyResult == null) {
      emptyResult = new NonFilterQueryScannedResult(blockExecutionInfo);
      emptyResult.setNumberOfRows(new int[0]);
      emptyResult.setIndexes(new int[0][]);
    }
    return emptyResult;
  }

  @Override public boolean isScanRequired(BlocksChunkHolder blocksChunkHolder) throws IOException {
    // For non filter it is always true
    return true;
  }
}
