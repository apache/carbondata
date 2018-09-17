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

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.impl.NonFilterQueryScannedResult;
import org.apache.carbondata.core.scan.scanner.BlockletScanner;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;

/**
 * Blocklet scanner to do full scan of a blocklet,
 * returning all projection and filter column chunks
 */
public class BlockletFullScanner implements BlockletScanner {

  /**
   * block execution info
   */
  protected BlockExecutionInfo blockExecutionInfo;

  private QueryStatisticsModel queryStatisticsModel;

  private BlockletScannedResult emptyResult;

  public BlockletFullScanner(BlockExecutionInfo tableBlockExecutionInfos,
      QueryStatisticsModel queryStatisticsModel) {
    this.blockExecutionInfo = tableBlockExecutionInfos;
    this.queryStatisticsModel = queryStatisticsModel;
  }

  @Override
  public BlockletScannedResult scanBlocklet(
      RawBlockletColumnChunks rawBlockletColumnChunks)
      throws IOException, FilterUnsupportedException {
    long startTime = System.currentTimeMillis();
    BlockletScannedResult scannedResult =
        new NonFilterQueryScannedResult(blockExecutionInfo, queryStatisticsModel);
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
        validPages.getCount() + rawBlockletColumnChunks.getDataBlock().numberOfPages());
    // adding statistics for number of pages
    QueryStatistic totalPagesScanned = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_PAGE_SCANNED);
    totalPagesScanned.addCountStatistic(QueryStatisticsConstants.TOTAL_PAGE_SCANNED,
        totalPagesScanned.getCount() + rawBlockletColumnChunks.getDataBlock().numberOfPages());
    String blockletId = blockExecutionInfo.getBlockIdString() + CarbonCommonConstants.FILE_SEPARATOR
        + rawBlockletColumnChunks.getDataBlock().blockletIndex();
    scannedResult.setBlockletId(blockletId);
    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        rawBlockletColumnChunks.getDimensionRawColumnChunks();
    DimensionColumnPage[][] dimensionColumnDataChunks =
        new DimensionColumnPage[dimensionRawColumnChunks.length][rawBlockletColumnChunks
            .getDataBlock().numberOfPages()];
    MeasureRawColumnChunk[] measureRawColumnChunks =
        rawBlockletColumnChunks.getMeasureRawColumnChunks();
    ColumnPage[][] measureColumnPages =
        new ColumnPage[measureRawColumnChunks.length][rawBlockletColumnChunks.getDataBlock()
                       .numberOfPages()];
    scannedResult.setDimensionColumnPages(dimensionColumnDataChunks);
    scannedResult.setMeasureColumnPages(measureColumnPages);
    scannedResult.setDimRawColumnChunks(dimensionRawColumnChunks);
    scannedResult.setMsrRawColumnChunks(measureRawColumnChunks);
    int[] numberOfRows = null;
    if (blockExecutionInfo.getAllSelectedDimensionColumnIndexRange().length > 0) {
      for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
        if (dimensionRawColumnChunks[i] != null) {
          numberOfRows = dimensionRawColumnChunks[i].getRowCount();
          break;
        }
      }
    } else if (blockExecutionInfo.getAllSelectedMeasureIndexRange().length > 0) {
      for (int i = 0; i < measureRawColumnChunks.length; i++) {
        if (measureRawColumnChunks[i] != null) {
          numberOfRows = measureRawColumnChunks[i].getRowCount();
          break;
        }
      }
    }

    // count(*)  case there would not be any dimensions are measures selected.
    if (numberOfRows == null) {
      numberOfRows = new int[rawBlockletColumnChunks.getDataBlock().numberOfPages()];
      for (int i = 0; i < numberOfRows.length; i++) {
        numberOfRows[i] = rawBlockletColumnChunks.getDataBlock().getPageRowCount(i);
      }
    }
    scannedResult.setPageFilteredRowCount(numberOfRows);
    if (!blockExecutionInfo.isDirectVectorFill()) {
      scannedResult.fillDataChunks();
    }
    // adding statistics for carbon scan time
    QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
    scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
        scanTime.getCount() + (System.currentTimeMillis() - startTime));
    return scannedResult;
  }

  @Override
  public void readBlocklet(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws IOException {
    long startTime = System.currentTimeMillis();
    DimensionRawColumnChunk[] dimensionRawColumnChunks = rawBlockletColumnChunks.getDataBlock()
        .readDimensionChunks(rawBlockletColumnChunks.getFileReader(),
            blockExecutionInfo.getAllSelectedDimensionColumnIndexRange());
    rawBlockletColumnChunks.setDimensionRawColumnChunks(dimensionRawColumnChunks);
    MeasureRawColumnChunk[] measureRawColumnChunks = rawBlockletColumnChunks.getDataBlock()
        .readMeasureChunks(rawBlockletColumnChunks.getFileReader(),
            blockExecutionInfo.getAllSelectedMeasureIndexRange());
    rawBlockletColumnChunks.setMeasureRawColumnChunks(measureRawColumnChunks);
    // adding statistics for carbon read time
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + (System.currentTimeMillis() - startTime));
  }

  BlockletScannedResult createEmptyResult() {
    if (emptyResult == null) {
      emptyResult = new NonFilterQueryScannedResult(blockExecutionInfo, queryStatisticsModel);
      emptyResult.setPageFilteredRowCount(new int[0]);
      emptyResult.setPageFilteredRowId(new int[0][]);
    }
    return emptyResult;
  }

  @Override public boolean isScanRequired(DataRefNode dataBlock) {
    // For non filter it is always true
    return true;
  }
}
