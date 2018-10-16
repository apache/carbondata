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
import org.apache.carbondata.core.datastore.DataRefNode;
import org.apache.carbondata.core.datastore.FileReader;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitColumnFilterExecutor;
import org.apache.carbondata.core.scan.processor.RawBlockletColumnChunks;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.impl.FilterQueryScannedResult;
import org.apache.carbondata.core.stats.QueryStatistic;
import org.apache.carbondata.core.stats.QueryStatisticsConstants;
import org.apache.carbondata.core.stats.QueryStatisticsModel;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Below class will be used for filter query processing
 * this class will be first apply the filter then it will read the column page if
 * required and return the scanned result
 */
public class BlockletFilterScanner extends BlockletFullScanner {

  /**
   * filter executer to evaluate filter condition
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

  private boolean useBitSetPipeLine;

  public BlockletFilterScanner(BlockExecutionInfo blockExecutionInfo,
      QueryStatisticsModel queryStatisticsModel) {
    super(blockExecutionInfo, queryStatisticsModel);
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

    String useBitSetPipeLine = CarbonProperties.getInstance()
        .getProperty(CarbonCommonConstants.BITSET_PIPE_LINE,
            CarbonCommonConstants.BITSET_PIPE_LINE_DEFAULT);
    if (null != useBitSetPipeLine) {
      this.useBitSetPipeLine = Boolean.parseBoolean(useBitSetPipeLine);
    }
  }

  /**
   * Below method will be used to process the block
   *
   * @param rawBlockletColumnChunks block chunk holder which holds the data
   * @throws FilterUnsupportedException
   */
  @Override
  public BlockletScannedResult scanBlocklet(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws IOException, FilterUnsupportedException {
    if (blockExecutionInfo.isDirectVectorFill()) {
      return executeFilterForPages(rawBlockletColumnChunks);
    } else {
      return executeFilter(rawBlockletColumnChunks);
    }
  }

  @Override
  public boolean isScanRequired(DataRefNode dataBlock) {
    // adding statistics for number of pages
    QueryStatistic totalPagesScanned = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_PAGE_SCANNED);
    totalPagesScanned.addCountStatistic(QueryStatisticsConstants.TOTAL_PAGE_SCANNED,
        totalPagesScanned.getCount() + dataBlock.numberOfPages());
    // apply min max
    if (isMinMaxEnabled) {
      if (null == dataBlock.getColumnsMaxValue()
              || null == dataBlock.getColumnsMinValue()) {
        return true;
      }
      BitSet bitSet = null;
      // check for implicit include filter instance
      if (filterExecuter instanceof ImplicitColumnFilterExecutor) {
        String blockletId = blockExecutionInfo.getBlockIdString() +
            CarbonCommonConstants.FILE_SEPARATOR + dataBlock.blockletIndex();
        bitSet = ((ImplicitColumnFilterExecutor) filterExecuter)
            .isFilterValuesPresentInBlockOrBlocklet(
                dataBlock.getColumnsMaxValue(),
                dataBlock.getColumnsMinValue(), blockletId, dataBlock.minMaxFlagArray());
      } else {
        bitSet = this.filterExecuter
            .isScanRequired(dataBlock.getColumnsMaxValue(),
                dataBlock.getColumnsMinValue(), dataBlock.minMaxFlagArray());
      }
      return !bitSet.isEmpty();
    }
    return true;
  }

  @Override
  public void readBlocklet(RawBlockletColumnChunks rawBlockletColumnChunks) throws IOException {
    long startTime = System.currentTimeMillis();
    this.filterExecuter.readColumnChunks(rawBlockletColumnChunks);
    // adding statistics for carbon read time
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + (System.currentTimeMillis() - startTime));
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
   * @param rawBlockletColumnChunks
   * @throws FilterUnsupportedException
   */
  private BlockletScannedResult executeFilter(RawBlockletColumnChunks rawBlockletColumnChunks)
      throws FilterUnsupportedException, IOException {
    long startTime = System.currentTimeMillis();
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    // set the indexed data if it has any during fgdatamap pruning.
    BitSetGroup fgBitSetGroup = rawBlockletColumnChunks.getDataBlock().getIndexedData();
    rawBlockletColumnChunks.setBitSetGroup(fgBitSetGroup);
    // apply filter on actual data, for each page
    BitSetGroup bitSetGroup = this.filterExecuter.applyFilter(rawBlockletColumnChunks,
        useBitSetPipeLine);
    // if filter result is empty then return with empty result
    if (bitSetGroup.isEmpty()) {
      CarbonUtil.freeMemory(rawBlockletColumnChunks.getDimensionRawColumnChunks(),
          rawBlockletColumnChunks.getMeasureRawColumnChunks());

      QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
          .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
      scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
          scanTime.getCount() + (System.currentTimeMillis() - startTime));

      QueryStatistic scannedPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
          .get(QueryStatisticsConstants.PAGE_SCANNED);
      scannedPages.addCountStatistic(QueryStatisticsConstants.PAGE_SCANNED,
          scannedPages.getCount() + bitSetGroup.getScannedPages());
      return createEmptyResult();
    }

    BlockletScannedResult scannedResult =
        new FilterQueryScannedResult(blockExecutionInfo, queryStatisticsModel);
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockIdString() + CarbonCommonConstants.FILE_SEPARATOR +
            rawBlockletColumnChunks.getDataBlock().blockletIndex());
    // valid scanned blocklet
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    // adding statistics for valid number of pages
    QueryStatistic validPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_PAGE_SCANNED);
    validPages.addCountStatistic(QueryStatisticsConstants.VALID_PAGE_SCANNED,
        validPages.getCount() + bitSetGroup.getValidPages());
    QueryStatistic scannedPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.PAGE_SCANNED);
    scannedPages.addCountStatistic(QueryStatisticsConstants.PAGE_SCANNED,
        scannedPages.getCount() + bitSetGroup.getScannedPages());
    int[] pageFilteredRowCount = new int[bitSetGroup.getNumberOfPages()];
    // get the row indexes from bit set for each page
    int[][] pageFilteredRowId = new int[bitSetGroup.getNumberOfPages()][];
    int numPages = pageFilteredRowId.length;
    for (int pageId = 0; pageId < numPages; pageId++) {
      BitSet bitSet = bitSetGroup.getBitSet(pageId);
      if (bitSet != null && !bitSet.isEmpty()) {
        int[] matchedRowId = new int[bitSet.cardinality()];
        int index = 0;
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          matchedRowId[index++] = i;
        }
        pageFilteredRowCount[pageId] = matchedRowId.length;
        pageFilteredRowId[pageId] = matchedRowId;
      }
    }

    long dimensionReadTime = System.currentTimeMillis();
    dimensionReadTime = System.currentTimeMillis() - dimensionReadTime;

    FileReader fileReader = rawBlockletColumnChunks.getFileReader();


    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        new DimensionRawColumnChunk[blockExecutionInfo.getTotalNumberDimensionToRead()];
    int numDimensionChunks = dimensionRawColumnChunks.length;
    // read dimension chunk blocks from file which is not present
    for (int chunkIndex = 0; chunkIndex < numDimensionChunks; chunkIndex++) {
      dimensionRawColumnChunks[chunkIndex] =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
    }
    int[][] allSelectedDimensionColumnIndexRange =
        blockExecutionInfo.getAllSelectedDimensionColumnIndexRange();
    DimensionRawColumnChunk[] projectionListDimensionChunk = rawBlockletColumnChunks.getDataBlock()
        .readDimensionChunks(fileReader, allSelectedDimensionColumnIndexRange);
    for (int[] columnIndexRange : allSelectedDimensionColumnIndexRange) {
      System.arraycopy(projectionListDimensionChunk, columnIndexRange[0],
          dimensionRawColumnChunks, columnIndexRange[0],
          columnIndexRange[1] + 1 - columnIndexRange[0]);
    }

    /*
     * in case projection if the projected dimension are not loaded in the dimensionColumnDataChunk
     * then loading them
     */
    int[] projectionListDimensionIndexes = blockExecutionInfo.getProjectionListDimensionIndexes();
    for (int projectionListDimensionIndex : projectionListDimensionIndexes) {
      if (null == dimensionRawColumnChunks[projectionListDimensionIndex]) {
        dimensionRawColumnChunks[projectionListDimensionIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                fileReader, projectionListDimensionIndex);
      }
    }

    DimensionColumnPage[][] dimensionColumnPages =
        new DimensionColumnPage[numDimensionChunks][numPages];
    MeasureRawColumnChunk[] measureRawColumnChunks =
        new MeasureRawColumnChunk[blockExecutionInfo.getTotalNumberOfMeasureToRead()];
    int numMeasureChunks = measureRawColumnChunks.length;

    // read the measure chunk blocks which is not present
    for (int chunkIndex = 0; chunkIndex < numMeasureChunks; chunkIndex++) {
      if (null != rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        measureRawColumnChunks[chunkIndex] =
            rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex];
      }
    }

    int[][] allSelectedMeasureColumnIndexRange =
        blockExecutionInfo.getAllSelectedMeasureIndexRange();
    MeasureRawColumnChunk[] projectionListMeasureChunk = rawBlockletColumnChunks.getDataBlock()
        .readMeasureChunks(fileReader, allSelectedMeasureColumnIndexRange);
    for (int[] columnIndexRange : allSelectedMeasureColumnIndexRange) {
      System.arraycopy(projectionListMeasureChunk, columnIndexRange[0], measureRawColumnChunks,
          columnIndexRange[0], columnIndexRange[1] + 1 - columnIndexRange[0]);
    }
    /*
     * in case projection if the projected measure are not loaded in the ColumnPage
     * then loading them
     */
    int[] projectionListMeasureIndexes = blockExecutionInfo.getProjectionListMeasureIndexes();
    for (int projectionListMeasureIndex : projectionListMeasureIndexes) {
      if (null == measureRawColumnChunks[projectionListMeasureIndex]) {
        measureRawColumnChunks[projectionListMeasureIndex] = rawBlockletColumnChunks.getDataBlock()
            .readMeasureChunk(fileReader, projectionListMeasureIndex);
      }
    }
    ColumnPage[][] measureColumnPages = new ColumnPage[numMeasureChunks][numPages];
    scannedResult.setDimensionColumnPages(dimensionColumnPages);
    scannedResult.setPageFilteredRowId(pageFilteredRowId);
    scannedResult.setMeasureColumnPages(measureColumnPages);
    scannedResult.setDimRawColumnChunks(dimensionRawColumnChunks);
    scannedResult.setMsrRawColumnChunks(measureRawColumnChunks);
    scannedResult.setPageFilteredRowCount(pageFilteredRowCount);
    scannedResult.fillDataChunks();
    // adding statistics for carbon scan time
    QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
    scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
        scanTime.getCount() + (System.currentTimeMillis() - startTime - dimensionReadTime));
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + dimensionReadTime);
    return scannedResult;
  }

  /**
   * This method will process the data in below order
   * 1. first apply min max on the filter tree and check whether any of the filter
   * is fall on the range of min max, if not then return empty result
   * 2. If filter falls on min max range then apply filter on actual
   * data and get the pruned pages.
   * 3. if pruned pages are not empty then read only those blocks(measure or dimension)
   * which was present in the query but not present in the filter, as while applying filter
   * some of the blocks where already read and present in chunk holder so not need to
   * read those blocks again, this is to avoid reading of same blocks which was already read
   * 4. Set the blocks and filter pages to scanned result
   *
   * @param rawBlockletColumnChunks blocklet raw chunk of all columns
   * @throws FilterUnsupportedException
   */
  private BlockletScannedResult executeFilterForPages(
      RawBlockletColumnChunks rawBlockletColumnChunks)
      throws FilterUnsupportedException, IOException {
    long startTime = System.currentTimeMillis();
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    // apply filter on actual data, for each page
    BitSet pages = this.filterExecuter.prunePages(rawBlockletColumnChunks);
    // if filter result is empty then return with empty result
    if (pages.isEmpty()) {
      CarbonUtil.freeMemory(rawBlockletColumnChunks.getDimensionRawColumnChunks(),
          rawBlockletColumnChunks.getMeasureRawColumnChunks());

      QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
          .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
      scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
          scanTime.getCount() + (System.currentTimeMillis() - startTime));

      QueryStatistic scannedPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
          .get(QueryStatisticsConstants.PAGE_SCANNED);
      scannedPages.addCountStatistic(QueryStatisticsConstants.PAGE_SCANNED,
          scannedPages.getCount());
      return createEmptyResult();
    }

    BlockletScannedResult scannedResult =
        new FilterQueryScannedResult(blockExecutionInfo, queryStatisticsModel);

    // valid scanned blocklet
    QueryStatistic validScannedBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM);
    validScannedBlockletStatistic
        .addCountStatistic(QueryStatisticsConstants.VALID_SCAN_BLOCKLET_NUM,
            validScannedBlockletStatistic.getCount() + 1);
    // adding statistics for valid number of pages
    QueryStatistic validPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.VALID_PAGE_SCANNED);
    validPages.addCountStatistic(QueryStatisticsConstants.VALID_PAGE_SCANNED,
        validPages.getCount() + pages.cardinality());
    QueryStatistic scannedPages = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.PAGE_SCANNED);
    scannedPages.addCountStatistic(QueryStatisticsConstants.PAGE_SCANNED,
        scannedPages.getCount() + pages.cardinality());
    // get the row indexes from bit set for each page
    int[] pageFilteredPages = new int[pages.cardinality()];
    int index = 0;
    for (int i = pages.nextSetBit(0); i >= 0; i = pages.nextSetBit(i + 1)) {
      pageFilteredPages[index++] = i;
    }
    // count(*)  case there would not be any dimensions are measures selected.
    int[] numberOfRows = new int[pages.cardinality()];
    for (int i = 0; i < numberOfRows.length; i++) {
      numberOfRows[i] = rawBlockletColumnChunks.getDataBlock().getPageRowCount(i);
    }
    long dimensionReadTime = System.currentTimeMillis();
    dimensionReadTime = System.currentTimeMillis() - dimensionReadTime;

    FileReader fileReader = rawBlockletColumnChunks.getFileReader();


    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        new DimensionRawColumnChunk[blockExecutionInfo.getTotalNumberDimensionToRead()];
    int numDimensionChunks = dimensionRawColumnChunks.length;
    // read dimension chunk blocks from file which is not present
    for (int chunkIndex = 0; chunkIndex < numDimensionChunks; chunkIndex++) {
      dimensionRawColumnChunks[chunkIndex] =
          rawBlockletColumnChunks.getDimensionRawColumnChunks()[chunkIndex];
    }
    int[][] allSelectedDimensionColumnIndexRange =
        blockExecutionInfo.getAllSelectedDimensionColumnIndexRange();
    DimensionRawColumnChunk[] projectionListDimensionChunk = rawBlockletColumnChunks.getDataBlock()
        .readDimensionChunks(fileReader, allSelectedDimensionColumnIndexRange);
    for (int[] columnIndexRange : allSelectedDimensionColumnIndexRange) {
      System.arraycopy(projectionListDimensionChunk, columnIndexRange[0],
          dimensionRawColumnChunks, columnIndexRange[0],
          columnIndexRange[1] + 1 - columnIndexRange[0]);
    }

    /*
     * in case projection if the projected dimension are not loaded in the dimensionColumnDataChunk
     * then loading them
     */
    int[] projectionListDimensionIndexes = blockExecutionInfo.getProjectionListDimensionIndexes();
    for (int projectionListDimensionIndex : projectionListDimensionIndexes) {
      if (null == dimensionRawColumnChunks[projectionListDimensionIndex]) {
        dimensionRawColumnChunks[projectionListDimensionIndex] =
            rawBlockletColumnChunks.getDataBlock().readDimensionChunk(
                fileReader, projectionListDimensionIndex);
      }
    }

    DimensionColumnPage[][] dimensionColumnPages =
        new DimensionColumnPage[numDimensionChunks][pages.cardinality()];
    MeasureRawColumnChunk[] measureRawColumnChunks =
        new MeasureRawColumnChunk[blockExecutionInfo.getTotalNumberOfMeasureToRead()];
    int numMeasureChunks = measureRawColumnChunks.length;

    // read the measure chunk blocks which is not present
    for (int chunkIndex = 0; chunkIndex < numMeasureChunks; chunkIndex++) {
      if (null != rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex]) {
        measureRawColumnChunks[chunkIndex] =
            rawBlockletColumnChunks.getMeasureRawColumnChunks()[chunkIndex];
      }
    }

    int[][] allSelectedMeasureColumnIndexRange =
        blockExecutionInfo.getAllSelectedMeasureIndexRange();
    MeasureRawColumnChunk[] projectionListMeasureChunk = rawBlockletColumnChunks.getDataBlock()
        .readMeasureChunks(fileReader, allSelectedMeasureColumnIndexRange);
    for (int[] columnIndexRange : allSelectedMeasureColumnIndexRange) {
      System.arraycopy(projectionListMeasureChunk, columnIndexRange[0], measureRawColumnChunks,
          columnIndexRange[0], columnIndexRange[1] + 1 - columnIndexRange[0]);
    }
    /*
     * in case projection if the projected measure are not loaded in the ColumnPage
     * then loading them
     */
    int[] projectionListMeasureIndexes = blockExecutionInfo.getProjectionListMeasureIndexes();
    for (int projectionListMeasureIndex : projectionListMeasureIndexes) {
      if (null == measureRawColumnChunks[projectionListMeasureIndex]) {
        measureRawColumnChunks[projectionListMeasureIndex] = rawBlockletColumnChunks.getDataBlock()
            .readMeasureChunk(fileReader, projectionListMeasureIndex);
      }
    }
    ColumnPage[][] measureColumnPages = new ColumnPage[numMeasureChunks][pages.cardinality()];
    scannedResult.setDimensionColumnPages(dimensionColumnPages);
    scannedResult.setMeasureColumnPages(measureColumnPages);
    scannedResult.setDimRawColumnChunks(dimensionRawColumnChunks);
    scannedResult.setMsrRawColumnChunks(measureRawColumnChunks);
    scannedResult.setPageFilteredRowCount(numberOfRows);
    scannedResult.setPagesFiltered(pageFilteredPages);
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockIdString() + CarbonCommonConstants.FILE_SEPARATOR +
            rawBlockletColumnChunks.getDataBlock().blockletIndex());
    // adding statistics for carbon scan time
    QueryStatistic scanTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.SCAN_BLOCKlET_TIME);
    scanTime.addCountStatistic(QueryStatisticsConstants.SCAN_BLOCKlET_TIME,
        scanTime.getCount() + (System.currentTimeMillis() - startTime - dimensionReadTime));
    QueryStatistic readTime = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.READ_BLOCKlET_TIME);
    readTime.addCountStatistic(QueryStatisticsConstants.READ_BLOCKlET_TIME,
        readTime.getCount() + dimensionReadTime);
    return scannedResult;
  }
}
