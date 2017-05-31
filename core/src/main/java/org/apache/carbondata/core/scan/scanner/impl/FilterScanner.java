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
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.datastore.chunk.impl.MeasureRawColumnChunk;
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
import org.apache.carbondata.core.util.BitSetGroup;
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
    return fillScannedResult(blocksChunkHolder);
  }

  @Override public boolean isScanRequired(BlocksChunkHolder blocksChunkHolder) throws IOException {
    // adding statistics for number of pages
    QueryStatistic totalPagesScanned = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_PAGE_SCANNED);
    totalPagesScanned.addCountStatistic(QueryStatisticsConstants.TOTAL_PAGE_SCANNED,
        totalPagesScanned.getCount() + blocksChunkHolder.getDataBlock().numberOfPages());
    // apply min max
    if (isMinMaxEnabled) {
      BitSet bitSet = this.filterExecuter
          .isScanRequired(blocksChunkHolder.getDataBlock().getColumnsMaxValue(),
              blocksChunkHolder.getDataBlock().getColumnsMinValue());
      if (bitSet.isEmpty()) {
        CarbonUtil.freeMemory(blocksChunkHolder.getDimensionRawDataChunk(),
            blocksChunkHolder.getMeasureRawDataChunk());
        return false;
      }
    }
    return true;
  }

  @Override public void readBlocklet(BlocksChunkHolder blocksChunkHolder) throws IOException {
    long startTime = System.currentTimeMillis();
    this.filterExecuter.readBlocks(blocksChunkHolder);
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
   * @param blocksChunkHolder
   * @throws FilterUnsupportedException
   */
  private AbstractScannedResult fillScannedResult(BlocksChunkHolder blocksChunkHolder)
      throws FilterUnsupportedException, IOException {
    long startTime = System.currentTimeMillis();
    QueryStatistic totalBlockletStatistic = queryStatisticsModel.getStatisticsTypeAndObjMap()
        .get(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM);
    totalBlockletStatistic.addCountStatistic(QueryStatisticsConstants.TOTAL_BLOCKLET_NUM,
        totalBlockletStatistic.getCount() + 1);
    // apply filter on actual data
    BitSetGroup bitSetGroup = this.filterExecuter.applyFilter(blocksChunkHolder);
    // if indexes is empty then return with empty result
    if (bitSetGroup.isEmpty()) {
      CarbonUtil.freeMemory(blocksChunkHolder.getDimensionRawDataChunk(),
          blocksChunkHolder.getMeasureRawDataChunk());

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

    AbstractScannedResult scannedResult = new FilterQueryScannedResult(blockExecutionInfo);
    scannedResult.setBlockletId(
        blockExecutionInfo.getBlockId() + CarbonCommonConstants.FILE_SEPARATOR + blocksChunkHolder
            .getDataBlock().nodeNumber());
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
    int[] rowCount = new int[bitSetGroup.getNumberOfPages()];
    // get the row indexes from bot set
    int[][] indexesGroup = new int[bitSetGroup.getNumberOfPages()][];
    for (int k = 0; k < indexesGroup.length; k++) {
      BitSet bitSet = bitSetGroup.getBitSet(k);
      if (bitSet != null && !bitSet.isEmpty()) {
        int[] indexes = new int[bitSet.cardinality()];
        int index = 0;
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          indexes[index++] = i;
        }
        rowCount[k] = indexes.length;
        indexesGroup[k] = indexes;
      }
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

    long dimensionReadTime = System.currentTimeMillis();
    DimensionRawColumnChunk[] projectionListDimensionChunk = blocksChunkHolder.getDataBlock()
        .getDimensionChunks(fileReader, allSelectedDimensionBlocksIndexes);
    dimensionReadTime = System.currentTimeMillis() - dimensionReadTime;

    DimensionRawColumnChunk[] dimensionRawColumnChunks =
        new DimensionRawColumnChunk[blockExecutionInfo.getTotalNumberDimensionBlock()];
    // read dimension chunk blocks from file which is not present
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      if (null != blocksChunkHolder.getDimensionRawDataChunk()[i]) {
        dimensionRawColumnChunks[i] = blocksChunkHolder.getDimensionRawDataChunk()[i];
      }
    }
    for (int i = 0; i < allSelectedDimensionBlocksIndexes.length; i++) {
      for (int j = allSelectedDimensionBlocksIndexes[i][0];
           j <= allSelectedDimensionBlocksIndexes[i][1]; j++) {
        dimensionRawColumnChunks[j] = projectionListDimensionChunk[j];
      }
    }
    long dimensionReadTime1 = System.currentTimeMillis();
    /**
     * in case projection if the projected dimension are not loaded in the dimensionColumnDataChunk
     * then loading them
     */
    int[] projectionListDimensionIndexes = blockExecutionInfo.getProjectionListDimensionIndexes();
    int projectionListDimensionIndexesLength = projectionListDimensionIndexes.length;
    for (int i = 0; i < projectionListDimensionIndexesLength; i++) {
      if (null == dimensionRawColumnChunks[projectionListDimensionIndexes[i]]) {
        dimensionRawColumnChunks[projectionListDimensionIndexes[i]] =
            blocksChunkHolder.getDataBlock()
                .getDimensionChunk(fileReader, projectionListDimensionIndexes[i]);
      }
    }
    dimensionReadTime += (System.currentTimeMillis() - dimensionReadTime1);
    dimensionReadTime1 = System.currentTimeMillis();
    MeasureRawColumnChunk[] measureRawColumnChunks =
        new MeasureRawColumnChunk[blockExecutionInfo.getTotalNumberOfMeasureBlock()];
    int[][] allSelectedMeasureBlocksIndexes =
        blockExecutionInfo.getAllSelectedMeasureBlocksIndexes();
    MeasureRawColumnChunk[] projectionListMeasureChunk = blocksChunkHolder.getDataBlock()
        .getMeasureChunks(fileReader, allSelectedMeasureBlocksIndexes);
    dimensionReadTime += System.currentTimeMillis() - dimensionReadTime1;
    // read the measure chunk blocks which is not present
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      if (null != blocksChunkHolder.getMeasureRawDataChunk()[i]) {
        measureRawColumnChunks[i] = blocksChunkHolder.getMeasureRawDataChunk()[i];
      }
    }
    for (int i = 0; i < allSelectedMeasureBlocksIndexes.length; i++) {
      for (int j = allSelectedMeasureBlocksIndexes[i][0];
           j <= allSelectedMeasureBlocksIndexes[i][1]; j++) {
        measureRawColumnChunks[j] = projectionListMeasureChunk[j];
      }
    }
    dimensionReadTime1 = System.currentTimeMillis();
    /**
     * in case projection if the projected measure are not loaded in the measureColumnDataChunk
     * then loading them
     */
    int[] projectionListMeasureIndexes = blockExecutionInfo.getProjectionListMeasureIndexes();
    int projectionListMeasureIndexesLength = projectionListMeasureIndexes.length;
    for (int i = 0; i < projectionListMeasureIndexesLength; i++) {
      if (null == measureRawColumnChunks[projectionListMeasureIndexes[i]]) {
        measureRawColumnChunks[projectionListMeasureIndexes[i]] = blocksChunkHolder.getDataBlock()
            .getMeasureChunk(fileReader, projectionListMeasureIndexes[i]);
      }
    }
    dimensionReadTime += System.currentTimeMillis() - dimensionReadTime1;
    DimensionColumnDataChunk[][] dimensionColumnDataChunks =
        new DimensionColumnDataChunk[dimensionRawColumnChunks.length][indexesGroup.length];
    MeasureColumnDataChunk[][] measureColumnDataChunks =
        new MeasureColumnDataChunk[measureRawColumnChunks.length][indexesGroup.length];
    for (int i = 0; i < dimensionRawColumnChunks.length; i++) {
      for (int j = 0; j < indexesGroup.length; j++) {
        if (dimensionRawColumnChunks[i] != null) {
          dimensionColumnDataChunks[i][j] = dimensionRawColumnChunks[i].convertToDimColDataChunk(j);
        }
      }
    }
    for (int i = 0; i < measureRawColumnChunks.length; i++) {
      for (int j = 0; j < indexesGroup.length; j++) {
        if (measureRawColumnChunks[i] != null) {
          measureColumnDataChunks[i][j] = measureRawColumnChunks[i].convertToMeasureColDataChunk(j);
        }
      }
    }
    scannedResult.setDimensionChunks(dimensionColumnDataChunks);
    scannedResult.setIndexes(indexesGroup);
    scannedResult.setMeasureChunks(measureColumnDataChunks);
    scannedResult.setRawColumnChunks(dimensionRawColumnChunks);
    scannedResult.setNumberOfRows(rowCount);
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
