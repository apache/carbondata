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

package org.carbondata.query.executer.impl;

import java.util.concurrent.Callable;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.common.logging.impl.StandardLogService;
import org.carbondata.core.datastorage.store.FileHolder;
import org.carbondata.core.datastorage.store.impl.FileFactory;
import org.carbondata.core.util.CarbonUtil;
import org.carbondata.query.columnar.aggregator.ColumnarAggregatorInfo;
import org.carbondata.query.columnar.datastoreblockprocessor.ColumnarDataStoreBlockProcessorInfo;
import org.carbondata.query.columnar.scanner.ColumnarStorageScanner;
import org.carbondata.query.columnar.scanner.impl.ColumnarStorageAggregatedScannerImpl;
import org.carbondata.query.columnar.scanner.impl.ColumnarStorageScannerImpl;
import org.carbondata.query.datastorage.storeinterface.DataStoreBlock;
import org.carbondata.query.executer.processor.ScannedResultProcessor;
import org.carbondata.query.schema.metadata.ColumnarStorageScannerInfo;
import org.carbondata.query.schema.metadata.SliceExecutionInfo;
import org.carbondata.query.util.CarbonEngineLogEvent;

public class ColumnarSliceExecuter implements Callable<Void> {
  /**
   * LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(ColumnarSliceExecuter.class.getName());

  private FileHolder fileHolder;

  private ColumnarStorageScanner columnarstorageScanner;

  private String partitionID;

  private String queryID;

  public ColumnarSliceExecuter(SliceExecutionInfo info,
      ScannedResultProcessor scannedResultProcessor, DataStoreBlock dataStoreBlock,
      long numberOfNodesToScan) {
    this.fileHolder = FileFactory.getFileHolder(FileFactory.getFileType(info.getOutLocation()));
    this.partitionID = StandardLogService.getPartitionID(info.getCubeName());
    this.queryID = info.getQueryId();
    StandardLogService.setThreadName(partitionID, queryID);
    int[] noDictionaryColIndexes = CarbonUtil.getNoDictionaryColIndex(info.getCurrentDimTables());
    if (!info.isDetailQuery()) {
      this.columnarstorageScanner = new ColumnarStorageAggregatedScannerImpl(
          getColumnarStorageScannerInfo(info, scannedResultProcessor, dataStoreBlock,

              numberOfNodesToScan), noDictionaryColIndexes);
    } else {
      this.columnarstorageScanner = new ColumnarStorageScannerImpl(
          getColumnarStorageScannerInfo(info, scannedResultProcessor, dataStoreBlock,
              numberOfNodesToScan), noDictionaryColIndexes);
    }
  }

  private ColumnarStorageScannerInfo getColumnarStorageScannerInfo(SliceExecutionInfo info,
      ScannedResultProcessor scannedResultProcessor, DataStoreBlock dataStoreBlock,
      long numberOfNodesToScan) {
    ColumnarStorageScannerInfo columnarStorageScannerInfo = new ColumnarStorageScannerInfo();
    columnarStorageScannerInfo.setDatablock(dataStoreBlock);
    columnarStorageScannerInfo.setTotalNumberOfBlocksToScan(numberOfNodesToScan);
    columnarStorageScannerInfo.setColumnarAggregatorInfo(getColumnarAggregatorInfo(info));
    columnarStorageScannerInfo.setScannedResultProcessor(scannedResultProcessor);
    columnarStorageScannerInfo.setRestructurHolder(info.getRestructureHolder());
    columnarStorageScannerInfo.setAutoAggregateTableRequest(info.isCustomMeasure());
    if (null != info.getColumnarSplitter()) {
      columnarStorageScannerInfo
          .setKeySize(info.getColumnarSplitter().getKeySizeByBlock(info.getQueryDimOrdinal()));
    }
    columnarStorageScannerInfo.setFileHolder(fileHolder);
    columnarStorageScannerInfo.setDimColumnCount(info.getTotalNumerOfDimColumns());
    columnarStorageScannerInfo.setMsrColumnCount(info.getTotalNumberOfMeasuresInTable());
    columnarStorageScannerInfo.setQueryId(info.getQueryId());
    columnarStorageScannerInfo.setPartitionId(info.getPartitionId());
    return columnarStorageScannerInfo;
  }

  @Override public Void call() throws Exception {
    StandardLogService.setThreadName(partitionID, queryID);
    try {
      this.columnarstorageScanner.scanStore();
    } catch (Exception e) {
      LOGGER.error(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, e);
    } finally {
      this.fileHolder.finish();
    }
    return null;
  }

  private ColumnarAggregatorInfo getColumnarAggregatorInfo(SliceExecutionInfo sliceInfo) {
    ColumnarAggregatorInfo aggregatorInfo = new ColumnarAggregatorInfo();
    aggregatorInfo.setCountMsrIndex(sliceInfo.getCountMsrsIndex());
    aggregatorInfo.setCubeUniqueName(sliceInfo.getSlice().getCubeUniqueName());
    aggregatorInfo.setMeasureOrdinal(sliceInfo.getMeasureOrdinal());
    aggregatorInfo.setAggType(sliceInfo.getAggType());
    aggregatorInfo.setNoDictionaryType(sliceInfo.getNoDictionaryTypes());
    aggregatorInfo.setCustomExpressions(sliceInfo.getCustomExpressions());
    aggregatorInfo.setMsrMinValue(sliceInfo.getMsrMinValue());
    aggregatorInfo.setMeasureStartIndex(sliceInfo.getMeasureStartIndex());
    aggregatorInfo.setUniqueValue(sliceInfo.getUniqueValues());
    aggregatorInfo.setLatestKeyGenerator(sliceInfo.getActualKeyGenerator());
    aggregatorInfo.setActalMaskedByteRanges(sliceInfo.getActalMaskedByteRanges());
    aggregatorInfo.setActualMaskedKeyByteSize(sliceInfo.getActualMaskedKeyByteSize());
    aggregatorInfo.setActualMaxKeyBasedOnDimensions(sliceInfo.getActualMaxKeyBasedOnDimensions());
    aggregatorInfo.setLimit(
        sliceInfo.getDimensionSortOrder().length > 0 || null != sliceInfo.getMsrSortModel() ?
            -1 :
            sliceInfo.getLimit());
    aggregatorInfo.setCurrentSliceIndex(sliceInfo.getCurrentSliceIndex());
    aggregatorInfo.setDimensionAggInfos(sliceInfo.getDimAggInfo());
    aggregatorInfo.setSlices(sliceInfo.getSlices());
    aggregatorInfo.setCustomExpressions(sliceInfo.getCustomExpressions());
    aggregatorInfo.setExpressionStartIndex(sliceInfo.getExpressionStartIndex());
    aggregatorInfo.setMsrDefaultValue(sliceInfo.getMsrDefaultValue());
    aggregatorInfo.setIsMeasureExistis(sliceInfo.getIsMeasureExistis());
    aggregatorInfo.setQueryDimensionsLength(sliceInfo.getQueryDimensions().length);
    aggregatorInfo.setComplexQueryDims(sliceInfo.getComplexQueryDimensions());
    aggregatorInfo.setDimensions(sliceInfo.getDimensions());
    aggregatorInfo.setDataTypes(sliceInfo.getDataTypes());
    aggregatorInfo.setMeasureOrdinalMap(sliceInfo.getMeasureOrdinalMap());
    return aggregatorInfo;
  }

  private ColumnarDataStoreBlockProcessorInfo getColumnarDataStoreBlockProcessorInfo(
      SliceExecutionInfo sliceInfo) {
    ColumnarDataStoreBlockProcessorInfo blockProcessorInfo =
        new ColumnarDataStoreBlockProcessorInfo();
    blockProcessorInfo.setDimensionIndexes(sliceInfo.getQueryDimOrdinal());
    blockProcessorInfo.setFileHolder(fileHolder);
    if (null != sliceInfo.getColumnarSplitter()) {
      blockProcessorInfo.setKeySize(
          sliceInfo.getColumnarSplitter().getKeySizeByBlock(sliceInfo.getQueryDimOrdinal()));
    }
    blockProcessorInfo.setMeasureIndexes(sliceInfo.getMeasureOrdinal());
    blockProcessorInfo.setAutoGeneratedAggTableExecution(sliceInfo.isCustomMeasure());
    blockProcessorInfo.setTotalNumberOfMeasures(sliceInfo.getTotalNumberOfMeasuresInTable());
    blockProcessorInfo.setAllSelectedDimensions(sliceInfo.getAllSelectedDimensions());
    blockProcessorInfo.setTotalNumberOfDimension(sliceInfo.getTotalNumerOfDimColumns());
    blockProcessorInfo.setAllSelectedMeasures(sliceInfo.getAllSelectedMeasures());
    return blockProcessorInfo;
  }

}
