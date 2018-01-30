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
package org.apache.carbondata.core.scan.collector.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.ProjectionDimension;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.MeasureDataVectorProcessor;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class DictionaryBasedVectorResultCollector extends AbstractScannedResultCollector {

  protected ProjectionDimension[] queryDimensions;

  protected ProjectionMeasure[] queryMeasures;

  private ColumnVectorInfo[] dictionaryInfo;

  private ColumnVectorInfo[] noDictionaryInfo;

  private ColumnVectorInfo[] complexInfo;

  private ColumnVectorInfo[] measureColumnInfo;

  ColumnVectorInfo[] allColumnInfo;

  private ColumnVectorInfo[] implictColumnInfo;

  public DictionaryBasedVectorResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    // initialize only if the current block is not a restructured block else the initialization
    // will be taken care by RestructureBasedVectorResultCollector
    if (!blockExecutionInfos.isRestructuredBlock()) {
      queryDimensions = executionInfo.getProjectionDimensions();
      queryMeasures = executionInfo.getProjectionMeasures();
      allColumnInfo = new ColumnVectorInfo[queryDimensions.length + queryMeasures.length];
      prepareDimensionAndMeasureColumnVectors();
    }
  }

  void prepareDimensionAndMeasureColumnVectors() {
    measureColumnInfo = new ColumnVectorInfo[queryMeasures.length];
    List<ColumnVectorInfo> dictInfoList = new ArrayList<>();
    List<ColumnVectorInfo> noDictInfoList = new ArrayList<>();
    List<ColumnVectorInfo> complexList = new ArrayList<>();
    List<ColumnVectorInfo> implictColumnList = new ArrayList<>();
    for (int i = 0; i < queryDimensions.length; i++) {
      if (!dimensionInfo.getDimensionExists()[i]) {
        continue;
      }
      if (queryDimensions[i].getDimension().hasEncoding(Encoding.IMPLICIT)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        implictColumnList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getOrdinal()] = columnVectorInfo;
      } else if (!queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        noDictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getOrdinal()] = columnVectorInfo;
      } else if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        dictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getOrdinal()] = columnVectorInfo;
      } else if (queryDimensions[i].getDimension().isComplex()) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        complexList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        columnVectorInfo.genericQueryType =
            executionInfo.getComlexDimensionInfoMap().get(columnVectorInfo.ordinal);
        allColumnInfo[queryDimensions[i].getOrdinal()] = columnVectorInfo;
      } else {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        dictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getOrdinal()] = columnVectorInfo;
      }
    }
    for (int i = 0; i < queryMeasures.length; i++) {
      ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
      columnVectorInfo.measureVectorFiller = MeasureDataVectorProcessor.MeasureVectorFillerFactory
          .getMeasureVectorFiller(queryMeasures[i].getMeasure().getDataType());
      columnVectorInfo.ordinal = queryMeasures[i].getMeasure().getOrdinal();
      columnVectorInfo.measure = queryMeasures[i];
      this.measureColumnInfo[i] = columnVectorInfo;
      allColumnInfo[queryMeasures[i].getOrdinal()] = columnVectorInfo;
    }
    dictionaryInfo = dictInfoList.toArray(new ColumnVectorInfo[dictInfoList.size()]);
    noDictionaryInfo = noDictInfoList.toArray(new ColumnVectorInfo[noDictInfoList.size()]);
    complexInfo = complexList.toArray(new ColumnVectorInfo[complexList.size()]);
    implictColumnInfo = implictColumnList.toArray(new ColumnVectorInfo[implictColumnList.size()]);
    Arrays.sort(dictionaryInfo);
    Arrays.sort(complexInfo);
  }

  @Override
  public List<Object[]> collectResultInRow(BlockletScannedResult scannedResult, int batchSize) {
    throw new UnsupportedOperationException("collectResultInRow is not supported here");
  }

  @Override
  public void collectResultInColumnarBatch(BlockletScannedResult scannedResult,
      CarbonColumnarBatch columnarBatch) {
    int numberOfPages = scannedResult.numberOfpages();
    int filteredRows = 0;
    while (scannedResult.getCurrentPageCounter() < numberOfPages) {
      int currentPageRowCount = scannedResult.getCurrentPageRowCount();
      if (currentPageRowCount == 0) {
        scannedResult.incrementPageCounter();
        continue;
      }
      int rowCounter = scannedResult.getRowCounter();
      int availableRows = currentPageRowCount - rowCounter;
      // getRowCounter holds total number or rows being placed in Vector. Calculate the
      // Left over space through getRowCounter only.
      int requiredRows = columnarBatch.getBatchSize() - columnarBatch.getRowCounter();
      requiredRows = Math.min(requiredRows, availableRows);
      if (requiredRows < 1) {
        return;
      }
      fillColumnVectorDetails(columnarBatch, rowCounter, requiredRows);
      filteredRows = scannedResult.markFilteredRows(
          columnarBatch, rowCounter, requiredRows, columnarBatch.getRowCounter());
      fillResultToColumnarBatch(
          scannedResult, columnarBatch, rowCounter, availableRows, requiredRows);
      columnarBatch.setActualSize(columnarBatch.getActualSize() + requiredRows - filteredRows);
    }
  }

  void fillResultToColumnarBatch(BlockletScannedResult scannedResult,
      CarbonColumnarBatch columnarBatch, int rowCounter, int availableRows, int requiredRows) {
    scannedResult.fillColumnarDictionaryBatch(dictionaryInfo);
    scannedResult.fillColumnarNoDictionaryBatch(noDictionaryInfo);
    scannedResult.fillColumnarMeasureBatch(measureColumnInfo, measureInfo.getMeasureOrdinals());
    scannedResult.fillColumnarComplexBatch(complexInfo);
    scannedResult.fillColumnarImplicitBatch(implictColumnInfo);
    // it means fetched all data out of page so increment the page counter
    if (availableRows == requiredRows) {
      scannedResult.incrementPageCounter();
    } else {
      // Or set the row counter.
      scannedResult.setRowCounter(rowCounter + requiredRows);
    }
    columnarBatch.setRowCounter(columnarBatch.getRowCounter() + requiredRows);
  }

  void fillColumnVectorDetails(CarbonColumnarBatch columnarBatch, int rowCounter, int requiredRows)
  {
    for (int i = 0; i < allColumnInfo.length; i++) {
      allColumnInfo[i].size = requiredRows;
      allColumnInfo[i].offset = rowCounter;
      allColumnInfo[i].vectorOffset = columnarBatch.getRowCounter();
      allColumnInfo[i].vector = columnarBatch.columnVectors[i];
      if (null != allColumnInfo[i].dimension) {
        allColumnInfo[i].vector
            .setBlockDataType(allColumnInfo[i].dimension.getDimension().getDataType());
      }
    }
  }

}
