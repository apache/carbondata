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
package org.apache.carbondata.scan.collector.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.carbon.metadata.encoder.Encoding;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.scan.model.QueryDimension;
import org.apache.carbondata.scan.model.QueryMeasure;
import org.apache.carbondata.scan.result.AbstractScannedResult;
import org.apache.carbondata.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.scan.result.vector.MeasureDataVectorProcessor;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class DictionaryBasedVectorResultCollector extends AbstractScannedResultCollector {

  private ColumnVectorInfo[] dictionaryInfo;

  private ColumnVectorInfo[] noDictionaryInfo;

  private ColumnVectorInfo[] complexInfo;

  private ColumnVectorInfo[] measureInfo;

  private ColumnVectorInfo[] allColumnInfo;

  public DictionaryBasedVectorResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    QueryDimension[] queryDimensions = tableBlockExecutionInfos.getQueryDimensions();
    QueryMeasure[] queryMeasures = tableBlockExecutionInfos.getQueryMeasures();
    measureInfo = new ColumnVectorInfo[queryMeasures.length];
    allColumnInfo = new ColumnVectorInfo[queryDimensions.length + queryMeasures.length];
    List<ColumnVectorInfo> dictInfoList = new ArrayList<>();
    List<ColumnVectorInfo> noDictInfoList = new ArrayList<>();
    List<ColumnVectorInfo> complexList = new ArrayList<>();
    for (int i = 0; i < queryDimensions.length; i++) {
      if (!queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        noDictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      } else if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        dictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(queryDimensions[i].getDimension().getDataType());
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      } else if (queryDimensions[i].getDimension().isComplex()) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        complexList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        columnVectorInfo.genericQueryType =
            tableBlockExecutionInfos.getComlexDimensionInfoMap().get(columnVectorInfo.ordinal);
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      } else {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        dictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = queryDimensions[i];
        columnVectorInfo.ordinal = queryDimensions[i].getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      }
    }
    for (int i = 0; i < queryMeasures.length; i++) {
      ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
      columnVectorInfo.measureVectorFiller = MeasureDataVectorProcessor.MeasureVectorFillerFactory
          .getMeasureVectorFiller(queryMeasures[i].getMeasure().getDataType());
      columnVectorInfo.ordinal = queryMeasures[i].getMeasure().getOrdinal();
      columnVectorInfo.measure = queryMeasures[i];
      measureInfo[i] = columnVectorInfo;
      allColumnInfo[queryMeasures[i].getQueryOrder()] = columnVectorInfo;
    }
    dictionaryInfo = dictInfoList.toArray(new ColumnVectorInfo[dictInfoList.size()]);
    noDictionaryInfo = noDictInfoList.toArray(new ColumnVectorInfo[noDictInfoList.size()]);
    complexInfo = complexList.toArray(new ColumnVectorInfo[complexList.size()]);
    Arrays.sort(dictionaryInfo);
    Arrays.sort(noDictionaryInfo);
    Arrays.sort(complexInfo);
  }

  @Override public List<Object[]> collectData(AbstractScannedResult scannedResult, int batchSize) {
    throw new UnsupportedOperationException("collectData is not supported here");
  }

  @Override public void collectVectorBatch(AbstractScannedResult scannedResult,
      CarbonColumnarBatch columnarBatch) {
    int rowCounter = scannedResult.getRowCounter();
    int availableRows = scannedResult.numberOfOutputRows() - rowCounter;
    int requiredRows = columnarBatch.getBatchSize() - columnarBatch.getActualSize();
    requiredRows = Math.min(requiredRows, availableRows);
    if (requiredRows < 1) {
      return;
    }
    for (int i = 0; i < allColumnInfo.length; i++) {
      allColumnInfo[i].size = requiredRows;
      allColumnInfo[i].offset = rowCounter;
      allColumnInfo[i].vectorOffset = columnarBatch.getRowCounter();
      allColumnInfo[i].vector = columnarBatch.columnVectors[i];
    }

    scannedResult.fillColumnarDictionaryBatch(dictionaryInfo);
    scannedResult.fillColumnarNoDictionaryBatch(noDictionaryInfo);
    scannedResult.fillColumnarMeasureBatch(measureInfo, measuresOrdinal);
    scannedResult.fillColumnarComplexBatch(complexInfo);
    scannedResult.setRowCounter(rowCounter + requiredRows);
    columnarBatch.setActualSize(columnarBatch.getActualSize() + requiredRows);
    columnarBatch.setRowCounter(columnarBatch.getRowCounter() + requiredRows);
  }

}
