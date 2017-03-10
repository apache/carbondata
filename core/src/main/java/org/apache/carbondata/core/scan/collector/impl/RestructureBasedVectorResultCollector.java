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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.MeasureDataVectorProcessor;

import org.apache.spark.sql.types.Decimal;

/**
 * It is not a collector it is just a scanned result holder.
 */
public class RestructureBasedVectorResultCollector extends AbstractScannedResultCollector {

  private ColumnVectorInfo[] dictionaryInfo;

  private ColumnVectorInfo[] noDictionaryInfo;

  private ColumnVectorInfo[] complexInfo;

  private ColumnVectorInfo[] measureColumnInfo;

  private ColumnVectorInfo[] allColumnInfo;

  public RestructureBasedVectorResultCollector(BlockExecutionInfo blockExecutionInfos) {
    super(blockExecutionInfos);
    QueryDimension[] queryDimensions = tableBlockExecutionInfos.getActualQueryDimensions();
    QueryMeasure[] queryMeasures = tableBlockExecutionInfos.getActualQueryMeasures();
    measureColumnInfo = new ColumnVectorInfo[queryMeasures.length];
    allColumnInfo = new ColumnVectorInfo[queryDimensions.length + queryMeasures.length];
    List<ColumnVectorInfo> dictInfoList = new ArrayList<>();
    List<ColumnVectorInfo> noDictInfoList = new ArrayList<>();
    List<ColumnVectorInfo> complexList = new ArrayList<>();
    int dimensionExistIndex = 0;
    for (int i = 0; i < queryDimensions.length; i++) {
      if (!dimensionInfo.getDimensionExists()[i]) {
        // add a dummy column vector result collector object
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
        continue;
      }
      // get the current block dimension and fetch the required information from it
      QueryDimension currentBlockDimension =
          tableBlockExecutionInfos.getQueryDimensions()[dimensionExistIndex++];
      if (!queryDimensions[i].getDimension().hasEncoding(Encoding.DICTIONARY)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        noDictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = currentBlockDimension;
        columnVectorInfo.ordinal = currentBlockDimension.getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      } else if (queryDimensions[i].getDimension().hasEncoding(Encoding.DIRECT_DICTIONARY)) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        dictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = currentBlockDimension;
        columnVectorInfo.directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
            .getDirectDictionaryGenerator(currentBlockDimension.getDimension().getDataType());
        columnVectorInfo.ordinal = currentBlockDimension.getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      } else if (queryDimensions[i].getDimension().isComplex()) {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        complexList.add(columnVectorInfo);
        columnVectorInfo.dimension = currentBlockDimension;
        columnVectorInfo.ordinal = currentBlockDimension.getDimension().getOrdinal();
        columnVectorInfo.genericQueryType =
            tableBlockExecutionInfos.getComlexDimensionInfoMap().get(columnVectorInfo.ordinal);
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      } else {
        ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
        dictInfoList.add(columnVectorInfo);
        columnVectorInfo.dimension = currentBlockDimension;
        columnVectorInfo.ordinal = currentBlockDimension.getDimension().getOrdinal();
        allColumnInfo[queryDimensions[i].getQueryOrder()] = columnVectorInfo;
      }
    }
    int measureExistIndex = 0;
    for (int i = 0; i < queryMeasures.length; i++) {
      if (!measureInfo.getMeasureExists()[i]) {
        continue;
      }
      QueryMeasure currentBlockMeasure =
          tableBlockExecutionInfos.getQueryMeasures()[measureExistIndex++];
      ColumnVectorInfo columnVectorInfo = new ColumnVectorInfo();
      columnVectorInfo.measureVectorFiller = MeasureDataVectorProcessor.MeasureVectorFillerFactory
          .getMeasureVectorFiller(currentBlockMeasure.getMeasure().getDataType());
      columnVectorInfo.ordinal = currentBlockMeasure.getMeasure().getOrdinal();
      columnVectorInfo.measure = currentBlockMeasure;
      this.measureColumnInfo[i] = columnVectorInfo;
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
    int numberOfPages = scannedResult.numberOfpages();
    while (scannedResult.getCurrentPageCounter() < numberOfPages) {
      int currentPageRowCount = scannedResult.getCurrentPageRowCount();
      if (currentPageRowCount == 0) {
        scannedResult.incrementPageCounter();
        continue;
      }
      int rowCounter = scannedResult.getRowCounter();
      int availableRows = currentPageRowCount - rowCounter;
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
      scannedResult.fillColumnarMeasureBatch(measureColumnInfo, measureInfo.getMeasureOrdinals());
      scannedResult.fillColumnarComplexBatch(complexInfo);
      // fill default values for non existing dimensions and measures
      fillDataForNonExistingDimensions();
      fillDataForNonExistingMeasures();
      // it means fetched all data out of page so increment the page counter
      if (availableRows == requiredRows) {
        scannedResult.incrementPageCounter();
      } else {
        // Or set the row counter.
        scannedResult.setRowCounter(rowCounter + requiredRows);
      }
      columnarBatch.setActualSize(columnarBatch.getActualSize() + requiredRows);
      columnarBatch.setRowCounter(columnarBatch.getRowCounter() + requiredRows);
    }
  }

  /**
   * This method will fill the default values of non existing dimensions in the current block
   */
  private void fillDataForNonExistingDimensions() {
    for (int i = 0; i < tableBlockExecutionInfos.getActualQueryMeasures().length; i++) {
      if (!dimensionInfo.getDimensionExists()[i]) {
        CarbonDimension dimension =
            tableBlockExecutionInfos.getActualQueryDimensions()[i].getDimension();
        if (dimension.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          // fill direct dictionary column data
          fillDirectDictionaryData(allColumnInfo[i].vector, allColumnInfo[i],
              dimensionInfo.getDefaultValues()[i]);
        } else if (dimension.hasEncoding(Encoding.DICTIONARY)) {
          // fill dictionary column data
          fillDictionaryData(allColumnInfo[i].vector, allColumnInfo[i],
              dimensionInfo.getDefaultValues()[i]);
        } else {
          // fill no dictionary data
          fillNoDictionaryData(allColumnInfo[i].vector, allColumnInfo[i],
              dimension.getDefaultValue());
        }
      }
    }
  }

  /**
   * This method will fill the dictionary column data
   *
   * @param vector
   * @param columnVectorInfo
   * @param defaultValue
   */
  private void fillDictionaryData(CarbonColumnVector vector, ColumnVectorInfo columnVectorInfo,
      Object defaultValue) {
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    for (int j = offset; j < len; j++) {
      vector.putInt(vectorOffset++, (int) defaultValue);
    }
  }

  /**
   * This method will fill the direct dictionary column data
   *
   * @param vector
   * @param columnVectorInfo
   * @param defaultValue
   */
  private void fillDirectDictionaryData(CarbonColumnVector vector,
      ColumnVectorInfo columnVectorInfo, Object defaultValue) {
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    for (int j = offset; j < len; j++) {
      if (null != defaultValue) {
        vector.putLong(vectorOffset++, (long) defaultValue);
      } else {
        vector.putNull(vectorOffset++);
      }
    }
  }

  /**
   * This method will fill the no dictionary column data
   *
   * @param vector
   * @param columnVectorInfo
   * @param defaultValue
   */
  private void fillNoDictionaryData(CarbonColumnVector vector, ColumnVectorInfo columnVectorInfo,
      byte[] defaultValue) {
    int offset = columnVectorInfo.offset;
    int vectorOffset = columnVectorInfo.vectorOffset;
    int len = columnVectorInfo.size + offset;
    for (int j = offset; j < len; j++) {
      if (null != defaultValue) {
        vector.putBytes(vectorOffset++, defaultValue);
      } else {
        vector.putNull(vectorOffset++);
      }
    }
  }

  /**
   * This method will fill the default values of non existing measures in the current block
   */
  private void fillDataForNonExistingMeasures() {
    for (int i = 0; i < tableBlockExecutionInfos.getActualQueryMeasures().length; i++) {
      if (!measureInfo.getMeasureExists()[i]) {
        CarbonMeasure measure = tableBlockExecutionInfos.getActualQueryMeasures()[i].getMeasure();
        ColumnVectorInfo columnVectorInfo = allColumnInfo[i];
        CarbonColumnVector vector = allColumnInfo[i].vector;
        int offset = columnVectorInfo.offset;
        int len = offset + columnVectorInfo.size;
        int vectorOffset = columnVectorInfo.vectorOffset;
        // convert decimal default value to spark decimal type so that new object is not getting
        // created for every row added
        Object defaultValue = convertDecimalValue(measure, measureInfo.getDefaultValues()[i]);
        for (int j = offset; j < len; j++) {
          if (null == defaultValue) {
            vector.putNull(vectorOffset++);
          } else {
            switch (measureInfo.getMeasureDataTypes()[i]) {
              case SHORT:
                vector.putShort(vectorOffset++, (short) defaultValue);
                break;
              case INT:
                vector.putInt(vectorOffset++, (int) defaultValue);
                break;
              case LONG:
                vector.putLong(vectorOffset++, (long) defaultValue);
                break;
              case DECIMAL:
                vector.putDecimal(vectorOffset, (Decimal) defaultValue, measure.getPrecision());
                break;
              default:
                vector.putDouble(vectorOffset++, (double) defaultValue);
            }
          }
        }
      }
    }
  }

  /**
   * This method will parse the measure default value based on data type
   *
   * @param measure
   * @return
   */
  private Object convertDecimalValue(CarbonMeasure measure, Object defaultValue) {
    if (null != measure.getDefaultValue()) {
      switch (measure.getDataType()) {
        case DECIMAL:
          defaultValue = org.apache.spark.sql.types.Decimal.apply((BigDecimal) defaultValue);
          return defaultValue;
        default:
          return defaultValue;
      }
    }
    return defaultValue;
  }

}
