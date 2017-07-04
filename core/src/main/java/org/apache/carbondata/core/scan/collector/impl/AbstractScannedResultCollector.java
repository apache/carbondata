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
import java.math.RoundingMode;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.chunk.MeasureColumnDataChunk;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DimensionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.result.AbstractScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;

/**
 * It is not a collector it is just a scanned result holder.
 */
public abstract class AbstractScannedResultCollector implements ScannedResultCollector {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AbstractScannedResultCollector.class.getName());

  /**
   * table block execution infos
   */
  protected BlockExecutionInfo tableBlockExecutionInfos;

  /**
   * maintains the measure information like datatype, ordinal, measure existence
   */
  protected MeasureInfo measureInfo;

  /**
   * maintains the dimension information like datatype, ordinal, measure existence
   */
  protected DimensionInfo dimensionInfo;

  public AbstractScannedResultCollector(BlockExecutionInfo blockExecutionInfos) {
    this.tableBlockExecutionInfos = blockExecutionInfos;
    measureInfo = blockExecutionInfos.getMeasureInfo();
    dimensionInfo = blockExecutionInfos.getDimensionInfo();
  }

  protected void fillMeasureData(Object[] msrValues, int offset,
      AbstractScannedResult scannedResult) {
    int measureExistIndex = 0;
    for (short i = 0; i < measureInfo.getMeasureDataTypes().length; i++) {
      // if measure exists is block then pass measure column
      // data chunk to the collector
      if (measureInfo.getMeasureExists()[i]) {
        QueryMeasure queryMeasure = tableBlockExecutionInfos.getQueryMeasures()[measureExistIndex];
        msrValues[i + offset] = getMeasureData(
            scannedResult.getMeasureChunk(measureInfo.getMeasureOrdinals()[measureExistIndex]),
            scannedResult.getCurrentRowId(), queryMeasure.getMeasure());
        measureExistIndex++;
      } else {
        // if not then get the default value and use that value in aggregation
        Object defaultValue = measureInfo.getDefaultValues()[i];
        if (null != defaultValue && measureInfo.getMeasureDataTypes()[i] == DataType.DECIMAL) {
          // convert java big decimal to spark decimal type
          defaultValue = org.apache.spark.sql.types.Decimal.apply((BigDecimal) defaultValue);
        }
        msrValues[i + offset] = defaultValue;
      }
    }
  }

  protected Object getMeasureData(MeasureColumnDataChunk dataChunk, int index,
      CarbonMeasure carbonMeasure) {
    if (!dataChunk.getNullValueIndexHolder().getBitSet().get(index)) {
      switch (carbonMeasure.getDataType()) {
        case SHORT:
          return (short)dataChunk.getColumnPage().getLong(index);
        case INT:
          return (int)dataChunk.getColumnPage().getLong(index);
        case LONG:
          return dataChunk.getColumnPage().getLong(index);
        case DECIMAL:
          BigDecimal bigDecimalMsrValue =
              dataChunk.getColumnPage().getDecimal(index);
          if (null != bigDecimalMsrValue && carbonMeasure.getScale() > bigDecimalMsrValue.scale()) {
            bigDecimalMsrValue =
                bigDecimalMsrValue.setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
          }
          return org.apache.spark.sql.types.Decimal.apply(bigDecimalMsrValue);
        default:
          return dataChunk.getColumnPage().getDouble(index);
      }
    }
    return null;
  }

  @Override public void collectVectorBatch(AbstractScannedResult scannedResult,
      CarbonColumnarBatch columnarBatch) {
    throw new UnsupportedOperationException("Works only for batch collectors");
  }
}
