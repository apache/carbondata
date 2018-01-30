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

import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.collector.ScannedResultCollector;
import org.apache.carbondata.core.scan.executor.infos.BlockExecutionInfo;
import org.apache.carbondata.core.scan.executor.infos.DimensionInfo;
import org.apache.carbondata.core.scan.executor.infos.MeasureInfo;
import org.apache.carbondata.core.scan.model.ProjectionMeasure;
import org.apache.carbondata.core.scan.result.BlockletScannedResult;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnarBatch;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * It is not a collector it is just a scanned result holder.
 */
public abstract class AbstractScannedResultCollector implements ScannedResultCollector {

  /**
   * table block execution infos
   */
  BlockExecutionInfo executionInfo;

  /**
   * maintains the measure information like datatype, ordinal, measure existence
   */
  MeasureInfo measureInfo;

  /**
   * maintains the dimension information like datatype, ordinal, measure existence
   */
  DimensionInfo dimensionInfo;

  AbstractScannedResultCollector(BlockExecutionInfo blockExecutionInfos) {
    this.executionInfo = blockExecutionInfos;
    measureInfo = blockExecutionInfos.getMeasureInfo();
    dimensionInfo = blockExecutionInfos.getDimensionInfo();
  }

  protected void fillMeasureData(Object[] msrValues, int offset,
      BlockletScannedResult scannedResult) {
    int measureExistIndex = 0;
    for (short i = 0; i < measureInfo.getMeasureDataTypes().length; i++) {
      // if measure exists is block then pass measure column
      // data chunk to the collector
      if (measureInfo.getMeasureExists()[i]) {
        ProjectionMeasure queryMeasure = executionInfo.getProjectionMeasures()[measureExistIndex];
        msrValues[i + offset] = getMeasureData(
            scannedResult.getMeasureChunk(measureInfo.getMeasureOrdinals()[measureExistIndex]),
            scannedResult.getCurrentRowId(), queryMeasure.getMeasure());
        measureExistIndex++;
      } else {
        // if not then get the default value and use that value in aggregation
        Object defaultValue = measureInfo.getDefaultValues()[i];
        if (null != defaultValue && DataTypes.isDecimal(measureInfo.getMeasureDataTypes()[i])) {
          // convert data type as per the computing engine
          defaultValue = DataTypeUtil.getDataTypeConverter().convertToDecimal(defaultValue);
        }
        msrValues[i + offset] = defaultValue;
      }
    }
  }

  Object getMeasureData(ColumnPage dataChunk, int index, CarbonMeasure carbonMeasure) {
    if (!dataChunk.getNullBits().get(index)) {
      DataType dataType = carbonMeasure.getDataType();
      if (dataType == DataTypes.BOOLEAN) {
        return dataChunk.getBoolean(index);
      } else if (dataType == DataTypes.SHORT) {
        return (short) dataChunk.getLong(index);
      } else if (dataType == DataTypes.INT) {
        return (int) dataChunk.getLong(index);
      } else if (dataType == DataTypes.LONG) {
        return dataChunk.getLong(index);
      } else if (DataTypes.isDecimal(dataType)) {
        BigDecimal bigDecimalMsrValue = dataChunk.getDecimal(index);
        if (null != bigDecimalMsrValue && carbonMeasure.getScale() > bigDecimalMsrValue.scale()) {
          bigDecimalMsrValue =
              bigDecimalMsrValue.setScale(carbonMeasure.getScale(), RoundingMode.HALF_UP);
        }
        // convert data type as per the computing engine
        return DataTypeUtil.getDataTypeConverter().convertToDecimal(bigDecimalMsrValue);
      } else {
        return dataChunk.getDouble(index);
      }
    }
    return null;
  }

  @Override
  public void collectResultInColumnarBatch(BlockletScannedResult scannedResult,
      CarbonColumnarBatch columnarBatch) {
    throw new UnsupportedOperationException("Works only for batch collectors");
  }
}
