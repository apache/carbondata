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

package org.apache.carbondata.stream;

import java.io.IOException;

import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.format.BlockletHeader;
import org.apache.carbondata.hadoop.InputMetricsStats;
import org.apache.carbondata.hadoop.stream.StreamRecordReader;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Stream vector/row record reader
 */
public class CarbonStreamRecordReader extends StreamRecordReader {

  // vector reader
  protected boolean isVectorReader;
  private CarbonVectorProxy vectorProxy;
  private StructType outputSchema;
  private InternalRow outputRow;

  // InputMetricsStats
  private InputMetricsStats inputMetricsStats;

  public CarbonStreamRecordReader(boolean isVectorReader, InputMetricsStats inputMetricsStats,
      QueryModel mdl, boolean useRawRow) {
    super(mdl, useRawRow);
    this.isVectorReader = isVectorReader;
    this.inputMetricsStats = inputMetricsStats;
  }

  protected void initializeAtFirstRow() throws IOException {
    super.initializeAtFirstRow();
    outputRow = new GenericInternalRow(outputValues);
    outputSchema = new StructType((StructField[])
        DataTypeUtil.getDataTypeConverter().convertCarbonSchemaToSparkSchema(projection));
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    if (isFirstRow) {
      isFirstRow = false;
      initializeAtFirstRow();
    }
    if (isFinished) {
      return false;
    }

    if (isVectorReader) {
      return nextColumnarBatch();
    }

    return nextRow();
  }

  @Override
  public Object getCurrentValue() {
    if (isVectorReader) {
      int value = vectorProxy.numRows();
      if (inputMetricsStats != null) {
        inputMetricsStats.incrementRecordRead((long) value);
      }

      return vectorProxy.getColumnarBatch();
    }

    if (inputMetricsStats != null) {
      inputMetricsStats.incrementRecordRead(1L);
    }

    return outputRow;
  }

  /**
   * for vector reader, check next columnar batch
   */
  private boolean nextColumnarBatch() throws IOException {
    boolean hasNext;
    boolean scanMore = false;
    do {
      // move to the next blocklet
      hasNext = input.nextBlocklet();
      if (hasNext) {
        // read blocklet header
        BlockletHeader header = input.readBlockletHeader();
        if (isScanRequired(header)) {
          scanMore = !scanBlockletAndFillVector(header);
        } else {
          input.skipBlockletData(true);
          scanMore = true;
        }
      } else {
        isFinished = true;
        scanMore = false;
      }
    } while (scanMore);
    return hasNext;
  }

  private boolean scanBlockletAndFillVector(BlockletHeader header) throws IOException {
    // if filter is null and output projection is empty, use the row number of blocklet header
    if (skipScanData) {
      int rowNums = header.getBlocklet_info().getNum_rows();
      vectorProxy = new CarbonVectorProxy(MemoryMode.OFF_HEAP, outputSchema, rowNums, false);
      vectorProxy.setNumRows(rowNums);
      input.skipBlockletData(true);
      return rowNums > 0;
    }

    input.readBlockletData(header);
    vectorProxy =
        new CarbonVectorProxy(MemoryMode.OFF_HEAP, outputSchema, input.getRowNums(), false);
    int rowNum = 0;
    if (null == filter) {
      while (input.hasNext()) {
        readRowFromStream();
        putRowToColumnBatch(rowNum++);
      }
    } else {
      try {
        while (input.hasNext()) {
          readRowFromStream();
          if (filter.applyFilter(filterRow, carbonTable.getDimensionOrdinalMax())) {
            putRowToColumnBatch(rowNum++);
          }
        }
      } catch (FilterUnsupportedException e) {
        throw new IOException("Failed to filter row in vector reader", e);
      }
    }
    vectorProxy.setNumRows(rowNum);
    return rowNum > 0;
  }

  private void putRowToColumnBatch(int rowId) {
    for (int i = 0; i < projection.length; i++) {
      Object value = outputValues[i];
      vectorProxy.getColumnVector(i).putRowToColumnBatch(rowId,value);

    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (null != vectorProxy) {
      vectorProxy.close();
    }
  }
}
