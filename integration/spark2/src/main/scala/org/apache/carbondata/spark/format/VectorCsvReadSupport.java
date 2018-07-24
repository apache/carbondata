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

package org.apache.carbondata.spark.format;

import java.io.IOException;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * read support for csv vector reader
 */
@InterfaceStability.Evolving
@InterfaceAudience.Internal
public class VectorCsvReadSupport<T> implements CarbonReadSupport<T> {
  private static final int MAX_BATCH_SIZE =
      CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
  private CarbonColumn[] carbonColumns;
  private CarbonVectorProxy columnarVectorProxy;
  private StructType outputSchema;

  @Override
  public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable)
      throws IOException {
    this.carbonColumns = carbonColumns;
    outputSchema = new StructType(convertCarbonColumnSpark(carbonColumns));
  }

  private StructField[] convertCarbonColumnSpark(CarbonColumn[] columns) {
    return (StructField[]) new SparkDataTypeConverterImpl().convertCarbonSchemaToSparkSchema(
        columns);
  }

  @Override
  public T readRow(Object[] data) {
    columnarVectorProxy = new CarbonVectorProxy(MemoryMode.OFF_HEAP,outputSchema,MAX_BATCH_SIZE);
    int rowId = 0;
    for (; rowId < data.length; rowId++) {
      for (int colIdx = 0; colIdx < carbonColumns.length; colIdx++) {
        Object originValue = ((Object[]) data[rowId])[colIdx];
        columnarVectorProxy.putRowToColumnBatch(rowId,originValue,colIdx);
      }
    }
    columnarVectorProxy.setNumRows(rowId);
    return (T) columnarVectorProxy.getColumnarBatch();
  }

  @Override
  public void close() {
    if (columnarVectorProxy != null) {
      columnarVectorProxy.close();
    }
  }
}
