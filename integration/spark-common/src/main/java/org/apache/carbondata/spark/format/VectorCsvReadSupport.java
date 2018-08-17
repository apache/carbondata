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
import java.math.BigInteger;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.annotations.InterfaceStability;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * read support for csv vector reader
 */
@InterfaceStability.Evolving
@InterfaceAudience.Internal
public class VectorCsvReadSupport<T> implements CarbonReadSupport<T> {

  private static final int MAX_BATCH_SIZE =
      CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
  private CarbonColumn[] carbonColumns;
  private ColumnarBatch columnarBatch;
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
    columnarBatch = ColumnarBatch.allocate(outputSchema, MemoryMode.OFF_HEAP, MAX_BATCH_SIZE);
    int rowId = 0;
    for (; rowId < data.length; rowId++) {
      for (int colIdx = 0; colIdx < carbonColumns.length; colIdx++) {
        Object originValue = ((Object[]) data[rowId])[colIdx];
        ColumnVector col = columnarBatch.column(colIdx);
        org.apache.spark.sql.types.DataType t = col.dataType();
        if (null == originValue) {
          col.putNull(rowId);
        } else {
          String value = String.valueOf(originValue);
          if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
            col.putBoolean(rowId, Boolean.parseBoolean(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
            col.putByte(rowId, Byte.parseByte(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
            col.putShort(rowId, Short.parseShort(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
            col.putInt(rowId, Integer.parseInt(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
            col.putLong(rowId, Long.parseLong(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
            col.putFloat(rowId, Float.parseFloat(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
            col.putDouble(rowId, Double.parseDouble(value));
          } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
            UTF8String v = UTF8String.fromString(value);
            col.putByteArray(rowId, v.getBytes());
          } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
            DecimalType dt = (DecimalType)t;
            Decimal d = Decimal.fromDecimal(value);
            if (dt.precision() <= Decimal.MAX_INT_DIGITS()) {
              col.putInt(rowId, (int)d.toUnscaledLong());
            } else if (dt.precision() <= Decimal.MAX_LONG_DIGITS()) {
              col.putLong(rowId, d.toUnscaledLong());
            } else {
              final BigInteger integer = d.toJavaBigDecimal().unscaledValue();
              byte[] bytes = integer.toByteArray();
              col.putByteArray(rowId, bytes, 0, bytes.length);
            }
          } else if (t instanceof CalendarIntervalType) {
            CalendarInterval c = CalendarInterval.fromString(value);
            col.getChildColumn(0).putInt(rowId, c.months);
            col.getChildColumn(1).putLong(rowId, c.microseconds);
          } else if (t instanceof org.apache.spark.sql.types.DateType) {
            col.putInt(rowId, Integer.parseInt(value));
          } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
            col.putLong(rowId, Long.parseLong(value));
          }
        }
      }
    }
    columnarBatch.setNumRows(rowId);
    return (T) columnarBatch;
  }

  @Override
  public void close() {
    if (columnarBatch != null) {
      columnarBatch.close();
    }
  }
}
