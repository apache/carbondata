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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.hadoop.readsupport.CarbonReadSupport;
import org.apache.carbondata.spark.util.SparkDataTypeConverterImpl;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.CalendarIntervalType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.unsafe.types.CalendarInterval;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * read support for csv
 */
@InterfaceStability.Evolving
@InterfaceAudience.Internal
public class CsvReadSupport<T> implements CarbonReadSupport<T> {
  private CarbonColumn[] carbonColumns;
  private StructType outputSchema;
  private Object[] finalOutputValues;
  @Override
  public void initialize(CarbonColumn[] carbonColumns, CarbonTable carbonTable)
      throws IOException {
    this.carbonColumns = carbonColumns;
    this.finalOutputValues = new Object[carbonColumns.length];
    outputSchema = new StructType(convertCarbonColumnSpark(carbonColumns));
  }

  private StructField[] convertCarbonColumnSpark(CarbonColumn[] columns) {
    return (StructField[]) new SparkDataTypeConverterImpl().convertCarbonSchemaToSparkSchema(
        columns);
  }

  @Override
  public T readRow(Object[] data) {
    for (int i = 0; i < carbonColumns.length; i++) {
      Object originValue = data[i];
      org.apache.spark.sql.types.DataType t = outputSchema.apply(i).dataType();
      finalOutputValues[i] = convertToSparkValue(originValue, t);
    }
    return (T) new GenericInternalRow(finalOutputValues);
  }
  private Object convertToSparkValue(Object originValue, org.apache.spark.sql.types.DataType t) {
    if (null == originValue) {
      return null;
    } else {
      String value = String.valueOf(originValue);
      if (t == org.apache.spark.sql.types.DataTypes.BooleanType) {
        return Boolean.parseBoolean(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.ByteType) {
        return Byte.parseByte(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.ShortType) {
        return Short.parseShort(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.IntegerType) {
        return Integer.parseInt(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.LongType) {
        return Long.parseLong(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.FloatType) {
        return Float.parseFloat(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.DoubleType) {
        return Double.parseDouble(value);
      } else if (t == org.apache.spark.sql.types.DataTypes.StringType) {
        return UTF8String.fromString(value);
      } else if (t instanceof org.apache.spark.sql.types.DecimalType) {
        return Decimal.fromDecimal(value);
      } else if (t instanceof CalendarIntervalType) {
        return CalendarInterval.fromString(value);
      } else if (t instanceof org.apache.spark.sql.types.DateType) {
        return Integer.parseInt(value);
      } else if (t instanceof org.apache.spark.sql.types.TimestampType) {
        return Long.parseLong(value);
      } else {
        return null;
      }
    }
  }

  @Override
  public void close() {

  }
}
