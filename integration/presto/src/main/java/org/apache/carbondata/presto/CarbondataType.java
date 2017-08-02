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
package org.apache.carbondata.presto;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

public class CarbondataType {
  public static Type getSpiType(ColumnSchema columnSchema) {
    DataType carbonType = columnSchema.getDataType();
    switch (carbonType) {
      case BOOLEAN:
        return BooleanType.BOOLEAN;
      case SHORT:
        return SmallintType.SMALLINT;
      case INT:
        return IntegerType.INTEGER;
      case LONG:
        return BigintType.BIGINT;
      case FLOAT:
      case DOUBLE:
        return DoubleType.DOUBLE;
      case DECIMAL:
        if (columnSchema.getPrecision() > 0) {
          return DecimalType
              .createDecimalType(columnSchema.getPrecision(), columnSchema.getScale());
        } else {
          return DecimalType.createDecimalType();
        }
      case STRING:
        return VarcharType.VARCHAR;
      case DATE:
        return DateType.DATE;
      case TIMESTAMP:
        return TimestampType.TIMESTAMP;
      default:
        throw new PrestoException(CarbondataErrorCode.CARBON_NOT_SUPPORT_TYPE,
            "Unsupported Carbondata type: " + carbonType.getName());
    }
  }

  public static DataType toCarbonType(CarbondataColumnHandle columnHandle) {

    Type spiType = columnHandle.getColumnType();
    if (spiType.equals(BooleanType.BOOLEAN)) {
      return DataType.BOOLEAN;
    } else if (spiType.equals(SmallintType.SMALLINT)) {
      return DataType.SHORT;
    } else if (spiType.equals(IntegerType.INTEGER)) {
      return DataType.INT;
    } else if (spiType.equals(BigintType.BIGINT)) {
      return DataType.LONG;
    } else if (spiType.equals(DoubleType.DOUBLE)) {
      return DataType.DOUBLE;
    } else if (spiType.equals(VarcharType.VARCHAR)) {
      return DataType.STRING;
    } else if (spiType.equals(DateType.DATE)) {
      return DataType.DATE;
    } else if (spiType.equals(TimestampType.TIMESTAMP)) {
      return DataType.TIMESTAMP;
    } else if (spiType.getClass().isAssignableFrom(DecimalType.class)) {
      return DataType.DECIMAL;
    } else {
      throw new PrestoException(CarbondataErrorCode.CARBON_NOT_SUPPORT_TYPE,
          "not support type: " + spiType.getDisplayName());
    }
  }

  public static Object getDataByType(Object rawData, Type spiType) {
    if (spiType.equals(VarcharType.VARCHAR)) {
      return ((Slice) rawData).toStringUtf8();
    }
    //DATE,TIMESTAMP,DecimalType,BOOLEAN,SMALLINT,INTEGER,BIGINT,DOUBLE
    return rawData;
  }
}
