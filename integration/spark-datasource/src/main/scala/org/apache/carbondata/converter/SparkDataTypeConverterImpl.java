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

package org.apache.carbondata.converter;

import java.io.Serializable;
import java.math.BigDecimal;

import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.util.DataTypeConverter;

import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData;
import org.apache.spark.sql.catalyst.util.GenericArrayData;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.unsafe.types.UTF8String;

/**
 * Convert java data type to spark data type
 */
public final class SparkDataTypeConverterImpl implements DataTypeConverter, Serializable {

  private static final long serialVersionUID = -4379212832935070583L;

  @Override
  public Object convertFromStringToDecimal(Object data) {
    BigDecimal javaDecVal = new BigDecimal(data.toString());
    return org.apache.spark.sql.types.Decimal.apply(javaDecVal);
  }

  @Override
  public Object convertFromBigDecimalToDecimal(Object data) {
    if (null == data) {
      return null;
    }
    return org.apache.spark.sql.types.Decimal.apply((BigDecimal)data);
  }

  @Override
  public Object convertFromDecimalToBigDecimal(Object data) {
    return ((org.apache.spark.sql.types.Decimal) data).toJavaBigDecimal();
  }

  @Override
  public byte[] convertFromStringToByte(Object data) {
    if (null == data) {
      return null;
    }
    return UTF8String.fromString((String) data).getBytes();
  }

  @Override
  public Object convertFromByteToUTF8String(byte[] data) {
    if (null == data) {
      return null;
    }
    return UTF8String.fromBytes(data);
  }

  @Override
  public byte[] convertFromByteToUTF8Bytes(byte[] data) {
    return UTF8String.fromBytes(data).getBytes();
  }

  @Override
  public Object convertFromStringToUTF8String(Object data) {
    if (null == data) {
      return null;
    }
    return UTF8String.fromString((String) data);
  }

  @Override
  public Object wrapWithGenericArrayData(Object data) {
    return new GenericArrayData(data);
  }

  @Override
  public Object wrapWithGenericRow(Object[] fields) {
    return new GenericInternalRow(fields);
  }

  @Override
  public Object wrapWithArrayBasedMapData(Object[] keyArray, Object[] valueArray) {
    return new ArrayBasedMapData(new GenericArrayData(keyArray), new GenericArrayData(valueArray));
  }

  @Override
  public Object[] unwrapGenericRowToObject(Object data) {
    GenericInternalRow row = (GenericInternalRow) data;
    return row.values();
  }

  public static org.apache.spark.sql.types.DataType convertCarbonToSparkDataType(
      DataType carbonDataType) {
    if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.STRING
        || carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.VARCHAR) {
      return DataTypes.StringType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT) {
      return DataTypes.ShortType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT) {
      return DataTypes.IntegerType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG) {
      return DataTypes.LongType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE) {
      return DataTypes.DoubleType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN) {
      return DataTypes.BooleanType;
    } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(carbonDataType)) {
      return DataTypes.createDecimalType();
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.TIMESTAMP) {
      return DataTypes.TimestampType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.DATE) {
      return DataTypes.DateType;
    } else if (carbonDataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BINARY) {
      return DataTypes.BinaryType;
    } else {
      return null;
    }
  }

  /**
   * convert from CarbonColumn array to Spark's StructField array
   */
  @Override
  public Object[] convertCarbonSchemaToSparkSchema(CarbonColumn[] carbonColumns) {
    StructField[] fields = new StructField[carbonColumns.length];
    for (int i = 0; i < carbonColumns.length; i++) {
      CarbonColumn carbonColumn = carbonColumns[i];
      if (carbonColumn.isDimension()) {
        if (carbonColumn.hasEncoding(Encoding.DIRECT_DICTIONARY)) {
          DirectDictionaryGenerator generator = DirectDictionaryKeyGeneratorFactory
              .getDirectDictionaryGenerator(carbonColumn.getDataType());
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(generator.getReturnType()), true, null);
        } else if (!carbonColumn.hasEncoding(Encoding.DICTIONARY)) {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(carbonColumn.getDataType()), true, null);
        } else if (carbonColumn.isComplex()) {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(carbonColumn.getDataType()), true, null);
        } else {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(
                  org.apache.carbondata.core.metadata.datatype.DataTypes.INT), true, null);
        }
      } else if (carbonColumn.isMeasure()) {
        DataType dataType = carbonColumn.getDataType();
        if (dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BOOLEAN
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.SHORT
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.INT
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.LONG
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.BINARY
            || dataType == org.apache.carbondata.core.metadata.datatype.DataTypes.VARCHAR) {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(dataType), true, null);
        } else if (org.apache.carbondata.core.metadata.datatype.DataTypes.isDecimal(dataType)) {
          CarbonMeasure measure = (CarbonMeasure) carbonColumn;
          fields[i] = new StructField(carbonColumn.getColName(),
              new DecimalType(measure.getPrecision(), measure.getScale()), true, null);
        } else {
          fields[i] = new StructField(carbonColumn.getColName(),
              convertCarbonToSparkDataType(
                  org.apache.carbondata.core.metadata.datatype.DataTypes.DOUBLE), true, null);
        }
      }
    }
    return fields;
  }

}
