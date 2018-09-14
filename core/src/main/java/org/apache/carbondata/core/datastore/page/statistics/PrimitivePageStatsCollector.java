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

package org.apache.carbondata.core.datastore.page.statistics;

import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

import static org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert.FALSE_VALUE;
import static org.apache.carbondata.core.datastore.page.encoding.bool.BooleanConvert.TRUE_VALUE;

/** statics for primitive column page */
public class PrimitivePageStatsCollector implements ColumnPageStatsCollector, SimpleStatsResult {
  private DataType dataType;
  private byte minByte, maxByte;
  private short minShort, maxShort;
  private int minInt, maxInt;
  private long minLong, maxLong;
  private double minDouble, maxDouble;
  private float minFloat, maxFloat;
  private BigDecimal minDecimal, maxDecimal;

  // scale of the double value, apply adaptive encoding if this is positive
  private int decimal;

  // scale of the double value, only for complex primitive.
  private int decimalCountForComplexPrimitive;

  private boolean isFirst = true;
  private BigDecimal zeroDecimal;

  // this is for encode flow
  public static PrimitivePageStatsCollector newInstance(DataType dataType) {
    return new PrimitivePageStatsCollector(dataType);
  }

  // this is for decode flow, create stats from encoder meta in carbondata file
  public static PrimitivePageStatsCollector newInstance(ColumnPageEncoderMeta meta) {
    PrimitivePageStatsCollector instance =
        new PrimitivePageStatsCollector(meta.getSchemaDataType());
    // set min max from meta
    DataType dataType = meta.getSchemaDataType();
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      instance.minByte = (byte) meta.getMinValue();
      instance.maxByte = (byte) meta.getMaxValue();
    } else if (dataType == DataTypes.SHORT) {
      instance.minShort = (short) meta.getMinValue();
      instance.maxShort = (short) meta.getMaxValue();
    } else if (dataType == DataTypes.INT) {
      instance.minInt = (int) meta.getMinValue();
      instance.maxInt = (int) meta.getMaxValue();
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      instance.minLong = (long) meta.getMinValue();
      instance.maxLong = (long) meta.getMaxValue();
    } else if (dataType == DataTypes.DOUBLE) {
      instance.minDouble = (double) meta.getMinValue();
      instance.maxDouble = (double) meta.getMaxValue();
      instance.decimal = meta.getDecimal();
    } else if (dataType == DataTypes.FLOAT) {
      instance.minFloat = (float) meta.getMinValue();
      instance.maxFloat = (float) meta.getMaxValue();
      instance.decimal = meta.getDecimal();
    } else if (DataTypes.isDecimal(dataType)) {
      instance.minDecimal = (BigDecimal) meta.getMinValue();
      instance.maxDecimal = (BigDecimal) meta.getMaxValue();
      instance.decimal = meta.getDecimal();
    } else {
      throw new UnsupportedOperationException(
          "unsupported data type for stats collection: " + meta.getSchemaDataType());
    }
    return instance;
  }

  public static PrimitivePageStatsCollector newInstance(ValueEncoderMeta meta) {
    PrimitivePageStatsCollector instance =
        new PrimitivePageStatsCollector(DataType.getDataType(meta.getType()));
    // set min max from meta
    DataType dataType = DataType.getDataType(meta.getType());
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      instance.minByte = (byte) meta.getMinValue();
      instance.maxByte = (byte) meta.getMaxValue();
    } else if (dataType == DataTypes.SHORT) {
      instance.minShort = (short) meta.getMinValue();
      instance.maxShort = (short) meta.getMaxValue();
    } else if (dataType == DataTypes.INT) {
      instance.minInt = (int) meta.getMinValue();
      instance.maxInt = (int) meta.getMaxValue();
    } else if (dataType == DataTypes.LEGACY_LONG || dataType == DataTypes.LONG
        || dataType == DataTypes.TIMESTAMP) {
      instance.minLong = (long) meta.getMinValue();
      instance.maxLong = (long) meta.getMaxValue();
    } else if (dataType == DataTypes.DOUBLE) {
      instance.minDouble = (double) meta.getMinValue();
      instance.maxDouble = (double) meta.getMaxValue();
      instance.decimal = meta.getDecimal();
    } else if (dataType == DataTypes.FLOAT) {
      instance.minFloat = (float) meta.getMinValue();
      instance.maxFloat = (float) meta.getMaxValue();
      instance.decimal = meta.getDecimal();
    } else if (DataTypes.isDecimal(dataType)) {
      instance.minDecimal = (BigDecimal) meta.getMinValue();
      instance.maxDecimal = (BigDecimal) meta.getMaxValue();
      instance.decimal = meta.getDecimal();
    } else {
      throw new UnsupportedOperationException(
          "unsupported data type for Stats collection: " + meta.getType());
    }
    return instance;
  }

  private PrimitivePageStatsCollector(DataType dataType) {
    this.dataType = dataType;
    if (dataType == DataTypes.BOOLEAN) {
      minByte = TRUE_VALUE;
      maxByte = FALSE_VALUE;
    } else if (dataType == DataTypes.BYTE) {
      minByte = Byte.MAX_VALUE;
      maxByte = Byte.MIN_VALUE;
    } else if (dataType == DataTypes.SHORT) {
      minShort = Short.MAX_VALUE;
      maxShort = Short.MIN_VALUE;
    } else if (dataType == DataTypes.INT) {
      minInt = Integer.MAX_VALUE;
      maxInt = Integer.MIN_VALUE;
    } else if (dataType == DataTypes.LEGACY_LONG || dataType == DataTypes.LONG
        || dataType == DataTypes.TIMESTAMP) {
      minLong = Long.MAX_VALUE;
      maxLong = Long.MIN_VALUE;
    } else if (dataType == DataTypes.DOUBLE) {
      minDouble = Double.POSITIVE_INFINITY;
      maxDouble = Double.NEGATIVE_INFINITY;
      decimal = 0;
    } else if (dataType == DataTypes.FLOAT) {
      minFloat = Float.MAX_VALUE;
      maxFloat = Float.MIN_VALUE;
      decimal = 0;
    } else if (DataTypes.isDecimal(dataType)) {
      this.zeroDecimal = BigDecimal.ZERO;
      decimal = 0;
    } else {
      throw new UnsupportedOperationException(
          "unsupported data type for Stats collection: " + dataType);
    }
  }

  @Override
  public void updateNull(int rowId) {
    long value = 0;
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      update((byte) value);
    } else if (dataType == DataTypes.SHORT) {
      update((short) value);
    } else if (dataType == DataTypes.INT) {
      update((int) value);
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      update(value);
    } else if (dataType == DataTypes.DOUBLE) {
      update(0d);
    } else if (dataType == DataTypes.FLOAT) {
      update(0f);
    } else if (DataTypes.isDecimal(dataType)) {
      if (isFirst) {
        maxDecimal = zeroDecimal;
        minDecimal = zeroDecimal;
        isFirst = false;
      } else {
        maxDecimal = (maxDecimal.compareTo(zeroDecimal) > 0) ? maxDecimal : zeroDecimal;
        minDecimal = (minDecimal.compareTo(zeroDecimal) < 0) ? minDecimal : zeroDecimal;
      }
    } else {
      throw new UnsupportedOperationException(
          "unsupported data type for Stats collection: " + dataType);
    }
  }

  @Override
  public void update(byte value) {
    if (minByte > value) {
      minByte = value;
    }
    if (maxByte < value) {
      maxByte = value;
    }
  }

  @Override
  public void update(short value) {
    if (minShort > value) {
      minShort = value;
    }
    if (maxShort < value) {
      maxShort = value;
    }
  }

  @Override
  public void update(int value) {
    if (minInt > value) {
      minInt = value;
    }
    if (maxInt < value) {
      maxInt = value;
    }
  }

  @Override
  public void update(long value) {
    if (minLong > value) {
      minLong = value;
    }
    if (maxLong < value) {
      maxLong = value;
    }
  }

  /**
   * Return number of digit after decimal point
   * TODO: it operation is costly, optimize for performance
   */
  private int getDecimalCount(double value) {
    int decimalPlaces = 0;
    try {
      String strValue = BigDecimal.valueOf(Math.abs(value)).toPlainString();
      int integerPlaces = strValue.indexOf('.');
      if (-1 != integerPlaces) {
        decimalPlaces = strValue.length() - integerPlaces - 1;
      }
    } catch (NumberFormatException e) {
      if (!Double.isInfinite(value)) {
        throw e;
      }
    }
    return decimalPlaces;
  }

  private int getDecimalCount(float value) {
    int decimalPlaces = 0;
    try {
      String strValue = Float.valueOf(Math.abs(value)).toString();
      int integerPlaces = strValue.indexOf('.');
      if (-1 != integerPlaces) {
        decimalPlaces = strValue.length() - integerPlaces - 1;
      }
    } catch (NumberFormatException e) {
      if (!Double.isInfinite(value)) {
        throw e;
      }
    }
    return decimalPlaces;
  }

  @Override
  public void update(double value) {
    if (minDouble > value) {
      minDouble = value;
    }
    if (maxDouble < value) {
      maxDouble = value;
    }
    if (decimal >= 0) {
      int decimalCount = getDecimalCount(value);
      decimalCountForComplexPrimitive = decimalCount;
      if (decimalCount > 5) {
        // If deciaml count is too big, we do not do adaptive encoding.
        // So set decimal to negative value
        decimal = -1;
      } else if (decimalCount > decimal) {
        this.decimal = decimalCount;
      }
    }
  }
  @Override
  public void update(float value) {
    if (minFloat > value) {
      minFloat = value;
    }
    if (maxFloat < value) {
      maxFloat = value;
    }
    if (decimal >= 0) {
      int decimalCount = getDecimalCount(value);
      decimalCountForComplexPrimitive = decimalCount;
      if (decimalCount > 5) {
        // If deciaml count is too big, we do not do adaptive encoding.
        // So set decimal to negative value
        decimal = -1;
      } else if (decimalCount > decimal) {
        this.decimal = decimalCount;
      }
    }
  }

  public int getDecimalForComplexPrimitive() {
    decimal = decimalCountForComplexPrimitive;
    return decimalCountForComplexPrimitive;
  }

  @Override
  public void update(BigDecimal decimalValue) {
    if (isFirst) {
      maxDecimal = decimalValue;
      minDecimal = decimalValue;
      isFirst = false;
    } else {
      maxDecimal = (decimalValue.compareTo(maxDecimal) > 0) ? decimalValue : maxDecimal;
      minDecimal = (decimalValue.compareTo(minDecimal) < 0) ? decimalValue : minDecimal;
    }
  }

  @Override
  public void update(byte[] value) {
  }

  @Override
  public SimpleStatsResult getPageStats() {
    return this;
  }

  @Override
  public String toString() {
    if (dataType == DataTypes.BOOLEAN) {
      return String.format("min: %s, max: %s ", BooleanConvert.byte2Boolean(minByte),
              BooleanConvert.byte2Boolean(minByte));
    } else if (dataType == DataTypes.BYTE) {
      return String.format("min: %s, max: %s, decimal: %s ", minByte, maxByte, decimal);
    } else if (dataType == DataTypes.SHORT) {
      return String.format("min: %s, max: %s, decimal: %s ", minShort, maxShort, decimal);
    } else if (dataType == DataTypes.INT) {
      return String.format("min: %s, max: %s, decimal: %s ", minInt, maxInt, decimal);
    } else if (dataType == DataTypes.LONG) {
      return String.format("min: %s, max: %s, decimal: %s ", minLong, maxLong, decimal);
    } else if (dataType == DataTypes.DOUBLE) {
      return String.format("min: %s, max: %s, decimal: %s ", minDouble, maxDouble, decimal);
    } else if (dataType == DataTypes.FLOAT) {
      return String.format("min: %s, max: %s, decimal: %s ", minFloat, maxFloat, decimal);
    }
    return super.toString();
  }

  @Override
  public Object getMin() {
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      return minByte;
    } else if (dataType == DataTypes.SHORT) {
      return minShort;
    } else if (dataType == DataTypes.INT) {
      return minInt;
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      return minLong;
    } else if (dataType == DataTypes.DOUBLE) {
      return minDouble;
    } else if (dataType == DataTypes.FLOAT) {
      return minFloat;
    } else if (DataTypes.isDecimal(dataType)) {
      return minDecimal;
    }
    return null;
  }

  @Override
  public Object getMax() {
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      return maxByte;
    } else if (dataType == DataTypes.SHORT) {
      return maxShort;
    } else if (dataType == DataTypes.INT) {
      return maxInt;
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      return maxLong;
    } else if (dataType == DataTypes.DOUBLE) {
      return maxDouble;
    } else if (dataType == DataTypes.FLOAT) {
      return maxFloat;
    } else if (DataTypes.isDecimal(dataType)) {
      return maxDecimal;
    }
    return null;
  }

  @Override
  public int getDecimalCount() {
    return decimal;
  }

  @Override
  public DataType getDataType() {
    return dataType;
  }

  @Override public boolean writeMinMax() {
    return true;
  }

}