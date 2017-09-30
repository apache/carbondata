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
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.CarbonUtil;

/** statics for primitive column page */
public class PrimitivePageStatsCollector implements ColumnPageStatsCollector, SimpleStatsResult {
  private DataType dataType;
  private byte minByte, maxByte;
  private short minShort, maxShort;
  private int minInt, maxInt;
  private long minLong, maxLong;
  private double minDouble, maxDouble;
  private BigDecimal minDecimal, maxDecimal;
  private int scale, precision;

  // scale of the double value, apply adaptive encoding if this is positive
  private int decimal;

  private boolean isFirst = true;
  private BigDecimal zeroDecimal;

  // this is for encode flow
  public static PrimitivePageStatsCollector newInstance(DataType dataType,
      int scale, int precision) {
    switch (dataType) {
      default:
        return new PrimitivePageStatsCollector(dataType, scale, precision);
    }
  }

  // this is for decode flow, create stats from encoder meta in carbondata file
  public static PrimitivePageStatsCollector newInstance(ColumnPageEncoderMeta meta) {
    PrimitivePageStatsCollector instance = new PrimitivePageStatsCollector(meta.getSchemaDataType(),
        meta.getScale(), meta.getPrecision());
    // set min max from meta
    switch (meta.getSchemaDataType()) {
      case BYTE:
        instance.minByte = (byte) meta.getMinValue();
        instance.maxByte = (byte) meta.getMaxValue();
        break;
      case SHORT:
        instance.minShort = (short) meta.getMinValue();
        instance.maxShort = (short) meta.getMaxValue();
        break;
      case INT:
        instance.minInt = (int) meta.getMinValue();
        instance.maxInt = (int) meta.getMaxValue();
        break;
      case LONG:
        instance.minLong = (long) meta.getMinValue();
        instance.maxLong = (long) meta.getMaxValue();
        break;
      case DOUBLE:
        instance.minDouble = (double) meta.getMinValue();
        instance.maxDouble = (double) meta.getMaxValue();
        instance.decimal = meta.getDecimal();
        break;
      case DECIMAL:
        instance.minDecimal = (BigDecimal) meta.getMinValue();
        instance.maxDecimal = (BigDecimal) meta.getMaxValue();
        instance.decimal = meta.getDecimal();
        instance.scale = meta.getScale();
        instance.precision = meta.getPrecision();
        break;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type for stats collection: " + meta.getSchemaDataType());
    }
    return instance;
  }

  public static PrimitivePageStatsCollector newInstance(ValueEncoderMeta meta) {
    PrimitivePageStatsCollector instance =
        new PrimitivePageStatsCollector(CarbonUtil.getDataType(meta.getType()), -1, -1);
    // set min max from meta
    switch (CarbonUtil.getDataType(meta.getType())) {
      case BYTE:
        instance.minByte = (byte) meta.getMinValue();
        instance.maxByte = (byte) meta.getMaxValue();
        break;
      case SHORT:
        instance.minShort = (short) meta.getMinValue();
        instance.maxShort = (short) meta.getMaxValue();
        break;
      case INT:
        instance.minInt = (int) meta.getMinValue();
        instance.maxInt = (int) meta.getMaxValue();
        break;
      case LEGACY_LONG:
      case LONG:
        instance.minLong = (long) meta.getMinValue();
        instance.maxLong = (long) meta.getMaxValue();
        break;
      case DOUBLE:
        instance.minDouble = (double) meta.getMinValue();
        instance.maxDouble = (double) meta.getMaxValue();
        instance.decimal = meta.getDecimal();
        break;
      case DECIMAL:
        instance.minDecimal = (BigDecimal) meta.getMinValue();
        instance.maxDecimal = (BigDecimal) meta.getMaxValue();
        instance.decimal = meta.getDecimal();
        instance.scale = -1;
        instance.precision = -1;
        break;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type for Stats collection: " + meta.getType());
    }
    return instance;
  }

  private PrimitivePageStatsCollector(DataType dataType, int scale, int precision) {
    this.dataType = dataType;
    switch (dataType) {
      case BYTE:
        minByte = Byte.MAX_VALUE;
        maxByte = Byte.MIN_VALUE;
        break;
      case SHORT:
        minShort = Short.MAX_VALUE;
        maxShort = Short.MIN_VALUE;
        break;
      case INT:
        minInt = Integer.MAX_VALUE;
        maxInt = Integer.MIN_VALUE;
        break;
      case LEGACY_LONG:
      case LONG:
        minLong = Long.MAX_VALUE;
        maxLong = Long.MIN_VALUE;
        break;
      case DOUBLE:
        minDouble = Double.POSITIVE_INFINITY;
        maxDouble = Double.NEGATIVE_INFINITY;
        decimal = 0;
        break;
      case DECIMAL:
        this.zeroDecimal = BigDecimal.ZERO;
        decimal = scale;
        this.scale = scale;
        this.precision = precision;
        break;
      default:
        throw new UnsupportedOperationException(
            "unsupported data type for Stats collection: " + dataType);
    }
  }

  @Override
  public void updateNull(int rowId) {
    long value = 0;
    switch (dataType) {
      case BYTE:
        update((byte) value);
        break;
      case SHORT:
        update((short) value);
        break;
      case INT:
        update((int) value);
        break;
      case LONG:
        update(value);
        break;
      case DOUBLE:
        update(0d);
        break;
      case DECIMAL:
        if (isFirst) {
          maxDecimal = zeroDecimal;
          minDecimal = zeroDecimal;
          isFirst = false;
        } else {
          maxDecimal = (maxDecimal.compareTo(zeroDecimal) > 0) ? maxDecimal : zeroDecimal;
          minDecimal = (minDecimal.compareTo(zeroDecimal) < 0) ? minDecimal : zeroDecimal;
        }
        break;
      default:
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
    String strValue = BigDecimal.valueOf(Math.abs(value)).toPlainString();
    int integerPlaces = strValue.indexOf('.');
    int decimalPlaces = 0;
    if (-1 != integerPlaces) {
      decimalPlaces = strValue.length() - integerPlaces - 1;
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
    switch (dataType) {
      case BYTE:
        return String.format("min: %s, max: %s, decimal: %s ", minByte, maxByte, decimal);
      case SHORT:
        return String.format("min: %s, max: %s, decimal: %s ", minShort, maxShort, decimal);
      case INT:
        return String.format("min: %s, max: %s, decimal: %s ", minInt, maxInt, decimal);
      case LONG:
        return String.format("min: %s, max: %s, decimal: %s ", minLong, maxLong, decimal);
      case DOUBLE:
        return String.format("min: %s, max: %s, decimal: %s ", minDouble, maxDouble, decimal);
    }
    return super.toString();
  }

  @Override
  public Object getMin() {
    switch (dataType) {
      case BYTE:
        return minByte;
      case SHORT:
        return minShort;
      case INT:
        return minInt;
      case LONG:
        return minLong;
      case DOUBLE:
        return minDouble;
      case DECIMAL:
        return minDecimal;
    }
    return null;
  }

  @Override
  public Object getMax() {
    switch (dataType) {
      case BYTE:
        return maxByte;
      case SHORT:
        return maxShort;
      case INT:
        return maxInt;
      case LONG:
        return maxLong;
      case DOUBLE:
        return maxDouble;
      case DECIMAL:
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

  @Override public int getScale() {
    return scale;
  }

  @Override public int getPrecision() {
    return precision;
  }

}