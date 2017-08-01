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
import java.util.BitSet;

import org.apache.carbondata.core.metadata.ColumnPageCodecMeta;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;

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

  // scale of the double value
  private int decimal;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  private boolean isFirst = true;
  private BigDecimal zeroDecimal;

  // this is for encode flow
  public static PrimitivePageStatsCollector newInstance(DataType dataType, int pageSize, int
      scale, int precision) {
    switch (dataType) {
      default:
        return new PrimitivePageStatsCollector(dataType, pageSize, scale, precision);
    }
  }

  // this is for decode flow, we do not need to create nullBits, so passing 0 as pageSize
  public static PrimitivePageStatsCollector newInstance(ColumnPageCodecMeta meta) {
    PrimitivePageStatsCollector instance =
        new PrimitivePageStatsCollector(meta.getSrcDataType(), 0, meta.getScale(),
            meta.getPrecision());
    // set min max from meta
    switch (meta.getSrcDataType()) {
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
    }
    return instance;
  }

  public static PrimitivePageStatsCollector newInstance(ValueEncoderMeta meta) {
    PrimitivePageStatsCollector instance =
        new PrimitivePageStatsCollector(meta.getType(), 0, -1, -1);
    // set min max from meta
    switch (meta.getType()) {
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
        instance.scale = -1;
        instance.precision = -1;
        break;
    }
    return instance;
  }

  private PrimitivePageStatsCollector(DataType dataType, int pageSize, int scale, int precision) {
    this.dataType = dataType;
    this.nullBitSet = new BitSet(pageSize);
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
      case LONG:
        minLong = Long.MAX_VALUE;
        maxLong = Long.MIN_VALUE;
        break;
      case DOUBLE:
        minDouble = Double.MAX_VALUE;
        maxDouble = Double.MIN_VALUE;
        decimal = 0;
        break;
      case DECIMAL:
        this.zeroDecimal = BigDecimal.ZERO;
        decimal = scale;
        this.scale = scale;
        this.precision = precision;
    }
  }

  @Override
  public void updateNull(int rowId) {
    nullBitSet.set(rowId);
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

  @Override
  public void update(double value) {
    if (minDouble > value) {
      minDouble = value;
    }
    if (maxDouble < value) {
      maxDouble = value;
    }
    int scale = BigDecimal.valueOf(value).scale();
    if (scale < 0) {
      decimal = scale;
    } else {
      decimal = Math.max(decimal, scale);
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
  public Object getPageStats() {
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
  public BitSet getNullBits() {
    return nullBitSet;
  }

  @Override
  public int getDecimalPoint() {
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