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
import java.nio.ByteBuffer;

import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.DataTypeUtil;

/** statics for one column page */
public class ColumnPageStatsVO {
  private DataType dataType;

  /**
   * min and max value of the measures, only one of following will be used.
   * non-exist value will be used as storage key for null values of measures
   */
  private long minLong, maxLong;
  private double minDouble, maxDouble;
  private BigDecimal minDecimal, maxDecimal;

  /** decimal count of the measures */
  private int decimal;

  public ColumnPageStatsVO(DataType dataType) {
    this.dataType = dataType;
    switch (dataType) {
      case SHORT:
      case INT:
      case LONG:
        maxLong = Long.MIN_VALUE;
        minLong = Long.MAX_VALUE;
        break;
      case DOUBLE:
        maxDouble = Double.MIN_VALUE;
        minDouble = Double.MAX_VALUE;
        break;
      case DECIMAL:
        maxDecimal = new BigDecimal(Double.MIN_VALUE);
        minDecimal = new BigDecimal(Double.MAX_VALUE);
        break;
    }
    decimal = 0;
  }

  public static ColumnPageStatsVO copyFrom(ValueEncoderMeta meta) {
    ColumnPageStatsVO instance = new ColumnPageStatsVO(meta.getType());
    switch (meta.getType()) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        instance.minLong = (long) meta.getMinValue();
        instance.maxLong = (long) meta.getMaxValue();
        instance.decimal = meta.getDecimal();
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
        break;
    }
    return instance;
  }

  /**
   * update the statistics for the null
   */
  public void updateNull() {
//    switch (dataType) {
//      case BYTE:
//      case SHORT:
//      case INT:
//      case LONG:
//        maxLong = (max > 0) ? max : 0L;
//        min = ((long) min < 0) ? min : 0L;
//        nonExistValue = (long) min - 1;
//        break;
//      case DOUBLE:
//        max = ((double) max > 0d) ? max : 0d;
//        min = ((double) min < 0d) ? min : 0d;
//        int num = getDecimalCount(0d);
//        decimal = decimal > num ? decimal : num;
//        nonExistValue = (double) min - 1;
//        break;
//      case DECIMAL:
//        BigDecimal decimalValue = BigDecimal.ZERO;
//        decimal = decimalValue.scale();
//        BigDecimal val = (BigDecimal) min;
//        nonExistValue = (val.subtract(new BigDecimal(1.0)));
//        break;
//      case ARRAY:
//      case STRUCT:
//        // for complex type column, writer is not going to use stats, so, do nothing
//
//    }
  }

  public void updateLong(long value) {
    maxLong = Math.max(maxLong, value);
    minLong = Math.min(minLong, value);
  }

  public void updateDouble(double value) {
    maxDouble = Math.max(maxDouble, value);
    minDouble = Math.min(minDouble, value);
    int num = getDecimalCount(value);
    if (decimal < num) {
      decimal = num;
    }
  }

  public void updateDecimal(byte[] value) {
    //TODO: optimization it when changing format of output in sort step. do not create decimal
    BigDecimal decimalValue = DataTypeUtil.byteToBigDecimal(value);
    if (maxDecimal.compareTo(decimalValue) < 0) {
      maxDecimal = decimalValue;
    }
    if (minDecimal.compareTo(decimalValue) > 0) {
      minDecimal = decimalValue;
    }
    decimal = decimalValue.scale();
  }

  /**
   * return no of digit after decimal
   */
  private int getDecimalCount(double value) {
    return BigDecimal.valueOf(value).scale();
  }

  /**
   * return min value as byte array
   */
  public byte[] minBytes() {
    return getValueAsBytes(getMin());
  }

  /**
   * return max value as byte array
   */
  public byte[] maxBytes() {
    return getValueAsBytes(getMax());
  }

  /**
   * convert value to byte array
   */
  private byte[] getValueAsBytes(Object value) {
    ByteBuffer b;
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        b = ByteBuffer.allocate(8);
        b.putLong((Long) value);
        b.flip();
        return b.array();
      case DOUBLE:
        b = ByteBuffer.allocate(8);
        b.putDouble((Double) value);
        b.flip();
        return b.array();
      case DECIMAL:
        return DataTypeUtil.bigDecimalToByte((BigDecimal) value);
      default:
        throw new IllegalArgumentException("Invalid data type");
    }
  }

  public Object getMin() {
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return minLong;
      case FLOAT:
      case DOUBLE:
        return minDouble;
      case DECIMAL:
        return minDecimal;
      default:
        return null;
    }
  }

  public Object getMax() {
    switch (dataType) {
      case SHORT:
      case INT:
      case LONG:
        return maxLong;
      case FLOAT:
      case DOUBLE:
        return maxDouble;
      case DECIMAL:
        return maxDecimal;
      default:
        return null;
    }
  }

  public int getDecimal() {
    return decimal;
  }

  public DataType getDataType() {
    return dataType;
  }

  @Override
  public String toString() {
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return String.format("min: %s, max: %s, decimal: %s ", minLong, maxLong, decimal);
      case FLOAT:
      case DOUBLE:
        return String.format("min: %s, max: %s, decimal: %s ", minDouble, maxDouble, decimal);
      case DECIMAL:
        return String.format("min: %s, max: %s, decimal: %s ", minDecimal, maxDecimal, decimal);
      default:
        throw new RuntimeException("internal error, type: " + dataType);
    }
  }
}