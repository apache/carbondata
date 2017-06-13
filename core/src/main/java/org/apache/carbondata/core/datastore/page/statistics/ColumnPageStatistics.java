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

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.DataTypeUtil;

/** statics for one column page */
public class ColumnPageStatistics {
  private DataType dataType;

  /** min and max value of the measures */
  private Object min, max;

  /**
   * the unique value is the non-exist value in the row,
   * and will be used as storage key for null values of measures
   */
  private Object nonExistValue;

  /** decimal count of the measures */
  private int decimal;

  public ColumnPageStatistics(DataType dataType) {
    this.dataType = dataType;
    switch (dataType) {
      case SHORT:
      case INT:
      case LONG:
        max = Long.MIN_VALUE;
        min = Long.MAX_VALUE;
        nonExistValue = Long.MIN_VALUE;
        break;
      case DOUBLE:
        max = Double.MIN_VALUE;
        min = Double.MAX_VALUE;
        nonExistValue = Double.MIN_VALUE;
        break;
      case DECIMAL:
        max = new BigDecimal(Double.MIN_VALUE);
        min = new BigDecimal(Double.MAX_VALUE);
        nonExistValue = new BigDecimal(Double.MIN_VALUE);
        break;
    }
    decimal = 0;
  }

  /**
   * update the statistics for the input row
   */
  public void update(Object value) {
    switch (dataType) {
      case SHORT:
        max = ((long) max > ((Short) value).longValue()) ? max : ((Short) value).longValue();
        min = ((long) min < ((Short) value).longValue()) ? min : ((Short) value).longValue();
        nonExistValue = (long) min - 1;
        break;
      case INT:
        max = ((long) max > ((Integer) value).longValue()) ? max : ((Integer) value).longValue();
        min = ((long) min  < ((Integer) value).longValue()) ? min : ((Integer) value).longValue();
        nonExistValue = (long) min - 1;
        break;
      case LONG:
        max = ((long) max > (long) value) ? max : value;
        min = ((long) min < (long) value) ? min : value;
        nonExistValue = (long) min - 1;
        break;
      case DOUBLE:
        max = ((double) max > (double) value) ? max : value;
        min = ((double) min < (double) value) ? min : value;
        int num = getDecimalCount((double) value);
        decimal = decimal > num ? decimal : num;
        nonExistValue = (double) min - 1;
        break;
      case DECIMAL:
        BigDecimal decimalValue = DataTypeUtil.byteToBigDecimal((byte[]) value);
        decimal = decimalValue.scale();
        BigDecimal val = (BigDecimal) min;
        nonExistValue = (val.subtract(new BigDecimal(1.0)));
        break;
      case ARRAY:
      case STRUCT:
        // for complex type column, writer is not going to use stats, so, do nothing
    }
  }

  /**
   * return no of digit after decimal
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
      case DOUBLE:
        b = ByteBuffer.allocate(8);
        b.putDouble((Double) value);
        b.flip();
        return b.array();
      case LONG:
      case INT:
      case SHORT:
        b = ByteBuffer.allocate(8);
        b.putLong((Long) value);
        b.flip();
        return b.array();
      case DECIMAL:
        return DataTypeUtil.bigDecimalToByte((BigDecimal) value);
      default:
        throw new IllegalArgumentException("Invalid data type");
    }
  }

  public Object getMin() {
    return min;
  }

  public Object getMax() {
    return max;
  }

  public Object nonExistValue() {
    return nonExistValue;
  }

  public int getDecimal() {
    return decimal;
  }
}