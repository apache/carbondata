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

  /** min and max value of the measures */
  private Object min, max;

  private int scale;

  private int precision;

  public ColumnPageStatsVO(DataType dataType) {
    this.dataType = dataType;
    switch (dataType) {
      case SHORT:
      case INT:
      case LONG:
        max = Long.MIN_VALUE;
        min = Long.MAX_VALUE;
        break;
      case DOUBLE:
        max = Double.MIN_VALUE;
        min = Double.MAX_VALUE;
        break;
      case DECIMAL:
        max = new BigDecimal(Double.MIN_VALUE);
        min = new BigDecimal(Double.MAX_VALUE);
        break;
    }
  }

  public static ColumnPageStatsVO copyFrom(ValueEncoderMeta meta, int scale, int precision) {
    ColumnPageStatsVO instance = new ColumnPageStatsVO(meta.getType());
    instance.min = meta.getMinValue();
    instance.max = meta.getMaxValue();
    instance.scale = scale;
    instance.precision = precision;
    return instance;
  }

  /**
   * update the statistics for the input row
   */
  public void update(Object value) {
    switch (dataType) {
      case SHORT:
        max = ((long) max > ((Short) value).longValue()) ? max : ((Short) value).longValue();
        min = ((long) min < ((Short) value).longValue()) ? min : ((Short) value).longValue();
        break;
      case INT:
        max = ((long) max > ((Integer) value).longValue()) ? max : ((Integer) value).longValue();
        min = ((long) min  < ((Integer) value).longValue()) ? min : ((Integer) value).longValue();
        break;
      case LONG:
        max = ((long) max > (long) value) ? max : value;
        min = ((long) min < (long) value) ? min : value;
        break;
      case DOUBLE:
        max = ((double) max > (double) value) ? max : value;
        min = ((double) min < (double) value) ? min : value;
        break;
      case DECIMAL:
        break;
      case ARRAY:
      case STRUCT:
        // for complex type column, writer is not going to use stats, so, do nothing
    }
  }

  public void updateNull() {
    switch (dataType) {
      case SHORT:
        max = ((long) max > 0) ? max : 0L;
        min = ((long) min < 0) ? min : 0L;
        break;
      case INT:
        max = ((long) max > 0) ? max : 0L;
        min = ((long) min  < 0) ? min : 0L;
        break;
      case LONG:
        max = ((long) max > 0) ? max : 0L;
        min = ((long) min < 0) ? min : 0L;
        break;
      case DOUBLE:
        max = ((double) max > 0d) ? max : 0d;
        min = ((double) min < 0d) ? min : 0d;
        break;
      case DECIMAL:
        break;
      case ARRAY:
      case STRUCT:
        // for complex type column, writer is not going to use stats, so, do nothing
    }
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
        throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }

  public Object getMin() {
    return min;
  }

  public Object getMax() {
    return max;
  }

  public DataType getDataType() {
    return dataType;
  }

  public int getScale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }

  @Override
  public String toString() {
    return String.format("min: %s, max: %s", min, max);
  }
}