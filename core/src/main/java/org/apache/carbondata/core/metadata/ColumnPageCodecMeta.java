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

package org.apache.carbondata.core.metadata;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * It holds metadata for one column page
 */
public class ColumnPageCodecMeta extends ValueEncoderMeta implements Serializable {

  private BitSet nullBitSet;

  private DataType srcDataType;

  private DataType targetDataType;

  public static final char BYTE_VALUE_MEASURE = 'c';
  public static final char SHORT_VALUE_MEASURE = 'j';
  public static final char INT_VALUE_MEASURE = 'k';
  public static final char BIG_INT_MEASURE = 'd';
  public static final char DOUBLE_MEASURE = 'n';
  public static final char BIG_DECIMAL_MEASURE = 'b';

  static ColumnPageCodecMeta newInstance() {
    return new ColumnPageCodecMeta();
  }

  static ColumnPageCodecMeta newInstance(
      SimpleStatsResult stats, DataType targetDataType) {
    ColumnPageCodecMeta meta = new ColumnPageCodecMeta();
    meta.srcDataType = stats.getDataType();
    meta.targetDataType = targetDataType;
    meta.nullBitSet = stats.getNullBits();
    meta.setType(CodecMetaFactory.converType(stats.getDataType()));
    meta.setMaxValue(stats.getMax());
    meta.setMinValue(stats.getMin());
    meta.setDecimal(stats.getDecimalPoint());
    meta.setScale(stats.getScale());
    meta.setPrecision(stats.getPrecision());
    return meta;
  }

  public DataType getTargetDataType() {
    return targetDataType;
  }

  public void setSrcDataType(char type) {
    switch (type) {
      case BYTE_VALUE_MEASURE:
        srcDataType = DataType.BYTE;
        break;
      case SHORT_VALUE_MEASURE:
        srcDataType = DataType.SHORT;
        break;
      case INT_VALUE_MEASURE:
        srcDataType = DataType.INT;
        break;
      case BIG_INT_MEASURE:
        srcDataType = DataType.LONG;
        break;
      case DOUBLE_MEASURE:
        srcDataType = DataType.DOUBLE;
        break;
      case BIG_DECIMAL_MEASURE:
        srcDataType = DataType.DECIMAL;
        break;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

  private char getSrcDataTypeInChar() {
    switch (srcDataType) {
      case BYTE:
        return BYTE_VALUE_MEASURE;
      case SHORT:
        return SHORT_VALUE_MEASURE;
      case INT:
        return INT_VALUE_MEASURE;
      case LONG:
        return BIG_INT_MEASURE;
      case DOUBLE:
        return DOUBLE_MEASURE;
      case DECIMAL:
        return BIG_DECIMAL_MEASURE;
      default:
        throw new RuntimeException("Unexpected type: " + targetDataType);
    }
  }

  public BitSet getNullBitSet() {
    return nullBitSet;
  }

  public void setNullBitSet(BitSet nullBitSet) {
    this.nullBitSet = nullBitSet;
  }

  public DataType getSrcDataType() {
    return srcDataType;
  }

  public byte[] serialize() {
    ByteBuffer buffer = null;
    switch (srcDataType) {
      case BYTE:
        buffer = ByteBuffer.allocate(
            (CarbonCommonConstants.LONG_SIZE_IN_BYTE * 3) + CarbonCommonConstants.INT_SIZE_IN_BYTE
                + 3);
        buffer.putChar(getSrcDataTypeInChar());
        buffer.put((byte) getMaxValue());
        buffer.put((byte) getMinValue());
        buffer.putLong((Long) 0L); // unique value is obsoleted, maintain for compatibility
        break;
      case SHORT:
        buffer = ByteBuffer.allocate(
            (CarbonCommonConstants.LONG_SIZE_IN_BYTE * 3) + CarbonCommonConstants.INT_SIZE_IN_BYTE
                + 3);
        buffer.putChar(getSrcDataTypeInChar());
        buffer.putShort((short) getMaxValue());
        buffer.putShort((short) getMinValue());
        buffer.putLong((Long) 0L); // unique value is obsoleted, maintain for compatibility
        break;
      case INT:
        buffer = ByteBuffer.allocate(
            (CarbonCommonConstants.LONG_SIZE_IN_BYTE * 3) + CarbonCommonConstants.INT_SIZE_IN_BYTE
                + 3);
        buffer.putChar(getSrcDataTypeInChar());
        buffer.putInt((int) getMaxValue());
        buffer.putInt((int) getMinValue());
        buffer.putLong((Long) 0L); // unique value is obsoleted, maintain for compatibility
        break;
      case LONG:
        buffer = ByteBuffer.allocate(
            (CarbonCommonConstants.LONG_SIZE_IN_BYTE * 3) + CarbonCommonConstants.INT_SIZE_IN_BYTE
                + 3);
        buffer.putChar(getSrcDataTypeInChar());
        buffer.putLong((Long) getMaxValue());
        buffer.putLong((Long) getMinValue());
        buffer.putLong((Long) 0L); // unique value is obsoleted, maintain for compatibility
        break;
      case DOUBLE:
        buffer = ByteBuffer.allocate(
            (CarbonCommonConstants.DOUBLE_SIZE_IN_BYTE * 3) + CarbonCommonConstants.INT_SIZE_IN_BYTE
                + 3);
        buffer.putChar(getSrcDataTypeInChar());
        buffer.putDouble((Double) getMaxValue());
        buffer.putDouble((Double) getMinValue());
        buffer.putDouble((Double) 0d); // unique value is obsoleted, maintain for compatibility
        break;
      case DECIMAL:
        buffer = ByteBuffer.allocate(
            (CarbonCommonConstants.LONG_SIZE_IN_BYTE * 3) + (CarbonCommonConstants
                .INT_SIZE_IN_BYTE * 3)
                + 3);
        buffer.putChar(getSrcDataTypeInChar());
        buffer.putLong((Long) getMaxValue());
        buffer.putLong((Long) getMinValue());
        buffer.putLong((Long) 0L); // unique value is obsoleted, maintain for compatibility
        buffer.putInt(getScale());
        buffer.putInt(getPrecision());

        break;
    }
    buffer.putInt(getDecimal());
    buffer.put(getDataTypeSelected());
    buffer.flip();
    return buffer.array();
  }

  public void deserialize(byte[] encodeMeta) {
    ByteBuffer buffer = ByteBuffer.wrap(encodeMeta);
    char srcDataType = buffer.getChar();
    this.setSrcDataType(srcDataType);
    switch (srcDataType) {
      case DOUBLE_MEASURE:
        this.setMaxValue(buffer.getDouble());
        this.setMinValue(buffer.getDouble());
        buffer.getDouble(); // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case BIG_DECIMAL_MEASURE:
        this.setMaxValue(buffer.getLong());
        this.setMinValue(buffer.getLong());
        buffer.getLong();
        this.setScale(buffer.getInt());
        this.setPrecision(buffer.getInt());
        break;
      case BYTE_VALUE_MEASURE:
        this.setMaxValue(buffer.get());
        this.setMinValue(buffer.get());
        buffer.getLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case SHORT_VALUE_MEASURE:
        this.setMaxValue(buffer.getShort());
        this.setMinValue(buffer.getShort());
        buffer.getLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case INT_VALUE_MEASURE:
        this.setMaxValue(buffer.getInt());
        this.setMinValue(buffer.getInt());
        buffer.getLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case BIG_INT_MEASURE:
        this.setMaxValue(buffer.getLong());
        this.setMinValue(buffer.getLong());
        buffer.getLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      default:
        throw new IllegalArgumentException("invalid measure type");
    }
    this.setDecimal(buffer.getInt());
    buffer.get(); // for selectedDataType, obsoleted
  }

  public byte[] getMaxAsBytes() {
    return getValueAsBytes(getMaxValue());
  }

  public byte[] getMinAsBytes() {
    return getValueAsBytes(getMinValue());
  }

  /**
   * convert value to byte array
   */
  private byte[] getValueAsBytes(Object value) {
    ByteBuffer b;
    switch (srcDataType) {
      case BYTE:
        b = ByteBuffer.allocate(8);
        b.putLong((byte) value);
        b.flip();
        return b.array();
      case SHORT:
        b = ByteBuffer.allocate(8);
        b.putLong((short) value);
        b.flip();
        return b.array();
      case INT:
        b = ByteBuffer.allocate(8);
        b.putLong((int) value);
        b.flip();
        return b.array();
      case LONG:
        b = ByteBuffer.allocate(8);
        b.putLong((long) value);
        b.flip();
        return b.array();
      case DOUBLE:
        b = ByteBuffer.allocate(8);
        b.putDouble((double) value);
        b.flip();
        return b.array();
      case DECIMAL:
      case BYTE_ARRAY:
        return new byte[8];
      default:
        throw new IllegalArgumentException("Invalid data type: " + targetDataType);
    }
  }

}
