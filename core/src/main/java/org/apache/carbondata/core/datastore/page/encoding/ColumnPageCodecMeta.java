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

package org.apache.carbondata.core.datastore.page.encoding;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * It holds metadata for one column page
 */
public class ColumnPageCodecMeta extends ValueEncoderMeta implements Writable {

  private Encoding encoding;

  // data type of this column
  private DataType dataType;

  private int scale;

  private int precision;

  public static final char BYTE_VALUE_MEASURE = 'c';
  public static final char SHORT_VALUE_MEASURE = 'j';
  public static final char INT_VALUE_MEASURE = 'k';
  public static final char BIG_INT_MEASURE = 'd';
  public static final char DOUBLE_MEASURE = 'n';
  public static final char BIG_DECIMAL_MEASURE = 'b';

  public ColumnPageCodecMeta() {
  }

  public ColumnPageCodecMeta(DataType dataType, Encoding encoding, SimpleStatsResult stats) {
    this.dataType = dataType;
    this.encoding = encoding;
    setType(converType(stats.getDataType()));
    setDecimal(stats.getDecimalPoint());
    setMaxValue(stats.getMax());
    setMinValue(stats.getMin());
    setScale(stats.getScale());
    setPrecision(stats.getPrecision());
  }

  public int getScale() {
    return scale;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }

  public int getPrecision() {
    return precision;
  }

  public void setPrecision(int precision) {
    this.precision = precision;
  }

  private char converType(DataType type) {
    switch (type) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        return CarbonCommonConstants.BIG_INT_MEASURE;
      case DOUBLE:
        return CarbonCommonConstants.DOUBLE_MEASURE;
      case DECIMAL:
        return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

  public Encoding getEncoding() {
    return encoding;
  }

  public DataType getDataType() {
    return dataType;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeByte(encoding.ordinal());
    out.writeByte(dataType.ordinal());
    out.writeInt(getDecimal());
    out.writeByte(getDataTypeSelected());
    writeMinMax(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    encoding = Encoding.valueOf(in.readByte());
    dataType = DataType.valueOf(in.readByte());
    setDecimal(in.readInt());
    setDataTypeSelected(in.readByte());
    readMinMax(in);
  }

  private void writeMinMax(DataOutput out) throws IOException {
    switch (dataType) {
      case BYTE:
        out.writeByte((byte) getMaxValue());
        out.writeByte((byte) getMinValue());
        out.writeLong(0L); // unique value is obsoleted, maintain for compatibility
        break;
      case SHORT:
        out.writeShort((short) getMaxValue());
        out.writeShort((short) getMinValue());
        out.writeLong(0L); // unique value is obsoleted, maintain for compatibility
        break;
      case INT:
        out.writeInt((int) getMaxValue());
        out.writeInt((int) getMinValue());
        out.writeLong(0L); // unique value is obsoleted, maintain for compatibility
        break;
      case LONG:
        out.writeLong((Long) getMaxValue());
        out.writeLong((Long) getMinValue());
        out.writeLong(0L); // unique value is obsoleted, maintain for compatibility
        break;
      case DOUBLE:
        out.writeDouble((Double) getMaxValue());
        out.writeDouble((Double) getMinValue());
        out.writeDouble(0d); // unique value is obsoleted, maintain for compatibility
        break;
      case DECIMAL:
        byte[] maxAsBytes = getMaxAsBytes();
        byte[] minAsBytes = getMinAsBytes();
        byte[] unique = DataTypeUtil.bigDecimalToByte(BigDecimal.ZERO);
        out.writeShort((short) maxAsBytes.length);
        out.write(maxAsBytes);
        out.writeShort((short) minAsBytes.length);
        out.write(minAsBytes);
        // unique value is obsoleted, maintain for compatibility
        out.writeShort((short) unique.length);
        out.write(unique);
        out.writeInt(scale);
        out.writeInt(precision);
        break;
    }
  }

  private void readMinMax(DataInput in) throws IOException {
    switch (dataType) {
      case BYTE:
        this.setMaxValue(in.readByte());
        this.setMinValue(in.readByte());
        in.readLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case SHORT:
        this.setMaxValue(in.readShort());
        this.setMinValue(in.readShort());
        in.readLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case INT:
        this.setMaxValue(in.readInt());
        this.setMinValue(in.readInt());
        in.readLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case LONG:
        this.setMaxValue(in.readLong());
        this.setMinValue(in.readLong());
        in.readLong();  // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case DOUBLE:
        this.setMaxValue(in.readDouble());
        this.setMinValue(in.readDouble());
        in.readDouble(); // for non exist value which is obsoleted, it is backward compatibility;
        break;
      case DECIMAL:
        byte[] max = new byte[in.readShort()];
        in.readFully(max);
        this.setMaxValue(DataTypeUtil.byteToBigDecimal(max));
        byte[] min = new byte[in.readShort()];
        in.readFully(min);
        this.setMinValue(DataTypeUtil.byteToBigDecimal(min));
        // unique value is obsoleted, maintain for compatiability
        short uniqueLength = in.readShort();
        in.readFully(new byte[uniqueLength]);
        this.setScale(in.readInt());
        this.setPrecision(in.readInt());
        break;
      default:
        throw new IllegalArgumentException("invalid data type: " + dataType);
    }
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
    switch (dataType) {
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
        return DataTypeUtil.bigDecimalToByte((BigDecimal)value);
      case BYTE_ARRAY:
        return new byte[8];
      default:
        throw new IllegalArgumentException("Invalid data type: " + dataType);
    }
  }

}
