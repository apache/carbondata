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
import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.datastore.page.statistics.SimpleStatsResult;
import org.apache.carbondata.core.metadata.ValueEncoderMeta;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.schema.table.Writable;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * It holds metadata for one column page
 */
public class ColumnPageEncoderMeta extends ValueEncoderMeta implements Writable {

  private static final long serialVersionUID = 1905162071950251407L;

  // column spec of this column
  private transient TableSpec.ColumnSpec columnSpec;

  // storage data type of this column, it could be different from data type in the column spec
  private DataType storeDataType;

  // compressor name for compressing and decompressing this column
  private String compressorName;

  private int scale;
  private int precision;

  public static final char DOUBLE_MEASURE = 'n';
  public static final char STRING = 's';
  public static final char TIMESTAMP = 't';
  public static final char DATE = 'x';
  public static final char BYTE_ARRAY = 'y';

  public ColumnPageEncoderMeta() {
  }

  public ColumnPageEncoderMeta(TableSpec.ColumnSpec columnSpec, DataType storeDataType,
      SimpleStatsResult stats, String compressorName) {
    if (columnSpec == null) {
      throw new IllegalArgumentException("columm spec must not be null");
    }
    if (storeDataType == null) {
      throw new IllegalArgumentException("store data type must not be null");
    }
    if (compressorName == null) {
      throw new IllegalArgumentException("compressor must not be null");
    }
    this.columnSpec = columnSpec;
    this.storeDataType = storeDataType;
    this.compressorName = compressorName;
    setType(convertType(storeDataType));
    if (stats != null) {
      setDecimal(stats.getDecimalCount());
      setMaxValue(stats.getMax());
      setMinValue(stats.getMin());
      this.scale = stats.getScale();
      this.precision = stats.getPrecision();
    }
  }

  private char convertType(DataType type) {
    switch (type) {
      case BYTE:
      case SHORT:
      case SHORT_INT:
      case INT:
      case LONG:
        return CarbonCommonConstants.BIG_INT_MEASURE;
      case DOUBLE:
        return CarbonCommonConstants.DOUBLE_MEASURE;
      case DECIMAL:
        return CarbonCommonConstants.BIG_DECIMAL_MEASURE;
      case STRING:
        return STRING;
      case TIMESTAMP:
        return TIMESTAMP;
      case DATE:
        return DATE;
      case BYTE_ARRAY:
        return BYTE_ARRAY;
      default:
        throw new RuntimeException("Unexpected type: " + type);
    }
  }

  public DataType getStoreDataType() {
    return storeDataType;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    columnSpec.write(out);
    out.writeByte(storeDataType.ordinal());
    out.writeInt(getDecimal());
    out.writeByte(getDataTypeSelected());
    writeMinMax(out);
    out.writeUTF(compressorName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    columnSpec = new TableSpec.ColumnSpec();
    columnSpec.readFields(in);
    storeDataType = DataType.valueOf(in.readByte());
    setDecimal(in.readInt());
    setDataTypeSelected(in.readByte());
    readMinMax(in);
    compressorName = in.readUTF();
  }

  private void writeMinMax(DataOutput out) throws IOException {
    switch (columnSpec.getSchemaDataType()) {
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
        byte[] maxAsBytes = getMaxAsBytes(columnSpec.getSchemaDataType());
        byte[] minAsBytes = getMinAsBytes(columnSpec.getSchemaDataType());
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
      case BYTE_ARRAY:
        // for complex type, it will come here, ignoring stats for complex type
        // TODO: support stats for complex type
        break;
      default:
        throw new IllegalArgumentException("invalid data type: " + storeDataType);
    }
  }

  private void readMinMax(DataInput in) throws IOException {
    switch (columnSpec.getSchemaDataType()) {
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
        this.scale = in.readInt();
        this.precision = in.readInt();
        break;
      case BYTE_ARRAY:
        // for complex type, it will come here, ignoring stats for complex type
        // TODO: support stats for complex type
        break;
      default:
        throw new IllegalArgumentException("invalid data type: " + storeDataType);
    }
  }

  public byte[] getMaxAsBytes(DataType dataType) {
    return getValueAsBytes(getMaxValue(), dataType);
  }

  public byte[] getMinAsBytes(DataType dataType) {
    return getValueAsBytes(getMinValue(), dataType);
  }

  /**
   * convert value to byte array
   */
  private byte[] getValueAsBytes(Object value, DataType dataType) {
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
      case STRING:
      case TIMESTAMP:
      case DATE:
        return (byte[]) value;
      default:
        throw new IllegalArgumentException("Invalid data type: " + storeDataType);
    }
  }

  public int getScale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }

  public TableSpec.ColumnSpec getColumnSpec() {
    return columnSpec;
  }

  public String getCompressorName() {
    return compressorName;
  }

  public DataType getSchemaDataType() {
    return columnSpec.getSchemaDataType();
  }
}
