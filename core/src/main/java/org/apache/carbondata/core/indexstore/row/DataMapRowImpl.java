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
package org.apache.carbondata.core.indexstore.row;

import org.apache.carbondata.core.indexstore.schema.CarbonRowSchema;
import org.apache.carbondata.core.metadata.datatype.DataTypes;

/**
 * Data map row.
 */
public class DataMapRowImpl extends DataMapRow {

  private Object[] data;

  private int totalLengthInBytes;

  public DataMapRowImpl(CarbonRowSchema[] schemas) {
    super(schemas);
    this.data = new Object[schemas.length];
  }

  @Override public byte[] getByteArray(int ordinal) {
    return (byte[]) data[ordinal];
  }

  @Override public int getLengthInBytes(int ordinal) {
    return ((byte[]) data[ordinal]).length;
  }

  @Override public DataMapRow getRow(int ordinal) {
    return (DataMapRow) data[ordinal];
  }

  @Override public void setByteArray(byte[] byteArray, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.BYTE_ARRAY);
    data[ordinal] = byteArray;
  }

  @Override public int getInt(int ordinal) {
    return (Integer) data[ordinal];
  }

  @Override public void setInt(int value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.INT);
    data[ordinal] = value;
  }

  @Override public void setByte(byte value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.BYTE);
    data[ordinal] = value;
  }

  @Override public byte getByte(int ordinal) {
    return (Byte) data[ordinal];
  }

  @Override public void setShort(short value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.SHORT);
    data[ordinal] = value;
  }

  @Override public short getShort(int ordinal) {
    return (Short) data[ordinal];
  }

  @Override public void setLong(long value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.LONG);
    data[ordinal] = value;
  }

  @Override public long getLong(int ordinal) {
    return (Long) data[ordinal];
  }

  @Override public void setFloat(float value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.FLOAT);
    data[ordinal] = value;
  }

  @Override public float getFloat(int ordinal) {
    return (Float) data[ordinal];
  }

  @Override public void setDouble(double value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataTypes.DOUBLE);
    data[ordinal] = value;
  }

  @Override public void setRow(DataMapRow row, int ordinal) {
    assert (DataTypes.isStructType(schemas[ordinal].getDataType()));
    data[ordinal] = row;
  }

  @Override public double getDouble(int ordinal) {
    return (Double) data[ordinal];
  }

  public void setTotalLengthInBytes(int totalLengthInBytes) {
    this.totalLengthInBytes = totalLengthInBytes;
  }

  @Override public int getTotalSizeInBytes() {
    if (totalLengthInBytes > 0) {
      return totalLengthInBytes;
    } else {
      return super.getTotalSizeInBytes();
    }
  }
}
