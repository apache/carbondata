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
import org.apache.carbondata.core.memory.MemoryBlock;

import static org.apache.carbondata.core.memory.CarbonUnsafe.BYTE_ARRAY_OFFSET;
import static org.apache.carbondata.core.memory.CarbonUnsafe.getUnsafe;

/**
 * Unsafe implementation of data map row.
 */
public class UnsafeDataMapRow extends DataMapRow {

  private MemoryBlock block;

  private int pointer;

  public UnsafeDataMapRow(CarbonRowSchema[] schemas, MemoryBlock block, int pointer) {
    super(schemas);
    this.block = block;
    this.pointer = pointer;
  }

  @Override public byte[] getByteArray(int ordinal) {
    int length;
    int position = getPosition(ordinal);
    switch (schemas[ordinal].getSchemaType()) {
      case VARIABLE:
        length =
            getUnsafe().getShort(block.getBaseObject(), block.getBaseOffset() + pointer + position);
        position += 2;
        break;
      default:
        length = schemas[ordinal].getLength();
    }
    byte[] data = new byte[length];
    getUnsafe().copyMemory(block.getBaseObject(), block.getBaseOffset() + pointer + position, data,
        BYTE_ARRAY_OFFSET, data.length);
    return data;
  }

  @Override public int getLengthInBytes(int ordinal) {
    int length;
    int position = getPosition(ordinal);
    switch (schemas[ordinal].getSchemaType()) {
      case VARIABLE:
        length =
            getUnsafe().getShort(block.getBaseObject(), block.getBaseOffset() + pointer + position);
        break;
      default:
        length = schemas[ordinal].getLength();
    }
    return length;
  }

  private int getLengthInBytes(int ordinal, int position) {
    int length;
    switch (schemas[ordinal].getSchemaType()) {
      case VARIABLE:
        length =
            getUnsafe().getShort(block.getBaseObject(), block.getBaseOffset() + pointer + position);
        break;
      default:
        length = schemas[ordinal].getLength();
    }
    return length;
  }

  @Override public DataMapRow getRow(int ordinal) {
    CarbonRowSchema[] childSchemas =
        ((CarbonRowSchema.StructCarbonRowSchema) schemas[ordinal]).getChildSchemas();
    return new UnsafeDataMapRow(childSchemas, block, pointer + getPosition(ordinal));
  }

  @Override public void setByteArray(byte[] byteArray, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public int getInt(int ordinal) {
    return getUnsafe()
        .getInt(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setInt(int value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public void setByte(byte value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public byte getByte(int ordinal) {
    return getUnsafe()
        .getByte(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setShort(short value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public short getShort(int ordinal) {
    return getUnsafe()
        .getShort(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setLong(long value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public long getLong(int ordinal) {
    return getUnsafe()
        .getLong(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setFloat(float value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public float getFloat(int ordinal) {
    return getUnsafe()
        .getFloat(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setDouble(double value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public double getDouble(int ordinal) {
    return getUnsafe()
        .getDouble(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setRow(DataMapRow row, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  private int getSizeInBytes(int ordinal, int position) {
    switch (schemas[ordinal].getSchemaType()) {
      case FIXED:
        return schemas[ordinal].getLength();
      case VARIABLE:
        return getLengthInBytes(ordinal, position) + 2;
      case STRUCT:
        return getRow(ordinal).getTotalSizeInBytes();
      default:
        throw new UnsupportedOperationException("wrong type");
    }
  }

  private int getPosition(int ordinal) {
    int position = 0;
    for (int i = 0; i < ordinal; i++) {
      position += getSizeInBytes(i, position);
    }
    return position;
  }
}
