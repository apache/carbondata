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
 * Unsafe implementation of index row.
 */
public class UnsafeIndexRow extends IndexRow {

  private static final long serialVersionUID = -1156704133552046321L;

  // As it is an unsafe memory block it is not recommended to serialize.
  // If at all required to be serialized then override writeObject methods
  // to which should take care of clearing the unsafe memory post serialization
  private transient MemoryBlock block;

  private int pointer;

  public UnsafeIndexRow(CarbonRowSchema[] schemas, MemoryBlock block, int pointer) {
    super(schemas);
    this.block = block;
    this.pointer = pointer;
  }

  @Override
  public byte[] getByteArray(int ordinal) {
    int length;
    int currentOffset;
    switch (schemas[ordinal].getSchemaType()) {
      case VARIABLE_SHORT:
      case VARIABLE_INT:
        final int schemaOrdinal = schemas[ordinal].getBytePosition();
        currentOffset = getUnsafe().getInt(block.getBaseObject(),
            block.getBaseOffset() + pointer + schemaOrdinal);
        int nextOffset = getUnsafe().getInt(block.getBaseObject(),
            block.getBaseOffset() + pointer + schemaOrdinal + 4);
        length = nextOffset - currentOffset;
        break;
      default:
        currentOffset = schemas[ordinal].getBytePosition();
        length = schemas[ordinal].getLength();
    }
    byte[] data = new byte[length];
    getUnsafe()
        .copyMemory(block.getBaseObject(), block.getBaseOffset() + pointer + currentOffset, data,
            BYTE_ARRAY_OFFSET, data.length);
    return data;
  }

  @Override
  public int getLengthInBytes(int ordinal) {
    int length;
    int schemaOrdinal = schemas[ordinal].getBytePosition();
    switch (schemas[ordinal].getSchemaType()) {
      case VARIABLE_SHORT:
      case VARIABLE_INT:
        int currentOffset = getUnsafe().getInt(block.getBaseObject(),
            block.getBaseOffset() + pointer + schemaOrdinal);
        int nextOffset = getUnsafe().getInt(block.getBaseObject(),
            block.getBaseOffset() + pointer + schemaOrdinal + 4);
        length = nextOffset - currentOffset;
        break;
      default:
        length = schemas[ordinal].getLength();
    }
    return length;
  }

  @Override
  public void setBoolean(boolean value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public boolean getBoolean(int ordinal) {
    return getUnsafe().getBoolean(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public IndexRow getRow(int ordinal) {
    CarbonRowSchema[] childSchemas =
        ((CarbonRowSchema.StructCarbonRowSchema) schemas[ordinal]).getChildSchemas();
    return new UnsafeIndexRow(childSchemas, block, pointer);
  }

  @Override
  public void setByteArray(byte[] byteArray, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public int getInt(int ordinal) {
    return getUnsafe().getInt(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public void setInt(int value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public void setByte(byte value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public byte getByte(int ordinal) {
    return getUnsafe().getByte(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public void setShort(short value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public short getShort(int ordinal) {
    return getUnsafe().getShort(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public void setLong(long value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public long getLong(int ordinal) {
    return getUnsafe().getLong(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public void setFloat(float value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public float getFloat(int ordinal) {
    return getUnsafe().getFloat(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public void setDouble(double value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override
  public double getDouble(int ordinal) {
    return getUnsafe().getDouble(block.getBaseObject(),
        block.getBaseOffset() + pointer + schemas[ordinal].getBytePosition());
  }

  @Override
  public void setRow(IndexRow row, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }
}
