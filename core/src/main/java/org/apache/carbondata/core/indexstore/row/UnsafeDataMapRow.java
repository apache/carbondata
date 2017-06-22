package org.apache.carbondata.core.indexstore.row;

import org.apache.carbondata.core.indexstore.schema.DataMapSchema;
import org.apache.carbondata.core.memory.MemoryBlock;

import static org.apache.carbondata.core.memory.CarbonUnsafe.BYTE_ARRAY_OFFSET;
import static org.apache.carbondata.core.memory.CarbonUnsafe.unsafe;

/**
 * Created by root1 on 19/6/17.
 */
public class UnsafeDataMapRow extends DataMapRow {

  private MemoryBlock block;

  private int pointer;

  public UnsafeDataMapRow(DataMapSchema[] schemas, MemoryBlock block, int pointer) {
    super(schemas);
    this.block = block;
    this.pointer = pointer;
  }

  @Override public byte[] getByteArray(int ordinal) {
    int length;
    int position = getPosition(ordinal);
    switch (schemas[ordinal].getSchemaType()) {
      case VARIABLE:
        length = unsafe.getShort(block.getBaseObject(), block.getBaseOffset() + pointer + position);
        position += 2;
        break;
      default:
        length = schemas[ordinal].getLength();
    }
    byte[] data = new byte[length];
    unsafe.copyMemory(block.getBaseObject(), block.getBaseOffset() + pointer + position, data,
        BYTE_ARRAY_OFFSET, data.length);
    return data;
  }

  @Override public DataMapRow getRow(int ordinal) {
    DataMapSchema[] childSchemas =
        ((DataMapSchema.StructDataMapSchema) schemas[ordinal]).getChildSchemas();
    return new UnsafeDataMapRow(childSchemas, block, pointer + getPosition(ordinal));
  }

  @Override public void setByteArray(byte[] byteArray, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public int getInt(int ordinal) {
    return unsafe
        .getInt(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setInt(int value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public void setByte(byte value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public byte getByte(int ordinal) {
    return unsafe
        .getByte(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setShort(short value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public short getShort(int ordinal) {
    return unsafe
        .getShort(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setLong(long value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public long getLong(int ordinal) {
    return unsafe
        .getLong(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setFloat(float value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public float getFloat(int ordinal) {
    return unsafe
        .getFloat(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setDouble(double value, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  @Override public double getDouble(int ordinal) {
    return unsafe
        .getDouble(block.getBaseObject(), block.getBaseOffset() + pointer + getPosition(ordinal));
  }

  @Override public void setRow(DataMapRow row, int ordinal) {
    throw new UnsupportedOperationException("Not supported to set on unsafe row");
  }

  private int getPosition(int ordinal) {
    int position = 0;
    for (int i = 0; i < ordinal; i++) {
      position += getSizeInBytes(i);
    }
    return position;
  }
}
