package org.apache.carbondata.core.indexstore.row;

import org.apache.carbondata.core.indexstore.schema.DataMapSchema;
import org.apache.carbondata.core.metadata.datatype.DataType;

/**
 * Created by root1 on 17/6/17.
 */
public class DataMapRowImpl extends DataMapRow {

  private Object[] data;

  public DataMapRowImpl(DataMapSchema[] schemas) {
    super(schemas);
    this.data = new Object[schemas.length];
  }

  @Override public byte[] getByteArray(int ordinal) {
    return (byte[]) data[ordinal];
  }

  @Override public DataMapRow getRow(int ordinal) {
    return (DataMapRow) data[ordinal];
  }

  @Override public void setByteArray(byte[] byteArray, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.BYTE_ARRAY);
    data[ordinal] = byteArray;
  }

  @Override public int getInt(int ordinal) {
    return (Integer) data[ordinal];
  }

  @Override public void setInt(int value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.INT);
    data[ordinal] = value;
  }

  @Override public void setByte(byte value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.BYTE);
    data[ordinal] = value;
  }

  @Override public byte getByte(int ordinal) {
    return (Byte) data[ordinal];
  }

  @Override public void setShort(short value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.SHORT);
    data[ordinal] = value;
  }

  @Override public short getShort(int ordinal) {
    return (Short) data[ordinal];
  }

  @Override public void setLong(long value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.LONG);
    data[ordinal] = value;
  }

  @Override public long getLong(int ordinal) {
    return (Long) data[ordinal];
  }

  @Override public void setFloat(float value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.FLOAT);
    data[ordinal] = value;
  }

  @Override public float getFloat(int ordinal) {
    return (Float) data[ordinal];
  }

  @Override public void setDouble(double value, int ordinal) {
    assert (schemas[ordinal].getDataType() == DataType.DOUBLE);
    data[ordinal] = value;
  }

  @Override public double getDouble(int ordinal) {
    return (Double) data[ordinal];
  }

}
