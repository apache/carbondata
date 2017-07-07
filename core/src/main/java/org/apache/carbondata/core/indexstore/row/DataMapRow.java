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

import org.apache.carbondata.core.indexstore.schema.DataMapSchema;

/**
 * It is just a normal row to store data. Implementation classes could be safe and unsafe.
 * TODO move this class a global row and use across loading after DataType is changed class
 */
public abstract class DataMapRow {

  protected DataMapSchema[] schemas;

  public DataMapRow(DataMapSchema[] schemas) {
    this.schemas = schemas;
  }

  public abstract byte[] getByteArray(int ordinal);

  public abstract DataMapRow getRow(int ordinal);

  public abstract void setRow(DataMapRow row, int ordinal);

  public abstract void setByteArray(byte[] byteArray, int ordinal);

  public abstract int getInt(int ordinal);

  public abstract void setInt(int value, int ordinal);

  public abstract void setByte(byte value, int ordinal);

  public abstract byte getByte(int ordinal);

  public abstract void setShort(short value, int ordinal);

  public abstract short getShort(int ordinal);

  public abstract void setLong(long value, int ordinal);

  public abstract long getLong(int ordinal);

  public abstract void setFloat(float value, int ordinal);

  public abstract float getFloat(int ordinal);

  public abstract void setDouble(double value, int ordinal);

  public abstract double getDouble(int ordinal);

  public int getTotalSizeInBytes() {
    int len = 0;
    for (int i = 0; i < schemas.length; i++) {
      len += getSizeInBytes(i);
    }
    return len;
  }

  public int getSizeInBytes(int ordinal) {
    switch (schemas[ordinal].getSchemaType()) {
      case FIXED:
        return schemas[ordinal].getLength();
      case VARIABLE:
        return getByteArray(ordinal).length + 2;
      case STRUCT:
        return getRow(ordinal).getTotalSizeInBytes();
      default:
        throw new UnsupportedOperationException("wrong type");
    }
  }

  public int getColumnCount() {
    return schemas.length;
  }
}
