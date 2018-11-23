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

package org.apache.carbondata.core.datastore.page;

import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;

/**
 * This is a decorator of column page, it performs decoding lazily (when caller calls getXXX
 * method to get the value from the page)
 */
public class LazyColumnPage extends ColumnPage {

  // decorated column page
  private ColumnPage columnPage;

  // encode that will apply to page data in getXXX
  private ColumnPageValueConverter converter;

  private LazyColumnPage(ColumnPage columnPage, ColumnPageValueConverter converter) {
    super(columnPage.getColumnPageEncoderMeta(), columnPage.getPageSize());
    this.columnPage = columnPage;
    this.converter = converter;
  }

  public static ColumnPage newPage(ColumnPage columnPage, ColumnPageValueConverter codec) {
    return new LazyColumnPage(columnPage, codec);
  }

  @Override
  public String toString() {
    return String.format("[converter: %s, data type: %s", converter, columnPage.getDataType());
  }

  @Override
  public long getLong(int rowId) {
    DataType dataType = columnPage.getDataType();
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      return converter.decodeLong(columnPage.getByte(rowId));
    } else if (dataType == DataTypes.SHORT) {
      return converter.decodeLong(columnPage.getShort(rowId));
    } else if (dataType == DataTypes.SHORT_INT) {
      return converter.decodeLong(columnPage.getShortInt(rowId));
    } else if (dataType == DataTypes.INT) {
      return converter.decodeLong(columnPage.getInt(rowId));
    } else if (dataType == DataTypes.LONG) {
      return columnPage.getLong(rowId);
    } else {
      throw new RuntimeException("internal error: " + this.toString());
    }
  }

  @Override
  public double getDouble(int rowId) {
    DataType dataType = columnPage.getDataType();
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      return converter.decodeDouble(columnPage.getByte(rowId));
    } else if (dataType == DataTypes.SHORT) {
      return converter.decodeDouble(columnPage.getShort(rowId));
    } else if (dataType == DataTypes.SHORT_INT) {
      return converter.decodeDouble(columnPage.getShortInt(rowId));
    } else if (dataType == DataTypes.INT) {
      return converter.decodeDouble(columnPage.getInt(rowId));
    } else if (dataType == DataTypes.LONG) {
      return converter.decodeDouble(columnPage.getLong(rowId));
    } else if (dataType == DataTypes.FLOAT) {
      return converter.decodeDouble(columnPage.getFloat(rowId));
    } else if (dataType == DataTypes.DOUBLE) {
      return columnPage.getDouble(rowId);
    } else {
      throw new RuntimeException("internal error: " + this.toString());
    }
  }

  @Override
  public float getFloat(int rowId) {
    return (float) getDouble(rowId);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    DecimalConverterFactory.DecimalConverter decimalConverter =
        ((DecimalColumnPage) columnPage).getDecimalConverter();
    DataType dataType = columnPage.getDataType();
    if (dataType == DataTypes.BYTE) {
      return decimalConverter.getDecimal(converter.decodeLong(columnPage.getByte(rowId)));
    } else if (dataType == DataTypes.SHORT) {
      return decimalConverter.getDecimal(converter.decodeLong(columnPage.getShort(rowId)));
    } else if (dataType == DataTypes.SHORT_INT) {
      return decimalConverter.getDecimal(converter.decodeLong(columnPage.getShortInt(rowId)));
    } else if (dataType == DataTypes.INT) {
      return decimalConverter.getDecimal(converter.decodeLong(columnPage.getInt(rowId)));
    } else if (dataType == DataTypes.LONG || DataTypes.isDecimal(dataType)) {
      return columnPage.getDecimal(rowId);
    } else {
      throw new RuntimeException("internal error: " + this.toString());
    }
  }

  @Override
  public byte[] getBytes(int rowId) {
    return columnPage.getBytes(rowId);
  }

  @Override
  public byte[] getBytePage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public short[] getShortPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getShortIntPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int[] getIntPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public long[] getLongPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public float[] getFloatPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public double[] getDoublePage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[][] getByteArrayPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getDecimalPage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getLVFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getComplexChildrenLVFlattenedBytePage() {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte[] getComplexParentFlattenedBytePage() throws IOException {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setBytePage(byte[] byteData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setShortPage(short[] shortData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setIntPage(int[] intData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setLongPage(long[] longData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void freeMemory() {
    columnPage.freeMemory();
  }

  @Override
  public void putByte(int rowId, byte value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putShort(int rowId, short value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putInt(int rowId, int value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putLong(int rowId, long value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putBytes(int rowId, byte[] bytes) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putShortInt(int rowId, int value) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public byte getByte(int rowId) {
    return columnPage.getByte(rowId);
  }

  @Override
  public short getShort(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int getShortInt(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public int getInt(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }
}
