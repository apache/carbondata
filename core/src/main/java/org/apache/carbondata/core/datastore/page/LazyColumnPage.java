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

import java.math.BigDecimal;

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
    super(columnPage.getDataType(), columnPage.getPageSize(), columnPage.scale,
        columnPage.precision);
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
    switch (columnPage.getDataType()) {
      case BYTE:
        return converter.decodeLong(columnPage.getByte(rowId));
      case SHORT:
        return converter.decodeLong(columnPage.getShort(rowId));
      case SHORT_INT:
        return converter.decodeLong(columnPage.getShortInt(rowId));
      case INT:
        return converter.decodeLong(columnPage.getInt(rowId));
      case LONG:
        return columnPage.getLong(rowId);
      default:
        throw new RuntimeException("internal error: " + this.toString());
    }
  }

  @Override
  public double getDouble(int rowId) {
    switch (columnPage.getDataType()) {
      case BYTE:
        return converter.decodeDouble(columnPage.getByte(rowId));
      case SHORT:
        return converter.decodeDouble(columnPage.getShort(rowId));
      case SHORT_INT:
        return converter.decodeDouble(columnPage.getShortInt(rowId));
      case INT:
        return converter.decodeDouble(columnPage.getInt(rowId));
      case LONG:
        return converter.decodeDouble(columnPage.getLong(rowId));
      case FLOAT:
        return converter.decodeDouble(columnPage.getFloat(rowId));
      case DOUBLE:
        return columnPage.getDouble(rowId);
      default:
        throw new RuntimeException("internal error: " + this.toString());
    }
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException("internal error");
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    return columnPage.getDecimal(rowId);
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
  public byte[] getLVFlattenedBytePage() {
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
    throw new UnsupportedOperationException("internal error");
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
