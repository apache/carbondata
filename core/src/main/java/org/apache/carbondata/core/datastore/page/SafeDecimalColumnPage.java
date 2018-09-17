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

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Represent a columnar data in one page for one column of decimal data type
 */
public class SafeDecimalColumnPage extends DecimalColumnPage {

  // Only one of following fields will be used
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private byte[] shortIntData;
  private byte[][] byteArrayData;

  SafeDecimalColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    byteArrayData = new byte[pageSize][];
  }

  @Override
  public void setBytePage(byte[] byteData) {
    this.byteData = byteData;
  }

  @Override
  public void setShortPage(short[] shortData) {
    this.shortData = shortData;
  }

  @Override
  public void setShortIntPage(byte[] shortIntData) {
    this.shortIntData = shortIntData;
  }

  @Override
  public void setIntPage(int[] intData) {
    this.intData = intData;
  }

  @Override
  public void setLongPage(long[] longData) {
    this.longData = longData;
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    byteArrayData = byteArray;
  }

  /**
   * Set byte value at rowId
   */
  @Override
  public void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  /**
   * Set short value at rowId
   */
  @Override
  public void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  /**
   * Set integer value at rowId
   */
  @Override
  public void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  /**
   * Set long value at rowId
   */
  @Override
  public void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  @Override
  void putBytesAtRow(int rowId, byte[] bytes) {
    byteArrayData[rowId] = bytes;
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        if (null == intData) {
          intData = new int[pageSize];
        }
        putInt(rowId, (int) decimalConverter.convert(decimal));
        break;
      case DECIMAL_LONG:
        if (null == longData) {
          longData = new long[pageSize];
        }
        putLong(rowId, (long) decimalConverter.convert(decimal));
        break;
      default:
        putBytes(rowId, (byte[]) decimalConverter.convert(decimal));
    }
  }

  @Override
  public void putShortInt(int rowId, int value) {
    byte[] converted = ByteUtil.to3Bytes(value);
    System.arraycopy(converted, 0, shortIntData, rowId * 3, 3);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    byteArrayData[rowId] = new byte[length];
    System.arraycopy(bytes, offset, byteArrayData[rowId], 0, length);
  }

  @Override
  public byte getByte(int rowId) {
    return byteData[rowId];
  }

  @Override
  public byte[] getBytes(int rowId) {
    return byteArrayData[rowId];
  }

  @Override
  public short getShort(int rowId) {
    return shortData[rowId];
  }

  @Override
  public int getShortInt(int rowId) {
    return ByteUtil.valueOf3Bytes(shortIntData, rowId * 3);
  }

  @Override
  public int getInt(int rowId) {
    return intData[rowId];
  }

  @Override
  public long getLong(int rowId) {
    return longData[rowId];
  }

  @Override
  public void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    System.arraycopy(byteArrayData[rowId], 0, dest, destOffset, length);
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, intData[i]);
        }
        break;
      case DECIMAL_LONG:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, longData[i]);
        }
        break;
      default:
        throw new UnsupportedOperationException("not support value conversion on "
            + columnPageEncoderMeta.getStoreDataType() + " page");
    }
  }

  @Override public byte[] getByteData() {
    return byteData;
  }

  @Override public short[] getShortData() {
    return shortData;
  }

  @Override public int[] getShortIntData() {
    int[] ints = new int[pageSize];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = ByteUtil.valueOf3Bytes(shortIntData, i * 3);
    }
    return ints;
  }

  @Override public int[] getIntData() {
    return intData;
  }

  @Override public long[] getLongData() {
    return longData;
  }

  @Override public byte[][] getByteArrayPage() {
    return byteArrayData;
  }

  @Override
  public void freeMemory() {
    byteArrayData = null;
    super.freeMemory();
  }
}
