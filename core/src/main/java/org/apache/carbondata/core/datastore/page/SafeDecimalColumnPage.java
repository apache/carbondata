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
import java.nio.ByteBuffer;

import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.core.util.ByteUtil;

/**
 * Represent a columnar data in one page for one column of decimal data type
 */
public class SafeDecimalColumnPage extends DecimalColumnPage {

  private ByteBuffer flattenBytesBuffer;
  private byte[] flattenContentInBytes;

  SafeDecimalColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    int rcdPerSizeInBytes = columnPageEncoderMeta.getStoreDataType().getSizeInBytes();
    flattenContentInBytes = new byte[pageSize * rcdPerSizeInBytes];
    this.flattenBytesBuffer = ByteBuffer.wrap(flattenContentInBytes);
  }

  @Override
  public void setFlattenContentInBytes(byte[] flattenContentInBytes) {
    this.flattenContentInBytes = flattenContentInBytes;
    this.flattenBytesBuffer = ByteBuffer.wrap(flattenContentInBytes);
  }

  @Override
  public byte[] getFlattenContentInBytes() {
    return new byte[0];
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    for (int i = 0; i < byteArray.length; i++) {
      flattenBytesBuffer.putShort((short) byteArray.length);
      flattenBytesBuffer.put(byteArray[i]);
    }
  }

  /**
   * Set byte value at rowId
   */
  @Override
  public void putByte(int rowId, byte value) {
    flattenBytesBuffer.put(value);
  }

  /**
   * Set short value at rowId
   */
  @Override
  public void putShort(int rowId, short value) {
    flattenBytesBuffer.putShort(value);
  }

  /**
   * Set integer value at rowId
   */
  @Override
  public void putInt(int rowId, int value) {
    flattenBytesBuffer.putInt(value);
  }

  /**
   * Set long value at rowId
   */
  @Override
  public void putLong(int rowId, long value) {
    flattenBytesBuffer.putLong(value);
  }

  @Override
  void putBytesAtRow(int rowId, byte[] bytes) {
    flattenBytesBuffer.put(bytes);
  }

  @Override
  public void putDecimal(int rowId, BigDecimal decimal) {
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        putInt(rowId, (int) decimalConverter.convert(decimal));
        break;
      case DECIMAL_LONG:
        putLong(rowId, (long) decimalConverter.convert(decimal));
        break;
      default:
        putBytes(rowId, (byte[]) decimalConverter.convert(decimal));
    }
  }

  @Override
  public void putShortInt(int rowId, int value) {
    byte[] converted = ByteUtil.to3Bytes(value);
    flattenBytesBuffer.put(converted);
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    flattenBytesBuffer.put(bytes, offset, length);
  }

  @Override
  public byte getByte(int rowId) {
    return flattenBytesBuffer.get(rowId);
  }

  @Override
  public byte[] getBytes(int rowId) {
    for (int i = 0; i < rowId; i++) {
      short length = flattenBytesBuffer.getShort();
      flattenBytesBuffer.position(flattenBytesBuffer.position() + length);
    }
    short length = flattenBytesBuffer.getShort();
    // todo: try to avoid this allocation
    byte[] rtn = new byte[length];
    flattenBytesBuffer.get(rtn);
    return rtn;
  }

  @Override
  public short getShort(int rowId) {
    return flattenBytesBuffer.getShort(rowId * ByteUtil.SIZEOF_SHORT);
  }

  @Override
  public int getShortInt(int rowId) {
    return ByteUtil.valueOf3Bytes(flattenBytesBuffer.array(), rowId * ByteUtil.SIZEOF_SHORT_INT);
  }

  @Override
  public int getInt(int rowId) {
    return flattenBytesBuffer.getInt(rowId * ByteUtil.SIZEOF_INT);
  }

  @Override
  public long getLong(int rowId) {
    return flattenBytesBuffer.getLong(rowId * ByteUtil.SIZEOF_LONG);
  }

  @Override
  public void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    for (int i = 0; i < rowId; i++) {
      short len = flattenBytesBuffer.getShort();
      flattenBytesBuffer.position(flattenBytesBuffer.position() + len);
    }
    flattenBytesBuffer.get(dest, destOffset, length);
  }

  @Override
  public void convertValue(ColumnPageValueConverter codec) {
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, getInt(i));
        }
        break;
      case DECIMAL_LONG:
        for (int i = 0; i < pageSize; i++) {
          codec.encode(i, getLong(i));
        }
        break;
      default:
        throw new UnsupportedOperationException("not support value conversion on "
            + columnPageEncoderMeta.getStoreDataType() + " page");
    }
  }

  @Override
  public void freeMemory() {
    flattenBytesBuffer.clear();
    flattenContentInBytes = null;
    super.freeMemory();
  }
}
