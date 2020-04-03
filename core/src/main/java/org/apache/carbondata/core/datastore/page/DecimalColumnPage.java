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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalConverterFactory;

/**
 * Represent a columnar data in one page for one column of decimal data type
 */
public abstract class DecimalColumnPage extends UnsafeVarLengthColumnPageBase {

  /**
   * decimal converter instance
   */
  DecimalConverterFactory.DecimalConverter decimalConverter;

  DecimalColumnPage(ColumnPageEncoderMeta columnPageEncoderMeta, int pageSize) {
    super(columnPageEncoderMeta, pageSize);
    decimalConverter = DecimalConverterFactory.INSTANCE.getDecimalConverter(
        columnPageEncoderMeta.getColumnSpec().getPrecision(),
        columnPageEncoderMeta.getColumnSpec().getScale());
  }

  public DecimalConverterFactory.DecimalConverter getDecimalConverter() {
    return decimalConverter;
  }

  @Override
  public byte[] getBytePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public short[] getShortPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[] getShortIntPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public int[] getIntPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public long[] getLongPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public float[] getFloatPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public double[] getDoublePage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public byte[][] getByteArrayPage() {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public float getFloat(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public double getDouble(int rowId) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putDouble(int rowId, double value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void putFloat(int rowId, float value) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setFloatPage(float[] floatData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  @Override
  public void setDoublePage(double[] doubleData) {
    throw new UnsupportedOperationException(
        "invalid data type: " + columnPageEncoderMeta.getStoreDataType());
  }

  // used for building index in loading process
  private BigDecimal getDecimalFromRawData(int rowId) {
    long value;
    switch (decimalConverter.getDecimalConverterType()) {
      case DECIMAL_INT:
        value = getInt(rowId);
        break;
      case DECIMAL_LONG:
        value = getLong(rowId);
        break;
      default:
        value = getByte(rowId);
    }
    return decimalConverter.getDecimal(value);
  }

  private BigDecimal getDecimalFromDecompressData(int rowId) {
    long value;
    if (columnPageEncoderMeta.getStoreDataType() == DataTypes.BYTE) {
      value = getByte(rowId);
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT) {
      value = getShort(rowId);
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.SHORT_INT) {
      value = getShortInt(rowId);
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.INT) {
      value = getInt(rowId);
    } else if (columnPageEncoderMeta.getStoreDataType() == DataTypes.LONG) {
      value = getLong(rowId);
    } else {
      return decimalConverter.getDecimal(getBytes(rowId));
    }
    return decimalConverter.getDecimal(value);
  }

  @Override
  public BigDecimal getDecimal(int rowId) {
    // rowOffset is initialed for query in `VarLengthColumnPageBase.getDecimalColumnPage`
    // if its size is 0, we are in loading process and the data in column page is raw
    if (rowOffset.getActualRowCount() == 0) {
      return getDecimalFromRawData(rowId);
    } else {
      return getDecimalFromDecompressData(rowId);
    }
  }

}
