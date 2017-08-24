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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;

import org.apache.carbondata.core.datastore.TableSpec;
import org.apache.carbondata.core.metadata.datatype.DataType;

public class SafeVarLengthColumnPage extends VarLengthColumnPageBase {

  // for string and decimal data
  private byte[][] byteArrayData;
  private int[] intData;
  private long[] longData;

  SafeVarLengthColumnPage(TableSpec.ColumnSpec columnSpec, DataType dataType, int pageSize) {
    super(columnSpec, dataType, pageSize);
    byteArrayData = new byte[pageSize][];
  }

  @Override
  public void freeMemory() {
  }

  @Override
  public void putBytesAtRow(int rowId, byte[] bytes) {
    byteArrayData[rowId] = bytes;
  }

  @Override
  public void putBytes(int rowId, byte[] bytes, int offset, int length) {
    byteArrayData[rowId] = new byte[length];
    System.arraycopy(bytes, offset, byteArrayData[rowId], 0, length);
  }

  @Override public void putDecimal(int rowId, BigDecimal decimal) {
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
  public BigDecimal getDecimal(int rowId) {
    byte[] bytes = byteArrayData[rowId];
    return decimalConverter.getDecimal(bytes);
  }

  @Override
  public byte[] getBytes(int rowId) {
    return byteArrayData[rowId];
  }

  @Override
  public void setByteArrayPage(byte[][] byteArray) {
    byteArrayData = byteArray;
  }

  @Override
  public byte[] getLVFlattenedBytePage() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(stream);
    for (byte[] byteArrayDatum : byteArrayData) {
      out.writeInt(byteArrayDatum.length);
      out.write(byteArrayDatum);
    }
    return stream.toByteArray();
  }

  @Override
  public byte[][] getByteArrayPage() {
    return byteArrayData;
  }

  @Override
  void copyBytes(int rowId, byte[] dest, int destOffset, int length) {
    System.arraycopy(byteArrayData[rowId], 0, dest, destOffset, length);
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

  /**
   * apply encoding to page data
   *
   * @param codec type of transformation
   */
  @Override public void convertValue(ColumnPageValueConverter codec) {
    switch (dataType) {
      case DECIMAL:
        convertValueForDecimalType(codec);
        break;
      default:
        throw new UnsupportedOperationException(
            "not support value conversion on " + dataType + " page");
    }
  }

  private void convertValueForDecimalType(ColumnPageValueConverter codec) {
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
        throw new UnsupportedOperationException(
            "not support value conversion on " + dataType + " page");
    }
  }

}
