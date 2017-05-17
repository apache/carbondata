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
import java.util.BitSet;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.util.DataTypeUtil;

// Represent a columnar data in one page for one column.
public class FixLengthColumnPage extends ColumnPage {

  // Only one of following fields will be used
  private byte[] byteData;
  private short[] shortData;
  private int[] intData;
  private long[] longData;
  private double[] doubleData;

  private byte[][] byteArrayData;

  // The index of the rowId whose value is null, will be set to 1
  private BitSet nullBitSet;

  public FixLengthColumnPage(DataType dataType, int pageSize) {
    super(dataType, pageSize);
    nullBitSet = new BitSet(pageSize);
    switch (dataType) {
      case SHORT:
      case INT:
      case LONG:
        longData = new long[pageSize];
        break;
      case DOUBLE:
        doubleData = new double[pageSize];
        break;
      case DECIMAL:
        byteArrayData = new byte[pageSize][];
        break;
      default:
        throw new RuntimeException("Unsupported data dataType: " + dataType);
    }
  }

  public DataType getDataType() {
    return dataType;
  }

  private void putByte(int rowId, byte value) {
    byteData[rowId] = value;
  }

  private void putShort(int rowId, short value) {
    shortData[rowId] = value;
  }

  private void putInt(int rowId, int value) {
    intData[rowId] = value;
  }

  private void putLong(int rowId, long value) {
    longData[rowId] = value;
  }

  private void putDouble(int rowId, double value) {
    doubleData[rowId] = value;
  }

  // This method will do LV (length value) coded of input bytes
  private void putDecimalBytes(int rowId, byte[] decimalInBytes) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(decimalInBytes.length +
        CarbonCommonConstants.INT_SIZE_IN_BYTE);
    byteBuffer.putInt(decimalInBytes.length);
    byteBuffer.put(decimalInBytes);
    byteBuffer.flip();
    byteArrayData[rowId] = byteBuffer.array();
  }

  public void putData(int rowId, Object value) {
    if (value == null) {
      putNull(rowId);
      return;
    }
    switch (dataType) {
      case BYTE:
      case SHORT:
        putLong(rowId, ((Short) value).longValue());
        break;
      case INT:
        putLong(rowId, ((Integer) value).longValue());
        break;
      case LONG:
        putLong(rowId, (long) value);
        break;
      case DOUBLE:
        putDouble(rowId, (double) value);
        break;
      case DECIMAL:
        putDecimalBytes(rowId, (byte[]) value);
        break;
      default:
        throw new RuntimeException("unsupported data type: " + dataType);
    }
    updateStatistics(value);
  }

  private void putNull(int rowId) {
    nullBitSet.set(rowId);
    switch (dataType) {
      case BYTE:
      case SHORT:
      case INT:
      case LONG:
        putLong(rowId, 0L);
        break;
      case DOUBLE:
        putDouble(rowId, 0.0);
        break;
      case DECIMAL:
        byte[] decimalInBytes = DataTypeUtil.bigDecimalToByte(BigDecimal.ZERO);
        putDecimalBytes(rowId, decimalInBytes);
        break;
    }
  }

  public long[] getLongPage() {
    return longData;
  }

  public double[] getDoublePage() {
    return doubleData;
  }

  public byte[][] getDecimalPage() {
    return byteArrayData;
  }

  public BitSet getNullBitSet() {
    return nullBitSet;
  }
}