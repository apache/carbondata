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

package org.apache.carbondata.spark.vectorreader;

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;

class ColumnarVectorWrapper implements CarbonColumnVector {

  private ColumnVector columnVector;

  public ColumnarVectorWrapper(ColumnVector columnVector) {
    this.columnVector = columnVector;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    columnVector.putBoolean(rowId, value);
  }

  @Override public void putFloat(int rowId, float value) {
    columnVector.putFloat(rowId, value);
  }

  @Override public void putShort(int rowId, short value) {
    columnVector.putShort(rowId, value);
  }

  @Override public void putShorts(int rowId, int count, short value) {
    columnVector.putShorts(rowId, count, value);
  }

  @Override public void putInt(int rowId, int value) {
    columnVector.putInt(rowId, value);
  }

  @Override public void putInts(int rowId, int count, int value) {
    columnVector.putInts(rowId, count, value);
  }

  @Override public void putLong(int rowId, long value) {
    columnVector.putLong(rowId, value);
  }

  @Override public void putLongs(int rowId, int count, long value) {
    columnVector.putLongs(rowId, count, value);
  }

  @Override public void putDecimal(int rowId, Decimal value, int precision) {
    columnVector.putDecimal(rowId, value, precision);
  }

  @Override public void putDecimals(int rowId, int count, Decimal value, int precision) {
    for (int i = 0; i < count; i++) {
      putDecimal(rowId++, value, precision);
    }
  }

  @Override public void putDouble(int rowId, double value) {
    columnVector.putDouble(rowId, value);
  }

  @Override public void putDoubles(int rowId, int count, double value) {
    columnVector.putDoubles(rowId, count, value);
  }

  @Override public void putBytes(int rowId, byte[] value) {
    columnVector.putByteArray(rowId, value);
  }

  @Override public void putBytes(int rowId, int count, byte[] value) {
    for (int i = 0; i < count; i++) {
      putBytes(rowId++, value);
    }
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {
    columnVector.putByteArray(rowId, value, offset, length);
  }

  @Override public void putNull(int rowId) {
    columnVector.putNull(rowId);
  }

  @Override public void putNulls(int rowId, int count) {
    columnVector.putNulls(rowId, count);
  }

  @Override public boolean isNull(int rowId) {
    return columnVector.isNullAt(rowId);
  }

  @Override public void putObject(int rowId, Object obj) {
    //TODO handle complex types
  }

  @Override public Object getData(int rowId) {
    //TODO handle complex types
    return null;
  }

  @Override public void reset() {
//    columnVector.reset();
  }

  @Override public DataType getType() {
    return columnVector.dataType();
  }
}
