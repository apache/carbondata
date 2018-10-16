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

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;

import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil;
import org.apache.spark.sql.types.Decimal;

/**
 * Fills the vector directly with out considering any deleted rows.
 */
class ColumnarVectorWrapperDirect implements CarbonColumnVector {

  protected CarbonVectorProxy.ColumnVectorProxy sparkColumnVectorProxy;

  protected CarbonVectorProxy carbonVectorProxy;

  protected int ordinal;

  protected boolean isDictionary;

  private DataType blockDataType;

  private CarbonColumnVector dictionaryVector;

  ColumnarVectorWrapperDirect(CarbonVectorProxy writableColumnVector, int ordinal) {
    this.sparkColumnVectorProxy = writableColumnVector.getColumnVector(ordinal);
    this.carbonVectorProxy = writableColumnVector;
    this.ordinal = ordinal;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    sparkColumnVectorProxy.putBoolean(rowId, value, ordinal);
  }

  @Override public void putFloat(int rowId, float value) {
    sparkColumnVectorProxy.putFloat(rowId, value, ordinal);
  }

  @Override public void putShort(int rowId, short value) {
    sparkColumnVectorProxy.putShort(rowId, value, ordinal);
  }

  @Override public void putShorts(int rowId, int count, short value) {
    sparkColumnVectorProxy.putShorts(rowId, count, value, ordinal);
  }

  @Override public void putInt(int rowId, int value) {
    if (isDictionary) {
      sparkColumnVectorProxy.putDictionaryInt(rowId, value, ordinal);
    } else {
      sparkColumnVectorProxy.putInt(rowId, value, ordinal);
    }
  }

  @Override public void putInts(int rowId, int count, int value) {
    sparkColumnVectorProxy.putInts(rowId, count, value, ordinal);
  }

  @Override public void putLong(int rowId, long value) {
    sparkColumnVectorProxy.putLong(rowId, value, ordinal);
  }

  @Override public void putLongs(int rowId, int count, long value) {
    sparkColumnVectorProxy.putLongs(rowId, count, value, ordinal);
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    Decimal toDecimal = Decimal.apply(value);
    sparkColumnVectorProxy.putDecimal(rowId, toDecimal, precision, ordinal);
  }

  @Override public void putDecimals(int rowId, int count, BigDecimal value, int precision) {
    Decimal decimal = Decimal.apply(value);
    for (int i = 0; i < count; i++) {
      sparkColumnVectorProxy.putDecimal(rowId, decimal, precision, ordinal);
      rowId++;
    }
  }

  @Override public void putDouble(int rowId, double value) {
    sparkColumnVectorProxy.putDouble(rowId, value, ordinal);
  }

  @Override public void putDoubles(int rowId, int count, double value) {
    sparkColumnVectorProxy.putDoubles(rowId, count, value, ordinal);
  }

  @Override public void putByteArray(int rowId, byte[] value) {
    sparkColumnVectorProxy.putByteArray(rowId, value, ordinal);
  }

  @Override
  public void putBytes(int rowId, int count, byte[] value) {
    for (int i = 0; i < count; i++) {
      sparkColumnVectorProxy.putByteArray(rowId, value, ordinal);
      rowId++;
    }
  }

  @Override public void putByteArray(int rowId, int offset, int length, byte[] value) {
    sparkColumnVectorProxy.putByteArray(rowId, value, offset, length, ordinal);
  }

  @Override public void putNull(int rowId) {
    sparkColumnVectorProxy.putNull(rowId, ordinal);
  }

  @Override public void putNulls(int rowId, int count) {
    sparkColumnVectorProxy.putNulls(rowId, count, ordinal);
  }

  @Override public void putNotNull(int rowId) {
    sparkColumnVectorProxy.putNotNull(rowId, ordinal);
  }

  @Override public void putNotNull(int rowId, int count) {
    sparkColumnVectorProxy.putNotNulls(rowId, count, ordinal);
  }

  @Override public boolean isNull(int rowId) {
    return sparkColumnVectorProxy.isNullAt(rowId, ordinal);
  }

  @Override public void putObject(int rowId, Object obj) {
    //TODO handle complex types
  }

  @Override public Object getData(int rowId) {
    //TODO handle complex types
    return null;
  }

  @Override public void reset() {
    if (null != dictionaryVector) {
      dictionaryVector.reset();
    }
  }

  @Override public DataType getType() {
    return CarbonSparkDataSourceUtil
        .convertSparkToCarbonDataType(sparkColumnVectorProxy.dataType(ordinal));
  }

  @Override public DataType getBlockDataType() {
    return blockDataType;
  }

  @Override public void setBlockDataType(DataType blockDataType) {
    this.blockDataType = blockDataType;
  }

  @Override public void setDictionary(CarbonDictionary dictionary) {
    sparkColumnVectorProxy.setDictionary(dictionary, ordinal);
  }

  @Override public boolean hasDictionary() {
    return sparkColumnVectorProxy.hasDictionary(ordinal);
  }

  public void reserveDictionaryIds() {
    sparkColumnVectorProxy.reserveDictionaryIds(carbonVectorProxy.numRows(), ordinal);
    dictionaryVector = new ColumnarVectorWrapperDirect(carbonVectorProxy, ordinal);
    ((ColumnarVectorWrapperDirect) dictionaryVector).isDictionary = true;
  }

  @Override public CarbonColumnVector getDictionaryVector() {
    return dictionaryVector;
  }

  @Override public void putByte(int rowId, byte value) {
    sparkColumnVectorProxy.putByte(rowId, value, ordinal);
  }

  @Override public void setFilteredRowsExist(boolean filteredRowsExist) {

  }

  @Override public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    sparkColumnVectorProxy.putFloats(rowId, count, src, srcIndex);
  }

  @Override public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    sparkColumnVectorProxy.putShorts(rowId, count, src, srcIndex);
  }

  @Override public void putInts(int rowId, int count, int[] src, int srcIndex) {
    sparkColumnVectorProxy.putInts(rowId, count, src, srcIndex);
  }

  @Override public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    sparkColumnVectorProxy.putLongs(rowId, count, src, srcIndex);
  }

  @Override public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    sparkColumnVectorProxy.putDoubles(rowId, count, src, srcIndex);
  }

  @Override public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    sparkColumnVectorProxy.putBytes(rowId, count, src, srcIndex);
  }
}
