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
import org.apache.carbondata.core.scan.scanner.LazyPageLoader;

import org.apache.spark.sql.CarbonVectorProxy;
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil;
import org.apache.spark.sql.types.Decimal;

class ColumnarVectorWrapper implements CarbonColumnVector {

  private CarbonVectorProxy.ColumnVectorProxy sparkColumnVectorProxy;

  private CarbonVectorProxy carbonVectorProxy;

  private boolean[] filteredRows;

  private int counter;

  private int ordinal;

  private boolean isDictionary;

  private boolean filteredRowsExist;

  private DataType blockDataType;

  private CarbonColumnVector dictionaryVector;

  ColumnarVectorWrapper(CarbonVectorProxy writableColumnVector,
      boolean[] filteredRows, int ordinal) {
    this.sparkColumnVectorProxy = writableColumnVector.getColumnVector(ordinal);
    this.filteredRows = filteredRows;
    this.carbonVectorProxy = writableColumnVector;
    this.ordinal = ordinal;
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putBoolean(counter++, value);
    }
  }

  @Override
  public void putFloat(int rowId, float value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putFloat(counter++, value);
    }
  }

  @Override
  public void putShort(int rowId, short value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putShort(counter++, value);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          sparkColumnVectorProxy.putShort(counter++, value);
        }
        rowId++;
      }
    } else {
      sparkColumnVectorProxy.putShorts(rowId, count, value);
    }
  }

  @Override
  public void putInt(int rowId, int value) {
    if (!filteredRows[rowId]) {
      if (isDictionary) {
        sparkColumnVectorProxy.putDictionaryInt(counter++, value);
      } else {
        sparkColumnVectorProxy.putInt(counter++, value);
      }
    }
  }

  @Override
  public void putInts(int rowId, int count, int value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          sparkColumnVectorProxy.putInt(counter++, value);
        }
        rowId++;
      }
    } else {
      sparkColumnVectorProxy.putInts(rowId, count, value);
    }
  }

  @Override
  public void putLong(int rowId, long value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putLong(counter++, value);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          sparkColumnVectorProxy.putLong(counter++, value);
        }
        rowId++;
      }
    } else {
      sparkColumnVectorProxy.putLongs(rowId, count, value);
    }
  }

  @Override
  public void putDecimal(int rowId, BigDecimal value, int precision) {
    if (!filteredRows[rowId]) {
      Decimal toDecimal = Decimal.apply(value);
      sparkColumnVectorProxy.putDecimal(counter++, toDecimal, precision);
    }
  }

  @Override
  public void putDecimals(int rowId, int count, BigDecimal value, int precision) {
    Decimal decimal = Decimal.apply(value);
    for (int i = 0; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putDecimal(counter++, decimal, precision);
      }
      rowId++;
    }
  }

  @Override
  public void putDouble(int rowId, double value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putDouble(counter++, value);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          sparkColumnVectorProxy.putDouble(counter++, value);
        }
        rowId++;
      }
    } else {
      sparkColumnVectorProxy.putDoubles(rowId, count, value);
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putByte(counter++, value);
    }
  }

  @Override
  public void putByteArray(int rowId, byte[] value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putByteArray(counter++, value, 0, value.length);
    }
  }

  @Override
  public void putByteArray(int rowId, int count, byte[] value) {
    for (int i = 0; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putByteArray(counter++, value);
      }
      rowId++;
    }
  }

  @Override
  public void putByteArray(int rowId, int offset, int length, byte[] value) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putByteArray(counter++, value, offset, length);
    }
  }

  @Override
  public void putNull(int rowId) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putNull(counter++);
    }
  }

  @Override
  public void putNulls(int rowId, int count) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          sparkColumnVectorProxy.putNull(counter++);
        }
        rowId++;
      }
    } else {
      sparkColumnVectorProxy.putNulls(rowId, count);
    }
  }

  @Override
  public void putNotNull(int rowId) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putNotNull(counter++);
    }
  }

  @Override
  public void putNotNull(int rowId, int count) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          sparkColumnVectorProxy.putNotNull(counter++);
        }
        rowId++;
      }
    } else {
      sparkColumnVectorProxy.putNotNulls(rowId, count);
    }
  }

  @Override
  public boolean isNull(int rowId) {
    return sparkColumnVectorProxy.isNullAt(rowId);
  }

  @Override
  public void putObject(int rowId, Object obj) {
    //TODO handle complex types
  }

  @Override
  public Object getData(int rowId) {
    //TODO handle complex types
    return null;
  }

  @Override
  public void reset() {
    counter = 0;
    filteredRowsExist = false;
    if (null != dictionaryVector) {
      dictionaryVector.reset();
    }
  }

  @Override
  public DataType getType() {
    return CarbonSparkDataSourceUtil
        .convertSparkToCarbonDataType(sparkColumnVectorProxy.dataType());
  }

  @Override
  public DataType getBlockDataType() {
    return blockDataType;
  }

  @Override
  public void setBlockDataType(DataType blockDataType) {
    this.blockDataType = blockDataType;
  }

  @Override
  public void setFilteredRowsExist(boolean filteredRowsExist) {
    this.filteredRowsExist = filteredRowsExist;
  }

  @Override
  public void setDictionary(CarbonDictionary dictionary) {
      sparkColumnVectorProxy.setDictionary(dictionary);
  }

  @Override
  public boolean hasDictionary() {
    return sparkColumnVectorProxy.hasDictionary();
  }

  public void reserveDictionaryIds() {
    sparkColumnVectorProxy.reserveDictionaryIds(carbonVectorProxy.numRows());
    dictionaryVector = new ColumnarVectorWrapper(carbonVectorProxy, filteredRows, ordinal);
    ((ColumnarVectorWrapper) dictionaryVector).isDictionary = true;
  }

  @Override
  public CarbonColumnVector getDictionaryVector() {
    return dictionaryVector;
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putFloat(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putShort(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putInt(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putLong(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putDouble(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        sparkColumnVectorProxy.putByte(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override
  public void setLazyPage(LazyPageLoader lazyPage) {
    lazyPage.loadPage();
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    if (!filteredRows[rowId]) {
      sparkColumnVectorProxy.putArray(rowId, offset, length);
    }
  }

  @Override
  public void putAllByteArray(byte[] data, int offset, int length) {
    sparkColumnVectorProxy.putAllByteArray(data, offset, length);
  }
}
