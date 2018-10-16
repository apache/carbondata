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

package org.apache.carbondata.presto;

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

public class CarbonColumnVectorWrapper implements CarbonColumnVector {

  private CarbonColumnVectorImpl columnVector;

  private boolean[] filteredRows;

  private int counter;

  private boolean filteredRowsExist;

  private DataType blockDataType;

  public CarbonColumnVectorWrapper(CarbonColumnVectorImpl columnVector, boolean[] filteredRows) {
    this.columnVector = columnVector;
    this.filteredRows = filteredRows;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    if (!filteredRows[rowId]) {
      columnVector.putBoolean(counter++, value);
    }
  }

  @Override public void putFloat(int rowId, float value) {
    if (!filteredRows[rowId]) {
      columnVector.putFloat(counter++, value);
    }
  }

  @Override public void putShort(int rowId, short value) {
    if (!filteredRows[rowId]) {
      columnVector.putShort(counter++, value);
    }
  }

  @Override public void putShorts(int rowId, int count, short value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          columnVector.putShort(counter++, value);
        }
        rowId++;
      }
    } else {
      columnVector.putShorts(rowId, count, value);
    }
  }

  @Override public void putInt(int rowId, int value) {
    if (!filteredRows[rowId]) {
      columnVector.putInt(counter++, value);
    }
  }

  @Override public void putInts(int rowId, int count, int value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          columnVector.putInt(counter++, value);
        }
        rowId++;
      }
    } else {
      columnVector.putInts(rowId, count, value);
    }
  }

  @Override public void putLong(int rowId, long value) {
    if (!filteredRows[rowId]) {
      columnVector.putLong(counter++, value);
    }
  }

  @Override public void putLongs(int rowId, int count, long value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          columnVector.putLong(counter++, value);
        }
        rowId++;
      }
    } else {
      columnVector.putLongs(rowId, count, value);
    }
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    if (!filteredRows[rowId]) {
      columnVector.putDecimal(counter++, value, precision);
    }
  }

  @Override public void putDecimals(int rowId, int count, BigDecimal value, int precision) {
    for (int i = 0; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putDecimal(counter++, value, precision);
      }
      rowId++;
    }
  }

  @Override public void putDouble(int rowId, double value) {
    if (!filteredRows[rowId]) {
      columnVector.putDouble(counter++, value);
    }
  }

  @Override public void putDoubles(int rowId, int count, double value) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          columnVector.putDouble(counter++, value);
        }
        rowId++;
      }
    } else {
      columnVector.putDoubles(rowId, count, value);
    }
  }

  @Override public void putByte(int rowId, byte value) {
    if (!filteredRows[rowId]) {
      columnVector.putByte(counter++, value);
    }
  }

  @Override public void putByteArray(int rowId, byte[] value) {
    if (!filteredRows[rowId]) {
      columnVector.putByteArray(counter++, value);
    }
  }

  @Override public void putBytes(int rowId, int count, byte[] value) {
    for (int i = 0; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putByteArray(counter++, value);
      }
      rowId++;
    }
  }

  @Override public void putByteArray(int rowId, int offset, int length, byte[] value) {
    if (!filteredRows[rowId]) {
      columnVector.putByteArray(counter++, offset, length, value);
    }
  }

  @Override public void putNull(int rowId) {
    if (!filteredRows[rowId]) {
      columnVector.putNull(counter++);
    }
  }

  @Override public void putNulls(int rowId, int count) {
    if (filteredRowsExist) {
      for (int i = 0; i < count; i++) {
        if (!filteredRows[rowId]) {
          columnVector.putNull(counter++);
        }
        rowId++;
      }
    } else {
      columnVector.putNulls(rowId, count);
    }
  }

  @Override public void putNotNull(int rowId) {

  }

  @Override public void putNotNull(int rowId, int count) {

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
    counter = 0;
    filteredRowsExist = false;
  }

  @Override public DataType getType() {
    return columnVector.getType();
  }

  @Override
  public DataType getBlockDataType() {
    return blockDataType;
  }

  @Override
  public void setBlockDataType(DataType blockDataType) {
    this.blockDataType = blockDataType;
  }

  @Override public void setFilteredRowsExist(boolean filteredRowsExist) {
    this.filteredRowsExist = filteredRowsExist;
  }

  @Override public void setDictionary(CarbonDictionary dictionary) {
    this.columnVector.setDictionary(dictionary);
  }

  @Override public boolean hasDictionary() {
    return this.columnVector.hasDictionary();
  }

  @Override public CarbonColumnVector getDictionaryVector() {
    return this.columnVector;
  }

  @Override public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putFloat(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putShort(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override public void putInts(int rowId, int count, int[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putInt(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putLong(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putDouble(counter++, src[i]);
      }
      rowId++;
    }
  }

  @Override public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!filteredRows[rowId]) {
        columnVector.putByte(counter++, src[i]);
      }
      rowId++;
    }
  }


}
