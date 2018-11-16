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

package org.apache.carbondata.core.scan.result.vector.impl.directread;

import java.math.BigDecimal;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

/**
 * Column vector for column pages which has inverted index, so it uses inverted index
 * before filling to actual vector
 */
public class ColumnarVectorWrapperDirectWithInvertedIndex extends AbstractCarbonColumnarVector {

  protected int[] invertedIndex;


  protected boolean isnullBitsExists;

  public ColumnarVectorWrapperDirectWithInvertedIndex(CarbonColumnVector columnVector,
      int[] invertedIndex, boolean isnullBitsExists) {
    super(columnVector);
    this.invertedIndex = invertedIndex;
    this.isnullBitsExists = isnullBitsExists;
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    columnVector.putBoolean(invertedIndex[rowId], value);
  }

  @Override
  public void putFloat(int rowId, float value) {
    columnVector.putFloat(invertedIndex[rowId], value);
  }

  @Override
  public void putShort(int rowId, short value) {
    columnVector.putShort(invertedIndex[rowId], value);
  }

  @Override
  public void putInt(int rowId, int value) {
    columnVector.putInt(invertedIndex[rowId], value);
  }

  @Override
  public void putLong(int rowId, long value) {
    columnVector.putLong(invertedIndex[rowId], value);
  }

  @Override
  public void putDecimal(int rowId, BigDecimal value, int precision) {
    columnVector.putDecimal(invertedIndex[rowId], value, precision);
  }

  @Override
  public void putDouble(int rowId, double value) {
    columnVector.putDouble(invertedIndex[rowId], value);
  }

  @Override
  public void putByteArray(int rowId, byte[] value) {
    columnVector.putByteArray(invertedIndex[rowId], value);
  }

  @Override
  public void putByteArray(int rowId, int offset, int length, byte[] value) {
    columnVector.putByteArray(invertedIndex[rowId], offset, length, value);
  }


  @Override
  public void putByte(int rowId, byte value) {
    columnVector.putByte(invertedIndex[rowId], value);
  }

  @Override
  public void putNull(int rowId) {
    if (isnullBitsExists) {
      columnVector.putNull(rowId);
    } else {
      columnVector.putNull(invertedIndex[rowId]);
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      columnVector.putFloat(invertedIndex[rowId++], src[i]);
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      columnVector.putShort(invertedIndex[rowId++], src[i]);
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      columnVector.putInt(invertedIndex[rowId++], src[i]);
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      columnVector.putLong(invertedIndex[rowId++], src[i]);
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      columnVector.putDouble(invertedIndex[rowId++], src[i]);
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      columnVector.putByte(invertedIndex[rowId++], src[i]);
    }
  }

  @Override
  public DataType getBlockDataType() {
    return columnVector.getBlockDataType();
  }

  @Override public void putArray(int rowId, int offset, int length) {
    columnVector.putArray(invertedIndex[rowId], offset, length);
  }
}
