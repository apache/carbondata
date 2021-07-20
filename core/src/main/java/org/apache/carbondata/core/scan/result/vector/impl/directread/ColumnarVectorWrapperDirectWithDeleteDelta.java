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
import java.util.BitSet;

import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;

/**
 * Column vector for column pages which has delete delta, so it uses delta bitset to filter out
 * data before filling to actual vector.
 */
class ColumnarVectorWrapperDirectWithDeleteDelta extends AbstractCarbonColumnarVector {

  private BitSet deletedRows;

  private BitSet nullBits;

  private int counter;

  public ColumnarVectorWrapperDirectWithDeleteDelta(CarbonColumnVector vectorWrapper,
      BitSet deletedRows, BitSet nullBits) {
    super(vectorWrapper);
    this.deletedRows = deletedRows;
    this.nullBits = nullBits;
  }

  @Override
  public DataType getType() {
    return columnVector.getType();
  }

  @Override
  public void putBoolean(int rowId, boolean value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putBoolean(counter++, value);
      }
    }
  }

  @Override
  public void putObject(int rowId, Object obj) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putObject(counter++, obj);
      }
    }
  }

  @Override
  public void putFloat(int rowId, float value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putFloat(counter++, value);
      }
    }
  }

  @Override
  public void putShort(int rowId, short value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putShort(counter++, value);
      }
    }
  }

  @Override
  public void putInt(int rowId, int value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putInt(counter++, value);
      }
    }
  }

  @Override
  public void putLong(int rowId, long value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putLong(counter++, value);
      }
    }
  }

  @Override
  public void putDecimal(int rowId, BigDecimal value, int precision) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putDecimal(counter++, value, precision);
      }
    }
  }

  @Override
  public void putDouble(int rowId, double value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putDouble(counter++, value);
      }
    }
  }

  @Override
  public void putByteArray(int rowId, byte[] value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putByteArray(counter++, value);
      }
    }
  }

  @Override
  public void putByteArray(int rowId, int offset, int length, byte[] value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putByteArray(counter++, offset, length, value);
      }
    }
  }

  @Override
  public void putByte(int rowId, byte value) {
    if (!deletedRows.get(rowId)) {
      if (nullBits.get(rowId)) {
        columnVector.putNull(counter++);
      } else {
        columnVector.putByte(counter++, value);
      }
    }
  }

  @Override
  public void putNull(int rowId) {
    if (!deletedRows.get(rowId)) {
      columnVector.putNull(counter++);
    }
  }

  @Override
  public void putNotNull(int rowId) {
    if (!deletedRows.get(rowId)) {
      counter++;
    }
  }

  @Override
  public void putFloats(int rowId, int count, float[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!deletedRows.get(rowId++)) {
        columnVector.putFloat(counter++, src[i]);
      }
    }
  }

  @Override
  public void putShorts(int rowId, int count, short[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!deletedRows.get(rowId++)) {
        columnVector.putShort(counter++, src[i]);
      }
    }
  }

  @Override
  public void putInts(int rowId, int count, int[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!deletedRows.get(rowId++)) {
        columnVector.putInt(counter++, src[i]);
      }
    }
  }

  @Override
  public void putLongs(int rowId, int count, long[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!deletedRows.get(rowId++)) {
        columnVector.putLong(counter++, src[i]);
      }
    }
  }

  @Override
  public void putDoubles(int rowId, int count, double[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!deletedRows.get(rowId++)) {
        columnVector.putDouble(counter++, src[i]);
      }
    }
  }

  @Override
  public void putBytes(int rowId, int count, byte[] src, int srcIndex) {
    for (int i = srcIndex; i < count; i++) {
      if (!deletedRows.get(rowId++)) {
        columnVector.putByte(counter++, src[i]);
      }
    }
  }

  @Override
  public void putArray(int rowId, int offset, int length) {
    if (!deletedRows.get(rowId)) {
      columnVector.putArray(counter++, offset, length);
    }
  }

  public CarbonColumnVector getColumnVector() {
    return this.columnVector;
  }

  @Override
  public void setCarbonDataFileWrittenVersion(String carbonDataFileWrittenVersion) {
    this.columnVector.setCarbonDataFileWrittenVersion(carbonDataFileWrittenVersion);
  }
}
