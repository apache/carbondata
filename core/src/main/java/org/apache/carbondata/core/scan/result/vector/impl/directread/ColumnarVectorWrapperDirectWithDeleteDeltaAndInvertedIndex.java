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
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.DecimalType;
import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.impl.CarbonColumnVectorImpl;

class ColumnarVectorWrapperDirectWithDeleteDeltaAndInvertedIndex
    extends AbstractCarbonColumnarVector implements ConvertableVector {

  private BitSet deletedRows;

  private int[] invertedIndex;

  private CarbonColumnVectorImpl carbonColumnVector;

  private CarbonColumnVector columnVector;

  private int precision;

  private BitSet nullBits;

  public ColumnarVectorWrapperDirectWithDeleteDeltaAndInvertedIndex(
      CarbonColumnVector vectorWrapper, BitSet deletedRows, int[] invertedIndex, BitSet nullBits) {
    this.deletedRows = deletedRows;
    this.invertedIndex = invertedIndex;
    carbonColumnVector = new CarbonColumnVectorImpl(invertedIndex.length, vectorWrapper.getType());
    this.columnVector = vectorWrapper;
    this.nullBits = nullBits;
  }

  @Override public void putBoolean(int rowId, boolean value) {
    carbonColumnVector.putBoolean(invertedIndex[rowId], value);
  }

  @Override public void putFloat(int rowId, float value) {
    carbonColumnVector.putFloat(invertedIndex[rowId], value);
  }

  @Override public void putShort(int rowId, short value) {
    carbonColumnVector.putShort(invertedIndex[rowId], value);
  }

  @Override public void putInt(int rowId, int value) {
    carbonColumnVector.putInt(invertedIndex[rowId], value);
  }

  @Override public void putLong(int rowId, long value) {
    carbonColumnVector.putLong(invertedIndex[rowId], value);
  }

  @Override public void putDecimal(int rowId, BigDecimal value, int precision) {
    this.precision = precision;
    carbonColumnVector.putDecimal(invertedIndex[rowId], value, precision);
  }

  @Override public void putDouble(int rowId, double value) {
    carbonColumnVector.putDouble(invertedIndex[rowId], value);
  }

  @Override public void putBytes(int rowId, byte[] value) {
    carbonColumnVector.putBytes(invertedIndex[rowId], value);
  }

  @Override public void putBytes(int rowId, int offset, int length, byte[] value) {
    carbonColumnVector.putBytes(invertedIndex[rowId], offset, length, value);
  }

  @Override public void putByte(int rowId, byte value) {
    carbonColumnVector.putByte(invertedIndex[rowId], value);
  }

  @Override public void putNull(int rowId) {
    nullBits.set(rowId);
  }

  @Override public void convert() {
    DataType dataType = columnVector.getType();
    int length = invertedIndex.length;
    int counter = 0;
    if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
      byte[] dataArray = (byte[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putByte(counter++, dataArray[i]);
          }
        }
      }
    } else if (dataType == DataTypes.SHORT) {
      short[] dataArray = (short[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putShort(counter++, dataArray[i]);
          }
        }
      }
    } else if (dataType == DataTypes.INT) {
      int[] dataArray = (int[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putInt(counter++, dataArray[i]);
          }
        }
      }
    } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
      long[] dataArray = (long[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putLong(counter++, dataArray[i]);
          }
        }
      }
    } else if (dataType == DataTypes.FLOAT) {
      float[] dataArray = (float[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putFloat(counter++, dataArray[i]);
          }
        }
      }
    } else if (dataType == DataTypes.DOUBLE) {
      double[] dataArray = (double[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putDouble(counter++, dataArray[i]);
          }
        }
      }
    } else if (dataType instanceof DecimalType) {
      BigDecimal[] dataArray = (BigDecimal[]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putDecimal(counter++, dataArray[i], precision);
          }
        }
      }
    } else if (dataType == DataTypes.STRING || dataType == DataTypes.BYTE_ARRAY) {
      byte[][] dataArray = (byte[][]) carbonColumnVector.getDataArray();
      for (int i = 0; i < length; i++) {
        if (!deletedRows.get(i)) {
          if (nullBits.get(i)) {
            columnVector.putNull(counter++);
          } else {
            columnVector.putBytes(counter++, dataArray[i]);
          }
        }
      }
    }
  }
}
