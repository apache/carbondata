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

/**
 * Column vector for column pages which has delete delta and inverted index, so it uses delta
 * bitset to filter out data and use inverted index before filling to actual vector
 */
public class ColumnarVectorWrapperDirectWithDeleteDeltaAndInvertedIndex
    extends ColumnarVectorWrapperDirectWithInvertedIndex {

  private BitSet deletedRows;

  private CarbonColumnVector carbonColumnVector;

  private int precision;

  private BitSet nullBits;

  /**
   * Constructor
   * @param vectorWrapper vector to be filled
   * @param deletedRows deleted rows from delete delta.
   * @param invertedIndex Inverted index of the column
   * @param nullBits Null row ordinals in the bitset
   * @param isnullBitsExists whether to consider inverted index while setting null bitset or not.
   *                          we are having nullBitset even for dimensions also.
   *                          But some dimension columns still don't have nullBitset.
   *                          So if null bitset does not exist then
   *                          it should not inverted index while setting the null
   */
  public ColumnarVectorWrapperDirectWithDeleteDeltaAndInvertedIndex(
      CarbonColumnVector vectorWrapper, BitSet deletedRows, int[] invertedIndex, BitSet nullBits,
      boolean isnullBitsExists, boolean isDictVector) {
    super(new CarbonColumnVectorImpl(invertedIndex.length,
        isDictVector ? DataTypes.INT : vectorWrapper.getType()), invertedIndex, isnullBitsExists);
    this.deletedRows = deletedRows;
    this.carbonColumnVector = vectorWrapper;
    this.nullBits = nullBits;
  }

  @Override
  public void putDecimal(int rowId, BigDecimal value, int precision) {
    this.precision = precision;
    carbonColumnVector.putDecimal(invertedIndex[rowId], value, precision);
  }

  @Override
  public void putNull(int rowId) {
    if (isnullBitsExists) {
      nullBits.set(rowId);
    } else {
      nullBits.set(invertedIndex[rowId]);
    }
  }

  @Override
  public void putAllByteArray(byte[] data, int offset, int length) {
    carbonColumnVector.putAllByteArray(data, offset, length);
  }

  @Override
  public void convert() {
    if (columnVector instanceof CarbonColumnVectorImpl) {
      CarbonColumnVectorImpl localVector = (CarbonColumnVectorImpl) columnVector;
      DataType dataType = columnVector.getType();
      int length = invertedIndex.length;
      int counter = 0;
      if (dataType == DataTypes.BOOLEAN || dataType == DataTypes.BYTE) {
        byte[] dataArray = (byte[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putByte(counter++, dataArray[i]);
            }
          }
        }
      } else if (dataType == DataTypes.SHORT) {
        short[] dataArray = (short[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putShort(counter++, dataArray[i]);
            }
          }
        }
      } else if (dataType == DataTypes.INT) {
        int[] dataArray = (int[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putInt(counter++, dataArray[i]);
            }
          }
        }
      } else if (dataType == DataTypes.LONG || dataType == DataTypes.TIMESTAMP) {
        long[] dataArray = (long[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putLong(counter++, dataArray[i]);
            }
          }
        }
      } else if (dataType == DataTypes.FLOAT) {
        float[] dataArray = (float[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putFloat(counter++, dataArray[i]);
            }
          }
        }
      } else if (dataType == DataTypes.DOUBLE) {
        double[] dataArray = (double[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putDouble(counter++, dataArray[i]);
            }
          }
        }
      } else if (dataType instanceof DecimalType) {
        BigDecimal[] dataArray = (BigDecimal[]) localVector.getDataArray();
        for (int i = 0; i < length; i++) {
          if (!deletedRows.get(i)) {
            if (nullBits.get(i)) {
              carbonColumnVector.putNull(counter++);
            } else {
              carbonColumnVector.putDecimal(counter++, dataArray[i], precision);
            }
          }
        }
      } else if (dataType == DataTypes.STRING || dataType == DataTypes.BYTE_ARRAY) {
        int[] offsets = localVector.getOffsets();
        int[] lengths = localVector.getLengths();
        if (offsets != null && lengths != null) {
          for (int i = 0; i < length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBits.get(i)) {
                carbonColumnVector.putNull(counter++);
              } else {
                carbonColumnVector.putArray(counter++, offsets[i], lengths[i]);
              }
            }
          }
        } else {
          byte[][] dataArray = (byte[][]) localVector.getDataArray();
          for (int i = 0; i < length; i++) {
            if (!deletedRows.get(i)) {
              if (nullBits.get(i)) {
                carbonColumnVector.putNull(counter++);
              } else {
                carbonColumnVector.putByteArray(counter++, dataArray[i]);
              }
            }
          }
        }
      }
    }
  }
}
