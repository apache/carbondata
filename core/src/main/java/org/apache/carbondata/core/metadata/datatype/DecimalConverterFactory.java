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

package org.apache.carbondata.core.metadata.datatype;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.BitSet;

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.scan.result.vector.impl.directread.ColumnarVectorWrapperDirectFactory;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.format.Encoding;

/**
 * Decimal converter to keep the data compact.
 */
public final class DecimalConverterFactory {

  public static final DecimalConverterFactory INSTANCE = new DecimalConverterFactory();

  private int[] minBytesForPrecision = minBytesForPrecision();

  private DecimalConverterFactory() {

  }

  private int computeMinBytesForPrecision(int precision) {
    int numBytes = 1;
    while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
      numBytes += 1;
    }
    return numBytes;
  }

  private int[] minBytesForPrecision() {
    int[] data = new int[39];
    for (int i = 0; i < data.length; i++) {
      data[i] = computeMinBytesForPrecision(i);
    }
    return data;
  }

  public enum DecimalConverterType {
    DECIMAL_LV(-1), DECIMAL_INT(4), DECIMAL_LONG(8), DECIMAL_UNSCALED(-1);

    private int sizeInBytes;

    DecimalConverterType(int sizeInBytes) {
      this.sizeInBytes = sizeInBytes;
    }

    public int getSizeInBytes() {
      return sizeInBytes;
    }

  }

  public interface DecimalConverter {

    Object convert(BigDecimal decimal);

    BigDecimal getDecimal(Object valueToBeConverted);

    void fillVector(Object valuesToBeConverted, int size, ColumnVectorInfo info, BitSet nullBitset,
        DataType pageType);

    int getSize();

    DecimalConverterType getDecimalConverterType();

  }

  public static class DecimalIntConverter implements DecimalConverter {

    protected int scale;

    DecimalIntConverter(int scale) {
      this.scale = scale;
    }

    @Override
    public Object convert(BigDecimal decimal) {
      long longValue = decimal.unscaledValue().longValue();
      return (int) longValue;
    }

    @Override
    public BigDecimal getDecimal(Object valueToBeConverted) {
      return BigDecimal.valueOf((Long) valueToBeConverted, scale);
    }

    @Override
    public void fillVector(Object valuesToBeConverted, int size,
        ColumnVectorInfo vectorInfo, BitSet nullBitSet, DataType pageType) {
      if (!(valuesToBeConverted instanceof byte[])) {
        throw new UnsupportedOperationException("This object type " + valuesToBeConverted.getClass()
            + " is not supported in this method");
      }
      // TODO we need to find way to directly set to vector with out conversion. This way is very
      // inefficient.
      CarbonColumnVector vector = getCarbonColumnVector(vectorInfo, nullBitSet);
      int precision;
      int newMeasureScale;
      if (vectorInfo.measure == null) {
        // complex primitive decimal flow comes as dimension
        precision = ((DecimalType) vector.getType()).getPrecision();
        newMeasureScale = ((DecimalType) vector.getType()).getScale();
        size = ColumnVectorInfo.getUpdatedPageSizeForChildVector(vectorInfo, size);
      } else {
        precision = vectorInfo.measure.getMeasure().getPrecision();
        newMeasureScale = vectorInfo.measure.getMeasure().getScale();
      }
      int shortSizeInBytes = DataTypes.SHORT.getSizeInBytes();
      int intSizeInBytes = DataTypes.INT.getSizeInBytes();
      int lengthStoredInBytes;
      if (vectorInfo.encodings != null && vectorInfo.encodings.size() > 0 && CarbonUtil
          .hasEncoding(vectorInfo.encodings, Encoding.INT_LENGTH_COMPLEX_CHILD_BYTE_ARRAY)) {
        lengthStoredInBytes = intSizeInBytes;
      } else {
        // before to carbon 2.0, complex child length is stored as SHORT
        // for string, varchar, binary, date, decimal types
        lengthStoredInBytes = shortSizeInBytes;
      }
      byte[] data = (byte[]) valuesToBeConverted;
      if (pageType == DataTypes.BYTE) {
        for (int i = 0; i < size; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(i);
          } else {
            BigDecimal value = BigDecimal.valueOf(data[i], scale);
            if (value.scale() < newMeasureScale) {
              value = value.setScale(newMeasureScale);
            }
            vector.putDecimal(i, value, precision);
          }
        }
      } else if (pageType == DataTypes.SHORT) {
        for (int i = 0; i < size; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(i);
          } else {
            BigDecimal value = BigDecimal
                .valueOf(ByteUtil.toShortLittleEndian(data, i * shortSizeInBytes),
                    scale);
            if (value.scale() < newMeasureScale) {
              value = value.setScale(newMeasureScale);
            }
            vector.putDecimal(i, value, precision);
          }
        }
      } else if (pageType == DataTypes.SHORT_INT) {
        int shortIntSizeInBytes = DataTypes.SHORT_INT.getSizeInBytes();
        for (int i = 0; i < size; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(i);
          } else {
            BigDecimal value = BigDecimal
                .valueOf(ByteUtil.valueOf3Bytes(data, i * shortIntSizeInBytes),
                    scale);
            if (value.scale() < newMeasureScale) {
              value = value.setScale(newMeasureScale);
            }
            vector.putDecimal(i, value, precision);
          }
        }
      } else {
        if (pageType == DataTypes.INT) {
          for (int i = 0; i < size; i++) {
            if (nullBitSet.get(i)) {
              vector.putNull(i);
            } else {
              BigDecimal value = BigDecimal
                  .valueOf(ByteUtil.toIntLittleEndian(data, i * intSizeInBytes),
                      scale);
              if (value.scale() < newMeasureScale) {
                value = value.setScale(newMeasureScale);
              }
              vector.putDecimal(i, value, precision);
            }
          }
        } else if (pageType == DataTypes.LONG) {
          int longSizeInBytes = DataTypes.LONG.getSizeInBytes();
          for (int i = 0; i < size; i++) {
            if (nullBitSet.get(i)) {
              vector.putNull(i);
            } else {
              BigDecimal value = BigDecimal
                  .valueOf(ByteUtil.toLongLittleEndian(data, i * longSizeInBytes),
                      scale);
              if (value.scale() < newMeasureScale) {
                value = value.setScale(newMeasureScale);
              }
              vector.putDecimal(i, value, precision);
            }
          }
        } else if (pageType == DataTypes.BYTE_ARRAY) {
          // complex primitive decimal dimension
          int offset = 0;
          int length;
          for (int j = 0; j < size; j++) {
            // here decimal data will be Length[4 byte], scale[1 byte], value[Length byte]
            if (lengthStoredInBytes == intSizeInBytes) {
              length = ByteBuffer.wrap(data, offset, lengthStoredInBytes).getInt();
            } else {
              length = ByteBuffer.wrap(data, offset, lengthStoredInBytes).getShort();
            }
            offset += lengthStoredInBytes;
            if (length == 0) {
              vector.putNull(j);
              continue;
            }
            // jump the scale offset
            offset += 1;
            // remove scale from the length
            length -= 1;
            byte[] row = new byte[length];
            System.arraycopy(data, offset, row, 0, length);
            long val;
            if (length == 1) {
              val = row[0];
            } else if (length == 2) {
              val = ByteUtil.toShort(row, 0);
            } else if (length == 4) {
              val = ByteUtil.toInt(row, 0);
            } else if (length == 3) {
              val = ByteUtil.valueOf3Bytes(row, 0);
            } else {
              // TODO: check if other value can come
              val = ByteUtil.toLong(row, 0, length);
            }
            BigDecimal value = BigDecimal.valueOf(val, scale);
            if (value.scale() < newMeasureScale) {
              value = value.setScale(newMeasureScale);
            }
            vector.putDecimal(j, value, precision);
            offset += length;
          }
        }
      }
    }

    @Override
    public int getSize() {
      return 4;
    }

    @Override
    public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_INT;
    }
  }

  public static class DecimalLongConverter extends DecimalIntConverter {

    DecimalLongConverter(int scale) {
      super(scale);
    }

    @Override
    public Object convert(BigDecimal decimal) {
      return decimal.unscaledValue().longValue();
    }

    @Override
    public BigDecimal getDecimal(Object valueToBeConverted) {
      return BigDecimal.valueOf((Long) valueToBeConverted, scale);
    }

    @Override
    public int getSize() {
      return 8;
    }

    @Override
    public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_LONG;
    }
  }

  public class DecimalUnscaledConverter implements DecimalConverter {

    private int scale;

    private int numBytes;

    private byte[] decimalBuffer = new byte[minBytesForPrecision[38]];

    DecimalUnscaledConverter(int precision, int scale) {
      this.scale = scale;
      this.numBytes = minBytesForPrecision[precision];
    }

    @Override
    public Object convert(BigDecimal decimal) {
      byte[] bytes = decimal.unscaledValue().toByteArray();
      byte[] fixedLengthBytes = null;
      if (bytes.length == numBytes) {
        // If the length of the underlying byte array of the unscaled `BigInteger` happens to be
        // `numBytes`, just reuse it, so that we don't bother copying it to `decimalBuffer`.
        fixedLengthBytes = bytes;
      } else {
        // Otherwise, the length must be less than `numBytes`.  In this case we copy contents of
        // the underlying bytes with padding sign bytes to `decimalBuffer` to form the result
        // fixed-length byte array.
        byte signByte = 0;
        if (bytes[0] < 0) {
          signByte = (byte) -1;
        } else {
          signByte = (byte) 0;
        }
        Arrays.fill(decimalBuffer, 0, numBytes - bytes.length, signByte);
        System.arraycopy(bytes, 0, decimalBuffer, numBytes - bytes.length, bytes.length);
        fixedLengthBytes = decimalBuffer;
      }
      byte[] value = new byte[numBytes];
      System.arraycopy(fixedLengthBytes, 0, value, 0, numBytes);
      return value;
    }

    @Override
    public BigDecimal getDecimal(Object valueToBeConverted) {
      BigInteger bigInteger = new BigInteger((byte[]) valueToBeConverted);
      return new BigDecimal(bigInteger, scale);
    }

    @Override
    public void fillVector(Object valuesToBeConverted, int size,
        ColumnVectorInfo vectorInfo, BitSet nullBitSet, DataType pageType) {
      CarbonColumnVector vector = getCarbonColumnVector(vectorInfo, nullBitSet);
      //TODO handle complex child
      int precision = vectorInfo.measure.getMeasure().getPrecision();
      int newMeasureScale = vectorInfo.measure.getMeasure().getScale();
      if (scale < newMeasureScale) {
        scale = newMeasureScale;
      }
      if (valuesToBeConverted instanceof byte[][]) {
        byte[][] data = (byte[][]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(i);
          } else {
            BigInteger bigInteger = new BigInteger(data[i]);
            BigDecimal value = new BigDecimal(bigInteger, scale);
            if (value.scale() < newMeasureScale) {
              value = value.setScale(newMeasureScale);
            }
            vector.putDecimal(i, value, precision);
          }
        }
      }
    }

    @Override
    public int getSize() {
      return numBytes;
    }

    @Override
    public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_UNSCALED;
    }
  }

  public static class LVBytesDecimalConverter implements DecimalConverter {

    public static LVBytesDecimalConverter INSTANCE = new LVBytesDecimalConverter();

    @Override
    public Object convert(BigDecimal decimal) {
      return DataTypeUtil.bigDecimalToByte(decimal);
    }

    @Override
    public BigDecimal getDecimal(Object valueToBeConverted) {
      return DataTypeUtil.byteToBigDecimal((byte[]) valueToBeConverted);
    }

    @Override
    public void fillVector(Object valuesToBeConverted, int size,
        ColumnVectorInfo vectorInfo, BitSet nullBitSet, DataType pageType) {
      CarbonColumnVector vector = getCarbonColumnVector(vectorInfo, nullBitSet);
      //TODO handle complex child
      int precision = vectorInfo.measure.getMeasure().getPrecision();
      int newMeasureScale = vectorInfo.measure.getMeasure().getScale();
      if (valuesToBeConverted instanceof byte[][]) {
        byte[][] data = (byte[][]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitSet.get(i)) {
            vector.putNull(i);
          } else {
            BigDecimal value = DataTypeUtil.byteToBigDecimal(data[i]);
            if (value.scale() < newMeasureScale) {
              value = value.setScale(newMeasureScale);
            }
            vector.putDecimal(i, value, precision);
          }
        }
      }
    }

    @Override
    public int getSize() {
      return -1;
    }

    @Override
    public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_LV;
    }
  }

  private static CarbonColumnVector getCarbonColumnVector(ColumnVectorInfo vectorInfo,
      BitSet nullBitSet) {
    CarbonColumnVector vector = vectorInfo.vector;
    BitSet deletedRows = vectorInfo.deletedRows;
    vector = ColumnarVectorWrapperDirectFactory
        .getDirectVectorWrapperFactory(vectorInfo, vector, vectorInfo.invertedIndex, nullBitSet,
            deletedRows, true, false);
    return vector;
  }

  public DecimalConverter getDecimalConverter(int precision, int scale) {
    if (precision < 0) {
      return new LVBytesDecimalConverter();
    } else if (precision <= 9) {
      return new DecimalIntConverter(scale);
    } else if (precision <= 18) {
      return new DecimalLongConverter(scale);
    } else {
      return new DecimalUnscaledConverter(precision, scale);
    }
  }

}
