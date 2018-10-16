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
import java.util.Arrays;
import java.util.BitSet;

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.scan.result.vector.ColumnVectorInfo;
import org.apache.carbondata.core.util.DataTypeUtil;

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

    void fillVector(Object valuesToBeConverted, int size, ColumnVectorInfo info, BitSet nullBitset);

    int getSize();

    DecimalConverterType getDecimalConverterType();

  }

  public static class DecimalIntConverter implements DecimalConverter {

    protected int scale;

    DecimalIntConverter(int scale) {
      this.scale = scale;
    }

    @Override public Object convert(BigDecimal decimal) {
      long longValue = decimal.unscaledValue().longValue();
      return (int) longValue;
    }

    @Override public BigDecimal getDecimal(Object valueToBeConverted) {
      return BigDecimal.valueOf((Long) valueToBeConverted, scale);
    }

    @Override public void fillVector(Object valuesToBeConverted, int size, ColumnVectorInfo info,
        BitSet nullBitset) {
      // TODO we need to find way to directly set to vector with out conversion. This way is very
      // inefficient.
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      if (valuesToBeConverted instanceof byte[]) {
        byte[] data = (byte[]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitset.get(i)) {
            vector.putNull(i);
          } else {
            vector.putDecimal(i, BigDecimal.valueOf(data[i], scale), precision);
          }
        }
      } else if (valuesToBeConverted instanceof short[]) {
        short[] data = (short[]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitset.get(i)) {
            vector.putNull(i);
          } else {
            vector.putDecimal(i, BigDecimal.valueOf(data[i], scale), precision);
          }
        }
      } else if (valuesToBeConverted instanceof int[]) {
        int[] data = (int[]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitset.get(i)) {
            vector.putNull(i);
          } else {
            vector.putDecimal(i, BigDecimal.valueOf(data[i], scale), precision);
          }
        }
      } else if (valuesToBeConverted instanceof long[]) {
        long[] data = (long[]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitset.get(i)) {
            vector.putNull(i);
          } else {
            vector.putDecimal(i, BigDecimal.valueOf(data[i], scale), precision);
          }
        }
      }
    }

    @Override public int getSize() {
      return 4;
    }

    @Override public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_INT;
    }
  }

  public static class DecimalLongConverter extends DecimalIntConverter {

    DecimalLongConverter(int scale) {
      super(scale);
    }

    @Override public Object convert(BigDecimal decimal) {
      return decimal.unscaledValue().longValue();
    }

    @Override public BigDecimal getDecimal(Object valueToBeConverted) {
      return BigDecimal.valueOf((Long) valueToBeConverted, scale);
    }

    @Override public int getSize() {
      return 8;
    }

    @Override public DecimalConverterType getDecimalConverterType() {
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

    @Override public Object convert(BigDecimal decimal) {
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

    @Override public BigDecimal getDecimal(Object valueToBeConverted) {
      BigInteger bigInteger = new BigInteger((byte[]) valueToBeConverted);
      return new BigDecimal(bigInteger, scale);
    }

    @Override public void fillVector(Object valuesToBeConverted, int size, ColumnVectorInfo info,
        BitSet nullBitset) {
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      if (valuesToBeConverted instanceof byte[][]) {
        byte[][] data = (byte[][]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitset.get(i)) {
            vector.putNull(i);
          } else {
            BigInteger bigInteger = new BigInteger(data[i]);
            vector.putDecimal(i, new BigDecimal(bigInteger, scale), precision);
          }
        }
      }
    }

    @Override public int getSize() {
      return numBytes;
    }

    @Override public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_UNSCALED;
    }
  }

  public static class LVBytesDecimalConverter implements DecimalConverter {

    public static LVBytesDecimalConverter INSTANCE = new LVBytesDecimalConverter();

    @Override public Object convert(BigDecimal decimal) {
      return DataTypeUtil.bigDecimalToByte(decimal);
    }

    @Override public BigDecimal getDecimal(Object valueToBeConverted) {
      return DataTypeUtil.byteToBigDecimal((byte[]) valueToBeConverted);
    }

    @Override public void fillVector(Object valuesToBeConverted, int size, ColumnVectorInfo info,
        BitSet nullBitset) {
      CarbonColumnVector vector = info.vector;
      int precision = info.measure.getMeasure().getPrecision();
      if (valuesToBeConverted instanceof byte[][]) {
        byte[][] data = (byte[][]) valuesToBeConverted;
        for (int i = 0; i < size; i++) {
          if (nullBitset.get(i)) {
            vector.putNull(i);
          } else {
            vector.putDecimal(i, DataTypeUtil.byteToBigDecimal(data[i]), precision);
          }
        }
      }
    }

    @Override public int getSize() {
      return -1;
    }

    @Override public DecimalConverterType getDecimalConverterType() {
      return DecimalConverterType.DECIMAL_LV;
    }
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
