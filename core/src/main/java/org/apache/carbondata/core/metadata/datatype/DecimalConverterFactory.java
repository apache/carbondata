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

import org.apache.carbondata.core.scan.result.vector.CarbonColumnVector;
import org.apache.carbondata.core.util.DataTypeUtil;

/**
 * Decimal converter to keep the data compact.
 */
public final class DecimalConverterFactory {

  public static DecimalConverterFactory INSTANCE = new DecimalConverterFactory();

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

  public interface DecimalConverter {

    byte[] convert(BigDecimal decimal);

    BigDecimal getDecimal(byte[] bytes);

    void writeToColumnVector(byte[] bytes, CarbonColumnVector vector, int rowId);

    int getSize();

  }

  public class DecimalIntConverter implements DecimalConverter {

    private ByteBuffer buffer = ByteBuffer.allocate(4);

    private int precision;
    private int scale;

    public DecimalIntConverter(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override public byte[] convert(BigDecimal decimal) {
      long longValue = decimal.unscaledValue().longValue();
      buffer.putInt(0, (int) longValue);
      return buffer.array().clone();
    }

    @Override public BigDecimal getDecimal(byte[] bytes) {
      long unscaled = getUnscaledLong(bytes);
      return BigDecimal.valueOf(unscaled, scale);
    }

    @Override public void writeToColumnVector(byte[] bytes, CarbonColumnVector vector, int rowId) {
      long unscaled = getUnscaledLong(bytes);
      vector.putInt(rowId, (int) unscaled);
    }

    @Override public int getSize() {
      return 4;
    }
  }

  private long getUnscaledLong(byte[] bytes) {
    long unscaled = 0L;
    int i = 0;

    while (i < bytes.length) {
      unscaled = (unscaled << 8) | (bytes[i] & 0xff);
      i += 1;
    }

    int bits = 8 * bytes.length;
    unscaled = (unscaled << (64 - bits)) >> (64 - bits);
    return unscaled;
  }

  public class DecimalLongConverter implements DecimalConverter {

    private ByteBuffer buffer = ByteBuffer.allocate(8);

    private int precision;
    private int scale;

    public DecimalLongConverter(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
    }

    @Override public byte[] convert(BigDecimal decimal) {
      long longValue = decimal.unscaledValue().longValue();
      buffer.putLong(0, longValue);
      return buffer.array().clone();
    }

    @Override public BigDecimal getDecimal(byte[] bytes) {
      long unscaled = getUnscaledLong(bytes);
      return BigDecimal.valueOf(unscaled, scale);
    }

    @Override public void writeToColumnVector(byte[] bytes, CarbonColumnVector vector, int rowId) {
      long unscaled = getUnscaledLong(bytes);
      vector.putLong(rowId, unscaled);
    }

    @Override public int getSize() {
      return 8;
    }
  }

  public class DecimalUnscaledConverter implements DecimalConverter {

    private int precision;

    private int scale;

    private int numBytes;

    private byte[] decimalBuffer = new byte[minBytesForPrecision[38]];

    public DecimalUnscaledConverter(int precision, int scale) {
      this.precision = precision;
      this.scale = scale;
      this.numBytes = minBytesForPrecision[precision];
    }

    @Override public byte[] convert(BigDecimal decimal) {
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

    @Override public BigDecimal getDecimal(byte[] bytes) {
      BigInteger bigInteger = new BigInteger(bytes);
      return new BigDecimal(bigInteger, scale);
    }

    @Override public void writeToColumnVector(byte[] bytes, CarbonColumnVector vector, int rowId) {
      vector.putBytes(rowId, bytes);
    }

    @Override public int getSize() {
      return numBytes;
    }
  }

  public static class LegacyDecimalConverter implements DecimalConverter {

    public static LegacyDecimalConverter INSTANCE = new LegacyDecimalConverter();

    @Override public byte[] convert(BigDecimal decimal) {
      return DataTypeUtil.bigDecimalToByte(decimal);
    }

    @Override public BigDecimal getDecimal(byte[] bytes) {
      return DataTypeUtil.byteToBigDecimal(bytes);
    }

    @Override public void writeToColumnVector(byte[] bytes, CarbonColumnVector vector, int rowId) {
      throw new UnsupportedOperationException("Unsupported in vector reading for legacy format");
    }

    @Override public int getSize() {
      return -1;
    }
  }

  public DecimalConverter getDecimalConverter(int precision, int scale) {
    if (precision < 0) {
      return new LegacyDecimalConverter();
    } else if (precision <= 9) {
      return new DecimalIntConverter(precision, scale);
    } else if (precision <= 18) {
      return new DecimalLongConverter(precision, scale);
    } else {
      return new DecimalUnscaledConverter(precision, scale);
    }
  }

}
