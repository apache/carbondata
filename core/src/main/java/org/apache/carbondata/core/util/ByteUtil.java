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

package org.apache.carbondata.core.util;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.CarbonUnsafe;

/**
 * Util class for byte comparision
 */
public final class ByteUtil {

  public static final int SIZEOF_LONG = 8;

  public static final int SIZEOF_INT = 4;

  public static final int SIZEOF_SHORT = 2;

  public static final String UTF8_CSN = StandardCharsets.UTF_8.name();

  public static final byte[] ZERO_IN_BYTES = toBytes(0);

  private ByteUtil() {

  }

  /**
   * Compare method for bytes
   *
   * @param buffer1
   * @param buffer2
   * @return
   */
  public static int compare(byte[] buffer1, byte[] buffer2) {
    // Short circuit equal case
    if (buffer1 == buffer2) {
      return 0;
    }
    int len1 = buffer1.length;
    int len2 = buffer2.length;
    int offset1 = 0;
    int offset2 = 0;
    // Call UnsafeComparer compareTo for comparision.
    return ByteUtil.UnsafeComparer.INSTANCE
        .compareTo(buffer1, offset1, len1, buffer2, offset2, len2);
  }


  /**
   * covert the long[] to int[]
   *
   * @param longArray
   * @return
   */
  public static int[] convertToIntArray(long[] longArray) {
    int[] intArray = new int[longArray.length];
    for (int i = 0; i < longArray.length; i++) {
      intArray[i] = (int) longArray[i];

    }
    return intArray;
  }

  /**
   * convert number in byte to more readable format
   * @param sizeInbyte
   * @return
   */
  public static String convertByteToReadable(long sizeInbyte) {

    String readableSize;
    if (sizeInbyte < CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) {
      readableSize = sizeInbyte + " Byte";
    } else if (sizeInbyte < CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR *
            CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR) {
      readableSize = sizeInbyte / CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR + " KB";
    } else {
      readableSize = sizeInbyte / CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR /
              CarbonCommonConstants.BYTE_TO_KB_CONVERSION_FACTOR + " MB";
    }
    return readableSize;
  }

  /**
   * Unsafe comparator
   */
  public enum UnsafeComparer {
    /**
     * instance.
     */
    INSTANCE;

    /**
     * Returns true if x1 is less than x2, when both values are treated as
     * unsigned.
     */
    static boolean lessThanUnsigned(long x1, long x2) {
      return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
    }

    /**
     * Lexicographically compare two arrays.
     *
     * @param buffer1 left operand
     * @param buffer2 right operand
     * @param offset1 Where to start comparing in the left buffer
     * @param offset2 Where to start comparing in the right buffer
     * @param length1 How much to compare from the left buffer
     * @param length2 How much to compare from the right buffer
     * @return 0 if equal, < 0 if left is less than right, etc.
     */
    public int compareTo(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
        int length2) {
      // Short circuit equal case
      if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
        return 0;
      }
      int minLength = Math.min(length1, length2);
      int minWords = minLength / SIZEOF_LONG;
      int offset1Adj = offset1 + CarbonUnsafe.BYTE_ARRAY_OFFSET;
      int offset2Adj = offset2 + CarbonUnsafe.BYTE_ARRAY_OFFSET;

      /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes
       * at a time is no slower than comparing 4 bytes at a time even on
       * 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
        long lw = CarbonUnsafe.unsafe.getLong(buffer1, offset1Adj + (long) i);
        long rw = CarbonUnsafe.unsafe.getLong(buffer2, offset2Adj + (long) i);
        long diff = lw ^ rw;

        if (diff != 0) {
          if (!CarbonUnsafe.ISLITTLEENDIAN) {
            return lessThanUnsigned(lw, rw) ? -1 : 1;
          }

          // Use binary search
          int n = 0;
          int y;
          int x = (int) diff;
          if (x == 0) {
            x = (int) (diff >>> 32);
            n = 32;
          }

          y = x << 16;
          if (y == 0) {
            n += 16;
          } else {
            x = y;
          }

          y = x << 8;
          if (y == 0) {
            n += 8;
          }
          return (int) (((lw >>> n) & 0xFFL) - ((rw >>> n) & 0xFFL));
        }
      }

      // The epilogue to cover the last (minLength % 8) elements.
      for (int i = minWords * SIZEOF_LONG; i < minLength; i++) {
        int a = (buffer1[offset1 + i] & 0xff);
        int b = (buffer2[offset2 + i] & 0xff);
        if (a != b) {
          return a - b;
        }
      }
      return length1 - length2;
    }

    public int compareTo(byte[] buffer1, byte[] buffer2) {

      // Short circuit equal case
      if (buffer1 == buffer2) {
        return 0;
      }
      int len1 = buffer1.length;
      int len2 = buffer2.length;
      int minLength = (len1 <= len2) ? len1 : len2;
      return compareTo(buffer1, buffer2, len1, len2, minLength);
    }

    public int compareTo(byte[] buffer1, byte[] buffer2, int len1, int len2, int minLength) {
      int minWords = 0;
      /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes
       * at a time is no slower than comparing 4 bytes at a time even on
       * 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      if (minLength > 7) {
        minWords = minLength / SIZEOF_LONG;
        for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
          long lw = CarbonUnsafe.unsafe.getLong(buffer1, CarbonUnsafe.BYTE_ARRAY_OFFSET + (long) i);
          long rw = CarbonUnsafe.unsafe.getLong(buffer2, CarbonUnsafe.BYTE_ARRAY_OFFSET + (long) i);
          long diff = lw ^ rw;

          if (diff != 0) {
            if (!CarbonUnsafe.ISLITTLEENDIAN) {
              return lessThanUnsigned(lw, rw) ? -1 : 1;
            }

            // Use binary search
            int k = 0;
            int y;
            int x = (int) diff;
            if (x == 0) {
              x = (int) (diff >>> 32);
              k = 32;
            }
            y = x << 16;
            if (y == 0) {
              k += 16;
            } else {
              x = y;
            }

            y = x << 8;
            if (y == 0) {
              k += 8;
            }
            return (int) (((lw >>> k) & 0xFFL) - ((rw >>> k) & 0xFFL));
          }
        }
      }

      // The epilogue to cover the last (minLength % 8) elements.
      for (int i = minWords * SIZEOF_LONG; i < minLength; i++) {
        int a = (buffer1[i] & 0xff);
        int b = (buffer2[i] & 0xff);
        if (a != b) {
          return a - b;
        }
      }
      return len1 - len2;
    }

    public boolean equals(byte[] buffer1, byte[] buffer2) {
      if (buffer1.length != buffer2.length) {
        return false;
      }
      int len = buffer1.length / 8;
      long currentOffset = CarbonUnsafe.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < len; i++) {
        long lw = CarbonUnsafe.unsafe.getLong(buffer1, currentOffset);
        long rw = CarbonUnsafe.unsafe.getLong(buffer2, currentOffset);
        if (lw != rw) {
          return false;
        }
        currentOffset += 8;
      }
      len = buffer1.length % 8;
      if (len > 0) {
        for (int i = 0; i < len; i += 1) {
          long lw = CarbonUnsafe.unsafe.getByte(buffer1, currentOffset);
          long rw = CarbonUnsafe.unsafe.getByte(buffer2, currentOffset);
          if (lw != rw) {
            return false;
          }
          currentOffset += 1;
        }
      }
      return true;
    }

    public boolean equals(byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2,
        int length2) {
      if (length1 != length2) {
        return false;
      }
      int len = length1 / 8;
      long currentOffset = CarbonUnsafe.BYTE_ARRAY_OFFSET;
      for (int i = 0; i < len; i++) {
        long lw = CarbonUnsafe.unsafe.getLong(buffer1, currentOffset + offset1);
        long rw = CarbonUnsafe.unsafe.getLong(buffer2, currentOffset + offset2);
        if (lw != rw) {
          return false;
        }
        currentOffset += 8;
      }
      len = buffer1.length % 8;
      if (len > 0) {
        for (int i = 0; i < len; i += 1) {
          long lw = CarbonUnsafe.unsafe.getByte(buffer1, currentOffset + offset1);
          long rw = CarbonUnsafe.unsafe.getByte(buffer2, currentOffset + offset2);
          if (lw != rw) {
            return false;
          }
          currentOffset += 1;
        }
      }
      return true;
    }

    /**
     * Comparing the 2 byte buffers. This is used in case of data load sorting step.
     *
     * @param byteBuffer1
     * @param byteBuffer2
     * @return
     */
    public int compareTo(ByteBuffer byteBuffer1, ByteBuffer byteBuffer2) {

      // Short circuit equal case
      if (byteBuffer1 == byteBuffer2) {
        return 0;
      }
      int len1 = byteBuffer1.remaining();
      int len2 = byteBuffer2.remaining();
      byte[] buffer1 = new byte[len1];
      byte[] buffer2 = new byte[len2];
      byteBuffer1.get(buffer1);
      byteBuffer2.get(buffer2);
      return compareTo(buffer1, buffer2);
    }

  }

  /**
   * Stirng => byte[]
   *
   * @param s
   * @return
   */
  public static byte[] toBytes(String s) {
    try {
      return s.getBytes(UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 decoding is not supported", e);
    }
  }

  /**
   * byte[] => String
   *
   * @param b
   * @param off
   * @param len
   * @return
   */
  public static String toString(final byte[] b, int off, int len) {
    if (b == null) {
      return null;
    }
    if (len == 0) {
      return "";
    }
    try {
      return new String(b, off, len, UTF8_CSN);
    } catch (UnsupportedEncodingException e) {
      // should never happen!
      throw new IllegalArgumentException("UTF8 encoding is not supported", e);
    }
  }

  /**
   * boolean => byte[]
   *
   * @param b
   * @return
   */
  public static byte[] toBytes(final boolean b) {
    return new byte[] { b ? (byte) -1 : (byte) 0 };
  }

  /**
   * byte[] => boolean
   *
   * @param b
   * @return
   */
  public static boolean toBoolean(final byte[] b) {
    if (b.length != 1) {
      throw new IllegalArgumentException("Array has wrong size: " + b.length);
    }
    return b[0] != (byte) 0;
  }

  public static boolean toBoolean(final byte b) {
    return b != (byte) 0;
  }

  /**
   * short => byte[]
   *
   * @param val
   * @return
   */
  public static byte[] toBytes(short val) {
    val = (short)(val ^ Short.MIN_VALUE);
    byte[] b = new byte[SIZEOF_SHORT];
    b[1] = (byte) val;
    val >>= 8;
    b[0] = (byte) val;
    return b;
  }

  /**
   * byte[] => short
   *
   * @param bytes
   * @param offset
   * @param length
   * @return
   */
  public static short toShort(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_SHORT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_SHORT);
    }
    short n = 0;
    if (CarbonUnsafe.unsafe != null) {
      if (CarbonUnsafe.ISLITTLEENDIAN) {
        n = Short.reverseBytes(
            CarbonUnsafe.unsafe.getShort(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET));
      } else {
        n = CarbonUnsafe.unsafe.getShort(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET);
      }
    } else {

      n ^= bytes[offset] & 0xFF;
      n <<= 8;
      n ^= bytes[offset + 1] & 0xFF;
    }
    return (short)(n ^ Short.MIN_VALUE);
  }

  /**
   * int => byte[]
   *
   * @param val
   * @return
   */
  public static byte[] toBytes(int val) {
    val = val ^ Integer.MIN_VALUE;
    byte[] b = new byte[4];
    for (int i = 3; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * byte[] => int
   *
   * @param bytes
   * @param offset
   * @param length
   * @return
   */
  public static int toInt(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_INT || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
    }
    int n = 0;
    if (CarbonUnsafe.unsafe != null) {
      if (CarbonUnsafe.ISLITTLEENDIAN) {
        n = Integer.reverseBytes(
            CarbonUnsafe.unsafe.getInt(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET));
      } else {
        n = CarbonUnsafe.unsafe.getInt(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET);
      }
    } else {
      for (int i = offset; i < (offset + length); i++) {
        n <<= 8;
        n ^= bytes[i] & 0xFF;
      }
    }
    return n ^ Integer.MIN_VALUE;
  }

  public static int toInt(byte[] bytes, int offset) {
    return (((int)bytes[offset]) << 24) + (((int)bytes[offset + 1]) << 16) +
        (((int)bytes[offset + 2]) << 8) + bytes[offset + 3];
  }

  public static void setInt(byte[] data, int offset, int value) {
    data[offset] = (byte) (value >> 24);
    data[offset + 1] = (byte) (value >> 16);
    data[offset + 2] = (byte) (value >> 8);
    data[offset + 3] = (byte) value;
  }

  /**
   * float => byte[]
   *
   * @param f
   * @return
   */
  public static byte[] toBytes(final float f) {
    // Encode it as int
    return toBytes(Float.floatToRawIntBits(f));
  }

  /**
   * byte[] => float
   *
   * @param bytes
   * @param offset
   * @return
   */
  public static float toFloat(byte[] bytes, int offset) {
    return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
  }

  /**
   * long => byte[]
   *
   * @param val
   * @return
   */
  public static byte[] toBytes(long val) {
    val = val ^ Long.MIN_VALUE;
    byte[] b = new byte[8];
    for (int i = 7; i > 0; i--) {
      b[i] = (byte) val;
      val >>>= 8;
    }
    b[0] = (byte) val;
    return b;
  }

  /**
   * byte[] => long
   */
  public static long toLong(byte[] bytes, int offset, final int length) {
    if (length != SIZEOF_LONG || offset + length > bytes.length) {
      throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
    }
    long l = 0;
    if (CarbonUnsafe.unsafe != null) {
      if (CarbonUnsafe.ISLITTLEENDIAN) {
        l = Long.reverseBytes(
            CarbonUnsafe.unsafe.getLong(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET));
      } else {
        l = CarbonUnsafe.unsafe.getLong(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET);
      }
    } else {
      for (int i = offset; i < offset + length; i++) {
        l <<= 8;
        l ^= bytes[i] & 0xFF;
      }
    }
    return l ^ Long.MIN_VALUE;
  }

  /**
   * doube => byte[]
   *
   * @param d
   * @return
   */
  public static byte[] toBytes(final double d) {
    // Encode it as a long
    return toBytes(Double.doubleToRawLongBits(d));
  }

  /**
   * byte[] => double
   *
   * @param bytes
   * @param offset
   * @return
   */
  public static double toDouble(final byte[] bytes, final int offset) {
    return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
  }

  /**
   * BigDecimal => byte[]
   *
   * @param val
   * @return
   */
  public static byte[] toBytes(BigDecimal val) {
    byte[] valueBytes = val.unscaledValue().toByteArray();
    byte[] result = new byte[valueBytes.length + SIZEOF_INT];
    int offset = putInt(result, 0, val.scale());
    putBytes(result, offset, valueBytes, 0, valueBytes.length);
    return result;
  }

  /**
   * byte[] => BigDecimal
   *
   * @param bytes
   * @param offset
   * @param length
   * @return
   */
  public static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length) {
    if (bytes == null || length < SIZEOF_INT + 1 || (offset + length > bytes.length)) {
      return null;
    }

    int scale = toInt(bytes, offset, bytes.length);
    byte[] tcBytes = new byte[length - SIZEOF_INT];
    System.arraycopy(bytes, offset + SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
    return new BigDecimal(new BigInteger(tcBytes), scale);
  }

  private static IllegalArgumentException explainWrongLengthOrOffset(final byte[] bytes,
      final int offset, final int length, final int expectedLength) {
    String reason;
    if (length != expectedLength) {
      reason = "Wrong length: " + length + ", expected " + expectedLength;
    } else {
      reason = "offset (" + offset + ") + length (" + length + ") exceed the"
          + " capacity of the array: " + bytes.length;
    }
    return new IllegalArgumentException(reason);
  }

  /**
   * Put an int value out to the specified byte array position.
   *
   * @param bytes  the byte array
   * @param offset position in the array
   * @param val    int to write out
   * @return incremented offset
   * @throws IllegalArgumentException if the byte array given doesn't have
   *                                  enough room at the offset specified.
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (bytes.length - offset < SIZEOF_INT) {
      throw new IllegalArgumentException(
          "Not enough room to put an int at" + " offset " + offset + " in a " + bytes.length
              + " byte array");
    }
    if (CarbonUnsafe.unsafe != null) {
      if (CarbonUnsafe.ISLITTLEENDIAN) {
        val = Integer.reverseBytes(val);
      }
      CarbonUnsafe.unsafe.putInt(bytes, offset + CarbonUnsafe.BYTE_ARRAY_OFFSET, val);
      return offset + ByteUtil.SIZEOF_INT;
    } else {
      for (int i = offset + 3; i > offset; i--) {
        bytes[i] = (byte) val;
        val >>>= 8;
      }
      bytes[offset] = (byte) val;
      return offset + SIZEOF_INT;
    }
  }

  /**
   * Put bytes at the specified byte array position.
   *
   * @param tgtBytes  the byte array
   * @param tgtOffset position in the array
   * @param srcBytes  array to write out
   * @param srcOffset source offset
   * @param srcLength source length
   * @return incremented offset
   */
  public static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset,
      int srcLength) {
    System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
    return tgtOffset + srcLength;
  }

  /**
   * flatten input byte[][] to byte[] and return
   */
  public static byte[] flatten(byte[][] input) {
    int totalSize = 0;
    for (int i = 0; i < input.length; i++) {
      totalSize += input[i].length;
    }
    byte[] flattenedData = new byte[totalSize];
    int pos = 0;
    for (int i = 0; i < input.length; i++) {
      System.arraycopy(input[i], 0, flattenedData, pos, input[i].length);
      pos += input[i].length;
    }
    return flattenedData;
  }

}
