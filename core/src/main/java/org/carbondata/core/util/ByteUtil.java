/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.core.util;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Util class for byte comparision
 */
public final class ByteUtil {

  private static final int SIZEOF_LONG = 8;
  private static final int SIZEOF_INT = 4;

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
    // Bring WritableComparator code local
    int i = 0;
    int j = 0;
    for (; i < buffer1.length && j < buffer2.length; i++, j++) {
      int a = (buffer1[i] & 0xff);
      int b = (buffer2[j] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return 0;
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
   * Unsafe comparator
   */
  public enum UnsafeComparer {
    /**
     * instance.
     */
    INSTANCE;

    /**
     * unsafe .
     */
    static final sun.misc.Unsafe THEUNSAFE;

    /**
     * The offset to the first element in a byte array.
     */
    static final int BYTE_ARRAY_BASE_OFFSET;
    static final boolean LITTLEENDIAN = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

    static {
      THEUNSAFE = (sun.misc.Unsafe) AccessController.doPrivileged(new PrivilegedAction<Object>() {
        @Override public Object run() {
          try {
            Field f = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            return f.get(null);
          } catch (NoSuchFieldException e) {
            // It doesn't matter what we throw;
            // it's swallowed in getBestComparer().
            throw new Error();
          } catch (IllegalAccessException e) {
            throw new Error();
          }
        }
      });

      BYTE_ARRAY_BASE_OFFSET = THEUNSAFE.arrayBaseOffset(byte[].class);

      // sanity check - this should never fail
      if (THEUNSAFE.arrayIndexScale(byte[].class) != 1) {
        throw new AssertionError();
      }

    }

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
      int offset1Adj = offset1 + BYTE_ARRAY_BASE_OFFSET;
      int offset2Adj = offset2 + BYTE_ARRAY_BASE_OFFSET;

      /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes
       * at a time is no slower than comparing 4 bytes at a time even on
       * 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
        long lw = THEUNSAFE.getLong(buffer1, offset1Adj + (long) i);
        long rw = THEUNSAFE.getLong(buffer2, offset2Adj + (long) i);
        long diff = lw ^ rw;

        if (diff != 0) {
          if (!LITTLEENDIAN) {
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
      int minWords = 0;

      /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes
       * at a time is no slower than comparing 4 bytes at a time even on
       * 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      if (minLength > 7) {
        minWords = minLength / SIZEOF_LONG;
        for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {
          long lw = THEUNSAFE.getLong(buffer1, BYTE_ARRAY_BASE_OFFSET + (long) i);
          long rw = THEUNSAFE.getLong(buffer2, BYTE_ARRAY_BASE_OFFSET + (long) i);
          long diff = lw ^ rw;

          if (diff != 0) {
            if (!LITTLEENDIAN) {
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
      long currentOffset = BYTE_ARRAY_BASE_OFFSET;
      for (int i = 0; i < len; i++) {
        long lw = THEUNSAFE.getLong(buffer1, currentOffset);
        long rw = THEUNSAFE.getLong(buffer2, currentOffset);
        if (lw != rw) {
          return false;
        }
        currentOffset += 8;
      }
      len = buffer1.length % 8;
      if (len > 0) {
        for (int i = 0; i < len; i += 1) {
          long lw = THEUNSAFE.getByte(buffer1, currentOffset);
          long rw = THEUNSAFE.getByte(buffer2, currentOffset);
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
      int minLength = (len1 <= len2) ? len1 : len2;
      int minWords = 0;

      /*
       * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes
       * at a time is no slower than comparing 4 bytes at a time even on
       * 32-bit. On the other hand, it is substantially faster on 64-bit.
       */
      if (minLength > 7) {
        minWords = minLength / SIZEOF_LONG;
        for (int i = 0; i < minWords * SIZEOF_LONG; i += SIZEOF_LONG) {

          long lw = byteBuffer1.getLong();
          long rw = byteBuffer2.getLong();

          long diff = lw ^ rw;

          if (diff != 0) {
            if (!LITTLEENDIAN) {
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
        int a = (byteBuffer1.get() & 0xff);
        int b = (byteBuffer2.get() & 0xff);
        if (a != b) {
          return a - b;
        }
      }
      return len1 - len2;
    }

  }

}
