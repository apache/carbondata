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

package org.apache.carbondata.core.util;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;

import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * Util class for byte comparision
 */
public final class ByteUtil {

  private static final int SIZEOF_LONG = 8;

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


/*		public static int binarySearch(byte[] a, int fromIndex, int toIndex, byte[] key) {
			int keyLength = key.length;
			rangeCheck(a.length, fromIndex, toIndex, keyLength);
			return binarySearch0(a, fromIndex, toIndex / keyLength, key);
		}

		// Like public version, but without range checks.
		private static int binarySearch0(byte[] a, int fromIndex, int toIndex, byte[] key) {
			int low = fromIndex;
			int high = toIndex - 1;
			int keyLength = key.length;
			// int high = toIndex/keyLength;

			while (low <= high) {
				int mid = (low + high) >>> 1;
				// byte midVal = a[mid];

				int result = ByteUtil.UnsafeComparer.INSTANCE.compareTo(a, mid * keyLength, keyLength, key, 0,
						keyLength);

				if (result < 0)
					low = mid + keyLength;
				else if (result > 0)
					high = mid - keyLength;
				else
					return mid; // key found
			}
			return -(low + 1); // key not found.
		}*/
	/**
	 * Checks that {@code fromIndex} and {@code toIndex} are in the range and toIndex % keyLength = 0
	 * and throws an exception if they aren't.
	 */
	private static void rangeCheck(int arrayLength, int fromIndex, int toIndex, int keyWordLength) {
		if (fromIndex > toIndex || toIndex % keyWordLength != 0) {
			throw new IllegalArgumentException("fromIndex(" + fromIndex + ") > toIndex(" + toIndex + ")");
		}
		if (fromIndex < 0) {
			throw new ArrayIndexOutOfBoundsException(fromIndex);
		}
		if (toIndex > arrayLength) {
			throw new ArrayIndexOutOfBoundsException(toIndex);
		}
	}
	
	/**
	 * search a specific key's range boundary in sorted byte array
	 * 
	 * @param dataChunk
	 * @param fromIndex
	 * @param toIndex
	 *            it's max value should be word Number in dataChunk, equal to indataChunk.length/keyWord.length
	 * @param keyWord
	 * @return int[] contains a range's lower boundary and upper boundary
	 */
	public static int[] binaryRangeSearch(byte[] dataChunk, int fromIndex, int toIndex, byte[] keyWord) {


		int keyLength = keyWord.length;
		rangeCheck(dataChunk.length, fromIndex, toIndex, keyLength);
		
		// reset to index =  word total number in the dataChunk
		toIndex = toIndex/keyWord.length;	
		
		int[] rangeIndex = new int[2];
		int low = fromIndex;
		int high = toIndex - 1;

		while (low <= high) {
			int mid = (low + high) >>> 1;

			int result = ByteUtil.UnsafeComparer.INSTANCE.compareTo(dataChunk, mid * keyLength, keyLength, keyWord, 0,
					keyLength);

			if (result < 0)
				low = mid + 1;
			else if (result > 0)
				high = mid - 1;
			else {
				// key found  then find the range bound
				rangeIndex[0] = binaryRangeBoundarySearch(dataChunk, low, mid, keyWord, false);
				rangeIndex[1] = binaryRangeBoundarySearch(dataChunk, mid, high, keyWord, true);
				return rangeIndex;
			}

		}
		// key not found. return a not exist range
		rangeIndex[0] = 0;
		rangeIndex[1] = -1;
		return rangeIndex;
	}

  /**
   * use to search a specific keyword's lower boundary and upper boundary according to upFindFlg
   * @param dataChunk
   * @param fromIndex
   * @param toIndex
   * @param keyWord
   * @param upFindFlg true:find upper boundary  false: find lower boundary
   * @return boundary index 
   */
	public static int binaryRangeBoundarySearch(byte[] dataChunk, int fromIndex, int toIndex, byte[] keyWord, boolean upFindFlg) {
		int low = fromIndex;
		int high = toIndex;
		int keyLength = keyWord.length;

		while (low <= high) {
			int mid = (low + high) >>> 1;

			int result1 = ByteUtil.UnsafeComparer.INSTANCE.compareTo(dataChunk, mid * keyLength, keyLength, keyWord, 0,
					keyLength);

			if (upFindFlg) {
				if (result1 == 0) {

					low = mid + 1;
				} else if (result1 > 0) {

					high = mid - 1;
					int result2 = ByteUtil.UnsafeComparer.INSTANCE.compareTo(dataChunk, high * keyLength, keyLength, keyWord, 0,
							keyLength);
					if (result2 == 0) {
						return high;
					}
				} else {
					throw new UnsupportedOperationException();
				}
			} else {

				if (result1 == 0) {

					high = mid - 1;
				} else if (result1 < 0) {
					low = mid + 1;
					int result2 = ByteUtil.UnsafeComparer.INSTANCE.compareTo(dataChunk, low * keyLength, keyLength, keyWord, 0,
							keyLength);
					if (result2 == 0) {
						return low;
					}
				} else {
					throw new UnsupportedOperationException();
				}

			}
		}
		if (upFindFlg) {
			return high;
		} else {
			return low;
		}
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
      byte[] buffer1 = new byte[len1];
      byte[] buffer2 = new byte[len2];
      byteBuffer1.get(buffer1);
      byteBuffer2.get(buffer2);
      return compareTo(buffer1, buffer2);
    }

  }

}
