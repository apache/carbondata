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

package org.apache.carbondata.core.keygenerator.mdkey;

/**
 * It compresses the data as per max cardinality. It takes only the required bits for each key.
 */
public class NumberCompressor {

  /**
   * Bits MAX_LENGTH
   */
  private static final int MAX_LENGTH = 63;

  private static final int LONG_LENGTH = 64;

  private static final int BYTE_LENGTH = 8;

  /**
   * LONG_MAX.
   */
  private static final long LONG_MAX = 0x7fffffffffffffffL;

  private byte bitsLength;

  private int bytesPerKey;

  public NumberCompressor(int cardinality) {
    bitsLength = (byte) Long.toBinaryString(cardinality).length();
    bytesPerKey = bitsLength / BYTE_LENGTH;
    if (bitsLength % BYTE_LENGTH != 0) {
      bytesPerKey++;
    }
  }

  //  public byte[] compress(int[] keys) {
  //    int[] sizes = getWordsAndByteSize(keys.length);
  //    long[] words = get(keys, sizes[0]);
  //
  //    return getByteValues(sizes, words);
  //  }

  public byte[] compress(int[] keys) {
    int length = keys.length;
    byte[] b = new byte[bytesPerKey * length];

    int offset = 0;
    for (int k = 0; k < length; k++) {
      int val = keys[k];
      for (int i = offset + bytesPerKey - 1; i > offset; i--) {
        b[i] = (byte) val;
        val >>>= 8;
      }
      b[offset] = (byte) val;
      offset += bytesPerKey;
    }

    return b;
  }

  private byte[] getByteValues(int[] sizes, long[] words) {
    byte[] bytes = new byte[sizes[1]];

    int l = sizes[1] - 1;
    for (int i = 0; i < words.length; i++) {
      long val = words[i];

      for (int j = BYTE_LENGTH - 1; j > 0 && l > 0; j--) {
        bytes[l] = (byte) val;
        val >>>= 8;
        l--;
      }
      bytes[l] = (byte) val;
      l--;
    }
    return bytes;
  }

  protected long[] get(int[] keys, int wsize) {
    long[] words = new long[wsize];
    int ll = 0;
    int index = 0;
    int pos = 0;
    int nextIndex = 0;
    for (int i = keys.length - 1; i >= 0; i--) {

      long val = keys[i];

      index = ll >> 6;// divide by 64 to get the new word index
      pos = ll & 0x3f;// to ignore sign bit and consider the remaining
      //            val = val & controlBits;
      long mask = (val << pos);
      long word = words[index];
      words[index] = (word | mask);
      ll += bitsLength;

      nextIndex = ll >> 6;// This is divide by 64

      if (nextIndex != index) {
        int consideredBits = bitsLength - ll & 0x3f;
        if (consideredBits < bitsLength) // Check for spill over only if
        // all the bits are not
        // considered
        {
          // Check for spill over
          mask = (val >> (bitsLength - ll & 0x3f));
          words[nextIndex] |= mask;
        }
      }

    }
    return words;
  }

  protected long[] get(byte[] keys, int wsize) {
    long[] words = new long[wsize];
    int ll = 0;
    long val = 0L;
    for (int i = keys.length - 1; i >= 0; ) {

      int size = i;
      val = 0L;
      for (int j = i + 1; j <= size; ) {
        val <<= BYTE_LENGTH;
        val ^= keys[j++] & 0xFF;
        i--;
      }
      int index = ll >> 6;// divide by 64 to get the new word index
      words[index] |= (val << (ll & 0x3f));
      ll += bitsLength;

      int nextIndex = ll >> 6;// This is divide by 64

      if (nextIndex != index) {
        int consideredBits = bitsLength - ll & 0x3f;
        if (consideredBits < bitsLength) // Check for spill over only if
        // all the bits are not
        // considered
        {
          // Check for spill over
          words[nextIndex] |= (val >> (bitsLength - ll & 0x3f));
        }
      }

    }
    return words;
  }

  //  public int[] unCompress(byte[] key, int offset, int length) {
  //    int ls = length;
  //    int arrayLength = (ls * BYTE_LENGTH) / bitsLength;
  //    long[] words = new long[getWordsSizeFromBytesSize(ls)];
  //    unCompressVal(key, ls, words, offset);
  //    return getArray(words, arrayLength);
  //  }

  public int[] unCompress(byte[] key, int offset, int length) {
    int len = length / bytesPerKey;
    if (length % bytesPerKey != 0) {
      throw new IllegalArgumentException(
          "Some thing wrong while decompress" + length + " " + bytesPerKey);
    }
    int[] ints = new int[len];
    int count = offset;
    for (int i = 0; i < len; i++) {
      int n = 0;
      for (int k = 0; k < bytesPerKey; k++) {
        n <<= BYTE_LENGTH;
        n ^= key[count++] & 0xFF;
      }
      ints[i] = n;
    }
    return ints;
  }

  private void unCompressVal(byte[] key, int ls, long[] words, int offset) {
    for (int i = 0; i < words.length; i++) {
      long l = 0;
      ls -= BYTE_LENGTH;
      int m = 0;
      if (ls < 0) {
        m = ls + BYTE_LENGTH;
        ls = 0;
      } else {
        m = ls + BYTE_LENGTH;
      }
      for (int j = ls; j < m; j++) {
        l <<= BYTE_LENGTH;
        l ^= key[offset + j] & 0xFF;
      }
      words[i] = l;
    }
  }

  private int[] getArray(long[] words, int arrayLength) {
    int[] vals = new int[arrayLength];
    int ll = 0;
    long globalMask = LONG_MAX >>> (MAX_LENGTH - bitsLength);
    for (int i = arrayLength - 1; i >= 0; i--) {

      int index = ll >> 6;
      int pos = ll & 0x3f;
      long val = words[index];
      long mask = globalMask << pos;
      long value = (val & mask) >>> pos;
      ll += bitsLength;

      int nextIndex = ll >> 6;
      if (nextIndex != index) {
        pos = ll & 0x3f;
        if (pos != 0) // Number of bits pending for current key is zero, no spill over
        {
          mask = (LONG_MAX >>> (MAX_LENGTH - pos));
          val = words[nextIndex];
          value = value | ((val & mask) << (bitsLength - pos));
        }
      }
      vals[i] = (int) value;
    }
    return vals;
  }

  private int[] getWordsAndByteSize(int arrayLength) {
    int length = arrayLength * bitsLength;
    int wsize = length / LONG_LENGTH;
    int byteSize = length / BYTE_LENGTH;

    if (length % LONG_LENGTH != 0) {
      wsize++;
    }

    if (length % BYTE_LENGTH != 0) {
      byteSize++;
    }
    return new int[] { wsize, byteSize };
  }

  private int getWordsSizeFromBytesSize(int byteSize) {
    int wsize = byteSize / BYTE_LENGTH;
    if (byteSize % BYTE_LENGTH != 0) {
      wsize++;
    }
    return wsize;
  }

}
