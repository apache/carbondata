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

import java.io.Serializable;
import java.util.Arrays;

public class Bits implements Serializable {

  /**
   * Bits MAX_LENGTH
   */
  private static final int MAX_LENGTH = 63;
  private static final int LONG_LENGTH = 64;
  /**
   * serialVersionUID.
   */
  private static final long serialVersionUID = 1555114921503304849L;
  /**
   * LONG_MAX.
   */
  private static final long LONG_MAX = 0x7fffffffffffffffL;
  /**
   * length.
   */
  private int length = 100;
  /**
   * lens.
   */
  private int[] lens;
  /**
   * word size.
   */
  private int wSize;
  /**
   * byteSize.
   */
  private int byteSize;

  public Bits(int[] lens) {
    this.lens = lens;
    this.length = getTotalLength(lens);

    wSize = length / LONG_LENGTH;
    byteSize = length / 8;

    if (length % LONG_LENGTH != 0) {
      wSize++;
    }

    if (length % 8 != 0) {
      byteSize++;
    }
  }

  public int getByteSize() {
    return byteSize;
  }

  private int getTotalLength(int[] lens) {
    int tLen = 0;
    for (int len : lens) {
      tLen += len;
    }
    return tLen;
  }

  public int getDimCount() {
    return lens.length;
  }

  /**
   * Return the start and end Byte offsets of dimension in the MDKey. int []
   * {start, end}
   */
  public int[] getKeyByteOffsets(int index) {

    int priorLen = length % 8 == 0 ? 0 : (8 - length % 8);
    int start = 0;
    int end = 0;

    // Calculate prior length for all previous keys
    for (int i = 0; i < index; i++) {
      priorLen += lens[i];
    }

    // Start
    start = priorLen / 8;

    int tillKeyLength = priorLen + lens[index];

    // End key
    end = (tillKeyLength) / 8;

    // Consider if end is the last bit. No need to include the next byte.
    if (tillKeyLength % 8 == 0) {
      end--;
    }

    return new int[] { start, end };
  }

  protected long[] get(int[] keys) {
    long[] words = new long[wSize];
    int ll = 0;
    int minLength = Math.min(lens.length, keys.length);
    for (int i = minLength - 1; i >= 0; i--) {

      long val = keys[i];

      int index = ll >> 6; // divide by 64 to get the new word index
      int pos = ll & 0x3f; // to ignore sign bit and consider the remaining
      val = val & (LONG_MAX >> (MAX_LENGTH - lens[i])); // To control the
      // logic so that
      // any val do not
      // exceed the
      // cardinality
      long mask = (val << pos);
      long word = words[index];
      words[index] = (word | mask);
      ll += lens[i];

      int nextIndex = ll >> 6; // This is divide by 64

      if (nextIndex != index) {
        int consideredBits = lens[i] - ll & 0x3f;
        //Check for spill over only if all the bits are not considered
        if (consideredBits < lens[i]) {
          // Check for spill over
          mask = (val >> (lens[i] - ll & 0x3f));
          word = words[nextIndex];
          words[nextIndex] = (word | mask);
        }
      }

    }

    return words;
  }

  private long[] getArray(long[] words) {
    long[] values = new long[lens.length];
    int ll = 0;
    for (int i = lens.length - 1; i >= 0; i--) {

      int index = ll >> 6;
      int pos = ll & 0x3f;
      long val = words[index];
      long mask = (LONG_MAX >>> (MAX_LENGTH - lens[i]));
      mask = mask << pos;
      values[i] = (val & mask);
      values[i] >>>= pos;
      ll += lens[i];

      int nextIndex = ll >> 6;
      if (nextIndex != index) {
        pos = ll & 0x3f;
        // Number of bits pending for current key is zero, no spill over
        if (pos != 0) {
          mask = (LONG_MAX >>> (MAX_LENGTH - pos));
          val = words[nextIndex];
          values[i] = values[i] | ((val & mask) << (lens[i] - pos));
        }
      }
    }
    return values;
  }

  private byte[] getBytesVal(long[] words) {
    int length = 8;
    byte[] bytes = new byte[byteSize];

    int l = byteSize - 1;
    for (int i = 0; i < words.length; i++) {
      long val = words[i];

      for (int j = length - 1; j > 0 && l > 0; j--) {
        bytes[l] = (byte) val;
        val >>>= 8;
        l--;
      }
      bytes[l] = (byte) val;
      l--;
    }
    return bytes;
  }

  public byte[] getBytes(int[] keys) {

    long[] words = get(keys);

    return getBytesVal(words);
  }

  public long[] getKeyArray(byte[] key, int offset) {

    int length = 8;
    int ls = byteSize;
    long[] words = new long[wSize];
    for (int i = 0; i < words.length; i++) {
      long l = 0;
      ls -= 8;
      int m = 0;
      if (ls < 0) {
        m = ls + length;
        ls = 0;
      } else {
        m = ls + 8;
      }
      for (int j = ls; j < m; j++) {
        l <<= 8;
        l ^= key[j + offset] & 0xFF;
      }
      words[i] = l;
    }

    return getArray(words);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof Bits) {
      Bits other = (Bits) obj;
      return Arrays.equals(lens, other.lens);
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(lens);
  }
}
