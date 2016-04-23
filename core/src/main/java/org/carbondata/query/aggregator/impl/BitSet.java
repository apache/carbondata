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

package org.carbondata.query.aggregator.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * This class implements a vector of bits that grows as needed. Each component
 * of the bit set has a {@code boolean} value. The bits of a {@code BitSet} are
 * indexed by nonnegative integers. Individual indexed bits can be examined,
 * set, or cleared. One {@code BitSet} may be used to modify the contents of
 * another {@code BitSet} through logical AND, logical inclusive OR, and logical
 * exclusive OR operations.
 * By default, all bits in the set initially have the value {@code false}.
 * Every bit set has a current size, which is the number of bits of space
 * currently in use by the bit set. Note that the size is related to the
 * implementation of a bit set, so it may change with implementation. The length
 * of a bit set relates to logical length of a bit set and is defined
 * independently of implementation.
 * Unless otherwise noted, passing a null parameter to any of the methods in a
 * {@code BitSet} will result in a {@code NullPointerException}.
 * A {@code BitSet} is not safe for multithreaded use without external
 * synchronization.
 *
 * @author Arthur van Hoff
 * @author Michael McCloskey
 * @author Martin Buchholz
 * @since JDK1.0
 */
public class BitSet implements java.io.Serializable {
  /* use serialVersionUID from JDK 1.0.2 for interoperability */
  private static final long serialVersionUID = 7997698588986878753L;

  /*
   * BitSets are packed into arrays of "words." Currently a word is a long,
   * which consists of 64 bits, requiring 6 address bits. The choice of word
   * size is determined purely by performance concerns.
   */
  private static final int ADDRESS_BITS_PER_WORD = 6;

  private static final int BITS_PER_WORD = 1 << ADDRESS_BITS_PER_WORD;

  /* Used to shift left or right for a partial word mask */
  private static final long WORD_MASK = 0xffffffffffffffffL;

  /**
   * The internal field corresponding to the serialField "bits".
   */
  private long[] words;

  /**
   * The number of words in the logical size of this BitSet.
   */
  private transient int wordsInUse;

  /**
   * Creates a new bit set. All bits are initially {@code false}.
   */
  public BitSet() {
    initWords(BITS_PER_WORD);
  }

  /**
   * Creates a bit set using words as the internal representation. The last
   * word (if there is one) must be non-zero.
   */
  private BitSet(long[] words) {
    this.words = words;
    this.wordsInUse = words.length;
    checkInvariants();
  }

  /**
   * Given a bit index, return word index containing it.
   */
  private static int wordIndex(int bitIndex) {
    return bitIndex >> ADDRESS_BITS_PER_WORD;
  }

  /**
   * Returns a new bit set containing all the bits in the given byte array.
   * More precisely, <br>
   * {@code BitSet.valueOf(bytes).get(n) == ((bytes[n/8] & (1<<(n%8))) != 0)}
   * <br>
   * for all {@code n <  8 * bytes.length}.
   * This method is equivalent to
   * {@code BitSet.valueOf(ByteBuffer.wrap(bytes))}.
   *
   * @param bytes a byte array containing a little-endian representation of a
   *              sequence of bits to be used as the initial bits of the new bit
   *              set
   * @since 1.7
   */
  public static BitSet valueOf(byte[] bytes) {
    return BitSet.valueOf(ByteBuffer.wrap(bytes));
  }

  /**
   * Returns a new bit set containing all the bits in the given byte buffer
   * between its position and limit.
   * More precisely, <br>
   * {@code BitSet.valueOf(bb).get(n) == ((bb.get(bb.position()+n/8) & (1<<(n%8))) != 0)}
   * <br>
   * for all {@code n < 8 * bb.remaining()}.
   * The byte buffer is not modified by this method, and no reference to the
   * buffer is retained by the bit set.
   *
   * @param bb a byte buffer containing a little-endian representation of a
   *           sequence of bits between its position and limit, to be used as
   *           the initial bits of the new bit set
   * @since 1.7
   */
  public static BitSet valueOf(ByteBuffer bb) {
    bb = bb.slice().order(ByteOrder.LITTLE_ENDIAN);
    int n = 0;//CHECKSTYLE:OFF
    for (n = bb.remaining(); ; n--) {//CHECKSTYLE:ON
      if (n > 0 && bb.get(n - 1) == 0) {
        continue;
      } else {
        break;
      }
    }
    long[] words = new long[(n + 7) / 8];
    bb.limit(n);
    int i = 0;//CHECKSTYLE:OFF
    while (bb.remaining() >= 8) {//CHECKSTYLE:ON
      words[i++] = bb.getLong();
    }

    int j = 0;
    for (int remaining = bb.remaining(); j < remaining; j++) {
      words[i] |= (bb.get() & 0xffL) << (8 * j);
    }
    return new BitSet(words);
  }

  /**
   * Every public method must preserve these invariants.
   */
  private void checkInvariants() {
    assert(wordsInUse == 0 || words[wordsInUse - 1] != 0);
    assert(wordsInUse >= 0 && wordsInUse <= words.length);
    assert(wordsInUse == words.length || words[wordsInUse] == 0);
  }

  private void initWords(int nbits) {
    words = new long[wordIndex(nbits - 1) + 1];
  }

  /**
   * Returns a new byte array containing all the bits in this bit set.
   * More precisely, if <br>
   * {@code byte[] bytes = s.toByteArray();} <br>
   * then {@code bytes.length == (s.length()+7)/8} and <br>
   * {@code s.get(n) == ((bytes[n/8] & (1<<(n%8))) != 0)} <br>
   * for all {@code n < 8 * bytes.length}.
   *
   * @return a byte array containing a little-endian representation of all the
   * bits in this bit set
   * @since 1.7
   */
  public byte[] toByteArray() {
    int n = wordsInUse;
    if (n == 0) {
      return new byte[0];
    }
    int len = 8 * (n - 1);
    for (long x = words[n - 1]; x != 0; x >>>= 8) {
      len++;
    }
    byte[] bytes = new byte[len];
    ByteBuffer bb = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
    for (int i = 0; i < n - 1; i++) {
      bb.putLong(words[i]);
    }
    for (long x = words[n - 1]; x != 0; x >>>= 8) {
      bb.put((byte) (x & 0xff));
    }
    return bytes;
  }

  /**
   * Ensures that the BitSet can hold enough words.
   *
   * @param wordsRequired the minimum acceptable number of words.
   */
  private void ensureCapacity(int wordsRequired) {
    if (words.length < wordsRequired) {
      // Allocate larger of doubled size or required size
      int request = Math.max(2 * words.length, wordsRequired);
      words = Arrays.copyOf(words, request);
    }
  }

  /**
   * Ensures that the BitSet can accommodate a given wordIndex, temporarily
   * violating the invariants. The caller must restore the invariants before
   * returning to the user, possibly using recalculateWordsInUse().
   *
   * @param wordIndex the index to be accommodated.
   */
  private void expandTo(int wordIndex) {
    int wordsRequired = wordIndex + 1;
    if (wordsInUse < wordsRequired) {
      ensureCapacity(wordsRequired);
      wordsInUse = wordsRequired;
    }
  }

  /**
   * Sets the bit at the specified index to {@code true}.
   *
   * @param bitIndex a bit index
   * @throws IndexOutOfBoundsException if the specified index is negative
   * @since JDK1.0
   */
  public void set(int bitIndex) {
    if (bitIndex < 0) {
      throw new IndexOutOfBoundsException("bitIndex < 0: " + bitIndex);
    }
    int wordIndex = wordIndex(bitIndex);
    expandTo(wordIndex);

    words[wordIndex] |= (1L << bitIndex); // Restores invariants

    checkInvariants();
  }

  /**
   * Returns the index of the first bit that is set to {@code true} that
   * occurs on or after the specified starting index. If no such bit exists
   * then {@code -1} is returned.
   * To iterate over the {@code true} bits in a {@code BitSet}, use the
   * following loop:
   * <pre>
   * {@code
   * for (int i = bs.nextSetBit(0); i >= 0; i = bs.nextSetBit(i+1)) {
   *     // operate on index i here
   * }}
   * </pre>
   *
   * @param fromIndex the index to start checking from (inclusive)
   * @return the index of the next set bit, or {@code -1} if there is no such
   * bit
   * @throws IndexOutOfBoundsException if the specified index is negative
   * @since 1.4
   */
  public int nextSetBit(int fromIndex) {
    if (fromIndex < 0) {
      throw new IndexOutOfBoundsException("fromIndex < 0: " + fromIndex);
    }
    checkInvariants();

    int u = wordIndex(fromIndex);
    if (u >= wordsInUse) {
      return -1;
    }
    long word = words[u] & (WORD_MASK << fromIndex);

    while (true) {
      if (word != 0) {
        return (u * BITS_PER_WORD) + Long.numberOfTrailingZeros(word);
      }

      u++;
      if (u == wordsInUse) {
        return -1;
      }
      word = words[u];
    }
  }

  /**
   * Returns the number of bits set to {@code true} in this {@code BitSet}.
   *
   * @return the number of bits set to {@code true} in this {@code BitSet}
   * @since 1.4
   */
  public int cardinality() {
    int sum = 0;
    for (int i = 0; i < wordsInUse; i++) {
      sum += Long.bitCount(words[i]);
    }
    return sum;
  }

  /**
   * Performs a logical <b>OR</b> of this bit set with the bit set argument.
   * This bit set is modified so that a bit in it has the value {@code true}
   * if and only if it either already had the value {@code true} or the
   * corresponding bit in the bit set argument has the value {@code true}.
   *
   * @param set a bit set
   */
  public void or(BitSet set) {
    if (this == set) {
      return;
    }
    int wordsInCommon = Math.min(wordsInUse, set.wordsInUse);

    if (wordsInUse < set.wordsInUse) {
      ensureCapacity(set.wordsInUse);
      wordsInUse = set.wordsInUse;
    }

    // Perform logical OR on words in common
    for (int i = 0; i < wordsInCommon; i++) {
      words[i] |= set.words[i];
    }
    // Copy any remaining words
    if (wordsInCommon < set.wordsInUse) {
      System.arraycopy(set.words, wordsInCommon, words, wordsInCommon, wordsInUse - wordsInCommon);
    }
    // recalculateWordsInUse() is unnecessary
    checkInvariants();
  }

}
