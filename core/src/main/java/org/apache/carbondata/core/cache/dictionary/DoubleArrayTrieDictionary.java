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

package org.apache.carbondata.core.cache.dictionary;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

/**
 * A dictionary based on DoubleArrayTrie data structure that maps enumerations
 * of byte[] to int IDs. With DoubleArrayTrie the memory footprint of the mapping
 * is minimize,d if compared to HashMap.
 * This DAT implementation is inspired by https://linux.thai.net/~thep/datrie/datrie.html
 */

public class DoubleArrayTrieDictionary {

  private static final LogService LOGGER =
          LogServiceFactory.getLogService(DoubleArrayTrieDictionary.class.getName());

  private static final byte[] HEAD_MAGIC = new byte[]{
      0x44, 0x41, 0x54, 0x54, 0x72, 0x69, 0x65, 0x44, 0x69, 0x63, 0x74
  }; // "DATTrieDict"
  private static final int HEAD_LEN = HEAD_MAGIC.length;

  private static final int INIT_CAPA_VALUE = 256;  // init len of double array
  private static final int BASE_ROOT_VALUE = 1;    // root base value of trie root
  private static final int CHCK_ROOT_VALUE = -1;   // root check value of trie root
  private static final int UUSD_ROOM_VALUE = -2;   // unused position, only for zero
  private static final int EPTY_BACK_VALUE = 0;    // value of empty position

  private static final int ENCODE_BASE_VALUE = 10; // encode start number

  private int[] base;
  private int[] check;
  private int size;
  private int capacity;

  private int id = ENCODE_BASE_VALUE;

  public DoubleArrayTrieDictionary() {
    base = new int[INIT_CAPA_VALUE];
    check = new int[INIT_CAPA_VALUE];
    capacity = INIT_CAPA_VALUE;
    base[0] = UUSD_ROOM_VALUE;
    check[0] = UUSD_ROOM_VALUE;
    base[1] = BASE_ROOT_VALUE;
    check[1] = CHCK_ROOT_VALUE;
    size = 2;
  }

  private void init(int capacity, int size, int[] base, int[] check) {
    int blen = base.length;
    int clen = check.length;
    if (capacity < size || size < 0 || blen != clen) {
      throw new IllegalArgumentException("Illegal init parameters");
    }
    this.base = new int[capacity];
    this.check = new int[capacity];
    this.capacity = capacity;
    System.arraycopy(base, 0, this.base, 0, blen);
    System.arraycopy(check, 0, this.check, 0, clen);
    this.size = size;
  }

  public void clear() {
    base = null;
    check = null;
    size = 0;
    capacity = 0;
  }

  private int reSize(int newCapacity) {
    if (newCapacity < capacity) {
      return capacity;
    }
    int[] newBase = new int[newCapacity];
    int[] newCheck = new int[newCapacity];
    if (capacity > 0) {
      System.arraycopy(base, 0, newBase, 0, capacity);
      System.arraycopy(check, 0, newCheck, 0, capacity);
    }
    base = newBase;
    check = newCheck;
    capacity = newCapacity;
    return capacity;
  }

  public int getSize() {
    return size;
  }

  public int getCapacity() {
    return capacity;
  }

  /**
   * Get apply value of key
   *
   * @param key
   * @return
   */
  public int getValue(String key) {
    String k = key + '\0';
    byte[] bKeys = k.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    return getValue(bKeys);
  }

  /**
   * Get apply value of bKeys
   *
   * @param bKeys
   * @return
   */
  private int getValue(byte[] bKeys) {
    int from = 1;
    int to;
    int current;
    int len = bKeys.length;
    if (size == 0) return -1;
    for (int i = 0; i < len; i++) {
      current = bKeys[i] & 0xFF;
      to = base[from] + current;
      if (check[to] != from) return -1;
      int baseValue = base[to];
      if (baseValue <= -ENCODE_BASE_VALUE) {
        if (i == len - 1) {
          return -1 * baseValue;
        } else {
          return -1;
        }
      }
      from = to;

    }
    return -1;
  }

  /**
   * Get all children of one node
   *
   * @param pos
   * @return
   */
  private TreeSet<Integer> getChildren(int pos) {
    TreeSet<Integer> children = new TreeSet<Integer>();
    for (int i = 0; i < 0xFF; i++) {
      int cpos = base[pos] + i;
      if (cpos >= size) break;
      if (cpos < 0) {
        return null;
      }
      if (check[cpos] == pos) {
        children.add(i);
      }
    }
    return children;
  }

  /**
   * @TODO: need to optimize performance
   *
   * Find multiple free position for {values}
   * the distance between free position should be as same as {values}
   *
   * @param values
   * @return
   */
  private int findFreeRoom(SortedSet<Integer> values) {
    int min = values.first();
    int max = values.last();
    for (int i = min + 1; i < capacity; i++) {
      if (i + max >= capacity) {
        reSize(capacity + values.size());
      }
      int res = 0;
      for (Integer v : values) {
        res = res | base[v - min + i];
      }
      if (res == EPTY_BACK_VALUE) return i - min;
    }
    return -1;
  }

  /**
   * Find one empty position for value
   *
   * @param value
   * @return
   */
  private int findAvailableHop(int value) {
    reSize(size + 1);
    int result = size - 1;
    for (int i = value + 1; i < capacity; i++) {
      if (base[i] == EPTY_BACK_VALUE) {
        result = i - value;
        break;
      }
    }
    return result;
  }

  /**
   * Resolve when conflict and reset current node and its children.
   *
   * @param start current conflict position
   * @param bKey current byte value which for processing
   * @return
   */
  private int conflict(int start, int bKey) {
    int from = start;
    TreeSet<Integer> children = getChildren(from);
    children.add(bKey);
    int newBasePos = findFreeRoom(children);
    children.remove(bKey);

    int oldBasePos = base[start];
    base[start] = newBasePos;

    int oldPos, newPos;
    for (Integer child : children) {
      oldPos = oldBasePos + child;
      newPos = newBasePos + child;
      if (oldPos == from) from = newPos;
      base[newPos] = base[oldPos];
      check[newPos] = check[oldPos];
      if (newPos >= size) size = newPos + 1;
      if (base[oldPos] > 0) {
        TreeSet<Integer> cs = getChildren(oldPos);
        for (Integer c : cs) {
          check[base[oldPos] + c] = newPos;
        }
      }
      base[oldPos] = EPTY_BACK_VALUE;
      check[oldPos] = EPTY_BACK_VALUE;
    }
    return from;
  }

  /**
   * Insert element (byte[]) into DAT.
   * 1. if the element has been DAT then return.
   * 2. if position which is empty then insert directly.
   * 3. if conflict then resolve it.
   *
   * @param bKeys
   * @return
   */
  private boolean insert(byte[] bKeys) {
    int from = 1;
    int klen = bKeys.length;
    for (int i = 0; i < klen; i++) {
      int c = bKeys[i] & 0xFF;
      int to = base[from] + c;
      reSize((int) (to * 1.2) + 1);
      if (check[to] == from) {
        if (i == klen - 1) return true;
        from = to;
      } else if (check[to] == EPTY_BACK_VALUE) {
        check[to] = from;
        if (i == klen - 1) {
          base[to] = -id;
          id = id + 1;
          return true;
        } else {
          int next = bKeys[i + 1] & 0xFF;
          base[to] = findAvailableHop(next);
          from = to;
        }
        if (to >= size) size = to + 1;
      } else {
        int rConflict = conflict(from, c);
        int locate = base[rConflict] + c;
        if (check[locate] != EPTY_BACK_VALUE) {
          LOGGER.error("conflict");
        }
        check[locate] = rConflict;
        if (i == klen - 1) {
          base[locate] = -id;
          id = id + 1;
        } else {
          int nah = bKeys[i + 1] & 0xFF;
          base[locate] = findAvailableHop(nah);
        }
        if (locate >= size) size = locate + 1;
        from = locate;
        if (i == klen - 1) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * Insert element (String) into DAT, the element will be transformed to
   * byte[] firstly then insert into DAT.
   *
   * @param key
   * @return
   */
  public boolean insert(String key) {
    String k = key + '\0';
    byte[] bKeys = k.getBytes(Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
    if (!insert(bKeys)) {
      return false;
    }
    return true;
  }

  /**
   * Serialize the DAT to data output stream
   *
   * @param out
   * @throws IOException
   */
  public void write(DataOutputStream out) throws IOException {
    out.write(HEAD_MAGIC);
    out.writeInt(capacity);
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      out.writeInt(base[i]);
    }
    for (int i = 0; i < size; i++) {
      out.writeInt(check[i]);
    }
  }

  /**
   * Deserialize the DAT from data input stream
   *
   * @param in
   * @throws IOException
   */
  public void read(DataInputStream in) throws IOException {
    byte[] header = new byte[HEAD_LEN];
    in.readFully(header);
    int comp = 0;
    for (int i = 0; i < HEAD_LEN; i++) {
      comp = HEAD_MAGIC[i] - header[i];
      if (comp != 0) break;
    }
    if (comp != 0) throw new IllegalArgumentException("Illegal file type");
    int capacity = in.readInt();
    int size = in.readInt();
    if (capacity < size || size < 0) throw new IllegalArgumentException("Illegal parameters");
    int[] base = new int[size];
    int[] check = new int[size];
    for (int i = 0; i < size; i++) {
      base[i] = in.readInt();
    }
    for (int i = 0; i < size; i++) {
      check[i] = in.readInt();
    }
    init(capacity, size, base, check);
  }

  /**
   * Dump double array value about Trie
   */
  public void dump(PrintStream out) {
    out.println("Capacity = " + capacity + ", Size = " + size);
    for (int i = 0; i < size; i++) {
      if (base[i] != EPTY_BACK_VALUE) {
        out.print(i + ":[" + base[i] + "," + check[i] + "], ");
      }
    }
    out.println();
  }
}