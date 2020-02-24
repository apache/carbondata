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

package org.apache.hadoop.util.bloom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.BitSet;

import org.roaringbitmap.RoaringBitmap;

/**
 * It is the extendable class to hadoop bloomfilter, it is extendable to implement compressed bloom
 * and fast serialize and deserialize of bloom.
 */
public class CarbonBloomFilter extends BloomFilter {

  private RoaringBitmap bitmap;

  private boolean compress;

  private int blockletNo;

  // used for building blocklet when query
  private String shardName;

  public CarbonBloomFilter() {
  }

  public CarbonBloomFilter(int vectorSize, int nbHash, int hashType, boolean compress) {
    super(vectorSize, nbHash, hashType);
    this.compress = compress;
  }

  @Override
  public boolean membershipTest(Key key) {
    if (compress) {
      // If it is compressed check in roaring bitmap
      if (key == null) {
        throw new NullPointerException("key cannot be null");
      }
      int[] h = hash.hash(key);
      hash.clear();
      for (int i = 0; i < nbHash; i++) {
        if (!bitmap.contains(h[i])) {
          return false;
        }
      }
      return true;
    } else {
      // call super method to avoid IllegalAccessError for `bits` field
      return super.membershipTest(key);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(blockletNo);
    out.writeInt(this.nbHash);
    out.writeByte(this.hashType);
    out.writeInt(this.vectorSize);
    out.writeBoolean(compress);
    BitSet bits = getBitSet();
    if (!compress) {
      byte[] bytes = bits.toByteArray();
      out.writeInt(bytes.length);
      out.write(bytes);
    } else {
      RoaringBitmap bitmap = new RoaringBitmap();
      int length = bits.cardinality();
      int nextSetBit = bits.nextSetBit(0);
      for (int i = 0; i < length; ++i) {
        bitmap.add(nextSetBit);
        nextSetBit = bits.nextSetBit(nextSetBit + 1);
      }
      bitmap.serialize(out);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.blockletNo = in.readInt();
    this.nbHash = in.readInt();
    this.hashType = in.readByte();
    this.vectorSize = in.readInt();
    this.compress = in.readBoolean();
    if (!compress) {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      setBitSet(BitSet.valueOf(bytes));
    } else {
      this.bitmap = new RoaringBitmap();
      bitmap.deserialize(in);
    }
    this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
  }

  public int getSize() {
    int size = 14; // size of nbHash,hashType, vectorSize, compress
    if (compress) {
      size += bitmap.getSizeInBytes();
    } else {
      try {
        size += getBitSet().toLongArray().length * 8;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return size;
  }

  /**
   * Get bitset from super class using reflection, in some cases java cannot access the fields if
   * jars are loaded in separte class loaders.
   *
   * @return
   * @throws IOException
   */
  private BitSet getBitSet() throws IOException {
    try {
      Field field = BloomFilter.class.getDeclaredField("bits");
      field.setAccessible(true);
      return (BitSet)field.get(this);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Set bitset from super class using reflection, in some cases java cannot access the fields if
   * jars are loaded in separte class loaders.
   * @param bitSet
   * @throws IOException
   */
  private void setBitSet(BitSet bitSet) throws IOException {
    try {
      Field field = BloomFilter.class.getDeclaredField("bits");
      field.setAccessible(true);
      field.set(this, bitSet);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  public void setBlockletNo(int blockletNo) {
    this.blockletNo = blockletNo;
  }

  public int getBlockletNo() {
    return blockletNo;
  }

  public String getShardName() {
    return shardName;
  }

  public void setShardName(String shardName) {
    this.shardName = shardName;
  }

}
