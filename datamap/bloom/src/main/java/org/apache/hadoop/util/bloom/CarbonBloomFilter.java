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
import java.util.BitSet;

import org.roaringbitmap.RoaringBitmap;

public class CarbonBloomFilter extends BloomFilter {

  private RoaringBitmap bitmap;

  private boolean compress;

  public CarbonBloomFilter() {
  }

  public CarbonBloomFilter(int vectorSize, int nbHash, int hashType, boolean compress) {
    super(vectorSize, nbHash, hashType);
    this.compress = compress;
  }

  @Override
  public boolean membershipTest(Key key) {
    if (key == null) {
      throw new NullPointerException("key cannot be null");
    }

    int[] h = hash.hash(key);
    hash.clear();
    if (compress) {
      for (int i = 0; i < nbHash; i++) {
        if (!bitmap.contains(h[i])) {
          return false;
        }
      }
    } else {
      for (int i = 0; i < nbHash; i++) {
        if (!bits.get(h[i])) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(this.nbHash);
    out.writeByte(this.hashType);
    out.writeInt(this.vectorSize);
    out.writeBoolean(compress);
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
    this.nbHash = in.readInt();
    this.hashType = in.readByte();
    this.vectorSize = in.readInt();
    this.compress = in.readBoolean();
    if (!compress) {
      int len = in.readInt();
      byte[] bytes = new byte[len];
      in.readFully(bytes);
      this.bits = BitSet.valueOf(bytes);
    } else {
      this.bitmap = new RoaringBitmap();
      bitmap.deserialize(in);
    }
    this.hash = new HashFunction(this.vectorSize, this.nbHash, this.hashType);
  }
}
